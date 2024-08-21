import time
import logging
from collections import deque
from kombu.utils import cached_property

from tider.item import Item
from tider.network import Request
from tider.promise import EXECUTED, REJECTED
from tider.utils.decorators import inthread
from tider.utils.misc import symbol_by_name, create_instance

logger = logging.getLogger(__name__)


def _maybe_close(obj):
    if hasattr(obj, 'close'):
        obj.close()


class Parser:

    itemproc_cls = 'tider.pipelines.ItemPipelineManager'

    def __init__(self, tider):

        self.tider = tider
        # https://github.com/gevent/gevent/issues/1437
        # https://greenlet.readthedocs.io/en/stable/python_threads.html
        self.concurrency = tider.concurrency // 2 + 1
        self.pool = tider.create_pool(limit=self.concurrency, thread_name_prefix='ParserWorker')
        self.itemproc_cls = self.tider.settings.get('ITEM_PROCESSOR') or self.itemproc_cls
        self.max_depth = tider.settings.getint("DEPTH_LIMIT")
        self.depth_priority = tider.settings.getint("DEPTH_PRIORITY")

        self.queue = deque()
        self.parsing = set()
        self.running = False
        self._pool_started = False

    @cached_property
    def itemproc(self):
        return create_instance(symbol_by_name(self.itemproc_cls), self.tider.settings, self.tider)

    def active(self):
        return len(self.queue) + len(self.parsing) > 0

    def reset_status(self):
        """
        Should not be used in other cases except for broker.
        """
        self.queue = self.queue.clear()
        self.queue = deque()
        self.parsing = self.parsing.clear()
        self.parsing = set()

    def enqueue_parser(self, response):
        self.queue.append(response)

    def _start_pool(self):
        if not self._pool_started:
            self.pool.start()
        self._pool_started = True

    @inthread(name='parser')
    def async_parse_in_thread(self):
        if self.running:
            raise RuntimeError("Parser already running")
        self.running = True
        self._start_pool()
        self._blocking_parse()

    def _blocking_parse(self):
        while self.running:
            while len(self.parsing) < self.concurrency:
                try:
                    response = self.queue.popleft()
                except IndexError:
                    break
                if isinstance(response, Request):
                    self.parsing.add(1)
                    # if processed in explorer,
                    # the frame may be stuck for limited queue size.
                    self.tider.engine.schedule_request(response)
                    self.parsing.remove(1)
                else:
                    request = response.request
                    self.parsing.add(request)
                    self.pool.apply_async(
                        target=self._parse, args=(response,),
                        callback=lambda req: self.parsing.remove(req)
                    )
            time.sleep(0.01)

    def async_parse(self):
        if self.running:
            raise RuntimeError("Parser already running")
        self.running = True
        self._start_pool()
        for _ in range(self.concurrency):
            self.pool.apply_async(self.parse)

    def parse(self):
        # using `sys.getsizeof(self.queue)` in child thread
        # when using gevent pool may be stuck.
        while self.running:
            try:
                response = self.queue.popleft()
                response.meta['elapsed'] = time.time()
                # response.request or response(request)
                request = getattr(response, 'request', response)
                self.parsing.add(request)
                if isinstance(response, Request):
                    # if processed in explorer,
                    # the frame may be stuck for limited queue size.
                    self.tider.engine.schedule_request(response)
                else:
                    self._parse(response)
                self.parsing.discard(request)
            except IndexError:
                # sleep to switch thread
                time.sleep(0.01)

    def _parse(self, response):
        request = response.request
        callback = request.callback or self.tider.spider.parse
        errback = request.errback
        node = response.meta.pop("promise_node", None)

        spider_outputs = None
        try:
            if response.ok:
                spider_outputs = callback(response)
            else:
                if errback:
                    spider_outputs = errback(response)
                self.tider.stats.inc_value("request/count/failed")
            spider_outputs = spider_outputs or []
            order = 0
            for output in spider_outputs:
                if self._spider_output_filter(output, response):
                    if (node and isinstance(output, Request) and
                            (not output.broadcast or self.tider.spider_type != 'publisher')):
                        if not output.meta.get('promise_order'):
                            output.meta['promise_order'] = order
                            order += 1
                        node.add_child(output)
                    else:
                        self._process_spider_output(output, request)
        except Exception as e:
            logger.exception(f"Parser bug processing {request}")
            self.tider.stats.inc_value(f"parser/{e.__class__.__name__}/count")
        _maybe_close(response)
        _maybe_close(spider_outputs)
        del response, spider_outputs, callback, errback

        _success = True
        then_results = []
        if node:
            then_results = node.then()
            node.state = EXECUTED if _success else REJECTED
        try:
            for output in then_results:
                self._process_spider_output(output, request)
        except Exception as e:
            logger.exception(f"Promise bug processing {request}")
            self.tider.stats.inc_value(f"promise/{e.__class__.__name__}/count")
        _maybe_close(then_results)
        del then_results
        return request

    def _spider_output_filter(self, request, response):
        if isinstance(request, Request):
            if 'depth' not in response.meta:
                response.meta['depth'] = 0
            depth = response.meta['depth'] + 1
            request.meta['depth'] = depth
            if self.depth_priority:
                request.priority += depth * self.depth_priority
            if self.max_depth and depth > self.max_depth:
                logger.debug(
                    "Ignoring link (depth > %(maxdepth)d): %(requrl)s ",
                    {'maxdepth': self.max_depth, 'requrl': request.url},
                )
                return False

            if 'parse_times' not in request.meta:
                request.meta['parse_times'] = 0
            parse_times = request.meta['parse_times'] + 1
            max_parse_times = request.max_parse_times
            if max_parse_times != -1 and parse_times > max_parse_times:
                logger.debug(
                    "Ignoring link (parse_times > %(max_parse_times)d): %(requrl)s ",
                    {'max_parse_times': self.max_depth, 'requrl': request.url},
                )
                return False
        return True

    def _process_spider_output(self, output, request):
        if isinstance(output, Request):
            self.tider.engine.schedule_request(output)
        elif isinstance(output, Item):
            self.tider.stats.inc_value(f"item/{output.__class__.__name__}/count")
            self.itemproc.process_item(output)
        elif output is None:
            pass
        else:
            typename = type(output).__name__
            logger.error(f'Spider must return request, item, or None, got {typename!r} in {request}')

    def close(self, reason):
        self.running = False
        self.queue.clear()
        self.pool.stop()  # stop pool first
        self._pool_started = False
        self.parsing = self.parsing.clear()
        self.itemproc.close()
        logger.info("Parser closed (%(reason)s)", {'reason': reason})
