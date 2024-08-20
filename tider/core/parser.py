import logging
import time
from collections import deque

from tider.item import Item
from tider.network import Request
from tider.promise import EXECUTED, REJECTED
from tider.concurrency.base import worker
from tider.concurrency import get_implementation
from tider.utils.decorators import inthread
from tider.utils.misc import symbol_by_name, create_instance

logger = logging.getLogger(__name__)


class Parser:

    def __init__(self, crawler):

        self.crawler = crawler
        itemproc_cls = symbol_by_name(self.crawler.settings["ITEM_PROCESSOR"])
        self.itemproc = create_instance(itemproc_cls, self.crawler.settings, self.crawler)
        # https://github.com/gevent/gevent/issues/1437
        # https://greenlet.readthedocs.io/en/stable/python_threads.html
        self.concurrency = crawler.settings["CONCURRENCY"] // 2 + 1
        pool_cls = get_implementation(crawler.settings['POOL'])
        self.pool = pool_cls(limit=self.concurrency, thread_name_prefix='ParserWorker')

        self.max_depth = crawler.settings.getint("DEPTH_LIMIT")
        self.depth_priority = crawler.settings.getint("DEPTH_PRIORITY")

        self.queue = deque()
        self.parsing = set()
        self.running = False
        self._started = False

    def active(self):
        return len(self.queue) + len(self.parsing) > 0

    def enqueue_parser(self, response):
        self.queue.append(response)

    @inthread(name='parser')
    def async_parse_in_thread(self):
        return self.async_parse()

    def async_parse(self):
        if self.running:
            raise RuntimeError("Parser already running")
        self.running = True
        if not self._started:
            self.pool.start()
            self._started = True
        for _ in range(self.concurrency):
            self.pool.apply_async(
                worker,
                (self.queue, self.parse, None,
                 lambda: not self.running)
            )

    def parse(self, response):
        self.parsing.add(response)
        if isinstance(response, Request):
            # if processed in explorer,
            # the frame may be stuck for limited queue size.
            self.crawler.engine.schedule_request(response)
        else:
            response.meta['elapsed'] = time.perf_counter()
            self._parse(response)
            response.close()
        self.parsing.remove(response)

    def _parse(self, response):
        request = response.request
        callback = request.callback or self.crawler.spider.parse
        errback = request.errback
        node = response.meta.pop("promise_node", None)
        _success = True
        spider_outputs = None
        try:
            if response.ok:
                if callback:
                    spider_outputs = callback(response)
            else:
                if errback:
                    spider_outputs = errback(response)
                self.crawler.stats.inc_value("request/count/failed")
            spider_outputs = spider_outputs or []
            order = 0
            for output in spider_outputs:
                if self._spider_output_filter(output, response):
                    if node and isinstance(output, Request) and (not output.broadcast or self.crawler.role != 'producer'):
                        if not output.meta.get('promise_order'):
                            output.meta['promise_order'] = order
                            order += 1
                        node.add_child(output)
                    else:
                        self._process_spider_output(output, request)
        except Exception as e:
            logger.exception(f"Parser bug processing {request}")
            self.crawler.stats.inc_value(f"parser/{e.__class__.__name__}/count")
        if node:
            then_results = node.then()
            try:
                node.state = EXECUTED if _success else REJECTED
                for output in then_results:
                    self._process_spider_output(output, request)
            except Exception as e:
                logger.exception(f"Promise bug processing {request}")
                self.crawler.stats.inc_value(f"promise/{e.__class__.__name__}/count")
            if hasattr(then_results, 'close'):
                then_results.close()
            del then_results
        if hasattr(spider_outputs, 'close'):
            spider_outputs.close()
        del spider_outputs, callback, errback

    def _spider_output_filter(self, request, response):
        if isinstance(request, Request):
            self.crawler.engine.explorer.reset_proxy(request)
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
            if max_parse_times and parse_times > max_parse_times:
                logger.debug(
                    "Ignoring link (parse_times > %(max_parse_times)d): %(requrl)s ",
                    {'max_parse_times': self.max_depth, 'requrl': request.url},
                )
                return False
        return True

    def _process_spider_output(self, output, request):
        if isinstance(output, Request):
            self.crawler.engine.schedule_request(output)
        elif isinstance(output, Item):
            self.crawler.stats.inc_value(f"item/{output.__class__.__name__}/count")
            self.itemproc.process_item(output)
        elif output is None:
            pass
        else:
            typename = type(output).__name__
            logger.error(f'Spider must return request, item, or None, got {typename!r} in {request}')

    def close(self, reason):
        self.running = False
        self.queue.clear()
        self.itemproc.close()
        self.pool.stop()
        self._started = False
        self.crawler = None
        logger.info("Parser closed (%(reason)s)", {'reason': reason})
