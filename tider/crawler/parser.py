import time
from collections import deque

from tider import Item, Request
from tider.crawler import state
from tider.platforms import EX_FAILURE
from tider.promise import EXECUTED, REJECTED
from tider.utils.log import get_logger
from tider.utils.functional import iter_generator
from tider.utils.misc import symbol_by_name, build_from_crawler

__all__ = ('Parser', )

logger = get_logger(__name__)


def _maybe_close(obj):
    if hasattr(obj, 'close'):
        obj.close()
    elif hasattr(obj, 'clear'):
        obj.clear()


class Parser:

    def __init__(self, crawler):
        self.crawler = crawler
        # https://github.com/gevent/gevent/issues/1437
        # https://greenlet.readthedocs.io/en/stable/python_threads.html
        self.concurrency = crawler.concurrency // 2 + 1
        itemproc_cls = crawler.settings.get('ITEM_PROCESSOR')
        self.itemproc = build_from_crawler(symbol_by_name(itemproc_cls), self.crawler)
        self.max_items = crawler.settings.getint('ITEM_PROCESSOR_MAX_ITEMS', 1)
        if self.max_items <= 0 or not self.max_items:
            raise ValueError("Parser can not collect infinite items")
        if self.max_items > 1:
            self.pool = crawler.create_pool(limit=self.concurrency + 1, thread_name_prefix='Parser')
            self._quick_process_item = self.itemproc.process_item_future
        else:
            self.pool = crawler.create_pool(limit=self.concurrency, thread_name_prefix='Parser')
            self._quick_process_item = self.itemproc.process_item
        self.free_slots = deque(maxlen=self.concurrency)

        self.loop = crawler.settings.getbool('PARSER_USE_LOOP', False)
        self.max_depth = crawler.settings.getint("PARSER_DEPTH_LIMIT")
        self.depth_priority = crawler.settings.getint("PARSER_DEPTH_PRIORITY")

        self.queue = deque()
        self.parsing = set()
        self.running = False

    def active(self):
        return self.running and (len(self.queue) + len(self.parsing) > 0 or self.itemproc.active())

    def enqueue_parser(self, result):
        if not self.running:
            raise RuntimeError("Parser not running")
        self.queue.append(result)

        if not self.loop and len(self.parsing) < self.concurrency and len(self.queue) > 0:
            try:
                self.free_slots.pop()
                self.pool.apply_async(self._parse_next)
            except IndexError:
                return

    def _parse_next(self):
        # using `sys.getsizeof(self.queue)` in child thread
        # when using gevent pool may be stuck.
        while self.running:
            try:
                response = self.queue.popleft()
                self.parse(response)
            except IndexError:
                if not self.loop:
                    self.free_slots.append(None)
                    break
            except BaseException:
                self.running = False
                state.should_terminate = EX_FAILURE
                raise
            finally:
                # sleep to switch thread
                time.sleep(0.01)

    def start(self):
        if self.running:
            raise RuntimeError("Parser already running")
        self.running = True

        self.pool.start()
        if self.loop:
            for _ in range(self.concurrency):
                self.pool.apply_async(self._parse_next)
        else:
            for _ in range(self.concurrency):
                self.free_slots.append(None)
        if self.max_items > 1:
            self.pool.apply_async(self.itemproc.process_queue)

    def parse(self, response):
        request = response.request
        self.parsing.add(request)

        errback = request.errback
        callback = request.callback or self.crawler.spider.parse
        node = request.meta.pop("promise_node", None)

        spider_outputs = None
        try:
            if response.failed:
                if errback:
                    spider_outputs = errback(response)
                self.crawler.stats.inc_value("request/count/failed")
            else:
                spider_outputs = callback(response)

            order = 0
            # sleep to switch thread
            for output in iter_generator(spider_outputs, sleep=self.crawler.maybe_sleep):
                state.maybe_shutdown()
                # iter spider outputs directly.
                if self._spider_output_filter(output, response):
                    if node and isinstance(output, Request):
                        if not output.meta.get('promise_order'):
                            output.meta['promise_order'] = order
                            order += 1
                        node.add_child(output)  # already SCHEDULED
                        if output.meta.get('promise_node'):
                            self._process_spider_output(output, request)
                    else:
                        self._process_spider_output(output, request)
        except BaseException as e:
            logger.exception(f"Parser bug processing {request}")
            self.crawler.stats.inc_value(f"parser/{e.__class__.__name__}/count")

        _success = True  # reserved
        if node:
            node.state = EXECUTED if _success else REJECTED
            try:
                for output in iter_generator(node.then(), sleep=self.crawler.maybe_sleep):
                    state.maybe_shutdown()
                    # iter node.then() directly.
                    self._process_spider_output(output, request)
            except BaseException as e:
                logger.exception(f"Promise bug processing {request}")
                self.crawler.stats.inc_value(f"promise/{e.__class__.__name__}/count")

        request.close()
        self.parsing.remove(request)
        response.close()

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
        if not self.running:
            return
        if isinstance(output, Request):
            self.crawler.engine.schedule_request(output)
        elif isinstance(output, (Item, dict)):
            self.crawler.stats.inc_value(f"item/{output.__class__.__name__}/count")
            self._quick_process_item(output)
        elif output is None:
            pass
        else:
            typename = type(output).__name__
            logger.error(f'Spider must return request, item, or None, got {typename!r} in {request}')

    def close(self, reason):
        self.running = False
        self.itemproc.close()
        self.pool.stop()  # stop pool first
        logger.info("Parser closed (%(reason)s)", {'reason': reason})
