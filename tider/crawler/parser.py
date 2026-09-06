import gc
import time
import inspect
import traceback
import threading
from queue import Full
from collections import deque

from tider import Item, Request
from tider.crawler import state
from tider.promise import NodeState
from tider.platforms import EX_FAILURE
from tider.utils.log import get_logger
from tider.utils.functional import iter_generator
from tider.utils.imports import symbol_by_name
from tider.utils.misc import build_from_crawler
from tider.network.proxy import Proxy
from tider.exceptions import DownloadError, ProxyError, SpiderShutdown, SpiderTerminate

__all__ = ('Parser', )

logger = get_logger(__name__)


def _maybe_close(obj):
    if hasattr(obj, 'close'):
        obj.close()
    elif hasattr(obj, 'clear'):
        obj.clear()


def _close_in_background(func, name):
    error = []

    def _close():
        try:
            func()
        except Exception as exc:
            error.append(exc)

    thread = threading.Thread(target=_close, name=name, daemon=True)
    thread.start()
    while thread.is_alive():
        if state.terminate_requested():
            return False
        thread.join(timeout=1)
    if error:
        raise error[0]
    return True


def _get_accept_kwargs(func, cb_kwargs):
    """Extract callback kwargs based on function signature.

    Returns the kwargs that the function accepts (excluding 'response' parameter).
    """
    if not cb_kwargs:
        return {}

    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        return {}

    params = sig.parameters
    # Filter cb_kwargs to only include parameters that the function accepts
    # Exclude 'response' as it's passed separately
    filtered_kwargs = {}
    has_var_keyword = any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
    )
    for key, value in cb_kwargs.items():
        if key == 'response':
            continue
        if has_var_keyword or key in params:
            filtered_kwargs[key] = value

    return filtered_kwargs


def _get_spider_outputs(func, response, cb_kwargs):
    """Extract spider outputs based on function signature."""
    accept_kwargs = _get_accept_kwargs(func, cb_kwargs)
    return func(response, **accept_kwargs)


def _request_snapshot(request, spider):
    """Return a best-effort error snapshot without hiding the original error."""
    try:
        return request.to_dict(spider=spider, encode_body=True)
    except (AttributeError, ValueError):
        return {
            'url': request.url,
            'method': request.method,
            'callback': getattr(request.callback, '__qualname__', None),
            'errback': getattr(request.errback, '__qualname__', None),
        }


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
        self.maybe_wakeup()

    def maybe_wakeup(self):
        if self.loop:
            return

        queue = self.queue
        parse_func = self.parse
        if len(self.parsing) < self.concurrency and len(self.queue) > 0:
            try:
                self.free_slots.pop()
                self.pool.apply_async(
                    self._parse_next,
                    kwargs={'queue': queue, 'parse_func': parse_func}
                )
            except IndexError:
                pass
            except Full:
                self.free_slots.append(None)

    def _parse_next(self, queue, parse_func):
        # using `sys.getsizeof(self.queue)` in child thread
        # when using gevent pool may be stuck.
        while self.running:
            try:
                response = queue.popleft()
            except IndexError:
                if not self.loop or state.stop_requested() or state.terminate_requested():
                    self.free_slots.append(None)
                    break
                time.sleep(0.01)
                continue

            request = response.request
            self.parsing.add(request)
            try:
                parse_func(response)
                del response  # to reduce peak memory usage
            except (SpiderTerminate, SpiderShutdown):
                self.running = False
                break
            except Exception:
                self.running = False
                state.should_terminate = EX_FAILURE
                raise
            finally:
                self.parsing.discard(request)
                # sleep to switch thread
                time.sleep(0.01)

        del parse_func

    def start(self):
        if self.running:
            raise RuntimeError("Parser already running")
        self.running = True

        self.pool.start()
        if self.loop:
            queue = self.queue
            parse_func = self.parse
            for _ in range(self.concurrency):
                self.pool.apply_async(
                    self._parse_next,
                    kwargs={'queue': queue, 'parse_func': parse_func}
                )
        else:
            for _ in range(self.concurrency):
                self.free_slots.append(None)
        if self.max_items > 1:
            self.pool.apply_async(self.itemproc.process_queue)

    def _process_immediate_request(self, request):
        engine = self.crawler.engine
        # Keep retries in the current parse flow so immediate ordering is preserved.
        # Parser owns the request until parse() transfers or releases its proxy lease.
        response = engine.explorer.try_explore(request=request, check_error=False, release_proxy=False)
        if response.meta.get('immediate_parse', False):
            self.parse(response)
        else:
            engine.explorer.on_response(response)

    def parse(self, response):
        request = response.request
        callback = request.callback or self.crawler.spider.parse
        errback = request.errback
        cb_kwargs = request.cb_kwargs
        node = request.meta.pop("promise_node", None)

        try:
            spider_outputs = None
            try:
                if response.failed:
                    if errback:
                        spider_outputs = _get_spider_outputs(errback, response, cb_kwargs=cb_kwargs)
                    elif request.meta.get('save_unhandled_failure', True):
                        try:
                            response.check_error()
                        except DownloadError as exc:
                            failure = {
                                "request": _request_snapshot(request, self.crawler.spider),
                                "type": exc.__class__.__name__,
                                "reason": str(exc),
                                "time": int(time.time() * 1000)
                            }
                            self.crawler.spider.meta['failures'].append(failure)
                    self.crawler.stats.inc_value("request/count/failed")
                else:
                    spider_outputs = _get_spider_outputs(callback, response, cb_kwargs=cb_kwargs)

                order = 0
                # sleep to switch thread
                for output in iter_generator(spider_outputs, sleep=self.crawler.sleep):
                    # maybe iter spider outputs directly.
                    state.maybe_shutdown()
                    if self._spider_output_filter(output, response):
                        if isinstance(output, Request):
                            self._inherit_request_proxy(output, request)
                        if node and isinstance(output, Request):
                            if not output.meta.get('promise_order'):
                                output.meta['promise_order'] = order
                                order += 1
                            node.add_child(output)  # already SCHEDULED
                            if output.meta.get('promise_node'):
                                self._process_spider_output(output, request)
                        else:
                            self._process_spider_output(output, request)
            except Exception as e:
                logger.exception(f"Parser bug processing {request}")
                error = {
                    "request": _request_snapshot(request, self.crawler.spider),
                    "time": int(time.time() * 1000),
                    "type": e.__class__.__name__,
                    "traceback": traceback.format_exc()
                }
                self.crawler.spider.meta['errors'].append(error)
                self.crawler.stats.inc_value(f"parser/{e.__class__.__name__}/count")

            _success = True  # reserved
            try:
                if node:
                    node.state = NodeState.EXECUTED if _success else NodeState.REJECTED
                    for output in iter_generator(node.then(), sleep=self.crawler.sleep):
                        # maybe iter node.then() directly.
                        state.maybe_shutdown()
                        if isinstance(output, Request):
                            try:
                                selected_proxy = output.selected_proxy
                            except ProxyError:
                                selected_proxy = None
                            if not selected_proxy:
                                self._inherit_request_proxy(output, request)
                        self._process_spider_output(output, request)
            except Exception as e:
                logger.exception(f"Promise bug processing {request}")
                self.crawler.stats.inc_value(f"promise/{e.__class__.__name__}/count")
        except (SpiderTerminate, SpiderShutdown):
            raise
        finally:
            response.close()
            request.close()
            del callback, errback

    def _proxy_key(self, request):
        proxypool = self.crawler.engine.explorer.proxypool
        return proxypool.get_proxy_key(request.proxy_schema, request.proxy_params)

    def _inherit_request_proxy(self, output, request):
        # Inherit only inside the same proxy schema and user-defined proxy key.
        same_context = output.proxy_schema == request.proxy_schema
        if same_context:
            try:
                same_context = self._proxy_key(output) == self._proxy_key(request)
            except (AttributeError, KeyError):
                same_context = False
        # Drop an already inherited proxy when the child asks for a different proxy context.
        if (
            output.meta.get('reset_proxy')
            or output.meta.get('disposable_proxy')
            # Copied parent proxies still need lease transfer; child explicit proxies win.
            or (output.proxies_explicit and output.proxy is not request.proxy)
            or not same_context
        ):
            if output.proxy is request.proxy:
                output.bind_proxy(Proxy())
            return
        try:
            # Transfer the parent lease for empty or already copied parent proxy.
            if request.selected_proxy and (output.proxy is request.proxy or not output.selected_proxy):
                output.inherit_proxy(request, proxies_explicit=request.proxies_explicit)
        except ProxyError:
            if output.proxy is request.proxy:
                output.bind_proxy(Proxy())

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
                failure = {
                    "request": request.to_dict(spider=self.crawler.spider, encode_body=True),
                    "reason": "Exceeded max depth",
                    "time": int(time.time() * 1000)
                }
                self.crawler.spider.meta['failures'].append(failure)
                request.close()
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
                failure = {
                    "request": request.to_dict(spider=self.crawler.spider, encode_body=True),
                    "reason": "Exceeded max parse times",
                    "time": int(time.time() * 1000)
                }
                self.crawler.spider.meta['failures'].append(failure)
                request.close()
                return False

        return True

    def _process_spider_output(self, output, request):
        if isinstance(output, Request):
            if output.meta.get('immediate_download', False):
                self._process_immediate_request(output)
            else:
                self.crawler.engine.schedule_request(output)
        elif isinstance(output, (Item, dict)):
            self.crawler.stats.inc_value(f"item/count")
            self.crawler.stats.inc_value(f"item/count/{output.__class__.__name__}")
            self._quick_process_item(output)
        elif output is None:
            pass
        else:
            typename = type(output).__name__
            logger.error(f'Spider must return request, item, or None, got {typename!r} in {request}')

    def _discard_pending_itemproc(self):
        if hasattr(self.itemproc, 'running'):
            self.itemproc.running = False
        _maybe_close(getattr(self.itemproc, 'items', None))
        _maybe_close(getattr(self.itemproc, 'processing', None))

    def _close_itemproc(self):
        return _close_in_background(self.itemproc.close, name='ParserItemprocClose')

    def _close_pool(self):
        return _close_in_background(self.pool.stop, name='ParserPoolStop')

    def close(self, reason):
        self.running = False
        try:
            error = None
            itemproc_closed = pool_closed = False
            try:
                itemproc_closed = self._close_itemproc()
            except Exception as exc:
                error = exc
            try:
                pool_closed = self._close_pool()  # maybe throw exception.
            except Exception as exc:
                if error is None:
                    error = exc
            if not (itemproc_closed and pool_closed):
                self._discard_pending_itemproc()
                gc.collect()
            if error is not None:
                raise error
            logger.info("Parser closed (%(reason)s)", {'reason': reason})
        except Exception as e:
            logger.error("Parser exception when closing, reason: %(reason)s", {'reason': e}, exc_info=True)
