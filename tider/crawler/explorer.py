"""execute `Request`."""

import re
import time
from logging import getLevelName
from queue import Full
from uuid import uuid4
from operator import itemgetter
from threading import Semaphore, Lock
from w3lib.url import parse_data_uri
from collections import deque, defaultdict
from typing import Union

from tider import Request
from tider.network import Session, Proxy, ProxyPoolManager, Response
from tider.exceptions import (
    _InvalidMiddlewareOutput,
    DownloadError,
    HTTPError,
    ProxyError,
    InvalidProxy,
    InvalidSelectedProxy,
    ExclusiveProxy,
    SpiderShutdown,
    SpiderTerminate,
)
from tider.crawler import state
from tider.platforms import EX_FAILURE
from tider.utils.url import parse_url_host
from tider.utils.log import get_logger
from tider.utils.imports import symbol_by_name
from tider.utils.misc import build_from_crawler

logger = get_logger(__name__)


def _dummy_handler(response):
    logger.info(f"Got transport's output: {response}")


class ConcurrencyLimits:
    """Thread-safe request concurrency limiter."""

    def __init__(self, limits=None, domain_concurrency=None):
        self._limits = {'url': {}, 'downloader': {}}
        self._domain_active_counts = {}
        self._lock = Lock()  # protects transient domain state and limit registration

        limits = dict(limits or {})
        for limit_type in limits:
            if limit_type not in ('url', 'downloader'):
                continue
            for pattern, limit in limits[limit_type].items():
                self._add_limit(limit_type, pattern, limit)

        self._global_limits = None  # fetched from redis.
        self._domain_concurrency = int(domain_concurrency) if domain_concurrency else None

    @staticmethod
    def _domain_url_pattern(domain):
        """Build a regex that matches full URLs belonging to *domain*."""
        return rf'https?://{re.escape(domain)}(/|\?|#|$)'

    def _add_limit(self, limit_type, pattern, limit):
        """Add or update the concurrency cap for *pattern* at runtime.

        Uses double-checked locking for thread safety.
        """
        if pattern in self._limits[limit_type]:
            return
        with self._lock:
            # double-check after acquiring lock
            if pattern not in self._limits[limit_type]:
                self._limits[limit_type][pattern] = {
                    'pattern': re.compile(pattern),
                    'limit': int(limit),
                    'semaphore': Semaphore(int(limit)),
                }

    def _acquire_domain(self, pattern):
        """Acquire a transient per-domain slot."""
        with self._lock:
            actives = self._domain_active_counts.get(pattern, 0)
            if actives >= self._domain_concurrency:
                return False
            self._domain_active_counts[pattern] = actives + 1
            return True

    def _release_domain(self, pattern):
        with self._lock:
            actives = self._domain_active_counts.get(pattern, 0)
            if actives <= 0:
                return
            if actives == 1:
                self._domain_active_counts.pop(pattern, None)
            else:
                self._domain_active_counts[pattern] = actives - 1

    def acquire(self, request):
        """Try to acquire slots for the request. Thread-safe."""
        acquired = []  # [(limit_type, pattern)]
        if self._domain_concurrency:
            domain = parse_url_host(request.url)
            if domain:
                pattern = self._domain_url_pattern(domain)
                # A user-defined limit with the same pattern already covers the
                # domain and preserves the previous precedence behavior.
                if pattern not in self._limits['url']:
                    if not self._acquire_domain(pattern):
                        return None
                    acquired.append(('domain', pattern))

        for limit_type in self._limits:
            limits = self._limits[limit_type]
            attr = getattr(request, limit_type)
            if not attr:
                continue  # default downloader
            for pattern in limits:
                if limits[pattern]['pattern'].match(attr):
                    if limits[pattern]['semaphore'].acquire(blocking=False):
                        acquired.append((limit_type, pattern))
                    else:
                        self.release(acquired)
                        return None
        return acquired

    def release(self, acquired):
        """Release acquired slots. Thread-safe."""
        for limit_type, pattern in acquired:
            if limit_type == 'domain':
                self._release_domain(pattern)
            else:
                self._limits[limit_type][pattern]['semaphore'].release()

    def match(self, request):
        """Return all limits that match the request (without acquiring)."""
        matched = []  # [(limit, limit_type, pattern)]
        if self._domain_concurrency:
            domain = parse_url_host(request.url)
            if domain:
                pattern = self._domain_url_pattern(domain)
                if pattern not in self._limits['url']:
                    # Keep the existing URL slot format used by
                    # ExplorerAwarePriorityQueue without retaining idle domains.
                    matched.append((self._domain_concurrency, 'url', pattern))
        for limit_type in self._limits:
            limits = self._limits[limit_type]
            attr = getattr(request, limit_type)
            if not attr:
                continue  # default downloader
            for pattern in limits:
                if limits[pattern]['pattern'].match(attr):
                    matched.append((limits[pattern]['limit'], limit_type, pattern))
        return matched

    def qsize(self, limit_type, pattern):
        """Return available slots count."""
        # Semaphore._value is CPython implementation detail but stable
        limit = self._limits[limit_type].get(pattern)
        if limit is not None:
            return limit['semaphore']._value
        if limit_type == 'url' and self._domain_concurrency:
            with self._lock:
                actives = self._domain_active_counts.get(pattern, 0)
            return max(0, self._domain_concurrency - actives)
        raise KeyError(pattern)


class MiddlewareManager:

    def __init__(self, *middlewares, crawler):
        self.crawler = crawler
        self.middlewares = middlewares
        self.methods = defaultdict(deque)
        for mw in middlewares:
            self._add_middleware(mw)

    @classmethod
    def from_crawler(cls, crawler):
        middlewares_dict = crawler.settings.getdict("EXPLORER_MIDDLEWARES")
        mwlist = [k for k, v in sorted(middlewares_dict.items(), key=itemgetter(1))]
        middlewares = []
        for clspath in mwlist:
            try:
                mwcls = symbol_by_name(clspath)
                mw = build_from_crawler(mwcls, crawler)
                middlewares.append(mw)
            except ImportError:
                logger.warning(f"Invalid middleware: {clspath}")
        return cls(*middlewares, crawler=crawler)

    def _add_middleware(self, mw) -> None:
        if hasattr(mw, "process_request"):
            self.methods["process_request"].append(mw.process_request)
        if hasattr(mw, "before_download"):
            self.methods["before_download"].append(mw.before_download)
        if hasattr(mw, "process_response"):
            self.methods["process_response"].appendleft(mw.process_response)

    def process_request(self, request) -> Union[Response, Request]:
        """process explorer Request-like input."""
        for method in self.methods["process_request"]:
            result = method(request=request)
            if result is not None and not isinstance(result, (Response, Request)):
                raise _InvalidMiddlewareOutput(
                    f"Middleware {method.__qualname__} must return None, Response or "
                    f"Request, got {result.__class__.__name__}"
                )
            if isinstance(result, Response):
                return result
        return request

    def before_download(self, request) -> Union[Response, Request]:
        """process Request after proxy is selected and before download."""
        for method in self.methods["before_download"]:
            result = method(request=request)
            if result is not None and not isinstance(result, (Response, Request)):
                raise _InvalidMiddlewareOutput(
                    f"Middleware {method.__qualname__} must return None, Response or "
                    f"Request, got {result.__class__.__name__}"
                )
            if isinstance(result, Response):
                return result
        return request

    def process_response(self, request, response) -> Union[Response, Request]:
        """process middleware/downloader Response-like output."""
        if response is None:
            raise TypeError("Received None in process_response")
        if isinstance(response, Request):
            return response

        for method in self.methods["process_response"]:
            response = method(request=request, response=response)
            if not isinstance(response, (Response, Request)):
                raise _InvalidMiddlewareOutput(
                    f"Middleware {method.__qualname__} must return Response or Request, "
                    f"got {type(response)}"
                )
            if isinstance(response, Request):
                return response
        return response


class Explorer:

    def __init__(self, crawler, concurrency=4, domain_concurrency=None, concurrency_limits=None,
                 priority_adjust=0, retry_until_valid_proxy=True, give_up_log_level="DEBUG"):
        self._crawler = crawler
        if isinstance(give_up_log_level, str):
            level = getLevelName(give_up_log_level)
            if not isinstance(level, int):
                raise ValueError(f"Invalid give-up log level: {give_up_log_level!r}")
            give_up_log_level = level
        self.give_up_log_level = give_up_log_level

        self.concurrency = concurrency
        self.pool = crawler.create_pool(limit=concurrency, thread_name_prefix="ExplorerWorker")
        self.climits = ConcurrencyLimits(concurrency_limits, domain_concurrency=domain_concurrency)

        self.session: Session = build_from_crawler(Session, crawler)
        self.middleware: MiddlewareManager = build_from_crawler(MiddlewareManager, crawler)
        self.proxypool = build_from_crawler(ProxyPoolManager, crawler)
        self.priority_adjust = priority_adjust  # adjust priority when retrying.
        self.retry_until_valid_proxy = retry_until_valid_proxy

        self.loop = crawler.settings.getbool('EXPLORER_USE_LOOP', False)
        self.queue = deque()
        self.on_response = None
        self.free_slots = deque()
        self.transports = []  # transport ids

        self.running = False
        self.transferring = set()
        self._transferring_count = 0  # double-check active state.

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            concurrency=crawler.concurrency,
            domain_concurrency=crawler.settings.get('CONCURRENT_REQUESTS_PER_DOMAIN'),
            concurrency_limits=crawler.settings.getdict('EXPLORER_CONCURRENCY_LIMITS'),
            priority_adjust=crawler.settings.getint('EXPLORER_RETRY_PRIORITY_ADJUST'),
            retry_until_valid_proxy=crawler.settings.getint('EXPLORER_RETRY_UNTIL_VALID_PROXY', True),
            give_up_log_level=crawler.settings['EXPLORER_RETRY_GIVE_UP_LOG_LEVEL'],
        )

    def active(self):
        return self.running and self._transferring_count + len(self.transferring) + len(self.queue) > 0

    def close(self, reason):
        self.running = False
        self.on_response = None
        try:
            self.pool.stop()
            self.proxypool.close()
            self.session.close()
            logger.info("Explorer closed (%(reason)s)", {'reason': reason})
        except Exception as e:
            logger.error("Explorer exception when closing, reason: %(reason)s", {'reason': e}, exc_info=True)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close(reason="finished")

    def fetch(self, request):
        if not self.running:
            raise RuntimeError("Explorer can't fetch requests before start.")
        self.queue.append(request)
        self.maybe_wakeup()

    def maybe_wakeup(self):
        if self.loop:
            return
        download_func = self.explore
        queue = self.queue
        on_response = self.on_response
        if len(self.transferring) < self.concurrency and len(self.queue) > 0:
            # avoid stuck in threadpool executor when putting work item.
            try:
                transport_id = self.free_slots.pop()
                self.pool.apply_async(
                    self._transport,
                    (queue, download_func, on_response),
                    kwargs={'transport_id': transport_id, 'loop': self.loop},
                )
            except IndexError:
                pass
            except Full:
                self.free_slots.append(str(uuid4()))

    def clear_idle_conns(self):
        # don't use `len(self.transferring) + len(self.queue) < self.concurrency`,
        # even reached the concurrency limit, some pool is may not being used(related to connection pool key).
        if self.session is None:
            return
        self.proxypool.close_expired_connections()
        self.session.close_expired_connections()

    def needs_backout(self):
        return len(self.queue) >= self.concurrency + 1

    def _on_response_consumed(self, response):
        transport_id = response.meta.get('transport_id')
        if transport_id:
            try:
                self.transports.remove(transport_id)
            except ValueError:
                pass
        self._transferring_count -= 1
        self.transferring.discard(response.request)
        self.free_slots.append(str(uuid4()))  # release slot to control concurrency.

    def _transport(self, queue, download_func, output_handler=_dummy_handler, transport_id=None, loop=False):
        exc = None
        transferring = self.transferring
        if transport_id:
            self.transports.append(transport_id)
        keep_slot = False
        while self.running:
            try:
                request = queue.popleft()
            except IndexError:
                # avoid catching IndexError raised by output handler.
                if not loop or state.stop_requested() or state.terminate_requested():
                    break
                self._crawler.sleep(0.001)
                continue

            keep_slot = False
            completed = False
            response = None
            self._transferring_count += 1
            should_free = True
            transferring.add(request)  # thread switch.
            if transport_id:
                request.meta['transport_id'] = transport_id
            try:
                if request.meta.get('explore_after') and time.monotonic() < request.meta['explore_after']:
                    # queue.insert(len(queue)+1, request)  # append to the end of queue.
                    response = request
                else:
                    response = download_func(request)
                if not loop and request.stream and isinstance(response, Response) and not response.failed:
                    response.on_consumed = self._on_response_consumed
                    keep_slot = True  # Response owns the slot until consumed.
                # don't use thread lock in handler if possible.
                output_handler(response)
                completed = True
                response = None
                if keep_slot:
                    should_free = False
                    return  # keep slot due to stream.
            except (SpiderTerminate, SpiderShutdown):
                self.running = False
                should_free = True
                break
            except Exception as e:
                exc = e
                self.running = False
                should_free = True
                state.should_terminate = EX_FAILURE
                break
            finally:
                if not completed:
                    if isinstance(response, Response):
                        response.close()
                    elif isinstance(response, Request) and response is not request:
                        response.close()
                    if request is not None:
                        request.close()
                # Stream responses release transferring through on_consumed.
                if should_free and not keep_slot:
                    self._transferring_count -= 1
                    transferring and transferring.discard(request)
                # switch greenlet if using gevent
                # when the active one can't get the next request
                # to avoid keeping loop.
                self._crawler.sleep(0.001)
        # Stream responses release transport ownership through on_consumed.
        if transport_id and not keep_slot:
            self.transports.remove(transport_id)
        del download_func, output_handler
        # Stream responses release their slot through on_consumed.
        if not keep_slot:
            self.free_slots.append(str(uuid4()))
        if exc is not None:
            raise exc

    def build_request_proxy(self, request: Request):
        if request.proxies_explicit:
            return
        try:
            if request.selected_proxy:
                return
        except ProxyError:
            # Release the invalid lease before reserving room for its replacement.
            request.bind_proxy(Proxy())
        proxy = self.proxypool.get_proxy(
            schema=request.proxy_schema,
            proxy_args=request.proxy_params,
            disposable=request.meta.get('disposable_proxy', False),
            request=request,
        )
        request.bind_proxy(proxy)

    def async_explore(self, on_response=None):
        if self.running:
            raise RuntimeError("Explorer already running")
        self.running = True

        self.pool.start()
        self.on_response = on_response
        if not self.loop:
            self.free_slots.clear()
            for _ in range(self.concurrency):
                self.free_slots.append(str(uuid4()))
            return
        # By just passing a reference to the object allows the garbage collector
        # to free self if nobody else has a reference to it.
        queue = self.queue
        download_func = self.explore
        for _ in range(self.concurrency):
            transport_id = str(uuid4())
            self.pool.apply_async(
                self._transport,
                (queue, download_func, on_response),
                kwargs={'transport_id': transport_id, 'loop': self.loop},
            )

    def try_explore(
        self, method=None, url=None, params=None, headers=None, data=None, json=None, auth=None, cookies=None,
        files=None, timeout=None, proxies=None, allow_redirects=True, stream=None, verify=None, cert=None,
        raise_for_status=False, proxy_schema=0, proxy_params=None, delay=0, max_retries=0, encoding=None, http2=False,
        ignored_status_codes=(400, 412, 521), downloader=None, impersonate=None, request=None, check_error=True,
        release_proxy=True, retry_on_proxy_wait=True, **kwargs
    ):
        """Simple request api. Opt out of proxy waits to return the retry Request to its caller."""
        kwargs.pop('dup_check', None)
        if request is None:
            request = Request(
                url=url, method=method, params=params, headers=headers, data=data, json=json, auth=auth, cookies=cookies,
                files=files, timeout=timeout, proxies=proxies, allow_redirects=allow_redirects, stream=stream, verify=verify,
                cert=cert, raise_for_status=raise_for_status, proxy_schema=proxy_schema, proxy_params=proxy_params,
                encoding=encoding, ignored_status_codes=ignored_status_codes, http2=http2, downloader=downloader,
                impersonate=impersonate, max_retries=max_retries, dup_check=False, **kwargs
            )
        response = self._explore(request)
        while isinstance(response, Request):
            if response.meta.pop('_proxy_wait', False) and not retry_on_proxy_wait:
                return response
            state.maybe_shutdown()
            if response is not request:
                request.close()
            request = response
            explore_after = request.meta.get('explore_after')
            if explore_after:
                wait = explore_after - time.monotonic()
                if wait > 0:
                    self._crawler.sleep(wait)
            elif delay > 0:
                self._crawler.sleep(delay)
            else:
                # Keep immediate retries inline, but let another worker run
                # when middleware returns a retry without a delay.
                self._crawler.sleep(0)
            response = self._explore(request)

        if release_proxy:
            previous_on_consumed = response.on_consumed

            def unbind_proxy(response):
                if previous_on_consumed is not None:
                    previous_on_consumed(response=response)
                response.request.unbind_proxy()

            if response._content_consumed:
                response.on_consumed = None
                unbind_proxy(response)
            else:
                response.on_consumed = unbind_proxy
        try:
            if check_error:
                response.check_error()
        except DownloadError:
            try:
                response.close()
            finally:
                request.close()
            raise

        return response

    def explore(self, request):
        acquired = self.climits.acquire(request)
        if acquired is None:
            return request
        try:
            return self._explore(request)
        finally:
            self.climits.release(acquired)

    def _explore(self, request):
        # A proxy wait belongs to one attempt, not to retries that inherit its meta.
        request.meta.pop('_proxy_wait', None)
        self._crawler.stats.inc_value("request/count")
        # use `request.request_kwargs` directly may lead memory leak on CentOS.
        result = self.middleware.process_request(request)
        response = None
        if isinstance(result, Request):
            # Release the superseded owner after middleware returns a replacement.
            if result is not request:
                request.close()
            request = result
            if request.url == 'about:blank':
                response = Response(request)
            elif request.url.startswith('data:'):
                response = Response.dummy(request=request, content=parse_data_uri(request.url).data)
            else:
                try:
                    self.build_request_proxy(request)
                    result = self.middleware.before_download(request)
                    if isinstance(result, Response):
                        response = result
                    else:
                        response = self.session.download_request(request)
                except ExclusiveProxy as e:
                    response = Response(request)
                    response.fail(e)
            result = self.middleware.process_response(request=request, response=response)
        elif isinstance(result, Response):
            response = result
            result = self.middleware.process_response(request=request, response=response)
        if isinstance(result, Request):
            # Close the response and old request before handing the replacement to the scheduler.
            result.meta.pop('_proxy_wait', None)
            if response is not None:
                response.close()
            if result is not request:
                request.close()
            return result

        # default procedure
        response = result
        try:
            response.check_error()
            if request.raise_for_status:
                response.raise_for_status()
        except HTTPError as e:
            ignored_status_code = request.ignored_status_codes
            status_code = response.status_code
            if status_code not in ignored_status_code:
                if status_code == 407:
                    request.invalidate_proxy()
                response = self.get_retry_request(request, response, status_code=status_code, exc=e)
        except (DownloadError, TypeError) as e:
            # TypeError for no status_code.
            if isinstance(e, InvalidProxy):
                request.invalidate_proxy()
            response = self.get_retry_request(request, response, exc=e)
        return response

    def get_retry_request(self, request, response, status_code=None, exc=None):
        retry_times = request.meta.get('retry_times', 0)
        priority = request.priority
        if not isinstance(exc, ExclusiveProxy) and not (self.retry_until_valid_proxy and isinstance(exc, InvalidSelectedProxy)):
            # re-select proxy.
            retry_times += 1
            priority += self.priority_adjust
        max_retries = request.max_retries
        if max_retries == -1 or retry_times <= max_retries:
            logger.debug(
                "Retrying %(request)s (failed %(retry_times)d times): %(reason)s",
                {'request': request, 'retry_times': retry_times, 'reason': str(exc)}
            )
            if request.delay or (status_code and str(status_code).startswith("5")):
                # update explore_after
                request.meta['explore_after'] = time.monotonic() + (request.delay or 2)
            request.dup_check = False
            request.meta['retry_times'] = retry_times
            request.priority = priority
            if response is not None:
                response.close()
            if isinstance(exc, ExclusiveProxy):
                request.meta['_proxy_wait'] = True
                request.meta['explore_after'] = time.monotonic() + (request.delay or 0.1)
                self._crawler.stats.inc_value("request/count/wait")
            else:
                self._crawler.stats.inc_value("request/count/retry")
            return request

        logger.log(
            self.give_up_log_level,
            "Gave up retrying %(request)s (failed %(retry_times)d times): %(reason)s",
            {'request': request, 'retry_times': retry_times, 'reason': exc}
        )
        if response is None:
            response = Response(request)
        if not response.failed:
            response.fail(error=exc)
        return response
