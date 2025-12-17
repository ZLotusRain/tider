"""execute `Request`."""

import re
import time
from queue import Full
from collections import deque
from typing import Optional

from tider import Request
from tider.network import Session, ProxyPoolManager, Response
from tider.exceptions import (
    ConnectionError,
    DownloadError,
    HTTPError,
    ProxyError,
    InvalidProxy,
    InvalidSelectedProxy,
    ExclusiveProxy,
    Timeout,
    SpiderShutdown,
    SpiderTerminate
)
from tider.crawler import state
from tider.platforms import EX_FAILURE
from tider.utils.log import get_logger
from tider.utils.misc import build_from_crawler

logger = get_logger(__name__)

FREE_SLOTS = deque()


def _dummy_handler(response):
    request = response.request
    logger.info(f"Downloaded {request} and got {response}.")


class LimitsAllowed:

    def __init__(self, allowed, backtracks=None):
        self.allowed = allowed
        self.backtracks = backtracks or []

    def __bool__(self):
        return bool(self.allowed)


class ConcurrencyLimits:

    def __init__(self, limits=None):
        self._limits = {'url': {}, 'downloader': {}}

        limits = dict(limits or {})
        for limit_type in limits:
            if limit_type not in ('url', 'downloader'):
                continue
            for pattern, limit in limits[limit_type].items():
                self._limits[limit_type][pattern] = {
                    'pattern': re.compile(pattern),
                    'limit': limit,
                    'free_slots': deque([None] * int(limit))
                }
        self._global_limits = None  # fetched from redis.

    def allow(self, request):
        backtracks = []  # [(limit_type, pattern, request)], maybe processed in `on_response`
        for limit_type in self._limits:
            limits = self._limits[limit_type]
            attr = getattr(request, limit_type)
            if not attr:
                continue  # default downloader
            for pattern in limits:
                if limits[pattern]['pattern'].match(attr):
                    try:
                        limits[pattern]['free_slots'].pop()
                        backtracks.append((limit_type, pattern))
                    except IndexError:
                        self.backtrack(backtracks)
                        return LimitsAllowed(False)
        return LimitsAllowed(allowed=True, backtracks=backtracks)

    def backtrack(self, backtracks):
        for limit_type, pattern in backtracks:
            self._limits[limit_type][pattern]['free_slots'].append(None)

    def match(self, request):
        matched = []  # [(limit_type, pattern, request)]
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
        return len(self._limits[limit_type][pattern]['free_slots'])


class Explorer:

    def __init__(self, crawler, concurrency=4, concurrency_limits=None, priority_adjust=0, retry_until_valid_proxy=True):
        self._crawler = crawler
        self.concurrency = concurrency
        self.pool = crawler.create_pool(limit=concurrency, thread_name_prefix="ExplorerWorker")
        self.climits = ConcurrencyLimits(concurrency_limits)

        self.session: Optional[Session] = build_from_crawler(Session, crawler)
        self.proxypool = build_from_crawler(ProxyPoolManager, crawler)
        self.priority_adjust = priority_adjust  # adjust priority when retrying.
        self.retry_until_valid_proxy = retry_until_valid_proxy

        self.loop = crawler.settings.getbool('EXPLORER_USE_LOOP', False)
        self.queue = deque()
        self.on_response = None

        self.transferring = set()
        self.running = False

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            concurrency=crawler.concurrency,
            concurrency_limits=crawler.settings.getdict('EXPLORER_CONCURRENCY_LIMITS'),
            priority_adjust=crawler.settings.getint('EXPLORER_RETRY_PRIORITY_ADJUST'),
            retry_until_valid_proxy=crawler.settings.getint('EXPLORER_RETRY_UNTIL_VALID_PROXY', True)
        )

    def active(self):
        return self.running and len(self.transferring) + len(self.queue) > 0

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
        transferring = self.transferring
        if len(self.transferring) < self.concurrency and len(self.queue) > 0:
            # avoid stuck in threadpool executor when putting work item.
            try:
                FREE_SLOTS.pop()
                self.pool.apply_async(
                    self._transport,
                    (queue, download_func, on_response),
                    kwargs={'transferring': transferring, 'loop': self.loop},
                )
            except IndexError:
                pass
            except Full:
                FREE_SLOTS.append(None)

    def clear_idle_conns(self):
        # don't use `len(self.transferring) + len(self.queue) < self.concurrency`,
        # even reached the concurrency limit, some pool is may not being used(related to connection pool key).
        if self.session is None:
            return
        self.session.close_expired_connections()

    def needs_backout(self):
        return len(self.queue) >= self.concurrency * 2

    def _on_response_consumed(self, response):
        FREE_SLOTS.append(None)  # release slot to control concurrency.
        self.transferring.discard(response.request)

    def _transport(self, queue, download_func, output_handler=_dummy_handler, transferring=None, loop=False):
        while self.running:
            keep_slot = False
            try:
                request = queue.popleft()
                transferring and transferring.add(request)
                if request.meta.get('explore_after') and time.monotonic() < request.meta['explore_after']:
                    # queue.insert(len(queue)+1, request)  # append to the end of queue.
                    response = request
                else:
                    response = download_func(request)
                if request.stream and isinstance(response, Response) and not response.failed:
                    response.on_consumed = self._on_response_consumed
                    keep_slot = True
                # don't use thread lock in handler if possible.
                output_handler(response)
                del response

                if keep_slot:
                    return  # keep slot due to stream.
                transferring and transferring.discard(request)
            except IndexError:
                if not loop:
                    break
            except RuntimeError:
                break
            except (SpiderTerminate, SpiderShutdown):
                self.running = False
                break
            except Exception:
                self.running = False
                state.should_terminate = EX_FAILURE
                raise
            finally:
                # switch greenlet if using gevent
                # when the active one can't get the next request
                # to avoid keeping loop.
                self._crawler.sleep(0.001)

        del download_func, output_handler
        FREE_SLOTS.append(None)

    def build_request_proxy(self, request: Request):
        try:
            if request.selected_proxy:
                return
        except ProxyError:
            pass
        proxy = self.proxypool.get_proxy(
            schema=request.proxy_schema,
            proxy_args=request.proxy_params,
            disposable=request.meta.get('disposable_proxy', False),
            request=request,
        )
        request.update_proxy(proxy)

    def async_explore(self, on_response=None):
        if self.running:
            raise RuntimeError("Explorer already running")
        self.running = True

        self.pool.start()

        if not self.loop:
            FREE_SLOTS.clear()
            for _ in range(self.concurrency):
                FREE_SLOTS.append(None)
            self.on_response = on_response
            return
        # By just passing a reference to the object allows the garbage collector
        # to free self if nobody else has a reference to it.
        queue = self.queue
        download_func = self.explore
        transferring = self.transferring
        for _ in range(self.concurrency):
            self.pool.apply_async(
                self._transport,
                (queue, download_func, on_response),
                kwargs={'transferring': transferring, 'loop': self.loop},
            )

    def try_explore(self, method, url, params=None, data=None, headers=None, cookies=None,
                    files=None, auth=None, timeout=None, allow_redirects=True, proxies=None,
                    stream=None, verify=None, cert=None, json=None, raise_for_status=False,
                    proxy_schema=0, proxy_params=None, delay=0, max_retries=0, meta=None,
                    encoding=None, ignored_status_codes=(400, 412, 521), http2=False, downloader=None,
                    impersonate=None, session_cookies=None):
        """Simple request api."""
        request = Request(url=url, method=method, params=params, data=data, json=json, headers=headers,
                          cookies=cookies, files=files, auth=auth, timeout=timeout, allow_redirects=allow_redirects,
                          proxies=proxies, proxy_schema=proxy_schema, proxy_params=proxy_params,
                          stream=stream, verify=verify, cert=cert, raise_for_status=raise_for_status,
                          encoding=encoding, ignored_status_codes=ignored_status_codes, meta=meta,
                          http2=http2, downloader=downloader, impersonate=impersonate, session_cookies=session_cookies,
                          max_retries=max_retries, dup_check=False)
        response = self._explore(request)
        while isinstance(response, Request):
            retry_times = request.meta.get('retry_times', 0) + 1
            request.meta['retry_times'] = retry_times
            if delay > 0:
                self._crawler.sleep(delay)
            response = self._explore(request)
        try:
            response.check_error()
            return response
        except DownloadError:
            response.close()
            raise

    def explore(self, request):
        allowed = self.climits.allow(request)
        if not allowed:
            return request
        self.transferring.add(request)
        response = self._explore(request)
        if isinstance(response, Response):
            try:
                response.check_error()
            except Exception as e:
                retry_times = request.meta.get('retry_times', 0)
                logger.error(
                    "Gave up retrying %(request)s (failed %(retry_times)d times): "
                    "%(reason)s",
                    {'request': request, 'retry_times': retry_times, 'reason': e}
                )
        self.climits.backtrack(allowed.backtracks)
        self.transferring.discard(request)
        return response

    def _explore(self, request):
        self._crawler.stats.inc_value("request/count")
        # use `request.request_kwargs` directly may lead memory leak on CentOS.
        response = None
        try:
            self.build_request_proxy(request)
            if self.session is not None:
                response = self.session.download_request(request)
            else:
                with build_from_crawler(Session, self._crawler) as session:
                    response = session.download_request(request)
            response.check_error()
            if request.raise_for_status:
                response.raise_for_status()
        except HTTPError as e:
            ignored_status_code = request.ignored_status_codes
            status_code = response.status_code
            if str(status_code).startswith("4"):
                request.invalidate_proxy()
            if status_code not in ignored_status_code:
                response = self.get_retry_request(request, response, status_code=status_code, exc=e)
        except DownloadError as e:
            if isinstance(e, (ConnectionError, InvalidProxy, Timeout)):
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
            request.priority += self.priority_adjust
            if response is not None:
                response.close()
            self._crawler.stats.inc_value("request/count/retry")
            return request

        if response is None:
            response = Response(request)
        if not response.failed:
            response.fail(error=exc)
        return response
