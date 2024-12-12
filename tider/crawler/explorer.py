"""execute `Request`."""

import time
import threading
from collections import deque
from typing import Optional

from tider import Request
from tider.network import Session, ProxyPoolManager, Response
from tider.exceptions import (
    DownloadError,
    ConnectionError,
    HTTPError,
    ProxyError,
    Timeout,
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


def _transport(queue, download_func, output_handler=None, shutdown_event=None, loop=False):
    output_handler = output_handler if output_handler is not None else _dummy_handler
    shutdown_event = shutdown_event if shutdown_event is not None else threading.Event()
    while not shutdown_event.is_set():
        try:
            state.maybe_shutdown()
            request = queue.popleft()
            response = download_func(request)
            # don't use thread lock in handler if possible.
            output_handler(response)
        except IndexError:
            if not loop:
                FREE_SLOTS.append(None)
                break
        except BaseException:
            shutdown_event.set()
            state.should_terminate = EX_FAILURE
            raise
        finally:
            # switch greenlet if using gevent
            # when the active one can't get the next request
            # to avoid keeping loop.
            time.sleep(0.01)


class Explorer:

    def __init__(self, crawler, concurrency=4, use_session=True, priority_adjust=0):
        self._crawler = crawler
        self.concurrency = concurrency
        self.pool = crawler.create_pool(limit=concurrency, thread_name_prefix="ExplorerWorker")

        self.session: Optional[Session] = build_from_crawler(Session, crawler) if use_session else None
        self.proxypool = build_from_crawler(ProxyPoolManager, crawler)
        self.priority_adjust = priority_adjust  # adjust priority when retrying.

        self.loop = crawler.settings.getbool('EXPLORER_USE_LOOP', False)
        if not self.loop:
            FREE_SLOTS.clear()
            for _ in range(concurrency):
                FREE_SLOTS.append(None)
        self.queue = deque()

        self.transferring = set()
        self.running = False
        self._shutdown = threading.Event()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            concurrency=crawler.concurrency,
            priority_adjust=crawler.settings.getint('EXPLORER_RETRY_PRIORITY_ADJUST')
        )

    def active(self):
        return self.running and len(self.transferring) + len(self.queue) > 0

    def close(self, reason):
        self.running = False
        self._shutdown.set()

        self.queue.clear()
        self.pool.stop()
        self.transferring.clear()
        self.proxypool.close()
        self.session.close()
        logger.info("Explorer closed (%(reason)s)", {'reason': reason})

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close(reason="finished")

    def fetch(self, request):
        if self._shutdown.is_set():
            self.running = False
        if not self.running:
            raise RuntimeError("Explorer can't fetch requests before start.")
        self.queue.append(request)

        download_func = self.explore
        shutdown_event = self._shutdown
        queue = self.queue
        output_handler = self.output_handler
        if not self.loop and len(self.transferring) < self.concurrency and len(self.queue) > 0:
            # avoid stuck in threadpool executor when putting work item.
            try:
                FREE_SLOTS.pop()
                self.pool.apply_async(
                    _transport,
                    (queue, download_func, output_handler, shutdown_event)
                )
            except IndexError:
                pass

    def needs_backout(self):
        return len(self.queue) >= self.concurrency * 2

    def build_request_proxy(self, request: Request):
        try:
            if request.selected_proxy:
                return
        except ProxyError:
            pass
        proxy = self.proxypool.get_proxy(
            schema=request.proxy_schema,
            proxy_args=request.proxy_params,
            disposable=request.meta.get('new_proxy', False),
        )
        request.update_proxy(proxy)

    def async_explore(self, output_handler=None):
        if self.running:
            raise RuntimeError("Explorer already running")
        self.running = True

        self.pool.start()

        if not self.loop:
            self.output_handler = output_handler
            return
        # By just passing a reference to the object allows the garbage collector
        # to free self if nobody else has a reference to it.
        queue = self.queue
        download_func = self.explore
        shutdown_event = self._shutdown
        for _ in range(self.concurrency):
            self.pool.apply_async(
                _transport,
                (queue, download_func, output_handler, shutdown_event, self.loop)
            )

    def explore(self, request):
        self.transferring.add(request)
        self._crawler.stats.inc_value("request/count")
        if request.delay:
            time.sleep(request.delay)
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
        self.transferring.discard(request)
        return response

    def _explore(self, request):
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
                response = self.get_retry_request(request, response, exc=e)
                if str(status_code) == "500":
                    time.sleep(2)
                elif str(status_code).startswith("5"):
                    time.sleep(1)
        except DownloadError as e:
            if isinstance(e, (ConnectionError, ProxyError, Timeout)):
                request.invalidate_proxy()
            response = self.get_retry_request(request, response, exc=e)
        return response

    def get_retry_request(self, request, response, exc):
        retry_times = request.meta.get('retry_times', 0) + 1
        max_retries = request.max_retries
        if max_retries == -1 or retry_times <= max_retries:
            logger.debug(
                "Retrying %(request)s (failed %(retry_times)d times): %(reason)s",
                {'request': request, 'retry_times': retry_times, 'reason': str(exc)}
            )
            request.meta['retry_times'] = retry_times
            request.dup_check = False
            request.priority += self.priority_adjust
            if response is not None:
                response.close()
            self._crawler.stats.inc_value("request/count/retry")
            return request
        else:
            if response is None:
                response = Response(request)
            if not response.failed:
                response.fail(error=exc)
            return response
