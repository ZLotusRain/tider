import queue
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress

try:
    from curl_cffi import Curl
    from curl_cffi.const import CurlECode, CurlHttpVersion, CurlInfo, CurlOpt
    from curl_cffi.curl import CurlError
    from curl_cffi.requests.headers import Headers
    from curl_cffi.requests.utils import set_curl_options
except ImportError:
    Curl = CurlECode = CurlHttpVersion = CurlInfo = CurlOpt = CurlError = Headers = set_curl_options = None

from tider import Request, Response
from tider.utils.log import get_logger
from tider.utils.time import preferred_clock
from tider.exceptions import ImproperlyConfigured, ProxyError

logger = get_logger(__name__)


_STREAM_END = object()

_HTTP_VERSION_NAMES = {
    0: "HTTP/?",
    1: "HTTP/1.0",
    2: "HTTP/1.1",
    3: "HTTP/2",
    4: "HTTP/2TLS",
    5: "HTTP/2",
    30: "HTTP/3",
    31: "HTTP/3",
}


def _curl_code(name, default: int):
    if CurlECode is None:
        return default
    return int(getattr(CurlECode, name, default))


_IMMEDIATE_PROXY_ERROR_CODES = frozenset({
    _curl_code("COULDNT_RESOLVE_PROXY", 5),
    _curl_code("PROXY", 97),
})
_SOFT_PROXY_ERROR_CODES = frozenset({
    _curl_code("COULDNT_CONNECT", 7),
    _curl_code("OPERATION_TIMEDOUT", 28),
})


def _decode_info(value):
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return value.decode("latin-1", errors="replace")
    return value


def _curl_error_code(error):
    try:
        return int(error.code)
    except (AttributeError, TypeError, ValueError):
        return None


class CurlResponse:
    """Small response adapter for tider.Response.from_origin_resp.

    This adapter intentionally keeps only the data required by Tider after a
    completed Curl transfer.  The Curl handle can be closed immediately after
    parsing, so the downloader does not keep a Session or a long-lived Curl
    object across Tider thread/greenlet workers.
    """

    def __init__(self, curl, body_buffer, header_buffer):
        self.curl = curl
        self.url = _decode_info(curl.getinfo(CurlInfo.EFFECTIVE_URL)) or ""
        self.content = body_buffer.getvalue() if body_buffer is not None else b""
        self.status_code = int(curl.getinfo(CurlInfo.RESPONSE_CODE))
        self.status = self.status_code
        self.http_version = int(curl.getinfo(CurlInfo.HTTP_VERSION))
        self.version = _HTTP_VERSION_NAMES.get(self.http_version, self.http_version)
        self.headers, self.reason = self._parse_headers(curl, header_buffer)

    @staticmethod
    def _parse_headers(curl, header_buffer):
        header_lines = header_buffer.getvalue().splitlines() if header_buffer is not None else []
        header_list = []
        reason = None

        for header_line in header_lines:
            if not header_line.strip():
                continue
            if header_line.startswith(b"HTTP/"):
                with suppress(Exception):
                    reason = curl.get_reason_phrase(header_line).decode()
                if reason is None:
                    parts = header_line.split(None, 2)
                    reason = _decode_info(parts[2]) if len(parts) == 3 else ""
                # Keep only the last response header block after redirects.
                header_list = []
                continue
            if header_line.startswith((b" ", b"\t")) and header_list:
                header_list[-1] += header_line
                continue
            header_list.append(header_line)

        return Headers(header_list), reason

    def stream(self, chunk_size=None, **_):
        """Iterate buffered response content as bytes."""
        if chunk_size is not None and chunk_size <= 0:
            chunk_size = None
        if not chunk_size:
            yield self.content
            return
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start:start + chunk_size]

    def close(self):
        curl, self.curl = self.curl, None
        if curl is not None:
            with suppress(Exception):
                curl.close()


class StreamingCurlResponse(CurlResponse):
    """CurlResponse variant backed by curl_cffi's streaming write callback."""

    def __init__(self, curl, header_buffer, queue_, task, quit_now, wait_task, queue_get):
        super().__init__(curl, body_buffer=None, header_buffer=header_buffer)
        self.queue = queue_
        self.task = task
        self.quit_now = quit_now
        self._wait_task = wait_task
        self._queue_get = queue_get
        self._closed = False

    def stream(self, chunk_size=None, **_):
        """Iterate live response content from curl's WRITEFUNCTION queue."""
        if chunk_size:
            # stream mode cannot enforce user chunk size;
            # the chunk boundary is controlled by libcurl.
            pass
        while True:
            chunk = self._queue_get(self.queue)
            if chunk is _STREAM_END:
                self.close()
                return
            if isinstance(chunk, Exception):
                self.close()
                raise chunk
            yield chunk

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self.quit_now is not None:
            with suppress(Exception):
                self.quit_now.set()
        if self.task is not None:
            with suppress(Exception):
                self._wait_task(self.task)
            self.task = None
        super().close()


class ImpersonateDownloader:
    """curl_cffi impersonate downloader based on Curl, not Session."""

    lazy = True

    def __init__(self, concurrency=None):
        self._thread = None
        self._concurrency = concurrency
        self._executor = None
        self._gevent_threadpool = None
        self._proxy_failures = weakref.WeakKeyDictionary()
        self._proxy_failures_lock = threading.RLock()
        try:
            import gevent.monkey
            if gevent.monkey.is_anything_patched():
                self._thread = "gevent"
        except ImportError:
            try:
                import eventlet.patcher
                if eventlet.patcher.already_patched:
                    self._thread = "eventlet"
            except ImportError:
                pass
        if self._thread:
            logger.warning(
                "Using greenlet with ImpersonateDownloader will run Curl.perform "
                "in the greenlet threadpool to avoid blocking the hub."
            )
        if concurrency and concurrency >= 50:
            logger.warning("Consider decreasing concurrency to avoid causing python core dump.")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.concurrency)

    def close_expired_connections(self):
        pass

    @property
    def _threadpool_size(self):
        return max(1, int(self._concurrency or 10))

    @property
    def executor(self):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self._threadpool_size)
        return self._executor

    @property
    def gevent_threadpool(self):
        if self._gevent_threadpool is None:
            import gevent
            from gevent.threadpool import ThreadPool
            self._gevent_threadpool = ThreadPool(self._threadpool_size, hub=gevent.get_hub())
        return self._gevent_threadpool

    @staticmethod
    def _safe_perform(curl):
        try:
            return curl.perform()
        except BaseException as e:
            return e

    @staticmethod
    def _raise_if_failed(result):
        if isinstance(result, BaseException):
            raise result
        return result

    def _perform(self, curl):
        if self._thread == "eventlet":
            import eventlet.tpool
            result = eventlet.tpool.execute(self._safe_perform, curl)
        elif self._thread == "gevent":
            result = self.gevent_threadpool.spawn(self._safe_perform, curl).get()
        else:
            result = self._safe_perform(curl)
        return self._raise_if_failed(result)

    def _spawn_stream_perform(self, func):
        if self._thread == "eventlet":
            import eventlet
            import eventlet.tpool
            return eventlet.spawn(lambda: eventlet.tpool.execute(func))
        if self._thread == "gevent":
            return self.gevent_threadpool.spawn(func)
        return self.executor.submit(func)

    def _wait_stream_task(self, task):
        if self._thread == "eventlet":
            return task.wait()
        if self._thread == "gevent":
            return task.get()
        return task.result()

    def _wait_stream_event(self, event):
        if self._thread == "eventlet":
            import eventlet
            while not event.is_set():
                eventlet.sleep(0.001)
            return
        if self._thread == "gevent":
            import gevent
            while not event.is_set():
                gevent.sleep(0.001)
            return
        event.wait()

    def _stream_queue_get(self, queue_):
        if self._thread == "eventlet":
            import eventlet
            while True:
                try:
                    return queue_.get_nowait()
                except queue.Empty:
                    eventlet.sleep(0.001)
        if self._thread == "gevent":
            import gevent
            while True:
                try:
                    return queue_.get_nowait()
                except queue.Empty:
                    gevent.sleep(0.001)
        return queue_.get()

    def _reset_proxy_failures(self, request, selected_proxy):
        if not selected_proxy:
            return
        proxy = getattr(request, "proxy", None)
        if proxy is None:
            return
        with self._proxy_failures_lock:
            failures = self._proxy_failures.get(proxy)
            if failures:
                failures.pop(selected_proxy, None)

    def _record_soft_proxy_failure(self, request, selected_proxy):
        proxy = getattr(request, "proxy", None)
        if proxy is None:
            return 0
        with self._proxy_failures_lock:
            failures = self._proxy_failures.setdefault(proxy, {})
            failures[selected_proxy] = failures.get(selected_proxy, 0) + 1
            return failures[selected_proxy]

    def _should_invalidate_proxy(self, request, selected_proxy, error):
        if isinstance(error, ProxyError):
            return True
        if not selected_proxy or not isinstance(error, CurlError):
            return False

        code = _curl_error_code(error)
        if code in _IMMEDIATE_PROXY_ERROR_CODES:
            return True
        if code in _SOFT_PROXY_ERROR_CODES:
            failures = self._record_soft_proxy_failure(request, selected_proxy)
            return failures >= 3
        return False

    @staticmethod
    def _peek_queue(queue_, default=None):
        with getattr(queue_, "mutex", suppress()):
            try:
                return queue_.queue[0]
            except (AttributeError, IndexError):
                return default

    def _build_response(self, request, curl, body_buffer, header_buffer, queue_=None, task=None, quit_now=None):
        if request.stream:
            raw = StreamingCurlResponse(
                curl=curl,
                header_buffer=header_buffer,
                queue_=queue_,
                task=task,
                quit_now=quit_now,
                wait_task=self._wait_stream_task,
                queue_get=self._stream_queue_get,
            )
        else:
            raw = CurlResponse(curl, body_buffer, header_buffer)
            raw.close()

        response = Response.from_origin_resp(resp=raw, request=request)
        if not request.stream:
            response.read()
        return response

    def download_request(self, request: Request, trust_env=True):
        if not Curl:
            raise ImproperlyConfigured(
                "You need to install the curl_cffi library to use impersonate downloader."
            )

        curl = None
        selected_proxy = None
        try:
            curl = Curl()
            http_version = CurlHttpVersion.NONE if not request.http2 else CurlHttpVersion.V2_0
            impersonate = request.impersonate if isinstance(request.impersonate, str) else "chrome"

            headers = request.prepared.headers.copy()
            headers.pop("Content-Length", None)

            selected_proxy = request.selected_proxy
            body = b"" if request.body is None else request.body

            start = preferred_clock()
            _, body_buffer, header_buffer, queue_, header_recved, quit_now = set_curl_options(
                curl,
                method=request.method,
                url=request.url,
                params_list=[None, None],
                data=body,
                headers_list=[None, headers],
                cookies_list=[None, request.prepared.cookies],
                auth=request.prepared.auth,
                timeout=request.timeout,
                allow_redirects=request.allow_redirects,
                max_redirects=request.meta.get("impersonate_max_redirects", 30),
                proxies_list=[None, None],
                proxy=selected_proxy,
                verify_list=[True, request.verify],
                accept_encoding=request.meta.get("impersonate_accept_encoding", "gzip, deflate, br"),
                impersonate=impersonate,
                ja3=request.meta.get("impersonate_ja3"),
                akamai=request.meta.get("impersonate_akamai"),
                extra_fp=request.meta.get("impersonate_extra_fp"),
                default_headers=request.meta.get("impersonate_default_headers", True),
                http_version=http_version,
                interface=request.meta.get("impersonate_interface"),
                cert=request.cert,
                stream=request.stream,
                max_recv_speed=request.meta.get("impersonate_max_recv_speed", 0),
                curl_options=request.meta.get("impersonate_curl_options"),
                queue_class=queue.Queue,
                event_class=threading.Event,
            )

            # Tider has already resolved/merged environment proxies before
            # entering the downloader.  Force libcurl not to discover env
            # proxies by itself when no proxy was selected for this request.
            if not selected_proxy:
                curl.setopt(CurlOpt.PROXY, "")

            if request.stream:
                def perform():
                    try:
                        curl.perform()
                    except Exception as e:
                        queue_.put_nowait(e)
                    finally:
                        if not header_recved.is_set():
                            header_recved.set()
                        queue_.put(_STREAM_END)

                task = self._spawn_stream_perform(perform)
                self._wait_stream_event(header_recved)

                first_element = self._peek_queue(queue_)
                if isinstance(first_element, Exception):
                    if quit_now is not None:
                        with suppress(Exception):
                            quit_now.set()
                    with suppress(Exception):
                        self._wait_stream_task(task)
                    curl.close()
                    curl = None
                    response = Response(request)
                    if self._should_invalidate_proxy(request, selected_proxy, first_element):
                        request.invalidate_proxy()
                    response.fail(error=first_element)
                    response.elapsed = preferred_clock() - start
                    return response

                response = self._build_response(
                    request=request,
                    curl=curl,
                    body_buffer=None,
                    header_buffer=header_buffer,
                    queue_=queue_,
                    task=task,
                    quit_now=quit_now,
                )
            else:
                self._perform(curl)
                response = self._build_response(request, curl, body_buffer, header_buffer)
            response.elapsed = preferred_clock() - start
            self._reset_proxy_failures(request, selected_proxy)
            curl = None
            return response
        except (CurlError, ProxyError) as e:
            if self._should_invalidate_proxy(request, selected_proxy, e):
                request.invalidate_proxy()
            response = Response(request)
            if not response.failed:
                response.fail(error=e)
            return response
        finally:
            if curl is not None:
                with suppress(Exception):
                    curl.close()

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None
        if self._gevent_threadpool is not None:
            with suppress(Exception):
                self._gevent_threadpool.kill()
            self._gevent_threadpool = None
