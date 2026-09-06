import _queue
import threading
from collections import deque
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
from tider.exceptions import (
    ConnectionError,
    ConnectTimeout,
    DownloadError,
    ImproperlyConfigured,
    InvalidProxy,
    ProtocolError,
    ProxyError,
    ReadTimeout,
    SSLError,
)

logger = get_logger(__name__)


_STREAM_END = object()
_STREAM_QUEUE_MAXSIZE = 16
_GEVENT_POLL_INTERVAL = 0.003


def _drain_stream_queue(queue_):
    if queue_ is None:
        return
    while True:
        try:
            queue_.get_nowait()
        except _queue.Empty:
            return


class _StreamQueue:
    """Thread-safe bounded queue that is not affected by monkey patching."""

    def __init__(self, maxsize=_STREAM_QUEUE_MAXSIZE):
        self._queue = _queue.SimpleQueue()
        self._slots = _queue.SimpleQueue()
        for _ in range(max(1, int(maxsize))):
            self._slots.put(None)
        self.quit_now = None

    def put_nowait(self, item):
        while self.quit_now is None or not self.quit_now.is_set():
            try:
                self._slots.get(timeout=0.1)
            except _queue.Empty:
                continue
            if self.quit_now is not None and self.quit_now.is_set():
                self._slots.put(None)
                return
            self._queue.put(item)
            return

    def get(self):
        item = self._queue.get()
        self._slots.put(None)
        return item

    def get_nowait(self):
        item = self._queue.get_nowait()
        self._slots.put(None)
        return item

    def qsize(self):
        return self._queue.qsize()


class _ThreadEvent:
    """Cross-thread event that does not depend on patched threading locks."""

    def __init__(self):
        self._is_set = False
        self._signal = _queue.SimpleQueue()

    def is_set(self):
        return self._is_set

    def set(self):
        if not self._is_set:
            self._is_set = True
            self._signal.put(None)

    def wait(self, timeout=None):
        if self._is_set:
            return True
        try:
            self._signal.get(timeout=timeout)
        except _queue.Empty:
            return self._is_set
        self._signal.put(None)
        return True


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
_SSL_ERROR_CODES = frozenset(_curl_code(name, -1) for name in (
    "SSL_CACERT_BADFILE", "SSL_CERTPROBLEM", "SSL_CIPHER", "SSL_CLIENTCERT",
    "SSL_CONNECT_ERROR", "SSL_CRL_BADFILE", "SSL_ENGINE_INITFAILED",
    "SSL_ENGINE_NOTFOUND", "SSL_ENGINE_SETFAILED", "SSL_INVALIDCERTSTATUS",
    "SSL_ISSUER_ERROR", "SSL_PINNEDPUBKEYNOTMATCH", "SSL_SHUTDOWN_FAILED",
    "USE_SSL_FAILED",
))
_CONNECTION_ERROR_CODES = frozenset(_curl_code(name, -1) for name in (
    "COULDNT_RESOLVE_HOST", "COULDNT_CONNECT", "RECV_ERROR", "SEND_ERROR",
    "NO_CONNECTION_AVAILABLE", "QUIC_CONNECT_ERROR",
))
_PROTOCOL_ERROR_CODES = frozenset(_curl_code(name, -1) for name in (
    "HTTP2", "HTTP2_STREAM", "PARTIAL_FILE", "UNSUPPORTED_PROTOCOL",
))


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


def _map_curl_error(error, received_headers=False):
    """Map curl failures to Tider's retryable download error taxonomy."""
    if isinstance(error, DownloadError):
        return error
    code = _curl_error_code(error)
    if code in _IMMEDIATE_PROXY_ERROR_CODES:
        return InvalidProxy(error)
    if code == _curl_code("OPERATION_TIMEDOUT", 28):
        return ReadTimeout(error) if received_headers else ConnectTimeout(error)
    if code in _SSL_ERROR_CODES:
        return SSLError(error)
    if code in _PROTOCOL_ERROR_CODES:
        return ProtocolError(error)
    if code in _CONNECTION_ERROR_CODES:
        return ConnectionError(error)
    return DownloadError(error)


def _set_curl_option(curl, name, value):
    if CurlOpt is None:
        return
    opt = getattr(CurlOpt, name, None)
    if opt is not None:
        with suppress(Exception):
            curl.setopt(opt, value)


if Headers is not None:
    class CurlHeaders(Headers):
        """Headers adapter that keeps duplicate response headers for cookies."""

        def items(self):
            return self.multi_items()
else:
    CurlHeaders = None


class CurlPool:
    """Small bounded idle pool for curl_cffi Curl handles.

    A Curl handle keeps libcurl's connection cache across ``reset()``, so
    successful direct requests can reuse handles without adding another active
    concurrency limit beside Tider's explorer pipeline.
    """

    def __init__(
        self, maxsize=10, idle_ttl=60, max_lifetime=None, max_connects=None,
        curl_cls=None,
    ):
        self.maxsize = max(1, int(maxsize or 10))
        self.idle_ttl = float(idle_ttl) if idle_ttl is not None else None
        self.max_lifetime = float(max_lifetime) if max_lifetime is not None else None
        self.max_connects = max_connects
        self.curl_cls = curl_cls or Curl

        self._idle = deque()
        self._closed = False
        self._lock = threading.RLock()

    def new_curl(self):
        curl = self.curl_cls()
        self._configure_curl(curl)
        return curl

    def _configure_curl(self, curl):
        if CurlOpt is None:
            return
        if self.max_connects:
            _set_curl_option(curl, "MAXCONNECTS", int(self.max_connects))
        if self.idle_ttl:
            _set_curl_option(curl, "MAXAGE_CONN", int(self.idle_ttl))
            _set_curl_option(curl, "DNS_CACHE_TIMEOUT", int(self.idle_ttl))
        if self.max_lifetime:
            _set_curl_option(curl, "MAXLIFETIME_CONN", int(self.max_lifetime))

    def _discard_expired_locked(self, now=None):
        if not self.idle_ttl and not self.max_lifetime:
            return []
        now = preferred_clock() if now is None else now
        kept = deque()
        expired = []
        while self._idle:
            last_used, created_at, curl = self._idle.popleft()
            idle_expired = self.idle_ttl and now - last_used >= self.idle_ttl
            lifetime_expired = self.max_lifetime and now - created_at >= self.max_lifetime
            if idle_expired or lifetime_expired:
                expired.append(curl)
            else:
                kept.append((last_used, created_at, curl))
        self._idle = kept
        return expired

    @staticmethod
    def _close_curls(curls):
        for curl in curls:
            with suppress(Exception):
                curl.close()

    def acquire(self):
        """Return an idle Curl handle or create a new one."""
        expired = []
        curl = None
        created_at = None
        now = preferred_clock()
        with self._lock:
            if self._closed:
                raise RuntimeError("CurlPool is closed")
            while self._idle:
                last_used, created_at, curl = self._idle.pop()
                idle_expired = self.idle_ttl and now - last_used >= self.idle_ttl
                lifetime_expired = self.max_lifetime and now - created_at >= self.max_lifetime
                if not (idle_expired or lifetime_expired):
                    break
                expired.append(curl)
                curl = None

        self._close_curls(expired)
        if curl is None:
            curl = self.new_curl()
            created_at = preferred_clock()
        return _CurlHandle(self, curl, created_at=created_at)

    def release(self, curl, created_at, reusable=True):
        if curl is None:
            return

        now = preferred_clock()
        if reusable and self.max_lifetime and now - created_at >= self.max_lifetime:
            reusable = False
        if reusable:
            try:
                # perform() clears callback handles before the Curl is pooled.
                curl.reset()
                # curl_easy_reset drops options but retains native caches.
                self._configure_curl(curl)
            except Exception:
                reusable = False

        close_curl = True
        with self._lock:
            if reusable and not self._closed and len(self._idle) < self.maxsize:
                self._idle.append((now, created_at, curl))
                close_curl = False

        if close_curl:
            with suppress(Exception):
                curl.close()

    def close_expired_connections(self):
        with self._lock:
            expired = self._discard_expired_locked()
        self._close_curls(expired)

    def close(self):
        with self._lock:
            self._closed = True
            idle = [curl for _, _, curl in self._idle]
            self._idle.clear()
        self._close_curls(idle)


class _CurlHandle:
    __slots__ = ("pool", "curl", "created_at", "_released")

    def __init__(self, pool, curl, created_at):
        self.pool = pool
        self.curl = curl
        self.created_at = created_at
        self._released = False

    def release(self, reusable=True):
        if self._released:
            return
        self._released = True
        curl, self.curl = self.curl, None
        self.pool.release(curl, created_at=self.created_at, reusable=reusable)

    def discard(self):
        self.release(reusable=False)


class CurlResponse:
    """Small response adapter for tider.Response.from_origin_resp.

    This adapter intentionally keeps only the metadata required by Tider after
    a Curl transfer.  Non-stream response bytes are attached directly to
    ``Response._content`` so the raw adapter does not duplicate the body.
    """

    def __init__(self, curl, header_buffer):
        self.curl = curl
        self.url = _decode_info(curl.getinfo(CurlInfo.EFFECTIVE_URL)) or ""
        self.content = None
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

        return CurlHeaders(header_list), reason

    def stream(self, chunk_size=None, **_):
        """Iterate buffered response content as bytes."""
        if self.content is None:
            return
        if chunk_size is not None and chunk_size <= 0:
            chunk_size = None
        if not chunk_size:
            yield self.content
            return
        for start in range(0, len(self.content), chunk_size):
            yield self.content[start:start + chunk_size]

    def close(self):
        self.curl = None
        self.content = None


class StreamingCurlResponse(CurlResponse):
    """CurlResponse variant backed by curl_cffi's streaming write callback."""

    def __init__(
        self, curl, header_buffer, queue_, task, quit_now, wait_task, queue_get,
        on_complete=None, on_error=None,
    ):
        super().__init__(curl, header_buffer=header_buffer)
        self.header_buffer = header_buffer
        self.queue = queue_
        self.task = task
        self.quit_now = quit_now
        self._wait_task = wait_task
        self._queue_get = queue_get
        self._on_complete = on_complete
        self._on_error = on_error
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
                if self._on_complete is not None:
                    with suppress(Exception):
                        self._on_complete()
                self.close()
                return
            if isinstance(chunk, Exception):
                if self._on_error is not None:
                    with suppress(Exception):
                        self._on_error(chunk)
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
        _drain_stream_queue(self.queue)
        if self.task is not None:
            with suppress(Exception):
                self._wait_task(self.task)
            self.task = None
        curl, self.curl = self.curl, None
        if curl is not None:
            with suppress(Exception):
                curl.close()
        _drain_stream_queue(self.queue)
        if self.header_buffer is not None:
            with suppress(Exception):
                self.header_buffer.close()
        self.header_buffer = None
        self.queue = None
        self.quit_now = None
        self._wait_task = None
        self._queue_get = None
        self._on_complete = None
        self._on_error = None
        super().close()


class _CurlExecutor:
    """Run blocking Curl work without blocking a greenlet hub."""

    def __init__(self, kind=None, max_workers=1):
        self.kind = kind
        self.max_workers = max(1, int(max_workers))
        self._pool = None

    @property
    def pool(self):
        if self._pool is None:
            if self.kind == "gevent":
                import gevent
                from gevent.threadpool import ThreadPool
                self._pool = ThreadPool(self.max_workers, hub=gevent.get_hub())
            else:
                self._pool = ThreadPoolExecutor(max_workers=self.max_workers)
        return self._pool

    def perform(self, func):
        if self.kind == "eventlet":
            import eventlet.tpool
            return eventlet.tpool.execute(func)
        if self.kind == "gevent":
            return self.pool.spawn(func).get()
        if self.kind == "threads":
            return self.pool.submit(func).result()
        return func()

    def spawn(self, func):
        if self.kind == "eventlet":
            import eventlet
            import eventlet.tpool
            return eventlet.spawn(lambda: eventlet.tpool.execute(func))
        if self.kind == "gevent":
            return self.pool.spawn(func)
        return self.pool.submit(func)

    def wait(self, task):
        if self.kind == "eventlet":
            return task.wait()
        if self.kind == "gevent":
            return task.get()
        return task.result()

    def wait_event(self, event):
        if self.kind == "eventlet":
            import eventlet
            while not event.is_set():
                eventlet.sleep(0.001)
            return
        if self.kind == "gevent":
            import gevent
            while not event.is_set():
                gevent.sleep(_GEVENT_POLL_INTERVAL)
            return
        event.wait()

    def queue_get(self, queue_):
        if self.kind == "eventlet":
            import eventlet
            while True:
                try:
                    return queue_.get_nowait()
                except _queue.Empty:
                    eventlet.sleep(0.001)
        if self.kind == "gevent":
            import gevent
            while True:
                try:
                    return queue_.get_nowait()
                except _queue.Empty:
                    gevent.sleep(_GEVENT_POLL_INTERVAL)
        return queue_.get()

    def close(self):
        pool, self._pool = self._pool, None
        if pool is not None:
            if self.kind == "gevent":
                with suppress(Exception):
                    pool.kill()
            else:
                pool.shutdown(wait=False, cancel_futures=True)


class ImpersonateDownloader:
    """curl_cffi impersonate downloader based on Curl, not Session."""

    lazy = True

    def __init__(self, concurrency=None, thread=None, stream_queue_maxsize=_STREAM_QUEUE_MAXSIZE):
        self._thread = thread
        self._concurrency = concurrency
        self._stream_queue_maxsize = max(1, int(stream_queue_maxsize))
        self._pool_maxsize = max(1, int(concurrency or 10))
        self._idle_ttl = 60
        self._curl_max_lifetime = 600
        self._max_connects = 1
        self._curl_executor = _CurlExecutor(thread, max_workers=self._pool_maxsize)
        self.curl_pool = CurlPool(
            maxsize=self._pool_maxsize,
            idle_ttl=self._idle_ttl,
            max_lifetime=self._curl_max_lifetime,
            max_connects=self._max_connects,
        )
        if self._thread:
            logger.warning(
                "Using greenlet with ImpersonateDownloader will run Curl.perform "
                "in the greenlet threadpool to avoid blocking the hub."
            )
        if concurrency and concurrency >= 50:
            logger.warning("Consider decreasing concurrency to avoid causing python core dump.")

    @classmethod
    def from_crawler(cls, crawler):
        pool_cls = crawler.Pool
        module = pool_cls.__module__
        thread = "threads" if getattr(pool_cls, "is_green", False) else None
        if module.endswith(".gevent"):
            thread = "gevent"
        elif module.endswith(".eventlet"):
            thread = "eventlet"
        return cls(
            concurrency=crawler.concurrency,
            thread=thread,
            stream_queue_maxsize=crawler.settings.getint(
                "IMPERSONATE_STREAM_QUEUE_MAXSIZE", _STREAM_QUEUE_MAXSIZE,
            ),
        )

    def close_expired_connections(self):
        if self.curl_pool is not None:
            self.curl_pool.close_expired_connections()

    @staticmethod
    def _safe_perform(curl):
        try:
            return curl.perform()
        except BaseException as e:
            # Do not retain native worker frames through a failed Response.
            e.__traceback__ = None
            return e

    @staticmethod
    def _raise_if_failed(result):
        if isinstance(result, BaseException):
            raise result
        return result

    def _perform(self, curl):
        result = self._curl_executor.perform(lambda: self._safe_perform(curl))
        return self._raise_if_failed(result)

    def _spawn_stream_perform(self, func):
        return self._curl_executor.spawn(func)

    def _wait_stream_task(self, task):
        return self._curl_executor.wait(task)

    def _wait_stream_event(self, event):
        self._curl_executor.wait_event(event)

    def _stream_queue_get(self, queue_):
        return self._curl_executor.queue_get(queue_)

    def _should_invalidate_proxy(self, selected_proxy, error):
        if isinstance(error, ProxyError):
            return True
        if not selected_proxy or CurlError is None or not isinstance(error, CurlError):
            return False

        code = _curl_error_code(error)
        return code in _IMMEDIATE_PROXY_ERROR_CODES

    def _build_response(
        self, request, prepared, curl, body_buffer, header_buffer, queue_=None, task=None,
        quit_now=None, on_complete=None, on_error=None,
    ):
        if request.stream:
            raw = StreamingCurlResponse(
                curl=curl,
                header_buffer=header_buffer,
                queue_=queue_,
                task=task,
                quit_now=quit_now,
                wait_task=self._wait_stream_task,
                queue_get=self._stream_queue_get,
                on_complete=on_complete,
                on_error=on_error,
            )
        else:
            raw = CurlResponse(curl, header_buffer)
            if header_buffer is not None:
                with suppress(Exception):
                    header_buffer.close()

        response_prepared = prepared
        if raw.url and raw.url != prepared.url:
            response_prepared = prepared.copy_with(
                url=raw.url,
                apply_auth=False,
            )
        response = Response.from_origin_resp(
            resp=raw,
            request=request,
            prepared=response_prepared,
        )
        if not request.stream:
            # Attach the completed body directly instead of joining it again.
            response._content = body_buffer if body_buffer is not None else b""
            response._content_consumed = True

        response.url = response_prepared.url
        if not request.stream:
            raw.close()
        return response

    @staticmethod
    def _failed_response(request, error, start):
        error.__traceback__ = None
        response = Response(request)
        response.fail(error=error)
        response.elapsed = preferred_clock() - start
        return response

    def download_request(self, request: Request, prepared, trust_env=True, max_redirects=None):
        if not Curl:
            raise ImproperlyConfigured(
                "You need to install the curl_cffi library to use impersonate downloader."
            )

        max_redirects = request.meta.get(
            "impersonate_max_redirects",
            30 if max_redirects is None else max_redirects,
        )
        curl = curl_handle = None
        body_buffer = header_buffer = None
        body_chunks = []
        queue_ = quit_now = task = None
        selected_proxy = None
        start = preferred_clock()

        http_version = CurlHttpVersion.NONE if not request.http2 else CurlHttpVersion.V2_0
        impersonate = request.impersonate if isinstance(request.impersonate, str) else "chrome"

        headers = prepared.headers.copy()
        headers.pop("Content-Length", None)
        headers.pop("Cookie", None)
        headers.pop("Accept-Encoding", None)
        # Let curl_cffi manage hop-by-hop headers for the impersonated protocol.
        headers.pop("Connection", None)
        try:
            selected_proxy = request.selected_proxy
            reuse_curl = not request.stream and not selected_proxy
            if reuse_curl:
                curl_handle = self.curl_pool.acquire()
                curl = curl_handle.curl
            else:
                curl = self.curl_pool.new_curl()
            body = b"" if prepared.body is None else prepared.body
            _, body_buffer, header_buffer, queue_, header_recved, quit_now = set_curl_options(
                curl,
                method=prepared.method,
                url=prepared.url,
                params_list=[None, None],
                data=body,
                headers_list=[None, headers],
                cookies_list=[None, prepared.cookies],
                auth=prepared.auth,
                timeout=request.timeout,
                allow_redirects=request.allow_redirects,
                max_redirects=max_redirects,
                proxies_list=[None, None],
                proxy=selected_proxy,
                verify_list=[True, request.verify],
                accept_encoding=request.meta.get("impersonate_accept_encoding", "gzip, deflate, br"),
                content_callback=body_chunks.append if not request.stream else None,
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
                queue_class=lambda: _StreamQueue(self._stream_queue_maxsize),
                event_class=_ThreadEvent,
            )
            if queue_ is not None:
                queue_.quit_now = quit_now

            # Tider has already resolved/merged environment proxies before
            # entering the downloader.  Force libcurl not to discover env
            # proxies by itself when no proxy was selected for this request.
            if not selected_proxy:
                curl.setopt(CurlOpt.PROXY, "")

            if request.stream:
                stream_errors = []

                def perform(curl=curl, queue_=queue_, header_recved=header_recved):
                    try:
                        curl.perform()
                    except Exception as e:
                        e.__traceback__ = None
                        mapped = _map_curl_error(e, received_headers=header_recved.is_set())
                        stream_errors.append(mapped)
                        queue_.put_nowait(mapped)
                    finally:
                        if not header_recved.is_set():
                            header_recved.set()
                        queue_.put_nowait(_STREAM_END)

                task = self._spawn_stream_perform(perform)
                self._wait_stream_event(header_recved)

                first_error = stream_errors[0] if stream_errors else None
                if first_error is not None:
                    if self._should_invalidate_proxy(selected_proxy, first_error):
                        request.invalidate_proxy()
                    return self._failed_response(request, first_error, start)

                def on_error(error):
                    if self._should_invalidate_proxy(selected_proxy, error):
                        request.invalidate_proxy()

                response = self._build_response(
                    request=request,
                    prepared=prepared,
                    curl=curl,
                    body_buffer=None,
                    header_buffer=header_buffer,
                    queue_=queue_,
                    task=task,
                    quit_now=quit_now,
                    on_error=on_error,
                )
                curl = header_buffer = queue_ = quit_now = task = None
            else:
                self._perform(curl)
                body_buffer = b"".join(body_chunks)
                body_chunks.clear()
                response = self._build_response(
                    request, prepared, curl, body_buffer, header_buffer,
                )
                body_buffer = header_buffer = None
                if curl_handle is not None:
                    curl_handle.release()
                    curl_handle = None
                else:
                    curl.close()
                curl = None
            response.elapsed = preferred_clock() - start
            return response
        except Exception as e:
            if self._should_invalidate_proxy(selected_proxy, e):
                request.invalidate_proxy()
            return self._failed_response(request, _map_curl_error(e), start)
        finally:
            if task is not None:
                if quit_now is not None:
                    with suppress(Exception):
                        quit_now.set()
                _drain_stream_queue(queue_)
                with suppress(Exception):
                    self._wait_stream_task(task)
                _drain_stream_queue(queue_)
            if curl_handle is not None:
                curl_handle.discard()
            elif curl is not None:
                with suppress(Exception):
                    curl.close()
            body_chunks.clear()
            for buffer in (body_buffer, header_buffer):
                close = getattr(buffer, "close", None)
                if close is not None:
                    with suppress(Exception):
                        close()

    def close(self):
        if self.curl_pool is not None:
            self.curl_pool.close()
            self.curl_pool = None
        if self._curl_executor is not None:
            self._curl_executor.close()
            self._curl_executor = None
