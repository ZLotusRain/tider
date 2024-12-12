import warnings
from types import MethodType

try:
    from curl_cffi.requests import Session
    from curl_cffi.const import CurlHttpVersion
    from curl_cffi.requests.errors import RequestsError
except ImportError:
    Session = CurlHttpVersion = RequestsError = None

from requests.structures import CaseInsensitiveDict

from tider import Request, Response
from tider.utils.time import preferred_clock
from tider.utils.log import get_logger
from tider.utils.misc import try_import
from tider.utils.network import extract_cookies_to_jar, guess_encoding_from_headers

logger = get_logger(__name__)


def stream(self, chunk_size=None, **_):
    """
    iterate streaming content chunk by chunk in bytes.
    """
    if chunk_size:
        warnings.warn("chunk_size is ignored, there is no way to tell curl that.")
    while True:
        if getattr(self, 'queue', None) is not None:
            chunk = self.queue.get()  # type: ignore

            # re-raise the exception if something wrong happened.
            if isinstance(chunk, RequestsError):
                self.curl.reset()  # type: ignore
                raise chunk

            # end of stream.
            if chunk is None:
                self.curl.reset()  # type: ignore
                return

            yield chunk
        else:
            yield self.content
            return


class ImpersonateDownloader:

    lazy = True

    def __init__(self, trust_env=True, concurrency=None):
        self._thread = None
        gevent_monkey = try_import('gevent.monkey')
        eventlet_patcher = try_import('eventlet.patcher')
        if gevent_monkey and gevent_monkey.is_anything_patched():
            self._thread = 'gevent'
        elif eventlet_patcher and eventlet_patcher.already_patched:
            self._thread = 'eventlet'
        if self._thread:
            logger.warning("Using greenlet with ImpersonateDownloader will obviously decrease the crawling speed.")
        if concurrency and concurrency >= 50:
            logger.warning("Consider decreasing concurrency to avoid causing python core dump.")
        self.trust_env = trust_env

    @classmethod
    def from_crawler(cls, crawler):
        return cls(trust_env=crawler.settings.get('SESSION_TRUST_ENV'),
                   concurrency=crawler.concurrency,)

    def close_expired_connections(self):
        pass

    def download_request(self, request: Request, session_cookies=None, max_redirects=None, **_):
        request.proxy.connect()
        try:
            http_version = CurlHttpVersion.NONE if not request.http2 else CurlHttpVersion.V2_0
            if isinstance(request.impersonate, str):
                impersonate = request.impersonate
            else:
                impersonate = 'chrome100'
            headers = request.prepared.headers.copy()
            headers.pop('Content-Length', None)

            # Start time (approximately) of the request
            start = preferred_clock()
            with Session(thread=self._thread, trust_env=self.trust_env) as session:
                # don't use stream here to avoid core dump.
                resp = session.request(
                    method=request.method,
                    url=request.url,
                    headers=headers,
                    cookies=request.prepared.cookies,
                    data=b"" if not request.body else request.body.replace(b" ", b""),  # may cause 403.
                    auth=request.prepared.auth,
                    allow_redirects=request.allow_redirects,
                    timeout=request.timeout,
                    proxies=request.proxies,
                    verify=request.verify,
                    cert=request.cert,
                    http_version=http_version,
                    impersonate=impersonate,
                    max_redirects=max_redirects,
                )
            # Total elapsed time of the request (approximately)
            elapsed = preferred_clock() - start

            resp.stream = MethodType(stream, resp)  # hijack stream
            response = self.build_response(request, resp)
            response.elapsed = elapsed
            extract_cookies_to_jar(session_cookies, request.prepared, resp)
            if not request.stream:
                response.read()
            return response
        except RequestsError as e:
            # https://curl.se/libcurl/c/libcurl-errors.html
            request.invalidate_proxy()
            resp = getattr(e, 'response', None)
            if resp is not None:
                resp.close()
            response = Response(request)
            response.fail(error=e)
            return response
        finally:
            request.proxy.disconnect()

    def build_response(self, request, resp):
        response = Response(request)

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = getattr(resp, "status_code", None)

        # Make headers case-insensitive.
        response.headers = CaseInsensitiveDict(getattr(resp, "headers", {}))

        # Set encoding.
        response.encoding = guess_encoding_from_headers(response.headers)
        if isinstance(resp.http_version, int) and resp.http_version == 11:
            response.version = "HTTP/1.1"
        elif isinstance(resp.http_version, int) and resp.http_version == 2:
            response.version = "HTTP/2"
        elif resp.http_version:
            response.version = resp.http_version
        else:
            response.version = "HTTP/?"
        response.raw = resp
        response.reason = response.raw.reason
        response.url = request.url

        # Add new cookies from the server.
        extract_cookies_to_jar(response.cookies, request.prepared, resp)

        # response.downloader = self
        return response

    def close(self):
        pass
