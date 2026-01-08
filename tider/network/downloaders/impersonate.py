import sys
import warnings
from types import MethodType

try:
    from curl_cffi.requests import Session
    from curl_cffi.const import CurlHttpVersion
    from curl_cffi.curl import CurlError
except ImportError:
    Session = CurlHttpVersion = CurlError = None

from tider import Request, Response
from tider.utils.time import preferred_clock
from tider.utils.log import get_logger
from tider.exceptions import ImproperlyConfigured

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
            if isinstance(chunk, CurlError):
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

    def __init__(self, concurrency=None):
        self._thread = None
        try:
            import gevent.monkey
            if gevent.monkey.is_anything_patched():
                self._thread = 'gevent'
        except ImportError:
            try:
                import eventlet.patcher
                if eventlet.patcher.already_patched:
                    self._thread = 'eventlet'
            except ImportError:
                pass
        if self._thread:
            logger.warning("Using greenlet with ImpersonateDownloader will obviously decrease the crawling speed.")
        if concurrency and concurrency >= 50:
            logger.warning("Consider decreasing concurrency to avoid causing python core dump.")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(concurrency=crawler.concurrency)

    def close_expired_connections(self):
        pass

    def download_request(self, request: Request, trust_env=True):
        if not Session:
            raise ImproperlyConfigured(
                'You need to install the curl_cffi library to use impersonate downloader.'
            )

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

            session_kwargs = {}
            if sys.version_info > (3, 8):
                session_kwargs.update(cert=request.cert)
            # don't keep session to avoid stuck in greenlet.
            with Session(thread=self._thread, trust_env=trust_env) as session:
                # don't use stream here to avoid core dump.
                resp = session.request(
                    method=request.method,
                    url=request.url,
                    headers=headers,
                    cookies=request.prepared.cookies,
                    data=b"" if not request.body else request.body,
                    auth=request.prepared.auth,
                    allow_redirects=request.allow_redirects,
                    timeout=request.timeout,
                    proxies=request.proxies,
                    verify=request.verify,
                    http_version=http_version,
                    impersonate=impersonate,
                    max_redirects=None,
                    **session_kwargs,
                )
            # Total elapsed time of the request (approximately)
            elapsed = preferred_clock() - start

            resp.stream = MethodType(stream, resp)  # hijack stream
            response = Response.from_origin_resp(resp=resp, request=request)
            response.elapsed = elapsed
            if not request.stream:
                response.read()
            return response
        except CurlError as e:
            # https://curl.se/libcurl/c/libcurl-errors.html
            request.invalidate_proxy()
            resp = getattr(e, 'response', None)
            if resp is not None:
                try:
                    resp.close()
                except AttributeError:
                    pass
            response = Response(request)
            if not response.failed:  # maybe already failed in response.read().
                response.fail(error=e)
            return response

    def close(self):
        pass
