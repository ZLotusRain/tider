from urllib3.response import HTTPResponse
from urllib3.util import Timeout as TimeoutSauce
from urllib3.exceptions import ClosedPoolError, ConnectTimeoutError
from urllib3.exceptions import HTTPError as _HTTPError
from urllib3.exceptions import InvalidHeader as _InvalidHeader
from urllib3.exceptions import (
    LocationValueError,
    MaxRetryError,
    NewConnectionError,
    ProtocolError,
)
from urllib3.exceptions import ProxyError as _ProxyError
from urllib3.exceptions import ReadTimeoutError, ResponseError
from urllib3.exceptions import SSLError as _SSLError

from requests import Response
from requests.cookies import extract_cookies_to_jar
from requests.structures import CaseInsensitiveDict
from requests.utils import get_encoding_from_headers
from requests.adapters import HTTPAdapter as _HTTPAdapter
from requests.exceptions import (
    ConnectionError,
    ConnectTimeout,
    InvalidHeader,
    InvalidURL,
    ProxyError,
    ReadTimeout,
    RetryError,
    SSLError,
)
from requests.sessions import Session as _Session


DEFAULT_POOL_TIMEOUT = None


def _encoding_from_content_type(headers):
    content_type = headers.get("Content-Type", "")
    charset = list(filter(lambda x: "charset" in x, content_type.split(";")))

    if not charset:
        encoding = get_encoding_from_headers(headers)
        if "text" in content_type:
            encoding = None  # maybe "ISO-8859-1" or 'UTF-8'
    else:
        encoding = charset[0].split("=")[-1]
    return encoding


class HTTPAdapter(_HTTPAdapter):

    def build_response(self, req, resp):
        """Builds a :class:`Response <requests.Response>` object from a urllib3
        response. This should not be called from user code, and is only exposed
        for use when subclassing the
        :class:`HTTPAdapter <requests.adapters.HTTPAdapter>`

        :param req: The :class:`PreparedRequest <PreparedRequest>` used to generate the response.
        :param resp: The urllib3 response object.
        :rtype: requests.Response
        """
        response = Response()

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = getattr(resp, "status", None)

        # Make headers case-insensitive.
        response.headers = CaseInsensitiveDict(getattr(resp, "headers", {}))

        # Set encoding.
        response.encoding = _encoding_from_content_type(response.headers)
        response.raw = resp
        response.reason = resp.reason

        if isinstance(req.url, bytes):
            response.url = req.url.decode("utf-8")
        else:
            response.url = req.url

        # Add new cookies from the server.
        extract_cookies_to_jar(response.cookies, req, resp)

        # Give the Response some context.
        # use connection(adapter) to manage pool and proxy
        response.request = req
        # response.connection = self

        return response

    def send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        try:
            conn = self.get_connection(request.url, proxies)
        except LocationValueError:
            raise InvalidURL(request=request)

        self.cert_verify(conn, request.url, verify, cert)
        url = self.request_url(request, proxies)
        self.add_headers(
            request,
            stream=stream,
            timeout=timeout,
            verify=verify,
            cert=cert,
            proxies=proxies,
        )

        chunked = not (request.body is None or "Content-Length" in request.headers)

        if isinstance(timeout, tuple):
            try:
                connect, read = timeout
                timeout = TimeoutSauce(connect=connect, read=read)
            except ValueError:
                raise ValueError(
                    f"Invalid timeout {timeout}. Pass a (connect, read) timeout tuple, "
                    f"or a single float to set both timeouts to the same value."
                )
        elif isinstance(timeout, TimeoutSauce):
            pass
        else:
            timeout = TimeoutSauce(connect=timeout, read=timeout)

        if not chunked:
            try:
                resp = conn.urlopen(
                    method=request.method,
                    url=url,
                    body=request.body,
                    headers=request.headers.copy(),
                    redirect=False,
                    assert_same_host=False,
                    preload_content=False,
                    decode_content=False,
                    retries=self.max_retries,
                    timeout=timeout,
                )
            except (ProtocolError, OSError) as err:
                raise ConnectionError(err, request=request)

            except MaxRetryError as e:
                if isinstance(e.reason, ConnectTimeoutError):
                    # TODO: Remove this in 3.0.0: see #2811
                    if not isinstance(e.reason, NewConnectionError):
                        raise ConnectTimeout(e, request=request)

                if isinstance(e.reason, ResponseError):
                    raise RetryError(e, request=request)

                if isinstance(e.reason, _ProxyError):
                    raise ProxyError(e, request=request)

                if isinstance(e.reason, _SSLError):
                    # This branch is for urllib3 v1.22 and later.
                    raise SSLError(e, request=request)

                raise ConnectionError(e, request=request)

            except ClosedPoolError as e:
                raise ConnectionError(e, request=request)

            except _ProxyError as e:
                raise ProxyError(e)

            except (_SSLError, _HTTPError) as e:
                if isinstance(e, _SSLError):
                    # This branch is for urllib3 versions earlier than v1.22
                    raise SSLError(e, request=request)
                elif isinstance(e, ReadTimeoutError):
                    raise ReadTimeout(e, request=request)
                elif isinstance(e, _InvalidHeader):
                    raise InvalidHeader(e, request=request)
                else:
                    raise
        else:
            if hasattr(conn, "proxy_pool"):
                conn = conn.proxy_pool

            low_conn = conn._get_conn(timeout=DEFAULT_POOL_TIMEOUT)
            try:
                skip_host = "Host" in request.headers
                low_conn.putrequest(
                    request.method,
                    url,
                    skip_accept_encoding=True,
                    skip_host=skip_host,
                )

                for header, value in request.headers.items():
                    low_conn.putheader(header, value)

                low_conn.endheaders()

                for i in request.body:
                    low_conn.send(hex(len(i))[2:].encode("utf-8"))
                    low_conn.send(b"\r\n")
                    low_conn.send(i)
                    low_conn.send(b"\r\n")
                low_conn.send(b"0\r\n\r\n")

                # Receive the response from the server
                r = low_conn.getresponse()

                resp = HTTPResponse.from_httplib(
                    r,
                    pool=conn,
                    connection=low_conn,
                    preload_content=False,
                    decode_content=False,
                )
            except Exception:
                # If we hit any problems here, clean up the connection.
                # Then, raise so that we can handle the actual exception.
                low_conn.close()
                raise

        return self.build_response(request, resp)


class Session(_Session):

    def __init__(self):
        super(Session, self).__init__()
        self.mount("https://", HTTPAdapter(pool_maxsize=10, pool_block=True))
        self.mount("http://", HTTPAdapter(pool_maxsize=10, pool_block=True))
