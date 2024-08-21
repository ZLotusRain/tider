import typing
import urllib3
import httpcore
from http import HTTPStatus
from urllib3.util import parse_url
from urllib3.util.timeout import Timeout

from requests.structures import CaseInsensitiveDict
from requests.utils import (
    get_encoding_from_headers,
    select_proxy,
    prepend_scheme_if_needed
)
from requests.exceptions import InvalidProxyURL
from requests.cookies import extract_cookies_to_jar
from requests.adapters import BaseAdapter, HTTPAdapter as _HTTPAdapter

from tider.network import Response
from tider.utils.url import parse_url_host
from tider.session.http2 import PoolManager, ProxyManager
from tider.session.compat import MockHTTPResponse, map_httpcore_exceptions

urllib3.disable_warnings()


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


def _get_reason_phrase(status_code):
    for status in HTTPStatus:
        if int(status_code) == status:
            return status.phrase
    return ""


class ProxyManagerMixin:

    def evict_manager_for(self, proxy):
        manager = self.proxy_manager.pop(proxy, None)
        if manager is not None:
            manager.clear()


class HTTPAdapter(_HTTPAdapter, ProxyManagerMixin):

    def build_response(self, req, resp):
        response = Response()

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = getattr(resp, "status", None)

        # Make headers case-insensitive.
        response.headers = CaseInsensitiveDict(getattr(resp, "headers", {}))

        # Set encoding.
        response.encoding = _encoding_from_content_type(response.headers)
        if resp.version:
            response.version = f"HTTP/{float(resp.version / 10)}"
        else:
            response.version = "HTTP/?"
        response.raw = resp
        response.reason = response.raw.reason

        if isinstance(req.url, bytes):
            response.url = req.url.decode("utf-8")
        else:
            response.url = req.url

        # Add new cookies from the server.
        extract_cookies_to_jar(response.cookies, req, resp)

        return response


class HTTP2Adapter(BaseAdapter, ProxyManagerMixin):

    def __init__(self, num_pools=10, pool_connections=100, pool_maxsize=100,
                 keepalive_expiry=10, max_retries=0):
        super().__init__()
        self._num_pools = num_pools
        self._pool_connections = pool_connections
        self._pool_maxsize = pool_maxsize
        self._keepalive_expiry = keepalive_expiry
        self._max_retries = max_retries

        self._context = {
            'max_connections': pool_maxsize,
            'max_keepalive_connections': pool_connections,
            'keepalive_expiry': keepalive_expiry,
            'retries': max_retries
        }

        self.poolmanager = PoolManager(num_pools=num_pools, **self._context)
        self.proxy_manager = {}

    def proxy_manager_for(self, proxy):
        if proxy in self.proxy_manager:
            manager = self.proxy_manager[proxy]
        elif proxy.lower().startswith("http"):
            manager = self.proxy_manager[proxy] = ProxyManager(
                proxy,
                num_pools=self._num_pools,
                **self._context,
            )
        else:
            raise ValueError('socks proxy is not supported for now.')

        return manager

    def get_connection(self, url, proxies=None, verify=False, cert=None, trust_env=True):
        proxy = select_proxy(url, proxies)
        context = dict(verify=verify, cert=cert, trust_env=trust_env, **self._context)
        if proxy:
            proxy = prepend_scheme_if_needed(proxy, "http")
            proxy_url = parse_url(proxy)
            if not proxy_url.host:
                raise InvalidProxyURL(
                    "Please check proxy URL. It is malformed "
                    "and could be missing the host."
                )
            proxy_manager = self.proxy_manager_for(proxy)
            conn = proxy_manager.connection_from_context(context)
        else:
            conn = self.poolmanager.connection_from_context(context)

        return conn

    def send(self, request, stream=False, timeout=None,
             verify=True, cert=None, proxies=None, trust_env=True):

        if "Host" not in request.headers:
            request.headers["Host"] = parse_url_host(request.url)
        has_content_length = (
            "Content-Length" in request.headers or "Transfer-Encoding" in request.headers
        )
        if not has_content_length and request.method in ("POST", "PUT", "PATCH"):
            request.headers["Content-Length"] = "0"
        if isinstance(request.body, str):
            request.body = request.body.encode('utf-8')

        if isinstance(timeout, tuple):
            try:
                connect, read = timeout
                timeout = Timeout(connect=connect, read=read)
            except ValueError:
                raise ValueError(
                    f"Invalid timeout {timeout}. Pass a (connect, read) timeout tuple, "
                    f"or a single float to set both timeouts to the same value."
                )
        elif isinstance(timeout, Timeout):
            pass
        else:
            timeout = Timeout(connect=timeout, read=timeout)
        extensions = dict(timeout={'connect': timeout.connect_timeout,
                                   'read': timeout.read_timeout})

        req = httpcore.Request(
            method=request.method,
            url=request.url,
            headers=request.headers,
            content=request.body,
            extensions=extensions
        )
        conn = self.get_connection(request.url, proxies, verify, cert, trust_env)

        with map_httpcore_exceptions():
            resp = conn.handle_request(req)

        assert isinstance(resp.stream, typing.Iterable)

        return self.build_response(request, resp)

    @staticmethod
    def _normalize_headers(headers):
        h = []
        for k, v in headers:
            if not isinstance(k, bytes):
                k = k.encode("ascii")
            if not isinstance(v, bytes):
                v = v.encode("ascii")
            h.append((k, v))
        return h

    def decode_headers(self, headers):
        h = {}
        headers = self._normalize_headers(headers)
        for encoding in ["ascii", "utf-8", "iso-8859-1"]:
            for key, value in headers:
                try:
                    k = key.decode(encoding)
                    v = value.decode(encoding)
                    if k in h:
                        h[k] += f", {v}"
                    else:
                        h[k] = v
                except UnicodeDecodeError:
                    break
            else:
                break
        return h

    def build_response(self, req, resp):
        response = Response()

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = getattr(resp, "status", None)

        # Make headers case-insensitive.
        headers = self.decode_headers(getattr(resp, "headers", {}))
        response.headers = CaseInsensitiveDict(headers)

        # Set encoding.
        response.encoding = _encoding_from_content_type(response.headers)
        try:
            http_version = resp.extensions["http_version"]
        except KeyError:
            http_version = "HTTP/1.1"
        else:
            http_version = http_version.decode("ascii", errors="ignore")
        response.version = http_version
        try:
            response.reason = resp.extensions["reason_phrase"]
        except KeyError:
            response.reason = _get_reason_phrase(response.status_code)
        else:
            response.reason = response.reason.decode("ascii", errors="ignore")

        response.raw = MockHTTPResponse(resp.stream, headers=response.headers)

        if isinstance(req.url, bytes):
            response.url = req.url.decode("utf-8")
        else:
            response.url = req.url

        # Add new cookies from the server.
        extract_cookies_to_jar(response.cookies, req, response.raw)

        return response

    def close(self):
        self.poolmanager.clear()
        for proxy in self.proxy_manager.values():
            proxy.clear()
