import os
import certifi
import logging
import urllib3
import weakref
from base64 import b64encode
from datetime import timedelta
from typing import Dict, Any
from urllib.parse import ParseResult, urlparse, urljoin

from urllib3.exceptions import ClosedPoolError, ConnectTimeoutError
from urllib3.exceptions import HTTPError as _HTTPError
from urllib3.exceptions import InvalidHeader as _InvalidHeader
from urllib3.exceptions import (
    LocationValueError,
    MaxRetryError,
    NewConnectionError,
    ProtocolError as _ProtocolError,
)
from urllib3.exceptions import ProxyError as _ProxyError
from urllib3.exceptions import ReadTimeoutError, ResponseError
from urllib3.exceptions import SSLError as _SSLError
from urllib3.util import parse_url, Retry, Timeout as TimeoutSauce
from urllib3.util.ssl_ import create_urllib3_context

from requests.status_codes import codes
from requests.structures import CaseInsensitiveDict
from requests.utils import (
    extract_zipped_paths,
    get_environ_proxies,
    get_netrc_auth,
    requote_uri,
    rewind_body,
    should_bypass_proxies,
    urldefragauth,
)
from requests.cookies import merge_cookies

from tider import Request
from tider.network import Response, Proxy
from tider.network.request import PreparedRequest
from tider.network.poolmanager import PoolManager, proxy_from_url
from tider.exceptions import (
    DownloadError,
    ConnectionError,
    ConnectTimeout,
    InvalidHeader,
    InvalidProxyURL,
    InvalidSchema,
    InvalidURL,
    ProtocolError,
    ProxyError,
    ReadTimeout,
    RetryError,
    SSLError,
    TooManyRedirects,
)
from tider.utils.time import preferred_clock
from tider.utils.text import enforce_str
from tider.utils.network import copy_cookie_jar, extract_cookies_to_jar, guess_encoding_from_headers
from tider.utils.url import get_auth_from_url

try:
    from urllib3.contrib.socks import SOCKSProxyManager
except ImportError:

    def SOCKSProxyManager(*args, **kwargs):
        raise InvalidSchema("Missing dependencies for SOCKS support.")

urllib3.disable_warnings()
logging.getLogger('urllib3').setLevel(logging.ERROR)

# you can change the definition of where() to return a separately
# packaged CA bundle.
DEFAULT_CA_BUNDLE_PATH = certifi.where()
DEFAULT_POOLBLOCK = False
DEFAULT_POOLSIZE = 10
DEFAULT_RETRIES = 0
DEFAULT_POOL_TIMEOUT = None

try:
    import ssl

    _preloaded_ssl_context = create_urllib3_context()  # ssl version is set to None by default.
    _preloaded_ssl_context.load_verify_locations(
        extract_zipped_paths(DEFAULT_CA_BUNDLE_PATH)
    )
except ImportError:
    # Bypass default SSLContext creation when Python
    # interpreter isn't built with the ssl module.
    _preloaded_ssl_context = None


def _port_or_default(parsed: ParseResult):
    if parsed.port is not None:
        return parsed.port
    return {"http": 80, "https": 443}.get(parsed.scheme)


def _same_origin(url: ParseResult, other: ParseResult):
    return (
        url.scheme == other.scheme
        and url.hostname == other.hostname
        and _port_or_default(url) == _port_or_default(other)
    )


def _is_https_redirect(url: ParseResult, location: ParseResult):
    if url.hostname != location.hostname:
        return False

    return (
        url.scheme == "http"
        and _port_or_default(url) == 80
        and location.scheme == "https"
        and _port_or_default(location) == 443
    )


def _basic_auth_str(username, password):
    if isinstance(username, str):
        username = username.encode("latin1")

    if isinstance(password, str):
        password = password.encode("latin1")

    authstr = "Basic " + b64encode(b":".join((username, password))).strip().decode('ascii')
    return authstr


def _urllib3_request_context(url, verify, client_cert, poolmanager) -> (Dict[str, Any], Dict[str, Any]):
    parsed_request_url = urlparse(url)
    scheme = parsed_request_url.scheme.lower()
    port = parsed_request_url.port
    host_params = {
        "scheme": scheme,
        "host": parsed_request_url.hostname,
        "port": port,
    }

    poolmanager_kwargs = getattr(poolmanager, "connection_pool_kw", {})
    has_poolmanager_ssl_context = poolmanager_kwargs.get("ssl_context")
    should_use_default_ssl_context = (
            _preloaded_ssl_context is not None and not has_poolmanager_ssl_context
    )

    pool_kwargs = {}
    cert_reqs = "CERT_NONE"
    if scheme.startswith("https") and verify:
        cert_reqs = "CERT_REQUIRED"
        if should_use_default_ssl_context:
            pool_kwargs["ssl_context"] = _preloaded_ssl_context
        elif isinstance(verify, str):
            if not os.path.isdir(verify):
                pool_kwargs["ca_certs"] = verify
            else:
                pool_kwargs['ca_cert_dir'] = verify
    pool_kwargs["cert_reqs"] = cert_reqs
    if client_cert is not None:
        if isinstance(client_cert, tuple) and len(client_cert) == 2:
            pool_kwargs["cert_file"] = client_cert[0]
            pool_kwargs["key_file"] = client_cert[1]
        else:
            # According to our docs, we allow users to specify just the client
            # cert path
            pool_kwargs["cert_file"] = client_cert

    return host_params, pool_kwargs


class RedirectMixin:

    def _redirect_method(self, method, status):
        # https://tools.ietf.org/html/rfc7231#section-6.4.4
        if status == codes.see_other and method != "HEAD":
            method = "GET"

        # Do what the browsers do, despite standards...
        # First, turn 302s into GETs.
        if status == codes.found and method != "HEAD":
            method = "GET"

        # Second, if a POST is responded to with a 301, turn it into a GET.
        # This bizarre behaviour is explained in Issue 1704.
        if status == codes.moved and method == "POST":
            method = "GET"
        return method

    def _redirect_url(self, url, location, http2=False):
        if not http2:
            # Currently the underlying http module on py3 decode headers
            # in latin1, but empirical evidence suggests that latin1 is very
            # rarely used with non-ASCII characters in HTTP headers.
            # It is more likely to get UTF8 header rather than latin1.
            # This causes incorrect handling of UTF8 encoded location headers.
            # To solve this, we re-encode the location in latin1.
            location = location.encode("latin1").decode('utf8')

        # Handle redirection without scheme (see: RFC 1808 Section 4)
        parsed_old_url = urlparse(url)
        if location.startswith("//"):
            location = ":".join([enforce_str(parsed_old_url.scheme), location])

        previous_fragment = parsed_old_url.fragment
        parsed_location = urlparse(location)
        if parsed_location.fragment == "" and previous_fragment:
            parsed_location = parsed_location._replace(fragment=previous_fragment)
        if parsed_location.scheme and not parsed_location.hostname:
            # https://github.com/encode/httpx/issues/771
            parsed_location = parsed_location._replace(netloc=parsed_old_url.hostname + parsed_location.netloc)

        location = parsed_location.geturl()

        # Facilitate relative 'location' headers, as allowed by RFC 7231.
        # (e.g. '/path/to/resource' instead of 'http://domain.tld/path/to/resource')
        # Compliant with RFC3986, we percent encode the url.
        if not parsed_location.netloc:
            location = urljoin(url, requote_uri(location))
        else:
            location = requote_uri(location)

        return enforce_str(location)

    def _redirect_headers_and_body(self, prepared, location, new_method, status):
        headers = prepared.headers.copy()
        body = prepared.body

        parsed_old_url = urlparse(prepared.url)
        parsed_location = urlparse(location)
        if not _same_origin(parsed_location, parsed_old_url):
            if not _is_https_redirect(parsed_old_url, parsed_location):
                # Strip Authorization headers when responses are redirected
                # away from the origin. (Except for direct HTTP to HTTPS redirects.)
                headers.pop("Authorization", None)
            # Update the Host header.
            headers["Host"] = parsed_location.netloc

        # https://github.com/psf/requests/issues/1084
        if status not in (
            codes.temporary_redirect,
            codes.permanent_redirect,
        ):
            # https://github.com/psf/requests/issues/3490
            purged_headers = ("Content-Length", "Content-Type", "Transfer-Encoding")
            for header in purged_headers:
                headers.pop(header, None)
            body = None

        if new_method != prepared.method and new_method == "GET":
            # If we've switched to a 'GET' request, then strip any headers which
            # are only relevant to the request body.
            headers.pop("Content-Length", None)
            headers.pop("Transfer-Encoding", None)
            body = None

        # We should use the client cookie store to determine any cookie header,
        # rather than whatever was on the original outgoing request.
        headers.pop("Cookie", None)
        return headers, body

    def _redirect_proxies(self, location, headers, proxy, trust_env=True):
        scheme = urlparse(location).scheme

        new_proxies = dict(proxy.proxies or {})
        no_proxy = new_proxies.get("no_proxy")

        if trust_env and not should_bypass_proxies(location, no_proxy=no_proxy):
            environ_proxies = get_environ_proxies(location, no_proxy=no_proxy)
            env_proxy = environ_proxies.get(scheme, environ_proxies.get("all"))
            if env_proxy:
                new_proxies.setdefault(scheme, env_proxy)

        if "Proxy-Authorization" in headers:
            del headers["Proxy-Authorization"]

        try:
            username, password = get_auth_from_url(new_proxies[scheme])
        except KeyError:
            username, password = None, None

        # urllib3 handles proxy authorization for us in the standard adapter.
        # Avoid appending this to TLS tunneled requests where it may be leaked.
        if not scheme.startswith('https') and username and password:
            headers["Proxy-Authorization"] = _basic_auth_str(username, password)

        new_proxy = Proxy(new_proxies)
        if new_proxy.select_proxy(location) != proxy.select_proxy(location):
            # use env_proxy instead and this won't update request's proxy.
            return new_proxy
        return proxy

    def _redirect_auth(self, location, trust_env=True):
        # .netrc might have more auth for us on our new host.
        new_auth = get_netrc_auth(location) if trust_env else None
        return new_auth

    def resolve_redirects(self, prepared: PreparedRequest, allow_redirects=True, max_redirects=None, session_cookies=None,
                          http2=False, timeout=None, verify=True, cert=None, proxy=None, trust_env=True):
        raw = self.send(prepared, http2=http2, timeout=timeout, verify=verify, cert=cert, proxy=proxy)
        if not allow_redirects:
            return raw

        location = raw.get_redirect_location()
        redirect_times = 0
        while location:
            # Release the connection back into the pool, so we may reuse it later.
            raw.drain_conn()

            redirect_times += 1
            if max_redirects is not None and redirect_times >= max_redirects:
                raise TooManyRedirects(f"Exceeded {max_redirects} redirects.")

            method = self._redirect_method(prepared.method, status=raw.status)
            location = self._redirect_url(prepared.url, location, http2=http2)
            headers, body = self._redirect_headers_and_body(prepared, location, new_method=method, status=raw.status)
            proxy = self._redirect_proxies(location, headers, proxy, trust_env=trust_env)
            auth = self._redirect_auth(location, trust_env=trust_env)

            cookies = copy_cookie_jar(prepared.cookies)
            extract_cookies_to_jar(cookies, prepared, raw)
            if session_cookies:
                merge_cookies(cookies, session_cookies)
            prepared = prepared.copy_with(
                method=method, url=location, headers=headers, auth=auth, cookies=cookies, body=body)

            # A failed tell() sets `_body_position` to `object()`. This non-None
            # value ensures `rewindable` will be True, allowing us to raise an
            # UnrewindableBodyError, instead of hanging the connection.
            rewindable = prepared._body_position is not None and (
                    "Content-Length" in headers or "Transfer-Encoding" in headers
            )

            # Attempt to rewind consumed file-like object.
            if rewindable:
                rewind_body(prepared)

            raw = self.send(
                prepared,
                http2=http2,
                timeout=timeout,
                verify=verify,
                cert=cert,
                proxy=proxy,
            )
            if session_cookies:
                extract_cookies_to_jar(session_cookies, prepared, raw)

            # extract redirect url, if any, for the next loop
            location = raw.get_redirect_location()
        return raw


class HTTPDownloader(RedirectMixin):

    lazy = True

    def __init__(
        self,
        pool_connections=DEFAULT_POOLSIZE,
        pool_maxsize=DEFAULT_POOLSIZE,
        max_retries=DEFAULT_RETRIES,
        pool_block=DEFAULT_POOLBLOCK,
    ):
        """
        :param pool_connections: The number of connection pools to cache.
        :param pool_maxsize: The maximum number of connections to save in the pool.
        :param pool_block: Block when no free connections are available.
        """
        if max_retries == DEFAULT_RETRIES:
            self.max_retries = Retry(0, read=False)
        else:
            self.max_retries = Retry.from_int(max_retries)
        self.proxy_manager = {}

        self._pool_connections = pool_connections
        self._pool_maxsize = pool_maxsize
        self._pool_block = pool_block

        # Normally, a different origin(scheme, host, port) will create a new
        # connection pool in urllib3, so we need to use poolmanager here.
        self.poolmanager = PoolManager(num_pools=pool_connections, maxsize=pool_maxsize, block=pool_block)

    @classmethod
    def from_crawler(cls, crawler):
        concurrency = crawler.concurrency
        return cls(
            pool_connections=concurrency,
            pool_maxsize=concurrency,  # avoid flooding requests.
            max_retries=crawler.settings.get('DOWNLOADER_MAX_RETRIES'),
            pool_block=crawler.settings.getbool('DOWNLOADER_CONNECTION_POOL_BLOCK'),
        )

    def proxy_manager_for(self, proxy, selected_proxy, **proxy_kwargs):
        """Return urllib3 ProxyManager for the given proxy.

        :param proxy: (Proxy) The proxy to return a urllib3 ProxyManager for.
        :param selected_proxy:
        :param proxy_kwargs: Extra keyword arguments used to configure the Proxy Manager.
        :returns: ProxyManager
        :rtype: urllib3.ProxyManager
        """
        for key in self.proxy_manager:
            if selected_proxy == key[1]:
                manager = self.proxy_manager[key]
                return manager

        if selected_proxy.lower().startswith("socks"):
            username, password = get_auth_from_url(selected_proxy)
            manager = SOCKSProxyManager(
                selected_proxy,
                username=username,
                password=password,
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                block=self._pool_block,
                **proxy_kwargs,
            )
        else:
            # clear proxymanager once the proxy is finalized.
            proxy_key = (proxy.weak(), selected_proxy)
            proxy_headers = {}
            username, password = get_auth_from_url(selected_proxy)
            if username:
                proxy_headers["Proxy-Authorization"] = _basic_auth_str(username, password)
            manager = self.proxy_manager[proxy_key] = proxy_from_url(
                selected_proxy,
                proxy_headers=proxy_headers,
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                block=self._pool_block,
                **proxy_kwargs,
            )
            weakref.finalize(proxy, self._on_proxy_collected, proxy_key=proxy_key)

        return manager

    def build_response(self, request: Request, resp):
        response = Response(request)

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = getattr(resp, "status", None)

        # Make headers case-insensitive.
        response.headers = CaseInsensitiveDict(getattr(resp, "headers", {}))

        # Set encoding.
        response.encoding = guess_encoding_from_headers(response.headers)
        if isinstance(resp.version, int) and resp.version == 11:
            response.version = "HTTP/1.1"
        elif isinstance(resp.version, int) and resp.version == 10:
            response.version = "HTTP/1.0"
        elif resp.version:
            response.version = resp.version
        else:
            response.version = "HTTP/?"
        response.raw = resp
        response.reason = response.raw.reason
        response.url = request.url

        # Add new cookies from the server.
        extract_cookies_to_jar(response.cookies, request.prepared, resp)

        # response.downloader = self
        return response

    def get_connection(self, url, proxy=None, http2=False, verify=False, cert=None):
        """Returns a urllib3 connection pool for the given URL."""
        http2_pool_kwargs = {'http2': http2, 'verify': verify, 'cert': cert}
        selected_proxy = proxy.select_proxy(url)
        if selected_proxy:
            proxy_url = parse_url(selected_proxy)
            if not proxy_url.host:
                raise InvalidProxyURL(
                    "Please check proxy URL. It is malformed "
                    "and could be missing the host."
                )
            manager = self.proxy_manager_for(proxy, selected_proxy)
        else:
            manager = self.poolmanager

        try:
            host_params, pool_kwargs = _urllib3_request_context(url, verify, cert, poolmanager=manager)
        except ValueError as e:
            raise InvalidURL(e)
        pool_kwargs.update(http2_pool_kwargs)

        # Only scheme should be lower case
        conn = manager.connection_from_host(**host_params, pool_kwargs=pool_kwargs)
        return conn

    def close(self):
        """Disposes of any internal state.

        Currently, this closes the PoolManager and any active ProxyManager,
        which closes any pooled connections.
        """
        self.poolmanager.clear()
        for proxy in self.proxy_manager.values():
            proxy.clear()

    def _on_proxy_collected(self, proxy_key):
        for key in list(iter(self.proxy_manager.keys())):
            if key[1] == proxy_key[1]:
                manager = self.proxy_manager.pop(key, None)
                manager and manager.clear()

    def _clear_invalid_proxy(self):
        for proxy_key in list(iter(self.proxy_manager.keys())):
            proxy_ref, proxy_str = proxy_key
            proxy = proxy_ref()
            if not proxy or not proxy.valid:
                # maybe timeout.
                manager = self.proxy_manager.pop(proxy_key, None)
                manager and manager.clear()
            else:
                manager = self.proxy_manager.get(proxy_key)
                manager and manager.close_expired_connections()

    def close_expired_connections(self):
        self._clear_invalid_proxy()
        self.poolmanager.close_expired_connections()

    def request_url(self, request, proxy, http2=False):
        """Obtain the url to use when making the final request.

        If the message is being sent through a HTTP proxy, the full URL has to
        be used. Otherwise, we should only use the path portion of the URL.
        """
        if http2:
            return request.url

        proxy = proxy.select_proxy(request.url)
        scheme = urlparse(request.url).scheme

        is_proxied_http_request = proxy and scheme != "https"
        using_socks_proxy = False
        if proxy:
            proxy_scheme = urlparse(proxy).scheme.lower()
            using_socks_proxy = proxy_scheme.startswith("socks")

        url = request.path_url
        if url.startswith("//"):  # Don't confuse urllib3
            url = f"/{url.lstrip('/')}"

        if is_proxied_http_request and not using_socks_proxy:
            url = urldefragauth(request.url)

        return url

    def download_request(self, request: Request, session_cookies=None, trust_env=True, max_redirects=None):
        timeout = request.timeout
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

        response = None
        try:
            # Start time (approximately) of the request
            start = preferred_clock()
            request.proxy.connect()  # raise ProxyError if proxy is invalid
            resp = self.resolve_redirects(request.prepared,
                                          allow_redirects=request.allow_redirects,
                                          max_redirects=max_redirects,
                                          http2=request.http2,
                                          timeout=timeout, proxy=request.proxy,
                                          verify=request.verify, cert=request.cert, trust_env=trust_env)
            # Total elapsed time of the request (approximately)
            elapsed = preferred_clock() - start

            response = self.build_response(request, resp)
            response.elapsed = timedelta(seconds=elapsed)
            extract_cookies_to_jar(session_cookies, request.prepared, resp)
            if not request.stream:
                response.read()
            return response
        except Exception as e:
            # DownloadError, RuntimeError, TypeError...
            if response is None:
                response = Response(request)
            if not response.failed:  # maybe already failed in response.read().
                response.fail(error=e)
            return response
        finally:
            request.proxy.disconnect()

    def send(self, request, http2=False, timeout=None, verify=True, cert=None, proxy=None):
        """Sends PreparedRequest object. Returns Response-like object."""

        # don't connect proxy here to avoid invalidating disposable proxy when redirecting.
        # invalidated proxy here may not be the same as request's proxy.
        try:
            conn = self.get_connection(request.url, proxy=proxy, http2=http2, verify=verify, cert=cert)  # connection pool
        except LocationValueError as e:
            raise InvalidURL(e)

        chunked = not http2 and not (request.body is None or "Content-Length" in request.headers)

        try:
            url = self.request_url(request, proxy, http2=http2)
            # if connection obtained but error occurs,
            # the connection will be discarded and put back to the pool.
            resp = conn.urlopen(
                method=request.method,
                url=url,
                body=request.body,
                headers=request.headers,
                redirect=False,
                assert_same_host=False,
                preload_content=False,  # don't release the connection back to the pool.
                decode_content=False,
                retries=self.max_retries,
                timeout=timeout,
                chunked=chunked,
                pool_timeout=None,
            )

        # IOError, BrokenPipeError
        except (_ProtocolError, OSError) as err:
            raise ProtocolError(err)

        except MaxRetryError as e:
            if isinstance(e.reason, ConnectTimeoutError):
                if not isinstance(e.reason, NewConnectionError):
                    proxy.invalidate()
                    raise ConnectTimeout(e)

            if isinstance(e.reason, ResponseError):
                raise RetryError(e)

            if isinstance(e.reason, _ProxyError):
                proxy.invalidate()
                raise ProxyError(e)

            if isinstance(e.reason, _SSLError):
                # This branch is for urllib3 v1.22 and later.
                proxy.invalidate()
                raise SSLError(e)

            raise ConnectionError(e)

        except ClosedPoolError as e:
            raise ConnectionError(e)

        except _ProxyError as e:
            proxy.invalidate()
            raise ProxyError(e)

        except (_SSLError, _HTTPError) as e:
            if isinstance(e, _SSLError):
                # This branch is for urllib3 versions earlier than v1.22
                proxy.invalidate()
                raise SSLError(e)
            elif isinstance(e, ReadTimeoutError):
                raise ReadTimeout(e)
            elif isinstance(e, _InvalidHeader):
                raise InvalidHeader(e)
            else:
                raise DownloadError(e)
        return resp
