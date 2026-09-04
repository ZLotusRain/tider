import os
import certifi
import logging
import urllib3
import weakref
from base64 import b64encode
from threading import RLock
from typing import Dict, Any
from urllib.parse import ParseResult, urlparse

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
from requests.utils import (
    extract_zipped_paths,
    get_environ_proxies,
    get_netrc_auth,
    rewind_body,
    should_bypass_proxies,
    urldefragauth,
)
from requests.cookies import merge_cookies

from tider import Request
from tider.local import try_import
from tider.network import Response, Proxy
from tider.network.prepared import PreparedRequest
from tider.network.poolmanager import PoolManager, SOCKSProxyManager, proxy_from_url
from tider.exceptions import (
    DownloadError,
    ConnectionError,
    ConnectTimeout,
    InvalidHeader,
    InvalidProxyURL,
    InvalidURL,
    ProtocolError,
    InvalidProxy,
    ReadTimeout,
    RetryError,
    SSLError,
    TooManyRedirects,
)
from tider.utils.time import preferred_clock
from tider.utils.cookies import copy_cookie_jar
from tider.utils.url import get_auth_from_url, resolve_redirect_url

urllib3.disable_warnings()
logging.getLogger('urllib3').setLevel(logging.ERROR)

# you can change the definition of where() to return a separately
# packaged CA bundle.
DEFAULT_CA_BUNDLE_PATH = certifi.where()
DEFAULT_POOLBLOCK = False
DEFAULT_POOLSIZE = 10
DEFAULT_RETRIES = 0
DEFAULT_POOL_TIMEOUT = None

_socks = try_import('socks')

URLLIB3_SUPPORTS_HTTPS = tuple(map(int, urllib3.__version__.split('.')[:2])) >= (1, 26)

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


def _is_socks_proxy_endpoint_error(error):
    if _socks is None:
        return False
    # Only endpoint connect/auth failures prove the SOCKS proxy is unusable.
    cause = error.__cause__ or error.__context__
    return isinstance(cause, (_socks.ProxyConnectionError, _socks.SOCKS5AuthError))


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

    def _redirect_url(self, url, location, http2=False):
        return resolve_redirect_url(url, location, http2=http2)

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

    @staticmethod
    def _release_redirect_response(response):
        """Drain an intermediate response without closing a reusable socket."""
        raw, response.raw = response.raw, None
        response.request = None
        if raw is None:
            return
        try:
            raw.drain_conn()
        finally:
            raw.release_conn()

    def resolve_redirects(self, request: Request, prepared: PreparedRequest,
                          allow_redirects=True, max_redirects=None,
                          session_cookies=None, http2=False, timeout=None,
                          verify=True, cert=None, proxy=None, trust_env=True):
        """Return the final Tider response after following HTTP redirects."""
        redirect_times = 0
        while True:
            raw = self.send(
                prepared,
                http2=http2,
                timeout=timeout,
                verify=verify,
                cert=cert,
                proxy=proxy,
            )
            try:
                response = Response.from_origin_resp(resp=raw, prepared=prepared, request=request)
            except BaseException:
                try:
                    raw.close()
                finally:
                    raw.release_conn()
                raise

            if not allow_redirects:
                response.request = request
                return response
            try:
                location = (
                    response.redirect_target
                    if response.headers.get("Location") else None
                )
            except BaseException:
                response.close()
                raise
            if not location:
                response.request = request
                return response

            # Release the connection back into the pool, so we may reuse it later.
            self._release_redirect_response(response)

            redirect_times += 1
            if max_redirects is not None and redirect_times >= max_redirects:
                raise TooManyRedirects(f"Exceeded {max_redirects} redirects.")

            method = self._redirect_method(prepared.method, status=response.status_code)
            headers, body = self._redirect_headers_and_body(prepared, location, new_method=method, status=response.status_code)
            proxy = self._redirect_proxies(location, headers, proxy, trust_env=trust_env)
            auth = self._redirect_auth(location, trust_env=trust_env)

            cookies = copy_cookie_jar(prepared.cookies)
            merge_cookies(cookies, response.cookies)
            if session_cookies is not None:
                merge_cookies(session_cookies, response.cookies)
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


class _ProxyManagerState:
    """Track one proxy manager generation and its weak Proxy owners."""

    def __init__(self, manager):
        self.manager = manager
        self.refs = set()
        self.lock = RLock()

    def add(self, proxy_ref):
        with self.lock:
            if self.manager is None:
                return None
            if proxy_ref in self.refs:
                return False
            self.refs.add(proxy_ref)
            return True

    def discard(self, proxy_ref):
        with self.lock:
            if self.manager is None:
                return None
            self.refs.discard(proxy_ref)
            if self.refs:
                return None
            manager, self.manager = self.manager, None
            return manager

    def snapshot(self):
        with self.lock:
            return tuple(self.refs)

    def get_manager(self):
        with self.lock:
            return self.manager

    def close(self):
        with self.lock:
            if self.manager is None:
                return None
            self.refs.clear()
            manager, self.manager = self.manager, None
            return manager


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
        # A urllib3 manager is shared by Proxy objects that resolve to the
        # same endpoint and cleared after the final Proxy owner is collected.
        self._manager_states = weakref.WeakValueDictionary()
        self._manager_lock = RLock()

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
        # urllib3 releases before 1.26 cannot establish TLS to an HTTPS
        # proxy. Keep an explicit HTTPS proxy intact on supported releases.
        if not URLLIB3_SUPPORTS_HTTPS and selected_proxy.lower().startswith('https://'):
            selected_proxy = 'http://' + selected_proxy[len('https://'):]

        # Share a manager by proxy URL and clear it after the final Proxy
        # owner is collected.
        manager_key = selected_proxy
        with self._manager_lock:
            state = self._manager_states.get(manager_key)
            proxy_ref = proxy.weak()
            owner_added = state.add(proxy_ref) if state is not None else None
            manager = state.get_manager() if owner_added is not None else None
            if manager is None or owner_added is None:
                manager = self._build_proxy_manager(selected_proxy, **proxy_kwargs)
                state = _ProxyManagerState(manager)
                self._manager_states[manager_key] = state
                owner_added = state.add(proxy_ref)

            # Register one finalizer for each Proxy owner of this manager.
            # A fresh weakref compares equal to the existing one while the
            # Proxy is alive, so repeated lookups don't register another callback.
            if owner_added:
                weakref.finalize(proxy, self._on_proxy_collected, state, proxy_ref)
        return manager

    def _build_proxy_manager(self, selected_proxy, **proxy_kwargs):
        username, password = get_auth_from_url(selected_proxy)
        if selected_proxy.lower().startswith("socks"):
            return SOCKSProxyManager(
                selected_proxy,
                username=username,
                password=password,
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                block=self._pool_block,
                **proxy_kwargs,
            )
        proxy_headers = {}
        if username:
            proxy_headers["Proxy-Authorization"] = _basic_auth_str(username, password)
        return proxy_from_url(
            selected_proxy,
            proxy_headers=proxy_headers,
            num_pools=self._pool_connections,
            maxsize=self._pool_maxsize,
            block=self._pool_block,
            **proxy_kwargs,
        )

    def get_connection(self, url, proxy=None, http2=False, verify=False, cert=None):
        """Returns a urllib3 connection pool for the given URL."""
        selected_proxy = proxy.select_proxy(url)
        http2_pool_kwargs = {'http2': http2, 'verify': verify, 'cert': cert}
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
        with self._manager_lock:
            states = list(self._manager_states.values())
            self._manager_states.clear()
        for state in states:
            manager = state.close()
            if manager is not None:
                manager.clear()

    @staticmethod
    def _on_proxy_collected(state, proxy_ref):
        # Do not acquire the downloader lock from a finalizer. The state
        # selects the last owner, then manager.clear() runs outside both locks.
        manager = state.discard(proxy_ref)
        if manager is not None:
            manager.clear()

    def _clear_invalid_proxy(self):
        with self._manager_lock:
            state_snapshots = [
                (selected_proxy, state, state.snapshot())
                for selected_proxy, state in tuple(self._manager_states.items())
            ]

        # Proxy.valid takes the Proxy lock. Do not nest it under either proxy
        # manager lock; another native thread may be releasing the same owner.
        invalid_states = [
            (selected_proxy, state, tuple(ref for ref in owner_refs if not self._owner_is_valid(ref)))
            for selected_proxy, state, owner_refs in state_snapshots
        ]

        managers_to_clear = []
        for _, state, invalid_refs in invalid_states:
            for proxy_ref in invalid_refs:
                manager = state.discard(proxy_ref)
                if manager is not None:
                    managers_to_clear.append(manager)

        with self._manager_lock:
            for selected_proxy, state, _ in invalid_states:
                if state.get_manager() is None and self._manager_states.get(selected_proxy) is state:
                    self._manager_states.pop(selected_proxy, None)
            active_managers = [state.get_manager() for state in tuple(self._manager_states.values())]
        for manager in managers_to_clear:
            manager.clear()
        for manager in active_managers:
            if manager is not None:
                manager.close_expired_connections()

    @staticmethod
    def _owner_is_valid(proxy_ref):
        proxy = proxy_ref()
        return proxy is not None and not proxy.can_discard()

    def close_expired_connections(self):
        self._clear_invalid_proxy()
        self.poolmanager.close_expired_connections()

    def request_url(self, prepared, proxy, http2=False):
        """Obtain the url to use when making the final request.

        If the message is being sent through a HTTP proxy, the full URL has to
        be used. Otherwise, we should only use the path portion of the URL.
        """
        if http2:
            return prepared.url

        proxy = proxy.select_proxy(prepared.url)
        scheme = urlparse(prepared.url).scheme

        is_proxied_http_request = proxy and scheme != "https"
        using_socks_proxy = False
        if proxy:
            proxy_scheme = urlparse(proxy).scheme.lower()
            using_socks_proxy = proxy_scheme.startswith("socks")

        url = prepared.path_url
        if url.startswith("//"):  # Don't confuse urllib3
            url = f"/{url.lstrip('/')}"

        if is_proxied_http_request and not using_socks_proxy:
            url = urldefragauth(prepared.url)

        return url

    def download_request(self, request: Request, prepared, trust_env=True, max_redirects=None):
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
            response = self.resolve_redirects(
                request,
                prepared,
                allow_redirects=request.allow_redirects,
                max_redirects=max_redirects,
                session_cookies=request.session_cookies,
                http2=request.http2,
                timeout=timeout, proxy=request.proxy,
                verify=request.verify, cert=request.cert, trust_env=trust_env
            )
            # Total elapsed time of the request (approximately)
            elapsed = preferred_clock() - start

            response.elapsed = elapsed
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

    def send(self, prepared, http2=False, timeout=None, verify=True, cert=None, proxy=None):
        """Sends PreparedRequest object. Returns Response-like object."""

        # don't connect proxy here to avoid invalidating disposable proxy when redirecting.
        # invalidated proxy here may not be the same as the logical request's proxy.
        try:
            conn = self.get_connection(prepared.url, proxy=proxy, http2=http2, verify=verify, cert=cert)
        except LocationValueError as e:
            raise InvalidURL(e)

        chunked = not http2 and not (prepared.body is None or "Content-Length" in prepared.headers)

        try:
            url = self.request_url(prepared, proxy, http2=http2)
            # if connection obtained but error occurs,
            # the connection will be discarded and put back to the pool.
            resp = conn.urlopen(
                method=prepared.method,
                url=url,
                body=prepared.body,
                headers=prepared.headers,
                redirect=False,
                assert_same_host=False,
                preload_content=False,  # don't release the connection back to the pool.
                release_conn=False,
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
            if isinstance(e.reason, NewConnectionError):
                selected_proxy = proxy.select_proxy(prepared.url)
                proxy_scheme = urlparse(selected_proxy).scheme.lower() if selected_proxy else None
                invalid_proxy = proxy_scheme in ("http", "https")
                if proxy_scheme and proxy_scheme.startswith("socks"):
                    invalid_proxy = _is_socks_proxy_endpoint_error(e.reason)
                if invalid_proxy:
                    proxy.invalidate()
                    raise InvalidProxy(e)
                raise ConnectionError(e)

            if isinstance(e.reason, ConnectTimeoutError):
                raise ConnectTimeout(e)

            if isinstance(e.reason, ResponseError):
                raise RetryError(e)

            if isinstance(e.reason, _ProxyError):
                proxy.invalidate()
                raise InvalidProxy(e)

            if isinstance(e.reason, _SSLError):
                # This branch is for urllib3 v1.22 and later.
                raise SSLError(e)

            raise ConnectionError(e)

        except ClosedPoolError as e:
            raise ConnectionError(e)

        except _ProxyError as e:
            proxy.invalidate()
            raise InvalidProxy(e)

        except (_SSLError, _HTTPError) as e:
            if isinstance(e, _SSLError):
                # This branch is for urllib3 versions earlier than v1.22
                raise SSLError(e)
            elif isinstance(e, ReadTimeoutError):
                raise ReadTimeout(e)
            elif isinstance(e, _InvalidHeader):
                raise InvalidHeader(e)
            else:
                raise DownloadError(e)
        return resp
