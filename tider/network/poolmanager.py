from __future__ import absolute_import

import collections
import functools
from urllib.parse import urljoin, urlparse

from urllib3._collections import HTTPHeaderDict
from urllib3.poolmanager import SSL_KEYWORDS, PoolManager as UPoolManager
from urllib3.exceptions import MaxRetryError, ProxySchemeUnknown

from urllib3.util.retry import Retry
from urllib3.util.url import parse_url
from urllib3.util.proxy import connection_requires_http_tunnel

from tider.utils.log import get_logger
from tider.network.compat import (
    RecentlyUsedContainer,
    HTTPConnectionPool,
    HTTPSConnectionPool,
    close_expired_connections,
)
from tider.network.http2.connectionpool import is_same_host

__all__ = ["PoolManager", "ProxyManager", "proxy_from_url"]


log = get_logger('urllib3.poolmanager')

# Default value for `blocksize` - a new parameter introduced to
# http.client.HTTPConnection & http.client.HTTPSConnection in Python 3.7
_DEFAULT_BLOCKSIZE = 16384

port_by_scheme = {"http": 80, "https": 443}

# All known keyword arguments that could be provided to the pool manager, its
# pools, or the underlying connections. This is used to construct a pool key.
_key_fields = (
    "key_scheme",  # str
    "key_host",  # str
    "key_port",  # int
    "key_timeout",  # int or float or Timeout
    "key_retries",  # int or Retry
    "key_strict",  # bool
    "key_block",  # bool
    "key_source_address",  # str
    "key_key_file",  # str
    "key_key_password",  # str
    "key_cert_file",  # str
    "key_cert_reqs",  # str
    "key_ca_certs",  # str
    "key_ca_cert_data",  # str
    "key_ssl_version",  # str
    "key_ssl_minimum_version",
    "key_ssl_maximum_version",
    "key_ca_cert_dir",  # str
    "key_ssl_context",  # instance of ssl.SSLContext or urllib3.util.ssl_.SSLContext
    "key_maxsize",  # int
    "key_headers",  # dict
    "key__proxy",  # parsed proxy url
    "key__proxy_headers",  # dict
    "key__proxy_config",  # class
    "key_socket_options",  # list of (level (int), optname (int), value (int or str)) tuples
    "key__socks_options",  # dict
    "key_assert_hostname",  # bool or string
    "key_assert_fingerprint",  # str
    "key_server_hostname",  # str
    "key_blocksize",  # int | None

    # http2 keys

    "key_http2",  # str
    'key_verify',
    'key_cert',
    'key_trust_env',
    'key_max_keepalive_connections',
    'key_keepalive_expiry',
)


#: The namedtuple class used to construct keys for the connection pool.
#: All custom key schemes should include the fields in this key at a minimum.
PoolKey = collections.namedtuple("PoolKey", _key_fields)

_proxy_config_fields = ("ssl_context", "use_forwarding_for_https")
ProxyConfig = collections.namedtuple("ProxyConfig", _proxy_config_fields)


def _default_key_normalizer(key_class, request_context):
    """
    Create a pool key out of a request context dictionary.

    According to RFC 3986, both the scheme and host are case-insensitive.
    Therefore, this function normalizes both before constructing the pool
    key for an HTTPS request. If you wish to change this behaviour, provide
    alternate callables to ``key_fn_by_scheme``.
    """
    # Since we mutate the dictionary, make a copy first
    context = request_context.copy()
    context["scheme"] = context["scheme"].lower()
    context["host"] = context["host"].lower()

    # These are both dictionaries and need to be transformed into frozensets
    for key in ("headers", "_proxy_headers", "_socks_options"):
        if key in context and context[key] is not None:
            context[key] = frozenset(context[key].items())

    # The socket_options key may be a list and needs to be transformed into a
    # tuple.
    socket_opts = context.get("socket_options")
    if socket_opts is not None:
        context["socket_options"] = tuple(socket_opts)

    # Map the kwargs to the names in the namedtuple - this is necessary since
    # namedtuples can't have fields starting with '_'.
    for key in list(context.keys()):
        context["key_" + key] = context.pop(key)

    # Default to ``None`` for keys missing from the context
    for field in key_class._fields:
        if field not in context:
            context[field] = None

    # Default key_blocksize to _DEFAULT_BLOCKSIZE if missing from the context
    if context.get("key_blocksize") is None:
        context["key_blocksize"] = _DEFAULT_BLOCKSIZE

    return key_class(**context)


key_fn_by_scheme = {
    "http": functools.partial(_default_key_normalizer, PoolKey),
    "https": functools.partial(_default_key_normalizer, PoolKey),
}

pool_classes_by_scheme = {"http": HTTPConnectionPool, "https": HTTPSConnectionPool}


class PoolManager(UPoolManager):

    def __init__(self, num_pools=10, headers=None, **connection_pool_kw):
        super().__init__(num_pools, headers, **dict(connection_pool_kw))
        self.pools.clear()
        self.pools = RecentlyUsedContainer(maxsize=num_pools, dispose_func=lambda v: v.close(),
                                           clean_func=close_expired_connections)
        self.pool_classes_by_scheme = pool_classes_by_scheme
        self.key_fn_by_scheme = key_fn_by_scheme.copy()

    def close_expired_connections(self):
        self.pools.clean_up()

    def _new_pool(self, scheme, host, port, request_context=None):
        """
        Create a new :class:`urllib3.connectionpool.ConnectionPool` based on host, port, scheme, and
        any additional pool keyword arguments.

        If ``request_context`` is provided, it is provided as keyword arguments
        to the pool class used. This method is used to actually create the
        connection pools handed out by :meth:`connection_from_url` and
        companion methods. It is intended to be overridden for customization.
        """
        if request_context is None:
            request_context = self.connection_pool_kw.copy()

        # Default blocksize to _DEFAULT_BLOCKSIZE if missing or explicitly
        # set to 'None' in the request_context.
        if request_context.get("blocksize") is None:
            request_context["blocksize"] = _DEFAULT_BLOCKSIZE

        if request_context.pop('http2', False):
            from tider.network.http2.connectionpool import HTTP2ConnectionPool
            pool_cls = HTTP2ConnectionPool
        else:
            request_context.pop('verify', None)
            request_context.pop('cert', None)
            request_context.pop('trust_env', None)
            pool_cls = self.pool_classes_by_scheme[scheme]

        # Although the context has everything necessary to create the pool,
        # this function has historically only used the scheme, host, and port
        # in the positional args. When an API change is acceptable these can
        # be removed.
        for key in ("scheme", "host", "port"):
            request_context.pop(key, None)

        if scheme == "http":
            for kw in SSL_KEYWORDS:
                request_context.pop(kw, None)
        return pool_cls(host=host, port=port, **request_context)

    def urlopen(self, method, url, redirect=True, http2=False, **kw):
        """
        Same as :meth:`urllib3.HTTPConnectionPool.urlopen`
        with custom cross-host redirect logic and only sends the request-uri
        portion of the ``url``.

        The given ``url`` parameter must be absolute, such that an appropriate
        :class:`urllib3.connectionpool.ConnectionPool` can be chosen for it.
        """
        u = parse_url(url)
        conn = self.connection_from_host(u.host, port=u.port, scheme=u.scheme, pool_kwargs={'http2': http2})

        kw["assert_same_host"] = False
        kw["redirect"] = False

        if "headers" not in kw:
            kw["headers"] = self.headers.copy()

        if http2 or self._proxy_requires_url_absolute_form(u):
            response = conn.urlopen(method, url, **kw)
        else:
            response = conn.urlopen(method, u.request_uri, **kw)

        redirect_location = redirect and response.get_redirect_location()
        if not redirect_location:
            return response

        # Support relative URLs for redirecting.
        redirect_location = urljoin(url, redirect_location)

        if response.status == 303:
            # Change the method according to RFC 9110, Section 15.4.4.
            method = "GET"
            # And lose the body not to transfer anything sensitive.
            kw["body"] = None
            kw["headers"] = HTTPHeaderDict(kw["headers"])._prepare_for_method_change()

        retries = kw.get("retries")
        if not isinstance(retries, Retry):
            retries = Retry.from_int(retries, redirect=redirect)

        if hasattr(conn, 'is_same_host'):
            same_host = conn.is_same_host(redirect_location)
        else:
            same_host = is_same_host(url, redirect_location)
        # Strip headers marked as unsafe to forward to the redirected location.
        # Check remove_headers_on_redirect to avoid a potential network call within
        # conn.is_same_host() which may use socket.gethostbyname() in the future.
        if retries.remove_headers_on_redirect and not same_host:
            headers = list(iter(kw["headers"].keys()))
            for header in headers:
                if header.lower() in retries.remove_headers_on_redirect:
                    kw["headers"].pop(header, None)

        try:
            retries = retries.increment(method, url, response=response, _pool=conn)
        except MaxRetryError:
            if retries.raise_on_redirect:
                response.drain_conn()
                raise
            return response

        kw["retries"] = retries
        kw["redirect"] = redirect

        log.info("Redirecting %s -> %s", url, redirect_location)

        response.drain_conn()
        return self.urlopen(method, redirect_location, http2=http2, **kw)


class ProxyManager(PoolManager):
    """
    Behaves just like :class:`PoolManager`, but sends all requests through
    the defined proxy, using the CONNECT method for HTTPS URLs.

    :param proxy_url:
        The URL of the proxy to be used.

    :param proxy_headers:
        A dictionary containing headers that will be sent to the proxy. In case
        of HTTP they are being sent with each request, while in the
        HTTPS/CONNECT case they are sent only once. Could be used for proxy
        authentication.

    :param proxy_ssl_context:
        The proxy SSL context is used to establish the TLS connection to the
        proxy when using HTTPS proxies.

    :param use_forwarding_for_https:
        (Defaults to False) If set to True will forward requests to the HTTPS
        proxy to be made on behalf of the client instead of creating a TLS
        tunnel via the CONNECT method. **Enabling this flag means that request
        and response headers and content will be visible from the HTTPS proxy**
        whereas tunneling keeps request and response headers and content
        private.  IP address, target hostname, SNI, and port are always visible
        to an HTTPS proxy even when this flag is disabled.
    """

    def __init__(
        self,
        proxy_url,
        num_pools=10,
        headers=None,
        proxy_headers=None,
        proxy_ssl_context=None,
        use_forwarding_for_https=False,
        **connection_pool_kw
    ):

        proxy = parse_url(proxy_url)

        if proxy.scheme not in ("http", "https"):
            raise ProxySchemeUnknown(proxy.scheme)

        if not proxy.port:
            port = port_by_scheme.get(proxy.scheme, 80)
            proxy = proxy._replace(port=port)

        self.proxy = proxy
        parsed = urlparse(proxy.url)
        if parsed.username or parsed.password:
            self.proxy_auth = (parsed.username.encode('utf-8') if parsed.username else None,
                               parsed.password.encode('utf-8') if parsed.password else None)
        else:
            self.proxy_auth = None
        self.proxy_headers = proxy_headers or {}
        self.proxy_ssl_context = proxy_ssl_context
        self.proxy_config = ProxyConfig(proxy_ssl_context, use_forwarding_for_https)

        connection_pool_kw["_proxy"] = self.proxy
        connection_pool_kw["_proxy_headers"] = self.proxy_headers
        connection_pool_kw["_proxy_config"] = self.proxy_config

        super(ProxyManager, self).__init__(num_pools, headers, **connection_pool_kw)

    def _new_pool(self, scheme, host, port, request_context=None):
        """
        Create a new :class:`urllib3.connectionpool.ConnectionPool` based on host, port, scheme, and
        any additional pool keyword arguments.

        If ``request_context`` is provided, it is provided as keyword arguments
        to the pool class used. This method is used to actually create the
        connection pools handed out by :meth:`connection_from_url` and
        companion methods. It is intended to be overridden for customization.
        """
        if request_context is None:
            request_context = self.connection_pool_kw.copy()

        if request_context.pop('http2', False):
            from tider.network.http2.connectionpool import HTTPProxy

            if scheme == 'https':
                return HTTPProxy(
                    proxy_url=self.proxy._replace(auth=None).url,
                    proxy_headers=self.proxy_headers,  # provided by requests
                    **request_context
                )
        return super()._new_pool(scheme, host, port, request_context)

    def connection_from_host(self, host, port=None, scheme="http", pool_kwargs=None):
        if scheme == "https":
            return super().connection_from_host(
                host, port, scheme, pool_kwargs=pool_kwargs
            )

        return super().connection_from_host(
            self.proxy.host, self.proxy.port, self.proxy.scheme, pool_kwargs=pool_kwargs
        )

    def _set_proxy_headers(self, url, headers=None):
        """
        Sets headers needed by proxies: specifically, the Accept and Host
        headers. Only sets headers not provided by the user.
        """
        headers_ = {"Accept": "*/*"}

        netloc = parse_url(url).netloc
        if netloc:
            headers_["Host"] = netloc

        if headers:
            headers_.update(headers)
        return headers_

    def urlopen(self, method, url, redirect=True, **kw):
        """Same as HTTP(S)ConnectionPool.urlopen, ``url`` must be absolute."""
        u = parse_url(url)
        if not connection_requires_http_tunnel(self.proxy, self.proxy_config, u.scheme):
            # For connections using HTTP CONNECT, httplib sets the necessary
            # headers on the CONNECT to the proxy. If we're not using CONNECT,
            # we'll definitely need to set 'Host' at the very least.
            headers = kw.get("headers", self.headers)
            kw["headers"] = self._set_proxy_headers(url, headers)

        return super().urlopen(method, url, redirect=redirect, **kw)


def proxy_from_url(url, **kw):
    return ProxyManager(proxy_url=url, **kw)
