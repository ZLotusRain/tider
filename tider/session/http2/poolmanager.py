import httpcore
import collections
from urllib3.util import parse_url
from urllib3.request import RequestMethods
from urllib3.connectionpool import port_by_scheme
from urllib3.exceptions import ProxySchemeUnknown
from urllib3._collections import RecentlyUsedContainer

from tider.session.http2 import create_ssl_context


_key_fields = (
    'key_verify',
    'key_cert',
    'key_trust_env',
    'key_http1',
    'key_http2',
    'key_max_connections',
    'key_max_keepalive_connections',
    'key_keepalive_expiry',
    'key_retries',
    'key_local_address',
    'key_uds',
    'key_async',
    'key__proxy',
    'key__proxy_headers'
)

PoolKey = collections.namedtuple("PoolKey", _key_fields)


def _default_key_normalizer(key_class, request_context):
    """
    Create a pool key out of a request context dictionary.
    """
    # Since we mutate the dictionary, make a copy first
    context = request_context.copy()

    # Map the kwargs to the names in the namedtuple - this is necessary since
    # namedtuples can't have fields starting with '_'.
    for key in list(context.keys()):
        context["key_" + key] = context.pop(key)

    # Default to ``None`` for keys missing from the context
    for field in key_class._fields:
        if field not in context:
            context[field] = None

    return key_class(**context)


class PoolManager(RequestMethods):

    def __init__(self, num_pools=10, headers=None, **connection_pool_kw):
        RequestMethods.__init__(self, headers)
        self.connection_pool_kw = dict(connection_pool_kw)
        self.pools = RecentlyUsedContainer(num_pools, dispose_func=lambda p: p.close())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear()
        # Return False to re-raise any potential exceptions
        return False

    def _new_pool(self, verify, cert, trust_env, request_context=None):
        request_context = dict(request_context or self.connection_pool_kw)
        ssl_context = create_ssl_context(verify=verify, cert=cert, trust_env=trust_env)
        return httpcore.ConnectionPool(ssl_context=ssl_context, http1=False, http2=True, **request_context)

    def clear(self):
        self.pools.clear()

    def connection_from_context(self, request_context):
        pool_key = _default_key_normalizer(PoolKey, request_context)
        return self.connection_from_pool_key(pool_key, request_context=request_context)

    def connection_from_pool_key(self, pool_key, request_context=None):
        with self.pools.lock:
            # If the cert, verify, or trust_env doesn't match existing open
            # connections, open a new ConnectionPool.
            pool = self.pools.get(pool_key)
            if pool:
                return pool

            # Make a fresh ConnectionPool of the desired type
            verify = request_context.pop("verify", False)
            cert = request_context.pop("cert", None)
            trust_env = request_context.pop("trust_env", True)
            pool = self._new_pool(verify, cert, trust_env, request_context=request_context)
            self.pools[pool_key] = pool

        return pool


class ProxyManager(PoolManager):

    def __init__(self, proxy_url, proxy_headers=None, **connection_pool_kw):
        proxy = parse_url(proxy_url)

        if proxy.scheme not in ("http", "https"):
            raise ProxySchemeUnknown(proxy.scheme)

        if not proxy.port:
            port = port_by_scheme.get(proxy.scheme, 80)
            proxy = proxy._replace(port=port)

        auth = proxy.auth
        raw_auth = None if auth is None else (auth[0].encode("utf-8"), auth[1].encode("utf-8"))
        self.proxy_auth = raw_auth

        self.proxy = proxy
        self.proxy_url = proxy_url
        self.proxy_headers = dict(proxy_headers or {})
        connection_pool_kw["_proxy"] = self.proxy
        connection_pool_kw["_proxy_headers"] = self.proxy_headers
        super().__init__(**connection_pool_kw)

    def _new_pool(self, verify, cert, trust_env, request_context=None):
        request_context = dict(request_context or self.connection_pool_kw)
        ssl_context = create_ssl_context(verify=verify, cert=cert, trust_env=trust_env)
        return httpcore.HTTPProxy(
            proxy_url=self.proxy_url,
            proxy_auth=self.proxy_auth,
            proxy_headers=self.proxy_headers,
            ssl_context=ssl_context,
            http1=False, http2=True, **request_context
        )
