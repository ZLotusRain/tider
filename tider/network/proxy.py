import time
import random
import inspect
import weakref
from datetime import timedelta
from threading import RLock
from abc import abstractmethod
from typing import Union, List
from contextlib import suppress

from urllib.parse import urlparse

from tider.utils.misc import symbol_by_name, build_from_crawler
from tider.utils.collections import DummyLock
from tider.utils.time import preferred_clock
from tider.utils.url import prepend_scheme_if_needed
from tider.exceptions import InvalidProxy, ExclusiveProxy


class _ProxyPoolMeta(type):
    """
    Metaclass to check proxy pool classes against the necessary interface
    """

    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))  # pylint: disable=no-value-for-parameter

    def __subclasscheck__(cls, subclass):
        return (
            hasattr(subclass, "get_proxy")
            and callable(subclass.get_proxy)
        )


class Proxy:

    def __init__(self, proxies=None, disposable=False, keepalive_expiry=None, max_used_times=None,
                 on_discard=None, pool=None):
        proxies = dict(proxies) if proxies else {}
        for proxy_key in proxies:
            proxy = proxies[proxy_key]
            # In most cases, the proxy URL for the https:// key should use the http:// scheme (that's not a typo!).
            # This is because HTTP proxying requires initiating a connection with the proxy server.
            # While it's possible that your proxy supports doing it via HTTPS, most proxies only support doing it via HTTP.
            proxies[proxy_key] = prepend_scheme_if_needed(proxy, "http") if proxy else None
        self._proxies = proxies

        self._lock = RLock()
        self._init_time = preferred_clock()
        self._disposable = disposable

        self._on_discard = on_discard
        self._pool = pool
        self._binding_reqs = set()

        self._actives = 0
        self._invalid = False
        self._discarded = False
        self._expire_at = None
        self._keepalive_expiry = keepalive_expiry
        self._used_times = 0
        if disposable:
            max_used_times = 1
        self._max_used_times = max_used_times

    @property
    def proxies(self):
        return dict(self._proxies)  # can't be updated.

    @property
    def disposable(self):
        """
        Whether this is a one-time proxy
        """
        return self._disposable

    @disposable.setter
    def disposable(self, value):
        self._disposable = value

    @property
    def actives(self):
        """
        Nums of requests bound to this proxy.
        """
        return self._actives

    @property
    def elapsed(self):
        return timedelta(seconds=preferred_clock() - self._init_time)

    def weak(self):
        return weakref.ref(self)

    def has_bound(self):
        return len(self._binding_reqs) > 0

    def unbind(self, request):
        self._binding_reqs.discard(request)

    def connect(self, bound_to=None):
        with self._lock:
            self._used_times += 1
            if not self.valid:
                # maybe exceed max used times
                raise InvalidProxy("Can't connect to the invalid proxy.")
            if self.has_bound() and bound_to not in self._binding_reqs:
                raise ExclusiveProxy("Unmatched binding request.")
            if bound_to:
                if self._actives > 0:
                    raise ExclusiveProxy("Can't bind to an active proxy.")
                self._binding_reqs.add(bound_to)
            self._expire_at = None
            self._actives += 1

    def disconnect(self, invalidate=False):
        if invalidate or self.disposable:
            self.invalidate()
        with self._lock:
            self._actives -= 1
            if self._keepalive_expiry is not None and self._actives <= 0:
                now = time.monotonic()
                self._expire_at = now + self._keepalive_expiry
            self._binding_reqs = set()
        if not self.valid:
            self.discard()

    def invalidate(self):
        """
        Invalidate this Proxy when occurred proxy-related error.
        """
        self._invalid = True

    def discard(self, force=False):
        """
        Discard the proxy if invalid and no request is using it , force to call this directly
        may close connections which are used by other requests.
        """
        with self._lock:
            if self._discarded:
                return

            if not force and (self._actives > 0 or self.valid):
                return

            if self._pool is not None:
                self._pool.remove(self)
            self._pool = None
            self._discarded = True
            # may affect speed due to the RecentlyUsedContainer lock.
            if self._on_discard is not None:
                self._on_discard(proxy=self)
            del self._on_discard

    def __enter__(self):
        self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        invalidate = isinstance(exc_val, InvalidProxy)
        self.disconnect(invalidate=invalidate)

    @property
    def valid(self):
        """
        Whether the proxy is valid.
        If the proxy is disposable or forbidden or has already been discarded,
        we consider it as invalid.
        """
        valid = not (self._invalid or self._discarded)
        if self._expire_at is not None:
            now = time.monotonic()
            # if we don't use lock here, `_expire_at` will be None anytime.
            valid = valid and now <= (self._expire_at or now)
        if self._max_used_times is not None and self._max_used_times > 0:
            valid = valid and self._used_times <= self._max_used_times
        return valid

    def select_proxy(self, url):
        """Select a proxy for the url, if applicable.

        :param url: The url being for the request
        """
        if not self.valid:
            raise InvalidProxy("Can't select from a discarded proxy.")
        urlparts = urlparse(url)
        if urlparts.hostname is None:
            return self.proxies.get(urlparts.scheme, self.proxies.get("all"))

        proxy_keys = [
            urlparts.scheme + "://" + urlparts.hostname,
            urlparts.scheme + "://",
            urlparts.scheme,
            "all://" + urlparts.hostname,
            "all",
        ]
        proxy = None
        for proxy_key in proxy_keys:
            if proxy_key in self.proxies:
                proxy = self.proxies[proxy_key]
                break

        return proxy


class ProxyPool(metaclass=_ProxyPoolMeta):

    @abstractmethod
    def get_proxy(self, **kwargs) -> Union[dict, str, Proxy, List[Union[dict, Proxy]]]:
        raise NotImplementedError

    get = get_proxies = get_proxy  # COMPAT

    def close(self):
        pass


class ProxyContainer:

    def __init__(self, schema, proxypool, maxsize, use_lock=False, stats=None):
        self.schema = schema
        self._proxypool = proxypool
        self._maxsize = maxsize
        self._queue = []
        self.stats = stats
        self.lock = RLock() if use_lock else DummyLock()

    def _new_proxy(self, disposable=False, on_discard=None, request=None, **kwargs):
        """arguments are only used if a new proxy is created."""
        result = None
        while result is None:
            result = self._proxypool.get_proxy(**kwargs)
        if inspect.isgenerator(result):
            tmp = [r for r in result]
            result = tmp
        if isinstance(result, list):
            result = random.choice(result)
        if isinstance(result, str):
            if result.lower().startswith('socks'):
                result = {'socks': result}
            else:
                result = {'https': result, 'http': result}
        if isinstance(result, dict):
            result = Proxy(proxies=result, disposable=disposable, on_discard=on_discard, pool=self, keepalive_expiry=60)
        if isinstance(result, Proxy):
            result.disposable = disposable
            result._pool = self
            self.stats and self.stats.inc_value('proxy/count')
            self.stats and self.stats.inc_value(f'proxy/count/{self.schema}')
            return result
        typename = type(result).__name__
        raise TypeError(f"Received unexpected type {typename!r} from "
                        f"{self._proxypool.__class__.__name__}.get_proxy")

    def clear(self):
        for proxy in list(self._queue):
            proxy.discard(force=True)
        del self._queue[:]

    def remove(self, proxy):
        with suppress(ValueError):
            self._queue.remove(proxy)

    def _get(self):
        """
        get the shortest valid proxy.
        """
        valids = []
        for proxy in list(self._queue):
            if not proxy.valid:
                proxy.discard(force=True)
            else:
                valids.append(proxy)
        return max(valids, key=lambda x: x.elapsed)

    def get_proxy(self, disposable=False, on_discard=None, request=None, **kwargs):
        if disposable:
            return self._new_proxy(disposable, on_discard, **kwargs)
        with self.lock:
            try:
                return self._get()
            except ValueError:
                # no valid Proxy in queue.
                if len(self) >= self._maxsize:
                    self.clear()
                proxy = self._new_proxy(disposable, on_discard, **kwargs)
                self._queue.append(proxy)
                return proxy

    def __len__(self):
        return len(self._queue)

    def __contains__(self, item):
        return item in self._queue

    def close(self):
        self.clear()
        self._proxypool.close()


class ProxyPoolManager:

    NO_PROXY_FIELDS = ('no_proxy', 'dummy', 0)

    def __init__(self, crawler, proxy_schemas=None, proxypool_kw=None, on_proxy_discard=None):
        self._crawler = crawler
        self.stats = crawler.stats

        self.containers = {}  # ProxyContainer, get proxy by schema
        for schema in proxy_schemas:
            proxy_cls = symbol_by_name(proxy_schemas[schema])
            proxy_ins = build_from_crawler(proxy_cls, crawler)
            self.register(schema, proxy_ins)
        self.proxypool_kw = dict(proxypool_kw) if proxypool_kw else {}  # global params for ProxyPool.get_proxy
        self._on_proxy_discard = on_proxy_discard

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            proxy_schemas=crawler.settings.getdict('PROXY_SCHEMAS'),
            proxypool_kw=crawler.settings.getdict('PROXY_PARAMS'),
        )

    def register(self, schema, proxypool, maxsize=1):
        if not isinstance(proxypool, ProxyPool):
            raise ValueError('proxypool must be the instance of `tider.network.ProxyPool`')
        self.containers[schema] = ProxyContainer(schema, proxypool, maxsize, stats=self.stats)

    def get_proxy(self, schema, proxy_args=None, disposable=False, request=None, on_discard=None):
        if schema in self.NO_PROXY_FIELDS:
            return Proxy()

        proxypool_kw = self.proxypool_kw.copy()
        proxypool_kw.update(proxy_args)
        reserved_params = ('disposable', 'on_discard', 'request')
        for param in reserved_params:
            proxypool_kw.pop(param, None)

        on_discard = on_discard or self._on_proxy_discard
        container = self.containers[schema]
        proxy = container.get_proxy(disposable=disposable, on_discard=on_discard, request=request, **proxypool_kw)
        return proxy

    def close(self):
        for container in self.containers.values():
            container.close()

    clear = close
