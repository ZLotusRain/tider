import time
import random
import inspect
import urllib3
from threading import RLock
from typing import Union, List
from abc import abstractmethod

from tider import Item, Field
from tider.structures.lock import DummyLock
from tider.exceptions import UnSupportedMethod

URLLIB3_SUPPORTS_HTTPS = tuple(urllib3.__version__.split('.')) > ('1', '26')


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


class ProxyPool(metaclass=_ProxyPoolMeta):

    @abstractmethod
    def get_proxy(self, **kwargs) -> Union[dict, List[dict]]:
        raise NotImplementedError()

    get = get_proxies = get_proxy  # COMPAT

    def close(self):
        pass


class ProxyContainer:

    def __init__(self, proxypool, maxsize, use_lock=False, stats=None):
        self._proxypool = proxypool
        self._maxsize = maxsize
        self._queue = []
        self.stats = stats
        self.newed = 0
        self.lock = RLock() if use_lock else DummyLock()

    def _new_proxy(self, disposable=False, on_discard=None, **kwargs):
        result = None
        while result is None:
            result = self._proxypool.get_proxy(**kwargs)
        if inspect.isgenerator(result):
            tmp = [r for r in result]
            result = tmp
        if isinstance(result, list):
            result = random.choice(result)
        if isinstance(result, dict):
            self.newed += 1
            self.stats and self.stats.inc_value('proxy/count')
            return Proxy(disposable=disposable, on_discard=on_discard, **result)
        typename = type(result).__name__
        raise TypeError(f"Received unexpected type {typename!r} from "
                        f"{self._proxypool.__class__.__name__}.get_proxy")

    def clear(self):
        for proxy in self._queue:
            proxy.discard()
        self._queue[:] = []

    def _get(self):
        """
        get the shortest valid proxy.
        """
        valids = list(filter(lambda x: x.valid, self._queue))
        return min(valids, key=lambda x: x.elapsed)

    def get_proxy(self, disposable=False, on_discard=None, **kwargs):
        if disposable:
            return self._new_proxy(disposable, on_discard, **kwargs)
        with self.lock:
            try:
                return self._get()
            except ValueError:
                # no valid Proxy in queue.
                if len(self) >= self._maxsize:
                    self._queue[:] = []
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

    def __init__(self, proxypool_kw=None, on_proxy_discard=None, stats=None):
        self.containers = {}  # ProxyContainer, get proxy by schema
        self.proxypool_kw = dict(proxypool_kw) if proxypool_kw else {}  # global params for ProxyPool.get_proxy
        self._on_proxy_discard = on_proxy_discard
        self.stats = stats

    def register(self, schema, proxypool, maxsize=1):
        if not isinstance(proxypool, ProxyPool):
            raise ValueError('proxypool must be the instance of `tider.network.ProxyPool`')
        self.containers[schema] = ProxyContainer(proxypool, maxsize, stats=self.stats)

    def get_proxy(self, schema, proxy_args=None, disposable=False, on_discard=None):
        if schema in self.NO_PROXY_FIELDS:
            return Proxy()
        proxypool_kw = self.proxypool_kw.copy()
        proxypool_kw.update(proxy_args)

        on_discard = on_discard or self._on_proxy_discard
        container = self.containers[schema]
        proxy = container.get_proxy(disposable=disposable, on_discard=on_discard, **proxypool_kw)
        self.stats and self.stats.set_value(f'proxy/count/{schema}', container.newed)
        return proxy

    def clear(self):
        for container in self.containers.values():
            container.clear()

    def close(self):
        for container in self.containers.values():
            container.close()


class Proxy(Item):

    http = Field()
    https = Field()

    def __init__(self, timeout=None, max_used_times=None,
                 disposable=False, on_discard=None, *args, **kwargs):
        self._initialized = False
        super().__init__(*args, **kwargs)
        self._initialized = True
        self._init_time = time.time()
        self._active_count = 0
        self._used_times = 0
        self._timeout = timeout
        self._max_used_times = max_used_times
        self._disposable = disposable
        self._elapsed = None
        self._forbidden = False
        self._on_discard = on_discard

    @property
    def elapsed(self):
        if not self.discarded:
            self._elapsed = int(time.time() - self._init_time)
        return self._elapsed

    def __setitem__(self, key, value):
        if self._initialized:
            raise ValueError("Can not update the Proxy after initialized.")
        if URLLIB3_SUPPORTS_HTTPS and key == 'https':
            value = value.replace('https', 'http')
        super().__setitem__(key, value)

    def forbid(self):
        """
        Forbid this Proxy when occurred proxy-related error.
        """
        self._forbidden = True

    def discard(self):
        """
        Force to discard the proxy, use this directly
        may close connections which are used by other requests.
        """
        if self._discarded:
            return
        self._elapsed = int(time.time() - self._init_time)
        # may affect speed due to the RecentlyUsedContainer lock.
        self._on_discard and self._on_discard(proxy=self)
        self._discarded = True

    def maybe_discard(self):
        """
        Call this method if the proxy is no longer needed.
        """
        self.deactivate()
        if self.valid or self.active():
            return
        self.discard()

    @property
    def disposable(self):
        """
        Whether this is a one-time proxy
        """
        return self._disposable

    def update(self, **kwargs):
        raise UnSupportedMethod("Can not update the Proxy, recreate a Proxy instance instead.")

    def active(self):
        return self._active_count > 0

    @property
    def valid(self):
        """
        Whether the proxy is valid.
        If the proxy is disposable or forbidden or has already been discarded,
        we consider it as invalid.
        """
        valid = not (self.disposable or self._forbidden or self.discarded)
        if self._timeout is not None:
            valid = valid and self.elapsed < self._timeout
        if self._max_used_times is not None:
            valid = valid and self._used_times <= self._max_used_times
        return valid

    def activate(self):
        """
        Call this method after assigned.
        """
        self._active_count += 1

    def deactivate(self):
        self._active_count -= 1

    def fetch(self):
        if self.discarded or not self.active():
            raise TypeError("Can not fetch from an inactive or discarded proxy.")
        self._used_times += 1
        return dict(self)
