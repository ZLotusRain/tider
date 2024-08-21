from abc import abstractmethod
from collections import deque
from contextlib import suppress


class _ProxyMeta(type):
    """
    Metaclass to check proxy classes against the necessary interface
    """

    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))  # pylint: disable=no-value-for-parameter

    def __subclasscheck__(cls, subclass):
        return (
            hasattr(subclass, "get_proxy")
            and callable(subclass.get_proxy)
        )


class Proxy(metaclass=_ProxyMeta):

    @abstractmethod
    def get_proxy(self, **kwargs):
        raise NotImplementedError()

    get = get_proxy

    def close(self):
        pass


class DummyProxy(Proxy):

    def get_proxy(self, **kwargs):
        return {}


class ProxyManager:

    def __init__(self, proxy_args=None, stats=None):
        self.proxy_schemas = {}
        self.proxy_args = dict(proxy_args) if proxy_args else {}

        self._reset_signals = {}
        self._running_signals = {}
        self._proxies = {}

        self.stats = stats
        self.register(0, DummyProxy())
        self.register('local', DummyProxy())

    def register(self, schema, proxy):
        if not isinstance(proxy, Proxy):
            raise ValueError('Proxy instance must inherit from `tider.network.proxy`')
        self.proxy_schemas[schema] = proxy
        self._running_signals[schema] = deque(maxlen=1)
        self._reset_signals[schema] = deque(maxlen=1)
        self._proxies[schema] = None

    def reset_proxy(self, schema, old_proxies=None):
        if self._running_signals[schema]:
            return
        if old_proxies and self._proxies[schema]:
            # 503 Server Error: Too Many Connection for url
            if old_proxies == self._proxies[schema]:
                self._reset_signals[schema].append(1)
        else:
            self._reset_signals[schema].append(1)

    def maybe_reset_proxy(self, schema):
        try:
            self._reset_signals[schema].pop()
        except IndexError:
            return False
        return True

    def get_proxy(self, schema, proxy_args=None, new_proxy=False):
        proxy_args = dict(proxy_args) if proxy_args else self.proxy_args
        if not new_proxy:
            return self._get_proxy(schema, proxy_args)
        else:
            return self._get_new_proxy(schema, proxy_args)

    def _get_proxy(self, schema, proxy_args):
        self._running_signals[schema].append(1)
        proxy_ins = self.proxy_schemas[schema]
        if self.maybe_reset_proxy(schema):
            self._proxies[schema] = None
        while self._proxies[schema] is None:
            self._proxies[schema] = proxy_ins.get_proxy(**proxy_args)
            if self._proxies[schema] is not None and self.stats and schema not in (0, 'local'):
                self.stats.inc_value('proxy/count')
                self.stats.inc_value(f'proxy/count/{schema}')
        with suppress(IndexError):
            self._running_signals[schema].pop()
        return dict(self._proxies[schema])

    def _get_new_proxy(self, schema, proxy_args):
        proxies = None
        while proxies is None:
            proxies = self.proxy_schemas[schema].get_proxy(**proxy_args)
            if proxies is not None and self.stats and schema not in (0, 'local'):
                self.stats.inc_value('proxy/count')
                self.stats.inc_value(f'proxy/count/{schema}')
        return dict(proxies)

    def close(self):
        for proxy in self.proxy_schemas.values():
            proxy.close()
        self.proxy_schemas.clear()
        self.stats = None
