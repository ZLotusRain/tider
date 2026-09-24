import time
import inspect
import weakref
from datetime import timedelta
from threading import RLock
from typing import Union, List, Generator

from urllib.parse import urlparse

from tider.utils.imports import symbol_by_name
from tider.utils.misc import build_from_crawler
from tider.utils.collections import DummyLock
from tider.utils.time import preferred_clock
from tider.utils.url import prepend_scheme_if_needed
from tider.exceptions import InvalidProxy, InvalidSelectedProxy, ExclusiveProxy


NO_PROXY_FIELDS = ('no_proxy', 'dummy', 0)
_NO_PROXY_FIELDS = NO_PROXY_FIELDS
_PROXY_FETCH_RETRY_INTERVAL = 0.1


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


def _freeze_proxy_params(value):
    if isinstance(value, dict):
        return tuple(
            (k, _freeze_proxy_params(v))
            for k, v in sorted(value.items(), key=lambda item: repr(item[0]))
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_proxy_params(v) for v in value)
    if isinstance(value, set):
        return tuple(sorted((_freeze_proxy_params(v) for v in value), key=repr))
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


class Proxy:

    def __init__(self, proxies=None, disposable=False, keepalive_expiry=None, max_used_times=None,
                 lease_capacity=1, on_discard=None, pool=None, expires_at=None):
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
        # Provider expiry is independent of idle expiry and is never extended
        # by binding, transferring or releasing a request lease.
        self._deadline_at = time.monotonic() + max(0, expires_at - time.time()) if expires_at is not None else None
        self._keepalive_expiry = keepalive_expiry
        self._set_idle_expiry()
        self._used_times = 0
        if disposable:
            max_used_times = 1
            lease_capacity = 1
        self._max_used_times = max_used_times
        self._lease_capacity = max(1, int(lease_capacity or 1))

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
    def available(self):
        """
        Whether this proxy can be leased from pool.
        """
        with self._lock:
            return (
                self.valid
                and self._actives < self._lease_capacity
                and len(self._binding_reqs) < self._lease_capacity
            )

    @property
    def lease_capacity(self):
        return self._lease_capacity

    @property
    def elapsed(self):
        return timedelta(seconds=preferred_clock() - self._init_time)

    def weak(self):
        return weakref.ref(self)

    def has_bound(self):
        with self._lock:
            return len(self._binding_reqs) > 0

    def can_discard(self):
        """Return whether this proxy is invalid and no request is using it."""
        with self._lock:
            return not self.valid and self._actives <= 0 and not self._binding_reqs

    def _set_idle_expiry(self):
        """Start the expiry timer when the proxy is completely idle."""
        if (
            self._keepalive_expiry is not None
            and self._actives <= 0
            and not self._binding_reqs
        ):
            self._expire_at = time.monotonic() + self._keepalive_expiry

    def bind(self, request, exclusive=False):
        if request is None:
            return
        with self._lock:
            if exclusive:
                if not self.valid:
                    raise InvalidProxy("Can't bind to the invalid proxy.")
                if (
                    request not in self._binding_reqs
                    and (
                        self._actives >= self._lease_capacity
                        or len(self._binding_reqs) >= self._lease_capacity
                    )
                ):
                    raise ExclusiveProxy("Can't bind to an unavailable proxy.")
            self._binding_reqs.add(request)
            self._expire_at = None

    def transfer_lease(self, from_request, to_request):
        """
        Move a reserved proxy lease from one request to another.
        """
        if to_request is None:
            return
        with self._lock:
            if not self.valid:
                raise InvalidProxy("Can't bind to the invalid proxy.")
            binding_reqs = set(self._binding_reqs)
            if from_request in self._binding_reqs and from_request is not to_request:
                binding_reqs.discard(from_request)
            if (
                to_request not in self._binding_reqs
                and (
                    self._actives >= self._lease_capacity
                    or len(binding_reqs) >= self._lease_capacity
                )
            ):
                raise ExclusiveProxy("Can't bind to an unavailable proxy.")
            if to_request in binding_reqs and len(binding_reqs) > self._lease_capacity:
                raise ExclusiveProxy("Can't bind to an unavailable proxy.")
            binding_reqs.add(to_request)
            self._binding_reqs = binding_reqs
            self._expire_at = None

    def unbind(self, request):
        with self._lock:
            self._binding_reqs.discard(request)
            self._set_idle_expiry()
        if not self.valid:
            self.discard()

    def connect(self, bound_to=None):
        with self._lock:
            self._used_times += 1
            if not self.valid:
                # maybe exceed max used times
                raise InvalidProxy("Can't connect to the invalid proxy.")
            if self._actives >= self._lease_capacity:
                raise ExclusiveProxy("Proxy lease capacity reached.")
            if bound_to:
                if self._binding_reqs and bound_to not in self._binding_reqs:
                    raise ExclusiveProxy("Unmatched binding request.")
                if bound_to not in self._binding_reqs and len(self._binding_reqs) >= self._lease_capacity:
                    raise ExclusiveProxy("Proxy lease capacity reached.")
                self._binding_reqs.add(bound_to)
            elif self._binding_reqs:
                raise ExclusiveProxy("Can't connect to a bound proxy.")
            self._expire_at = None
            self._actives += 1

    def disconnect(self, invalidate=False):
        if invalidate or self.disposable:
            self.invalidate()
        with self._lock:
            self._actives -= 1
            self._set_idle_expiry()
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
        pool = on_discard = None
        with self._lock:
            if self._discarded:
                return

            if not force and not self.can_discard():
                return

            pool, self._pool = self._pool, None
            on_discard, self._on_discard = self._on_discard, None
            self._discarded = True
        if pool is not None:
            pool.remove(self)
        # may affect speed due to the RecentlyUsedContainer lock.
        if on_discard is not None:
            on_discard(proxy=self)

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
        with self._lock:
            valid = not (self._invalid or self._discarded)
            if self._expire_at is not None:
                now = time.monotonic()
                # if we don't use lock here, `_expire_at` will be None anytime.
                valid = valid and now <= (self._expire_at or now)
            if self._deadline_at is not None:
                valid = valid and time.monotonic() < self._deadline_at
            if self._max_used_times is not None and self._max_used_times > 0:
                valid = valid and self._used_times <= self._max_used_times
            return valid

    def select_proxy(self, url):
        """Select a proxy for the url, if applicable.

        :param url: The url being for the request
        """
        if not self.valid:
            raise InvalidSelectedProxy("Can't select from a discarded proxy.")
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

    def get_proxy(self, **kwargs) -> Union[dict, str, Proxy, List, Generator]:
        raise NotImplementedError

    get = get_proxies = get_proxy  # COMPAT

    def get_proxy_key(self, **kwargs):
        return None

    def close(self):
        pass


class ProxyContainer:

    def __init__(self, schema, proxypool, maxsize, use_lock=True, stats=None):
        self.schema = schema
        self._proxypool = proxypool
        self._maxsize = max(1, int(maxsize or 1))
        self._queues = {}
        self._proxy_index = {}  # Proxy -> set[params_key]
        self._pending_fetches = {}  # params_key -> in-flight provider calls
        self.stats = stats
        self.lock = RLock() if use_lock else DummyLock()

    def _queue(self, params_key=None):
        return self._queues.setdefault(params_key, [])

    def _reserve_fetch(self, params_key, queue):
        """Reserve capacity for one provider call while holding ``self.lock``."""
        pending = self._pending_fetches.get(params_key, 0)
        if len(queue) + pending >= self._maxsize:
            return False
        self._pending_fetches[params_key] = pending + 1
        return True

    def _release_fetch(self, params_key):
        """Release one provider-call reservation while holding ``self.lock``."""
        pending = self._pending_fetches.get(params_key, 0) - 1
        if pending > 0:
            self._pending_fetches[params_key] = pending
        else:
            self._pending_fetches.pop(params_key, None)

    def get_proxy_key(self, **kwargs):
        get_proxy_key = getattr(self._proxypool, 'get_proxy_key', None)
        if get_proxy_key is None:
            return None
        return _freeze_proxy_params(get_proxy_key(**kwargs))

    def _new_proxy(self, disposable=False, on_discard=None, limit=None, **kwargs):
        """arguments are only used if a new proxy is created."""
        if limit is not None:
            limit = max(0, int(limit))
            if limit <= 0:
                return []

        proxies = []
        while not proxies:
            result = self._proxypool.get_proxy(**kwargs)
            if result is None:
                time.sleep(_PROXY_FETCH_RETRY_INTERVAL)
                continue
            if inspect.isgenerator(result):
                for each in result:
                    proxies.append(each)
                    if limit is not None and len(proxies) >= limit:
                        break
            elif isinstance(result, list):
                proxies = result[:limit]
            else:
                proxies = [result]
            if not proxies:
                time.sleep(_PROXY_FETCH_RETRY_INTERVAL)

        for idx, each in enumerate(proxies):
            if isinstance(each, str):
                if each.lower().startswith('socks'):
                    each = {'socks': each}
                else:
                    each = {'https': each, 'http': each}
            if isinstance(each, dict):
                each = Proxy(
                    proxies=each, disposable=disposable, on_discard=on_discard,
                    pool=self, keepalive_expiry=60
                )
            if isinstance(each, Proxy):
                each.disposable = disposable
                each._pool = self
                if each._on_discard is None:
                    each._on_discard = on_discard
                self.stats and self.stats.inc_value('proxy/count')
                self.stats and self.stats.inc_value(f'proxy/count/{self.schema}')
                proxies[idx] = each
                continue
            typename = type(each).__name__
            raise TypeError(f"Received unexpected type {typename!r} from "
                            f"{self._proxypool.__class__.__name__}.get_proxy")
        return proxies

    def clear(self):
        with self.lock:
            proxies = list(self._proxy_index)
            self._queues.clear()
            self._proxy_index.clear()
            for proxy in proxies:
                proxy.discard(force=True)

    def remove(self, proxy):
        with self.lock:
            params_keys = self._proxy_index.pop(proxy, ())
            for params_key in params_keys:
                queue = self._queues.get(params_key)
                if queue is None:
                    continue
                queue[:] = [queued_proxy for queued_proxy in queue if queued_proxy is not proxy]
                if not queue:
                    self._queues.pop(params_key, None)

    def close_expired_connections(self):
        with self.lock:
            proxies = list(self._proxy_index)
        for proxy in proxies:
            if not proxy.valid:
                proxy.discard()

    def _get(self, queue, request=None):
        """Get the oldest available proxy from queue."""
        for proxy in list(queue):
            if not proxy.valid:
                proxy.discard()
                continue
            if not proxy.available:
                continue
            try:
                proxy.bind(request, exclusive=True)
                # Stop at the first usable proxy: avoid sweeping later invalid
                # proxies here, which can churn downloader proxy connection pools.
                return proxy
            except ExclusiveProxy:
                pass
            except InvalidProxy:
                proxy.discard()
        return None

    def get_proxy(self, disposable=False, on_discard=None, request=None, params_key=None, **kwargs):
        if disposable:
            proxy_list = self._new_proxy(disposable, on_discard, limit=1, **kwargs)
            proxy = proxy_list[0]
            proxy.bind(request, exclusive=True)
            return proxy

        with self.lock:
            # Do not create an empty bucket until proxy fetching succeeds.
            queue = self._queues.get(params_key, ())
            proxy = self._get(queue, request=request)
            if proxy is not None:
                return proxy
            if not self._reserve_fetch(params_key, queue):
                raise ExclusiveProxy("No available proxy.")

        # Fetching a proxy may call external services; keep it outside the
        # container lock. A provider batch may exceed the reserved capacity.
        try:
            proxy_list = self._new_proxy(disposable, on_discard, **kwargs)
        except BaseException:
            with self.lock:
                self._release_fetch(params_key)
            raise

        with self.lock:
            self._release_fetch(params_key)
            queue = self._queue(params_key)
            queue.extend(proxy_list)
            for proxy in proxy_list:
                self._proxy_index.setdefault(proxy, set()).add(params_key)
            proxy = self._get(queue, request=request)
        if proxy is not None:
            return proxy
        raise ExclusiveProxy("No available proxy.")

    def __len__(self):
        with self.lock:
            return sum(len(queue) for queue in self._queues.values())

    def __contains__(self, item):
        with self.lock:
            return any(item in queue for queue in self._queues.values())

    def close(self):
        self.clear()
        self._proxypool.close()


class ProxyPoolManager:

    NO_PROXY_FIELDS = _NO_PROXY_FIELDS

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

    def register(self, schema, proxypool, maxsize=None):
        if not isinstance(proxypool, ProxyPool):
            raise ValueError('proxypool must be the instance of `tider.network.ProxyPool`')
        if maxsize is None:
            maxsize = self._crawler.concurrency * 2
        self.containers[schema] = ProxyContainer(schema, proxypool, maxsize, stats=self.stats)

    def _get_proxypool_kwargs(self, proxy_args=None):
        proxypool_kw = self.proxypool_kw.copy()
        if proxy_args:
            proxypool_kw.update(proxy_args)
        reserved_params = ('disposable', 'on_discard', 'request')
        for param in reserved_params:
            proxypool_kw.pop(param, None)
        return proxypool_kw

    def get_proxy_key(self, schema, proxy_args=None):
        if schema in self.NO_PROXY_FIELDS:
            return None
        container = self.containers[schema]
        return container.get_proxy_key(**self._get_proxypool_kwargs(proxy_args))

    def get_proxy(self, schema, proxy_args=None, disposable=False, request=None, on_discard=None):
        if schema in self.NO_PROXY_FIELDS:
            return Proxy()

        on_discard = on_discard or self._on_proxy_discard
        container = self.containers[schema]
        proxypool_kw = self._get_proxypool_kwargs(proxy_args)
        params_key = container.get_proxy_key(**proxypool_kw)
        proxy = container.get_proxy(
            disposable=disposable, on_discard=on_discard, request=request, params_key=params_key, **proxypool_kw
        )
        return proxy

    def close(self):
        for container in self.containers.values():
            container.close()

    def close_expired_connections(self):
        for container in self.containers.values():
            container.close_expired_connections()

    clear = close
