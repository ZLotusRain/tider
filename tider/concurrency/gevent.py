"""Gevent execution pool."""
from abc import ABC

from .base import apply_target, BasePool

try:
    from gevent import Timeout
except ImportError:  # pragma: no cover
    Timeout = None

__all__ = ('TaskPool',)

# pylint: disable=redefined-outer-name
# We cache globals and attribute lookups, so disable this warning.


def apply_timeout(target, args=(), kwargs=None, callback=None, cb_kwargs=None, timeout=None,
                  timeout_callback=None, **rest):
    cb_kwargs = cb_kwargs or {}
    try:
        with Timeout(timeout):
            return apply_target(target, args, kwargs,
                                callback, cb_kwargs,
                                propagate=(Timeout,), **rest)
    except Timeout:
        if timeout_callback:
            return timeout_callback(timeout, **cb_kwargs)
        raise


class TaskPool(BasePool, ABC):
    """GEvent Pool."""

    name = "gevent"

    signal_safe = False
    is_green = True
    task_join_will_block = False
    _pool = None
    _quick_put = None

    def __init__(self, *args, **kwargs):
        from gevent import spawn_raw
        from gevent.pool import Pool
        self.Pool = Pool
        self.spawn_n = spawn_raw
        self.timeout = kwargs.get('timeout')
        super().__init__(*args, **kwargs)

    def on_start(self):
        self._pool = self.Pool(self.limit)
        self._quick_put = self._pool.spawn

    def on_apply(self, target, args=None, kwargs=None, callback=None, cb_kwargs=None, timeout=None,
                 timeout_callback=None):
        timeout = self.timeout if timeout is None else timeout
        return self._quick_put(apply_timeout if timeout else apply_target,
                               target, args, kwargs, callback, cb_kwargs,
                               timeout=timeout, timeout_callback=timeout_callback)

    def grow(self, n=1):
        self._pool._semaphore.counter += n
        self._pool.size += n

    def shrink(self, n=1):
        self._pool._semaphore.counter -= n
        self._pool.size -= n

    @property
    def num_processes(self):
        return len(self._pool)

    def on_stop(self):
        if self._pool is not None:
            self._pool.join()

    def on_terminate(self):
        if self._pool is not None:
            for greenlet in list(self._pool.greenlets):
                self._pool.discard(greenlet)
                greenlet.kill()
            self._pool.join()
