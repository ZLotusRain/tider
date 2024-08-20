"""Eventlet execution pool."""
from abc import ABC
from greenlet import GreenletExit

from .base import apply_target, BasePool


class TaskPool(BasePool, ABC):
    """Eventlet Task Pool."""

    signal_safe = False
    is_green = True
    task_join_will_block = False
    _pool = None
    _pool_map = None
    _quick_put = None

    def __init__(self, *args, **kwargs):
        from eventlet import greenthread
        from eventlet.greenpool import GreenPool
        self.Pool = GreenPool
        self.getcurrent = greenthread.getcurrent
        self.getpid = lambda: id(greenthread.getcurrent())
        self.spawn_n = greenthread.spawn_n

        super().__init__(*args, **kwargs)

    def on_start(self):
        self._pool = self.Pool(self.limit)
        self._pool_map = {}
        self._quick_put = self._pool.spawn

    def on_stop(self):
        if self._pool is not None:
            self._pool.waitall()

    def on_apply(self, target, args=None, kwargs=None, callback=None, cb_kwargs=None, **_):
        target = TaskPool._make_killable_target(target)
        greenlet = self._quick_put(
            apply_target,
            target, args,
            kwargs,
            callback,
            cb_kwargs
        )
        self._add_to_pool_map(id(greenlet), greenlet)

    def grow(self, n=1):
        limit = self.limit + n
        self._pool.resize(limit)
        self.limit = limit

    def shrink(self, n=1):
        limit = self.limit - n
        self._pool.resize(limit)
        self.limit = limit

    def terminate_job(self, pid, signal=None):
        if pid in self._pool_map.keys():
            greenlet = self._pool_map[pid]
            greenlet.kill()
            greenlet.wait()

    @staticmethod
    def _make_killable_target(target):
        def killable_target(*args, **kwargs):
            try:
                return target(*args, **kwargs)
            except GreenletExit:
                return False, None, None
        return killable_target

    def _add_to_pool_map(self, pid, greenlet):
        self._pool_map[pid] = greenlet
        greenlet.link(
            TaskPool._cleanup_after_job_finish,
            self._pool_map,
            pid
        )

    @staticmethod
    def _cleanup_after_job_finish(greenlet, pool_map, pid):
        del pool_map[pid]
