"""Thread execution pool."""

import os
import queue
import weakref
from abc import ABC
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor, wait

from .base import BasePool, apply_target

__all__ = ('TaskPool',)


class ApplyResult:
    def __init__(self, future):
        self.f = future
        self.get = self.f.result

    def wait(self, timeout=None):
        wait([self.f], timeout)


class ThreadPoolExecutor(_ThreadPoolExecutor):

    def __init__(self, max_workers=None, **kwargs):
        if max_workers is None:
            # ThreadPoolExecutor is often used to:
            # * CPU bound task which releases GIL
            # * I/O bound task (which releases GIL, of course)
            #
            # We use cpu_count + 4 for both types of tasks.
            # But we limit it to 32 to avoid consuming surprisingly large resource
            # on many core machine.
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        super(ThreadPoolExecutor, self).__init__(max_workers=max_workers * 2, **kwargs)
        self._work_queue = queue.Queue(maxsize=self._max_workers)
        self._threads = weakref.WeakSet()


class TaskPool(BasePool, ABC):
    """Thread Task Pool."""

    name = "thread"
    body_can_be_buffer = True
    signal_safe = False

    def __init__(self, thread_name_prefix="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.thread_name_prefix = thread_name_prefix
        self.executor = ThreadPoolExecutor(max_workers=self.limit, thread_name_prefix=thread_name_prefix)

    def on_apply(self, target, args=None, kwargs=None, callback=None, cb_kwargs=None, **options):
        future = self.executor.submit(apply_target, target, args, kwargs,
                                      callback, cb_kwargs, **options)
        future.add_done_callback(lambda f: f.result())  # raise exceptions when failed
        return ApplyResult(future)

    def on_stop(self):
        self.executor.shutdown()
        super(TaskPool, self).on_stop()
