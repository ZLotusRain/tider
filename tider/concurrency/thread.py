"""Thread execution pool."""

import os
import queue
import threading
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

    def __init__(self, max_workers=None, thread_name_prefix='',
                 initializer=None, initargs=()):
        if max_workers is None:
            # ThreadPoolExecutor is often used to:
            # * CPU bound task which releases GIL
            # * I/O bound task (which releases GIL, of course)
            #
            # We use cpu_count + 4 for both types of tasks.
            # But we limit it to 32 to avoid consuming surprisingly large resource
            # on many core machine.
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        if initializer is not None and not callable(initializer):
            raise TypeError("initializer must be a callable")

        self._max_workers = max_workers  # dynamically adjust.
        # + 1 to avoid stuck in _python_exit due to waiting for putting None
        self._work_queue = queue.Queue(maxsize=self._max_workers + 1)
        self._idle_semaphore = threading.Semaphore(0)
        self._threads = set()
        self._broken = False
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._thread_name_prefix = (thread_name_prefix or
                                    ("ThreadPoolExecutor-%d" % self._counter()))
        self._initializer = initializer
        self._initargs = initargs

    def maybe_wakeup(self):
        # Send a wake-up to prevent threads calling
        # _work_queue.get(block=True) from permanently blocking.
        if self._work_queue.empty():
            self._work_queue.put(None)


class TaskPool(BasePool, ABC):
    """Thread Task Pool."""
    limit: int

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

    def maybe_shutdown(self):
        self.executor.maybe_wakeup()
