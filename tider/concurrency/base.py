import sys
import time
import psutil
from typing import Deque, Callable
from billiard.einfo import ExceptionInfo
from billiard.exceptions import WorkerLostError

from tider.utils.log import get_logger
from tider.exceptions import SpiderShutdown, SpiderTerminate, reraise

logger = get_logger(__name__)


def apply_target(target, args=(), kwargs=None, callback=None, propagate=(), **_):
    """Apply function within pool context."""
    kwargs = kwargs or {}
    args = args or ()
    try:
        ret = target(*args, **kwargs)
    except (SpiderShutdown, SpiderTerminate):
        raise
    except propagate:
        raise
    except Exception:
        raise
    except BaseException as exc:
        try:
            reraise(WorkerLostError, WorkerLostError(repr(exc)),
                    sys.exc_info()[2])
        except WorkerLostError:
            if callback:
                callback(ExceptionInfo())
    else:
        if callback:
            callback(ret)
    finally:
        del target, args, kwargs, callback


def worker(queue: Deque, processor: Callable, output_handler=None, shutdown_event=None):
    while True:
        try:
            obj = queue.popleft()
            result = processor(obj)
            # don't use thread lock in handler if possible.
            if output_handler:
                output_handler(result)
        except IndexError:
            if shutdown_event and shutdown_event():
                return
        finally:
            # switch greenlet if using gevent
            # when the active one can't get the next request
            # to avoid keeping loop.
            time.sleep(0.01)


class BasePool:
    """Task pool."""

    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3

    # Timer = timer2.Timer

    #: set to true if the pool can be shutdown from within
    #: a signal handler.
    signal_safe = True

    #: set to true if pool uses greenlets.
    is_green = False

    _state = None
    _pool = None

    task_join_will_block = True
    body_can_be_buffer = False

    def __init__(self, limit=None, **options):
        self.limit = limit or psutil.cpu_count()

    def on_start(self, **kwargs):
        pass

    @staticmethod
    def did_start_ok():
        return True

    def flush(self):
        pass

    def join(self):
        pass

    def on_stop(self):
        pass

    def on_close(self):
        pass

    def on_apply(self, *args, **kwargs):
        pass

    def on_terminate(self):
        pass

    def on_soft_timeout(self, job):
        pass

    def on_hard_timeout(self, job):
        pass

    def maintain_pool(self, *args, **kwargs):
        pass

    def terminate_job(self, pid, signal=None):
        raise NotImplementedError(
            f'{type(self)} does not implement kill_job')

    def restart(self):
        raise NotImplementedError(
            f'{type(self)} does not implement restart')

    def stop(self):
        self.on_stop()
        self._state = self.TERMINATE

    def terminate(self):
        self._state = self.TERMINATE
        self.on_terminate()

    def start(self, **kwargs):
        self.on_start(**kwargs)
        self._state = self.RUN

    def close(self):
        self._state = self.CLOSE
        self.on_close()

    def apply_async(self, target, args=None, kwargs=None, callback=None, cb_kwargs=None, **options):
        """Equivalent of the :func:`apply` built-in function.

        Callbacks should optimally return as soon as possible since
        otherwise the thread which handles the result will get blocked.
        """
        cb_kwargs = cb_kwargs or {}
        return self.on_apply(target, args, kwargs, callback, cb_kwargs, **options)

    @property
    def active(self):
        return self._state == self.RUN

    @property
    def max_concurrency(self):
        return self.limit
