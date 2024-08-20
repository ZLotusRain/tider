import os
import sys
import threading
import traceback

from threading import TIMEOUT_MAX as THREAD_TIMEOUT_MAX


class bgThread(threading.Thread):
    """Background service thread from celery."""

    def __init__(self, name=None, **kwargs):
        super().__init__()
        self.__is_shutdown = threading.Event()
        self.__is_stopped = threading.Event()
        self.daemon = True
        self.name = name or self.__class__.__name__

    def body(self):
        raise NotImplementedError()

    def on_crash(self, msg, *fmt, **kwargs):
        print(msg.format(*fmt), file=sys.stderr)
        traceback.print_exc(None, sys.stderr)

    def run(self):
        body = self.body
        shutdown_set = self.__is_shutdown.is_set
        try:
            while not shutdown_set():
                try:
                    body()
                except Exception as exc:  # pylint: disable=broad-except
                    try:
                        self.on_crash('{0!r} crashed: {1!r}', self.name, exc)
                        self._set_stopped()
                    finally:
                        sys.stderr.flush()
                        os._exit(1)  # exiting by normal means won't work
        finally:
            self._set_stopped()

    def _set_stopped(self):
        try:
            self.__is_stopped.set()
        except TypeError:  # pragma: no cover
            # we lost the race at interpreter shutdown,
            # so gc collected built-in modules.
            pass

    def stop(self):
        """Graceful shutdown."""
        self.__is_shutdown.set()
        self.__is_stopped.wait()
        if self.is_alive():
            self.join(THREAD_TIMEOUT_MAX)
