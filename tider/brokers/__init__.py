import os
import sys
import weakref
import inspect
import traceback
import threading
from functools import partial
from threading import TIMEOUT_MAX as THREAD_TIMEOUT_MAX

from tider.utils.log import get_logger

logger = get_logger(__name__)


class Broker(threading.Thread):
    """
    Consume messages with callback.
    """

    restore_at_shutdown = True

    def __init__(self, crawler, name=None, queues=None, on_message=None, on_message_consumed=None):
        super().__init__()
        self.crawler = crawler
        self.queues = queues

        if on_message is not None:
            try:
                on_message.__self__
                on_message.__func__
                on_message = weakref.WeakMethod(on_message)
            except AttributeError:
                pass
        self.on_message = on_message
        if inspect.ismethod(on_message_consumed):
            # do some cleanups in on_message_consumed
            self.on_message_consumed = weakref.WeakMethod(on_message_consumed)
        else:
            self.on_message_consumed = on_message_consumed

        self.__is_shutdown = threading.Event()
        self.__is_stopped = threading.Event()
        self.daemon = True
        self.name = name or self.__class__.__name__

    def on_crash(self, msg, *fmt, **kwargs):
        print(msg.format(*fmt), file=sys.stderr)
        traceback.print_exc(None, sys.stderr)

    def run(self):
        on_message = self.on_message
        on_message_consumed = self.on_message_consumed
        if isinstance(on_message, weakref.ReferenceType):
            on_message = on_message()
        if isinstance(on_message_consumed, weakref.ReferenceType):
            on_message_consumed = on_message_consumed()
        consume = partial(self.consume, queues=self.queues,
                          on_message=on_message,
                          on_message_consumed=on_message_consumed)
        try:
            try:
                consume()
            except Exception as exc:  # pylint: disable=broad-except
                try:
                    self.on_crash('{0!r} crashed: {1!r}', self.name, exc)
                    self._set_stopped()
                finally:
                    sys.stderr.flush()
                    os._exit(1)  # exiting by normal means won't work
        finally:
            self._set_stopped()
            del self.crawler, self.on_message, self.on_message_consumed

    def restore(self, message, leftmost=False):
        """left push if leftmost is True else do right push."""
        pass

    def consume(self, queues=None, on_message=None, on_message_consumed=None):
        """Consume messages and return the connection to trigger consuming."""
        raise NotImplementedError

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


class DummyBroker(Broker):
    """Generate requests from spider.start_requests without messages."""

    def consume(self, queues=None, on_message=None, on_message_consumed=None):
        on_message(message=None, payload=None, no_ack=True)
        if not self.crawler.engine._spider_closed.is_set():
            # start_requests consumed.
            self.crawler.engine._spider_closed.set()
        try:
            on_message_consumed(loop=True)
        except RuntimeError:
            return  # maybe shutdown
