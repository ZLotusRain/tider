"""Crawler Pidbox (remote control)."""
import time
import socket
import threading
from gevent import spawn_raw
from kombu.common import ignore_errors
from kombu.utils.encoding import safe_str, safe_repr

from tider.crawler import state
from tider.exceptions import SpiderShutdown, SpiderTerminate
from tider.crawler.control.panel import Panel
from tider.utils.text import truncate
from tider.utils.log import get_logger
from tider.utils.collections import AttributeDict

logger = get_logger(__name__)

MESSAGE_DECODE_ERROR = """\
Can't decode message body: %r [type:%r encoding:%r headers:%s]

body: %s
"""


def dump_body(m, body):
    """Format message body for debugging purposes."""
    body = m.body if body is None else body
    return '{} ({}b)'.format(truncate(safe_repr(body), 1024),
                             len(m.body))


class Pidbox:
    """Crawler mailbox."""

    consumer = None

    def __init__(self, crawler):
        self.crawler = crawler
        self.hostname = crawler.hostname
        self.node = crawler.app.control.mailbox.Node(
            safe_str(crawler.hostname),
            handlers=Panel.data,
            state=AttributeDict(
                crawler=crawler,
                hostname=crawler.hostname,
            ),
        )
        self._forward_clock = self.crawler.app.clock.forward

    def on_message(self, body, message):
        # just increase clock as clients usually don't
        # have a valid clock to adjust with.
        self._forward_clock()
        try:
            header = message.headers.get
            expires = header('expires')
            if expires and time.time() > expires:
                # kombu.exceptions.OperationalError: Command  # 1 of pipeline caused error:
                # command not allowed when used memory > 'maxmemory'.
                return
            self.node.handle_message(body, message)
        except KeyError as exc:
            logger.error('No such control command: %s', exc)
        except Exception as exc:
            logger.error('Control command error: %r', exc, exc_info=True)
            self.reset()

    def start(self):
        self.node.channel = self.crawler.connection.channel()
        self.consumer = self.node.listen(callback=self.on_message)
        # self.consumer.consume()  # already consumed in listen
        self.consumer.on_decode_error = self.on_decode_error

    @staticmethod
    def on_decode_error(message, exc):
        logger.critical(MESSAGE_DECODE_ERROR,
                        exc, message.content_type, message.content_encoding,
                        safe_repr(message.headers), dump_body(message, message.body),
                        exc_info=True)
        message.ack()

    def on_stop(self):
        pass

    def stop(self):
        self.on_stop()
        # noinspection PyNoneFunctionAssignment
        self.consumer = self._close_channel()

    def reset(self):
        self.stop()
        self.start()

    def _close_channel(self):
        if self.node and self.node.channel:
            ignore_errors(self.crawler.connection, self.node.channel.close)

    def shutdown(self):
        self.on_stop()
        if self.consumer:
            logger.debug('Canceling broadcast consumer...')
            ignore_errors(self.crawler.connection, self.node.channel.close)
        self.stop()


class gPidbox(Pidbox):
    """Crawler pidbox (greenlet)."""

    _node_shutdown = None
    _node_stopped = None
    _resets = 0

    def start(self):
        spawn_raw(self.loop)

    def on_stop(self):
        if self._node_stopped:
            self._node_shutdown.set()
            logger.debug('Waiting for broadcast thread to shutdown...')
            self._node_stopped.wait()
            self._node_stopped = self._node_shutdown = None

    def reset(self):
        self._resets += 1

    def _do_reset(self, connection):
        self._close_channel()
        self.node.channel = connection.channel()
        self.consumer = self.node.listen(callback=self.on_message)
        self.consumer.consume()

    def loop(self):
        resets = [self._resets]
        shutdown = self._node_shutdown = threading.Event()
        stopped = self._node_stopped = threading.Event()
        try:
            with self.crawler.app.connection_for_control() as connection:
                self._do_reset(connection)
                while not shutdown.is_set():
                    if resets[0] < self._resets:
                        resets[0] += 1
                        self._do_reset(connection)
                    try:
                        # maybe spend more time than timeout value.
                        connection.drain_events(timeout=2.0)
                    except socket.timeout:
                        pass
                    except SpiderShutdown:
                        state.should_stop = True
                        raise
                    except SpiderTerminate:
                        state.should_terminate = True
                        raise
        finally:
            stopped.set()
