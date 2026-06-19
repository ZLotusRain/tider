"""Crawler Pidbox (remote control)."""
import time
import socket
import threading
from kombu.common import ignore_errors
from kombu.utils.encoding import safe_str, safe_repr

from tider.crawler import state
from tider.exceptions import SpiderShutdown, SpiderTerminate
from tider.crawler.control.panel import Panel
from tider.platforms import EX_FAILURE, EX_OK
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


def _maybe_set_state(name, exc, default):
    if getattr(state, name) is None:
        code = exc.code if exc.code is not None else default
        setattr(state, name, code)


class Pidbox:
    """Crawler mailbox."""

    consumer = None
    # Runtime state for the dedicated control-consumer loop.
    _connection = None
    _node_shutdown = None
    _node_stopped = None
    _resets = 0
    _thread = None

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

    def _prepare_loop_state(self):
        # do not start another control loop while the previous one is alive.
        if self._node_stopped and not self._node_stopped.is_set():
            return False
        self._node_shutdown = threading.Event()
        self._node_stopped = threading.Event()
        return True

    def start(self):
        if not self._prepare_loop_state():
            return
        # threads pool needs a real background thread to drain control events.
        self._thread = threading.Thread(
            target=self.loop,
            name='Control', daemon=True,
        )
        self._thread.start()

    def _do_reset(self, connection):
        # rebuild only the pidbox channel/consumer on the existing connection.
        self._close_channel()
        self.node.channel = connection.channel()
        self.consumer = self.node.listen(callback=self.on_message)
        # already consumed in listen
        self.consumer.on_decode_error = self.on_decode_error

    def _reconnect(self, connection, shutdown):
        """Reconnect the underlying transport without replacing connection."""
        while not shutdown.is_set():
            # keep the shared Connection object, but replace its transport state.
            self._close_channel()
            self.consumer = None
            ignore_errors(connection, connection.release)
            try:
                connection.connect()
                self._do_reset(connection)
            except connection.connection_errors as exc:
                logger.warning('Control connection has broken, try reconnecting: %r', exc)
                time.sleep(1)
            else:
                return True
        return False

    def loop(self):
        resets = [self._resets]
        self._node_shutdown = self._node_shutdown or threading.Event()
        self._node_stopped = self._node_stopped or threading.Event()
        shutdown = self._node_shutdown
        stopped = self._node_stopped
        try:
            connection = self.crawler.connection
            self._connection = connection
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
                except connection.connection_errors:
                    if not self._reconnect(connection, shutdown):
                        break
                except SpiderShutdown as exc:
                    # Tider drains pidbox in a dedicated thread/greenlet, so translate
                    # it into the shared shutdown flag for the engine loop.
                    _maybe_set_state('should_stop', exc, EX_OK)
                    break
                except SpiderTerminate as exc:
                    _maybe_set_state('should_terminate', exc, EX_FAILURE)
                    break
        finally:
            # close channel inside the loop to avoid cross-greenlet races.
            self._close_channel()
            self._connection = None
            stopped.set()

    @staticmethod
    def on_decode_error(message, exc):
        logger.critical(MESSAGE_DECODE_ERROR,
                        exc, message.content_type, message.content_encoding,
                        safe_repr(message.headers), dump_body(message, message.body),
                        exc_info=True)
        message.ack()

    def on_stop(self):
        shutdown = self._node_shutdown
        stopped = self._node_stopped
        if stopped:
            if shutdown:
                shutdown.set()
            if self._thread is not threading.current_thread():
                # wait for loop-owned cleanup instead of closing from here.
                logger.debug('Waiting for broadcast thread to shutdown...')
                stopped.wait()
            self._thread = self._node_stopped = self._node_shutdown = None
        else:
            self._close_channel()

    def stop(self):
        self.on_stop()
        # noinspection PyNoneFunctionAssignment
        self.consumer = None

    def reset(self):
        # ask the loop to rebuild channel/consumer on its next iteration.
        self._resets += 1

    def _close_channel(self):
        connection = self._connection or self.crawler.connection
        if self.node and self.node.channel:
            ignore_errors(connection, self.node.channel.close)

    def shutdown(self):
        self.stop()


class gPidbox(Pidbox):
    """Crawler pidbox (greenlet)."""

    def start(self):
        if not self._prepare_loop_state():
            return
        from gevent import spawn_raw
        spawn_raw(self.loop)
