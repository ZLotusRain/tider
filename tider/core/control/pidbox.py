"""Spider Pidbox (remote control)."""
import logging
from contextlib import suppress
from kombu.clocks import LamportClock
from kombu.utils.encoding import safe_str, safe_repr

from tider.utils.text import truncate
from tider.utils.collections import AttributeDict
from tider.core.control.panel import Panel

logger = logging.getLogger(__name__)

MESSAGE_DECODE_ERROR = """\
Can't decode message body: %r [type:%r encoding:%r headers:%s]

body: %s
"""


def dump_body(m, body):
    """Format message body for debugging purposes."""
    # v2 protocol does not deserialize body
    body = m.body if body is None else body
    return '{} ({}b)'.format(truncate(safe_repr(body), 1024),
                             len(m.body))


class Pidbox:
    """Spider mailbox."""

    consumer = None

    def __init__(self, tider):
        self.tider = tider
        self.hostname = tider.nodename
        self.node = tider.control.mailbox.Node(
            safe_str(self.hostname),
            handlers=Panel.data,
            state=AttributeDict(
                tider=tider,
                hostname=tider.hostname)
        )
        self.clock = LamportClock()
        self._forward_clock = self.clock.forward

    def on_message(self, body, message):
        # just increase clock as clients usually don't
        # have a valid clock to adjust with.
        self._forward_clock()
        try:
            self.node.handle_message(body, message)
        except KeyError as exc:
            logger.error('No such control command: %s', exc)
        except Exception as exc:
            logger.error('Control command error: %r', exc, exc_info=True)
            self.reset()

    def start(self):
        self.node.channel = self.tider.engine.connection.channel()
        self.consumer = self.node.listen(callback=self.on_message)
        self.consumer.consume()  # if needed
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
            with suppress(Exception):
                self.node.channel.close()

    def shutdown(self):
        if self.consumer:
            logger.debug('Canceling broadcast consumer...')
            with suppress(Exception):
                self.consumer.cancel()
        self.stop()
