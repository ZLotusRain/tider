"""Spider Remote Control Client inspired by Celery.

Client for spider remote control commands.
Server implementation is in :mod:`tider.core.control.panel`.
There are two types of remote control commands:

* Inspect commands: Does not have side effects, will usually just return some value
  found in the spider, like the settings of currently running spider, etc.
  Commands are accessible via :class:`Inspect` class.

* Control commands: Performs side effects, like shutdown spider.
  Commands are accessible via :class:`Control` class.
"""
import logging
import warnings
from kombu.matcher import match
from kombu.pidbox import Mailbox
from kombu.utils import cached_property
from kombu.utils.compat import register_after_fork

from tider.utils.text import pluralize
from tider.exceptions import DuplicateSpiderWarning

logger = logging.getLogger(__name__)

DUPESPIDER = """\
Received multiple replies from spider {0}: {1}.
Please make sure you give each spider a unique name using
the tider crawl `-n` option.\
"""


def flatten_reply(reply):
    """Flatten node replies.

    Convert from a list of replies in this format::

        [{'a@example.com': reply},
         {'b@example.com': reply}]

    into this format::

        {'a@example.com': reply,
         'b@example.com': reply}
    """
    nodes, dupes = {}, set()
    for item in reply:
        [dupes.add(name) for name in item if name in nodes]
        nodes.update(item)
    if dupes:
        warnings.warn(DuplicateSpiderWarning(
            DUPESPIDER.format(
                pluralize(len(dupes), 'name'), ', '.join(sorted(dupes)),
            ),
        ))
    return nodes


def _after_fork_cleanup_control(control):
    try:
        # noinspection PyProtectedMember
        control._after_fork()
    except Exception as exc:  # pylint: disable=broad-except
        logger.info('after fork raised exception: %r', exc, exc_info=True)


class Inspect:
    """API for inspecting spiders."""

    tider = None

    def __init__(self, destination=None, timeout=1.0, callback=None,
                 connection=None, tider=None, limit=None, pattern=None,
                 matcher=None):
        self.tider = tider or self.tider
        self.destination = destination
        self.timeout = timeout
        self.callback = callback
        self.connection = connection
        self.limit = limit
        self.pattern = pattern
        self.matcher = matcher

    def _prepare(self, reply):
        if reply:
            by_node = flatten_reply(reply)
            if (self.destination and
                    not isinstance(self.destination, (list, tuple))):
                return by_node.get(self.destination)
            if self.pattern:
                pattern = self.pattern
                matcher = self.matcher
                return {node: reply for node, reply in by_node.items()
                        if match(node, pattern, matcher)}
            return by_node

    def _request(self, command, **kwargs):
        return self._prepare(self.tider.control.broadcast(
            command,
            arguments=kwargs,
            destination=self.destination,
            callback=self.callback,
            connection=self.connection,
            limit=self.limit,
            timeout=self.timeout, reply=True,
            pattern=self.pattern, matcher=self.matcher,
        ))

    def settings(self):
        return self._request('settings')

    def stats(self):
        """Return statistics of spider."""
        return self._request('stats')

    def engine(self):
        """Return statistics of spider."""
        return self._request('engine')

    def ping(self, destination=None):
        if destination:
            self.destination = destination
        return self._request('ping')


class Control:

    def __init__(self, tider=None):
        self.tider = tider
        self.mailbox = Mailbox(
            'tidercontrol',
            type='fanout',
            accept=('json', ),
            queue_ttl=300.0,
            reply_queue_ttl=300.0,
            queue_expires=10.0,
            reply_queue_expires=10.0
        )
        register_after_fork(self, _after_fork_cleanup_control)

    def _after_fork(self):
        delattr(self.mailbox, '_producer_pool')

    @cached_property
    def inspect(self):
        """Create new :class:`Inspect` instance."""
        return self.tider.subclass_with_self(Inspect, reverse='control.inspect')

    def broadcast(self, command, arguments=None, destination=None,
                  connection=None, reply=False, timeout=1.0, limit=None,
                  callback=None, channel=None, pattern=None, matcher=None,
                  **extra_kwargs):
        with connection or self.tider.connection_for_control() as conn:
            arguments = dict(arguments or {}, **extra_kwargs)
            if pattern and matcher:
                # tests pass easier without requiring pattern/matcher to
                # always be sent in
                # noinspection PyProtectedMember
                return self.mailbox(conn)._broadcast(
                    command, arguments, destination, reply, timeout,
                    limit, callback, channel=channel,
                    pattern=pattern, matcher=matcher,
                )
            else:
                # noinspection PyProtectedMember
                return self.mailbox(conn)._broadcast(
                    command, arguments, destination, reply, timeout,
                    limit, callback, channel=channel,
                )
