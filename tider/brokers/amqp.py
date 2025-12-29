"""Sending/Receiving Messages (Kombu integration)."""
import errno
import json
import redis
import socket
import threading
from time import time
from functools import partial
from uuid import uuid4
from queue import Empty
from types import MethodType
from collections.abc import Mapping
from weakref import WeakValueDictionary

from redis.exceptions import ResponseError
from kombu import Consumer, Exchange, Queue
from kombu.transport.virtual.base import Message, Channel
from kombu.connection import Connection as KombuConnection
from kombu.common import ignore_errors
from kombu.log import get_logger as k_get_logger
from kombu.utils.json import dumps, loads
from kombu.utils.encoding import safe_repr, bytes_to_str
from kombu.utils.functional import maybe_list
from kombu.utils.objects import cached_property
from kombu.utils.compat import detect_environment

from tider import signals
from tider.brokers import Broker
from tider.utils.log import get_logger
from tider.utils.crypto import set_md5
from tider.utils.time import humanize_seconds, preferred_clock
from tider.utils.text import truncate, indent as textindent

__all__ = ('AMQPBroker', )

logger = get_logger(__name__)

#: Human readable queue declaration.
QUEUE_FORMAT = """
.> {0.name:<16} exchange={0.exchange.name}({0.exchange.type}) \
key={0.routing_key}
"""

CONNECTION_RETRY_STEP = """\
Trying again {when}... ({retries}/{max_retries})\
"""

CONNECTION_ERROR = """\
consumer: Cannot connect to %s: %s.
%s
"""

CONNECTION_FAILOVER = """\
Will retry using next failover.\
"""

UNKNOWN_FORMAT = """\
Received and deleted unknown message.  Wrong destination?!?

Caused by: %r

The full contents of the message body was: %s
"""

MESSAGE_DECODE_ERROR = """\
Can't decode message body: %r [type:%r encoding:%r headers:%s]

body: %s
"""

MESSAGE_REPORT = """\
body: {0}
{{content_type:{1} content_encoding:{2}
  delivery_info:{3} headers={4}}}
"""

k_logger = k_get_logger('kombu.transport.redis')
crit, warn = k_logger.critical, logger.warn


def dump_body(m, body):
    """Format message body for debugging purposes."""
    body = m.body if body is None else body
    return '{} ({}b)'.format(truncate(safe_repr(body), 1024),
                             len(m.body))


def _quick_drain(connection, timeout=0.1):
    try:
        connection.drain_events(timeout=timeout)
    except Exception as exc:  # pylint: disable=broad-except
        exc_errno = getattr(exc, 'errno', None)
        if exc_errno is not None and exc_errno != errno.EAGAIN:
            raise


def _do_restore_message(self, payload, exchange, routing_key,
                        pipe, leftmost=False):
    try:
        try:
            payload['headers']['redelivered'] = True
            payload['properties']['delivery_info']['redelivered'] = True
        except KeyError:
            pass
        for queue in self._lookup(exchange, routing_key):
            # only push body.
            (pipe.lpush if leftmost else pipe.rpush)(
                queue, dumps(payload['body']),
            )
    except Exception:
        crit('Could not restore message: %r', payload, exc_info=True)


def _redis_brpop_read(self, **options):
    try:
        try:
            dest__item = self.client.parse_response(self.client.connection,
                                                    'BRPOP',
                                                    **options)
        except self.connection_errors:
            # if there's a ConnectionError, disconnect so the next
            # iteration will reconnect automatically.
            self.client.connection.disconnect()
            raise
        if dest__item:
            dest, item = dest__item
            dest = bytes_to_str(dest).rsplit(self.sep, 1)[0]
            self._queue_cycle.rotate(dest)
            item = bytes_to_str(item)
            try:
                item = loads(item)
            except json.JSONDecodeError:
                pass
            self.connection._deliver(item, dest)
            return True
        else:
            raise Empty()
    finally:
        self._in_poll = None


def qos_append(self, message, delivery_tag):
    """append message to unacked_key if using ack after consumed from queues."""
    delivery = message.delivery_info
    EX, RK = delivery['exchange'], delivery['routing_key']

    # Redis-py changed the format of zadd args in v3.0.0
    zadd_args = [{delivery_tag: time()}]

    with self.channel.conn_or_acquire() as client:
        with client.pipeline() as pipe:
            try:
                pipe.zadd(self.unacked_index_key, *zadd_args) \
                    .hset(self.unacked_key, delivery_tag,
                          dumps([message._raw, EX, RK])) \
                    .execute()
            except redis.ResponseError as e:
                warn(f'Saving unacked in-memory message failed by redis pipeline, '
                     f'try using redis client directly instead, reason: {e}')
                client.zadd(self.unacked_index_key, *zadd_args)
                client.hset(self.unacked_key, delivery_tag, dumps([message._raw, EX, RK]))
        if self._dirty:
            self._flush()
        self._quick_append(delivery_tag, message)


def qos_restore_by_tag(self, tag, client=None, leftmost=False):

    def restore_transaction(pipe, use_client=False):
        p = pipe.hget(self.unacked_key, tag)
        if not use_client:
            pipe.multi()
            self._remove_from_indices(tag, pipe)
        else:
            client.zrem(self.unacked_index_key, tag)
            client.hdel(self.unacked_key, tag)
        if p:
            M, EX, RK = loads(bytes_to_str(p))  # json is unicode
            self.channel._do_restore_message(M, EX, RK, pipe, leftmost)

    with self.channel.conn_or_acquire(client) as client:
        try:
            client.transaction(restore_transaction, self.unacked_key)
        except redis.ResponseError as e:
            warn(f'Restoring unacked in-memory message failed by redis pipeline, '
                 f'try using redis client directly instead, reason: {e}')
            restore_transaction(pipe=client, use_client=True)


def create_channel(self, connection, raw_create_channel):
    if not raw_create_channel:
        return
    chan = raw_create_channel(connection)
    # tricky way to process messages from non-kombu queues.
    chan.__class__.__bases__ = (RawMessageChannel,)
    chan.Message = RawMessage  # patch again to void overwriting
    # type(chan).mro()[1].basic_consume = MethodType(basic_consume, chan)
    # RawMessageChannel.__dict__ = type(chan).mro()[1].__dict__
    # type(chan).mro()[1] = type(class_.__name__, (class_,), attrs)
    if hasattr(chan, '_do_restore_message'):
        # redis transport
        # Pipelines do not work in cluster mode the same way they do in normal mode.
        chan._do_restore_message = MethodType(_do_restore_message, chan)
        chan._brpop_read = MethodType(_redis_brpop_read, chan)
        chan.handlers['BRPOP'] = chan._brpop_read
        chan.qos.append = MethodType(qos_append, chan.qos)
        chan.qos.restore_by_tag = MethodType(qos_restore_by_tag, chan.qos)

    return chan


class RawMessage(Message):
    def __init__(self, payload, channel=None, queue=None, **kwargs):
        if 'body' in payload and 'delivery_tag' in payload.get('properties', {}):
            payload = payload
        else:
            payload = {
                'body': payload,
                'properties': {
                    'delivery_tag': str(uuid4()),
                    'delivery_info': {'exchange': None, 'routing_key': queue}
                },
                'content-type': None,
            }
            if 'topic' in payload['body']:
                payload['topic'] = payload['body']['topic']

        self.topic = payload.get('topic')
        super(RawMessage, self).__init__(payload, channel, **kwargs)


class RawMessageChannel(Channel):

    Message = RawMessage

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        """Consume from `queue`."""
        self._tag_to_queue[consumer_tag] = queue
        self._active_queues.append(queue)

        def _callback(raw_message):
            message = self.Message(raw_message, channel=self, queue=queue)
            if not no_ack:
                self.qos.append(message, message.delivery_tag)
            return callback(message)

        self.connection._callbacks[queue] = _callback
        self._consumers.add(consumer_tag)

        self._reset_cycle()

    def _restore(self, message):
        """Redeliver message to its original destination."""
        delivery_info = message.delivery_info
        message = message.serializable()
        message['redelivered'] = True
        for queue in self._lookup(
            delivery_info['exchange'],
                delivery_info['routing_key']):
            self._put(queue, message.body)


class Connection(KombuConnection):

    def create_transport(self):
        transport = super().create_transport()
        raw_create_channel = getattr(transport, 'create_channel', None)
        transport.create_channel = MethodType(
            partial(create_channel, raw_create_channel=raw_create_channel), transport)
        return transport


class Queues(dict):
    """Queue name⇒ declaration mapping.

    Arguments:
        queues (Iterable): Initial list/tuple or dict of queues.
        create_missing (bool): By default any unknown queues will be
            added automatically, but if this flag is disabled the occurrence
            of unknown queues in `wanted` will raise :exc:`KeyError`.
        max_priority (int): Default x-max-priority for queues with none set.
    """

    #: If set, this is a subset of queues to consume from.
    #: The rest of the queues are then used for routing only.
    _consume_from = None

    def __init__(self, queues=None, default_exchange=None,
                 create_missing=True, autoexchange=None,
                 max_priority=None, default_routing_key=None):
        super().__init__()
        self.aliases = WeakValueDictionary()
        self.default_exchange = default_exchange
        self.default_routing_key = default_routing_key
        self.create_missing = create_missing
        self.autoexchange = Exchange if autoexchange is None else autoexchange
        self.max_priority = max_priority
        if queues is not None and not isinstance(queues, Mapping):
            queues = {q.name: q for q in queues}
        queues = queues or {}
        for name, q in queues.items():
            self.add(q) if isinstance(q, Queue) else self.add_compat(name, **q)

    def __getitem__(self, name):
        try:
            return self.aliases[name]
        except KeyError:
            return super().__getitem__(name)

    def __setitem__(self, name, queue):
        if self.default_exchange and not queue.exchange:
            queue.exchange = self.default_exchange
        super().__setitem__(name, queue)
        if queue.alias:
            self.aliases[queue.alias] = queue

    def __missing__(self, name):
        if self.create_missing:
            return self.add(self.new_missing(name))
        raise KeyError(name)

    def add(self, queue, **kwargs):
        """Add new queue.

        The first argument can either be a :class:`kombu.Queue` instance,
        or the name of a queue.  If the former the rest of the keyword
        arguments are ignored, and options are simply taken from the queue
        instance.

        Arguments:
            queue (kombu.Queue, str): Queue to add.
            exchange (kombu.Exchange, str):
                if queue is str, specifies exchange name.
            routing_key (str): if queue is str, specifies binding key.
            exchange_type (str): if queue is str, specifies type of exchange.
            **options (Any): Additional declaration options used when
                queue is a str.
        """
        if not isinstance(queue, Queue):
            return self.add_compat(queue, **kwargs)
        return self._add(queue)

    def add_compat(self, name, **options):
        # docs used to use binding_key as routing key
        options.setdefault('routing_key', options.get('binding_key'))
        if options['routing_key'] is None:
            options['routing_key'] = name
        return self._add(Queue.from_dict(name, **options))

    def _add(self, queue):
        if queue.exchange is None or queue.exchange.name == '':
            queue.exchange = self.default_exchange
        if not queue.routing_key:
            queue.routing_key = self.default_routing_key
        if self.max_priority is not None:
            if queue.queue_arguments is None:
                queue.queue_arguments = {}
            self._set_max_priority(queue.queue_arguments)
        self[queue.name] = queue
        return queue

    def _set_max_priority(self, args):
        if 'x-max-priority' not in args and self.max_priority is not None:
            return args.update({'x-max-priority': self.max_priority})

    def format(self, indent=0, indent_first=True):
        """Format routing table into string for log dumps."""
        active = self.consume_from
        if not active:
            return ''
        info = [QUEUE_FORMAT.strip().format(q)
                for _, q in sorted(active.items())]
        if indent_first:
            return textindent('\n'.join(info), indent)
        return info[0] + '\n' + textindent('\n'.join(info[1:]), indent)

    def select_add(self, queue, **kwargs):
        """Add new task queue that'll be consumed from.

        The queue will be active even when a subset has been selected
        using the :option:`tider crawl -Q` option.
        """
        q = self.add(queue, **kwargs)
        if self._consume_from is not None:
            self._consume_from[q.name] = q
        return q

    def select(self, include):
        """Select a subset of currently defined queues to consume from.

        Arguments:
            include (Sequence[str], str): Names of queues to consume from.
        """
        if include:
            self._consume_from = {
                name: self[name] for name in maybe_list(include)
            }

    def deselect(self, exclude):
        """Deselect queues so that they won't be consumed from.

        Arguments:
            exclude (Sequence[str], str): Names of queues to avoid
                consuming from.
        """
        if exclude:
            exclude = maybe_list(exclude)
            if self._consume_from is None:
                # using all queues
                return self.select(k for k in self if k not in exclude)
            # using selection
            for queue in exclude:
                self._consume_from.pop(queue, None)

    def new_missing(self, name):
        return Queue(name, self.autoexchange(name), name)

    @property
    def consume_from(self):
        if self._consume_from is not None:
            return self._consume_from
        return self


class AMQPBroker(Broker):
    """AMQP broker based on Kombu."""

    Connection = Connection
    Consumer = Consumer

    queues_cls = Queues

    #: Underlying producer pool instance automatically
    #: set by the :attr:`producer_pool`.
    _producer_pool = None

    # Exchange class/function used when defining automatic queues.
    # For example, you can use ``autoexchange = lambda n: None`` to use the
    # AMQP default exchange: a shortcut to bypass routing
    # and instead send directly to the queue named in the routing key.
    autoexchange = None

    #: This flag will be turned off after the first failed
    #: connection attempt.
    first_connection_attempt = True

    def __init__(self, queues=None, *args, **kwargs):
        super(AMQPBroker, self).__init__(*args, **kwargs)

        # this connection won't establish, only used for params
        self._conninfo = self._connection(url=self.crawler.settings.get('BROKER_URL'))
        self.connection_errors = self._conninfo.connection_errors
        if getattr(self.crawler.Pool, 'is_green', False):
            self.amqheartbeat = self.crawler.settings.get('BROKER_HEARTBEAT')
        else:
            self.amqheartbeat = 0
        self.amqheartbeat_rate = self.crawler.settings.get('BROKER_HEARTBEAT_CHECKRATE')  # reserved

        self.queues = self.queues_cls(queues=None, default_exchange=self.default_exchange)
        queues = queues or []
        for queue in queues:
            self.queues.add(queue)

        self.hub = None  # reserved
        self._stopped = threading.Event()

    @cached_property
    def default_exchange(self):
        return Exchange(self.crawler.settings.get('BROKER_DEFAULT_EXCHANGE'),
                        self.crawler.settings.get('BROKER_DEFAULT_EXCHANGE_TYPE'))

    def should_use_eventloop(self):
        return (detect_environment() == 'default' and
                self._conninfo.transport.implements.asynchronous and
                not self.crawler.app.IS_WINDOWS)

    def on_connection_error_before_connected(self, exc):
        logger.error(CONNECTION_ERROR, self._conninfo.as_uri(), exc,
                     'Trying to reconnect...')

    def _connection(self, url, userid=None, password=None,
                    virtual_host=None, port=None, ssl=None,
                    connect_timeout=None, transport=None,
                    transport_options=None, body_encoding=None,
                    heartbeat=None, login_method=None, failover_strategy=None):
        settings = self.crawler.settings
        ssl = ssl if ssl is not None else settings.get('BROKER_USE_SSL')
        connect_timeout = connect_timeout if connect_timeout is not None else settings.get('BROKER_CONNECTION_TIMEOUT')

        transport_options = dict(
            settings.get('BROKER_TRANSPORT_OPTIONS'), **transport_options or {}
        )
        # https://docs.celeryq.dev/en/latest/userguide/routing.html#redis-message-priorities
        # don't brpop from more keys to avoid brpop error when using redis cluster.
        transport_options.setdefault('priority_steps', [0])
        # set body_encoding to None instead of base64 by default.
        body_encoding = body_encoding or settings.get('BROKER_BODY_ENCODING')
        transport_options.update({'body_encoding': body_encoding})

        return self.Connection(
            url,
            userid or settings.get('BROKER_USER'),
            password or settings.get('BROKER_PASSWORD'),
            virtual_host or settings.get('BROKER_VHOST'),
            port or settings.get('BROKER_PORT'),
            transport=transport or settings.get('BROKER_TRANSPORT'),
            ssl=ssl,
            heartbeat=heartbeat,
            login_method=login_method or settings.get('BROKER_LOGIN_METHOD'),
            failover_strategy=(
                failover_strategy or settings.get('BROKER_FAILOVER_STRATEGY')
            ),
            transport_options=transport_options,
            connect_timeout=connect_timeout,
        )

    def connect(self, transport_options=None):
        """Establish the broker connection used for consuming tasks.

        Retries establishing the connection if the
        :setting:`broker_connection_retry` setting is enabled
        """
        conn = self.connection_for_read(heartbeat=self.amqheartbeat, transport_options=transport_options)
        if self.hub:
            conn.transport.register_with_event_loop(conn.connection, self.hub)
        return conn

    def connection_for_read(self, heartbeat=None, **kwargs):
        url = self.crawler.settings.get('BROKER_URL') or self.crawler.settings.get('BROKER_HOST')
        return self.ensure_connected(
            self._connection(url, heartbeat=heartbeat, **kwargs))

    def connection_for_write(self, heartbeat=None, **kwargs):
        url = self.crawler.settings.get('BROKER_URL') or self.crawler.settings.get('BROKER_HOST')
        return self.ensure_connected(
            self._connection(self.crawler.settings.get('BROKER_WRITE_URL') or url, heartbeat=heartbeat, **kwargs))

    def ensure_connected(self, conn):
        # Callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval, next_step=CONNECTION_RETRY_STEP):
            if getattr(conn, 'alt', None) and interval == 0:
                next_step = CONNECTION_FAILOVER
            next_step = next_step.format(
                when=humanize_seconds(interval, 'in', ' '),
                retries=int(interval / 2),
                max_retries=self.crawler.settings.get('BROKER_CONNECTION_MAX_RETRIES'))
            logger.error(CONNECTION_ERROR, conn.as_uri(), exc, next_step)

        # Remember that the connection is lazy, it won't establish
        # until needed.

        if self.first_connection_attempt:
            retry_disabled = not self.crawler.settings.get('BROKER_CONNECTION_RETRY_ON_STARTUP')
        else:
            retry_disabled = not self.crawler.settings.getbool('BROKER_CONNECTION_RETRY')

        if retry_disabled:
            # Retry disabled, just call connect directly.
            conn.connect()
            self.first_connection_attempt = False
            return conn

        # whether to check crawler running state here.
        conn = conn.ensure_connection(
            _error_handler, self.crawler.settings.get('BROKER_CONNECTION_MAX_RETRIES'),
        )
        self.first_connection_attempt = False
        return conn

    def MessageConsumer(self, channel, queues=None, accept=None, **kw):
        if accept is None:
            accept = self.crawler.settings.get('BROKER_ACCEPT_CONTENT')
        no_ack = kw.pop('no_ack', None) or self.crawler.settings.getbool('BROKER_NO_ACK')
        return self.Consumer(
            channel, accept=accept, prefetch_count=self.crawler.concurrency * 10,
            queues=queues, no_ack=no_ack, **kw
        )

    def _message_report(self, body, message):
        return MESSAGE_REPORT.format(dump_body(message, body),
                                     safe_repr(message.content_type),
                                     safe_repr(message.content_encoding),
                                     safe_repr(message.delivery_info),
                                     safe_repr(message.headers))

    def on_message_received(self, message, on_message=None, decode=False):
        """process raw message"""
        on_unknown_message = self.on_unknown_message

        try:
            if decode:
                payload = message.decode()
            else:
                payload = message.body
        except Exception as exc:  # pylint: disable=broad-except
            return self.on_decode_error(message, exc)

        try:
            if on_message is not None:
                on_message(message=message, payload=payload, no_ack=False)
            else:
                logger.warning(f"Message acked but with no handlers: {dump_body(message, message.body)}")
                message.ack()
        except Exception as e:
            on_unknown_message(payload, message, e)

    def on_unknown_message(self, body, message, error):
        logger.warning(UNKNOWN_FORMAT, error, self._message_report(body, message))
        message.reject_log_error(logger, self.connection_errors)
        signals.message_rejected.send(sender=self, message=message, exc=None)

    def on_decode_error(self, message, exc):
        """Callback called if an error occurs while decoding a message.

        Simply logs the error and acknowledges the message, so it
        doesn't enter a loop.

        Arguments:
            message: The message received.
            exc (Exception): The exception being handled.
        """
        logger.exception(
            MESSAGE_DECODE_ERROR,
            exc, message.content_type, message.content_encoding,
            safe_repr(message.headers), dump_body(message, message.body),
        )

        message.ack()

    def _restore_messages(self, consumer):
        chan = consumer.channel

        if not hasattr(chan, '_do_restore_message'):
            return

        def restore_transaction(pipe, use_client=False):
            data = pipe.hgetall(chan.unacked_key)
            not use_client and pipe.multi()
            # don't use multi here to avoid keys of command in MULTI calls must be in same slot.
            # pipe.multi()
            count = 0
            for tag, P in data.items():
                tag = bytes_to_str(tag)
                if not P:
                    continue
                M, EX, RK = loads(bytes_to_str(P))  # json is unicode
                if RK:
                    try:
                        # delete before successfully restore.
                        if not use_client:
                            pipe.zrem(chan.unacked_index_key, tag).hdel(chan.unacked_key, tag)
                        else:
                            pipe.zrem(chan.unacked_index_key, tag)
                            pipe.hdel(chan.unacked_key, tag)
                        # lpush if leftmost else rpush.
                        chan._do_restore_message(M, EX, RK, pipe, leftmost=False)
                        count += 1
                    except redis.exceptions.ResponseError:
                        logger.critical('Could not restore message: %r', M, exc_info=True)
            logger.info(f'Restored {count}/{len(data)} message(s) from {chan.unacked_key}')

        # restore at start.
        with chan.conn_or_acquire() as client:
            try:
                client.transaction(restore_transaction, chan.unacked_key)
            except Exception as e:
                logger.warning(f'Restoring messages from {chan.unacked_key} failed by redis pipeline, '
                               f'try using redis client directly instead, reason: {e}')
                restore_transaction(pipe=client, use_client=True)

    def consume(self, queues=None, on_message=None, on_message_consumed=None):
        on_decode_error = self.on_decode_error
        on_message = partial(self.on_message_received, on_message=on_message)

        if isinstance(queues, Queues):
            queues = list(queues.consume_from.values())
        key = "_".join([queue.name for queue in queues])
        key = set_md5(key)
        # no need to set polling_interval
        transport_options = {
            'unacked_restore_limit': None,  # maybe restore messages
            'unacked_key': f'{key}.unacked',
            'unacked_index_key': f'{key}.unacked_index',
            'max_connections': self.crawler.concurrency,
        }
        connection = self.connect(transport_options=transport_options)
        consumer = self.MessageConsumer(channel=connection, queues=queues,
                                        accept=None, on_decode_error=on_decode_error)
        consumer.on_message = on_message
        consumer.consume()
        # restore unacked messages from memory on startup.
        # consumer.recover(requeue=True)
        self._restore_messages(consumer)

        logged = False
        if getattr(connection.transport, 'driver_type', None):
            should_rebalance = connection.transport.driver_type in ('kafka', 'confluentkafka')
        else:
            should_rebalance = self.crawler.broker_transport in ('kafka', 'confluentkafka')
        response_errors = (ResponseError, )
        elapsed = 0
        try:
            while not self._stopped.is_set():
                try:
                    connection.drain_events(timeout=2.0)
                    logged = False
                    elapsed = 0
                except socket.timeout:
                    on_message_consumed and on_message_consumed()
                    if not logged:
                        logger.info(f"No messages fetched or reached prefetch limit, "
                                    f"prefetch count: {consumer.prefetch_count}, "
                                    f"queues: {','.join([queue.name for queue in consumer.queues])}")
                    logged = True
                    consumer.channel.qos._flush()  # clear dirty.
                    if should_rebalance:
                        if not elapsed:
                            elapsed = preferred_clock()
                        elif preferred_clock() - elapsed > 60 * 5:
                            elapsed = 0
                            logger.info('No messages fetched, try rebalancing...')
                            ignore_errors(connection, consumer.close)
                            connection.release()

                            connection = self.connect(transport_options=transport_options)
                            consumer = self.MessageConsumer(channel=connection, queues=queues,
                                                            accept=None, on_decode_error=on_decode_error)
                            consumer.on_message = on_message
                            consumer.consume()
                            self._restore_messages(consumer)
                except connection.connection_errors + response_errors as e:
                    if isinstance(e, ResponseError):
                        info = consumer.channel.client.info(section='replication')
                        if info.get('role') != 'slave':
                            raise
                    logger.warning('Broker connection has broken, try reconnecting...')
                    ignore_errors(connection, consumer.close)
                    connection.release()

                    connection = self.connect(transport_options=transport_options)  # change settings if needed.
                    consumer = self.MessageConsumer(channel=connection, queues=queues,
                                                    accept=None, on_decode_error=on_decode_error)
                    consumer.on_message = on_message
                    consumer.consume()
                    self._restore_messages(consumer)
                    on_message_consumed and on_message_consumed()
                except OSError:
                    if self.crawler.crawling:
                        raise
        finally:
            ignore_errors(connection, consumer.close)
            connection.close()

    def stop(self):
        self._stopped.set()
        super().stop()
