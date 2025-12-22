import time
import redis
import logging
import datetime
from functools import partial
from urllib.parse import unquote
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED

from kombu.utils.objects import cached_property

try:
    import redis.connection
    from kombu.transport.redis import get_redis_error_classes
except ImportError:
    redis = None
    get_redis_error_classes = None

from tider.backends.base import KeyValueStoreBackend
from tider.utils.url import url_to_parts
from tider.utils.time import humanize_seconds
from tider.utils.functional import retry_over_time, dictfilter
from tider.exceptions import ImproperlyConfigured, BackendStoreError

E_REDIS_MISSING = """
You need to install the redis library in order to use \
the Redis spider store backend.
"""

E_REDIS_LOST = 'Connection to Redis lost: Retry (%s/%s) %s.'

E_REDIS_SSL_PARAMS_AND_SCHEME_MISMATCH = """
SSL connection parameters have been provided but the specified URL scheme \
is redis://. A Redis SSL connection URL should use the scheme rediss://.
"""

E_REDIS_SSL_CERT_REQS_MISSING_INVALID = """
A rediss:// URL must have parameter ssl_cert_reqs and this must be set to \
CERT_REQUIRED, CERT_OPTIONAL, or CERT_NONE
"""

W_REDIS_SSL_CERT_OPTIONAL = """
Setting ssl_cert_reqs=CERT_OPTIONAL when connecting to redis means that \
tider might not validate the identity of the redis broker when connecting. \
This leaves you vulnerable to man in the middle attacks.
"""

W_REDIS_SSL_CERT_NONE = """
Setting ssl_cert_reqs=CERT_NONE when connecting to redis means that tider \
will not validate the identity of the redis broker when connecting. This \
leaves you vulnerable to man in the middle attacks.
"""

logger = logging.getLogger(__name__)


class RedisBackend(KeyValueStoreBackend):
    """Redis spider meta store like `redis://[:password]@host:port/db`."""

    #: Maximum number of connections in the pool.
    max_connections = None

    supports_autoexpire = True
    supports_native_join = True

    #: Maximal length of string value in Redis.
    #: 512 MB - https://redis.io/topics/data-types
    _MAX_STR_VALUE_SIZE = 536870912

    def __init__(self, host=None, port=None, db=None, password=None,
                 max_connections=None, url=None, connection_pool=None, **kwargs):
        super().__init__(expires_type=int, **kwargs)
        if redis is None:
            raise ImproperlyConfigured(E_REDIS_MISSING.strip())

        if host and '://' in host:
            url, host = host, None

        config = self.get_config('REDIS_SETTINGS', {})
        self.max_connections = (
            max_connections or self.get_config('REDIS_MAX_CONNECTIONS')
            or config.get('max_connections', self.max_connections)
        )
        self._ConnectionPool = connection_pool

        socket_timeout = self.get_config('REDIS_SOCKET_TIMEOUT') or config.get('socket_timeout')
        socket_connect_timeout = self.get_config('REDIS_SOCKET_CONNECT_TIMEOUT') or config.get('socket_connect_timeout')
        retry_on_timeout = self.get_config('REDIS_RETRY_ON_TIMEOUT') or config.get('retry_on_timeout')
        socket_keepalive = self.get_config('REDIS_SOCKET_KEEPALIVE') or config.get('socket_keepalive')
        health_check_interval = self.get_config('REDIS_HEALTH_CHECK_INTERVAL') or config.get('health_check_interval')

        self.connection_kwargs = {
            'host': host or self.get_config('REDIS_HOST') or config.get('host', 'localhost'),
            'port': port or self.get_config('REDIS_PORT') or config.get('port', 6379),
            'db': db or self.get_config('REDIS_DB') or config.get('db', 0),
            'password': password or self.get_config('REDIS_PASSWORD') or config.get('password'),
            'max_connections': self.max_connections,
            'socket_timeout': socket_timeout and float(socket_timeout),
            'retry_on_timeout': retry_on_timeout or False,
            'socket_connect_timeout':
                socket_connect_timeout and float(socket_connect_timeout),
            'encoding': self.get_config('REDIS_ENCODING') or config.get('encoding', 'utf-8'),
            'encoding_errors': self.get_config('REDIS_ENCODING_ERRORS') or config.get('encoding_errors', 'replace')
        }
        username = self.get_config('REDIS_USERNAME') or config.get('username')
        if username:
            # We're extra careful to avoid including this configuration value
            # if it wasn't specified since older versions of py-redis
            # don't support specifying a username.
            # Only Redis>6.0 supports username/password authentication.
            self.connection_kwargs['username'] = username
        if health_check_interval:
            self.connection_kwargs["health_check_interval"] = health_check_interval

        # absent in redis.connection.UnixDomainSocketConnection
        if socket_keepalive:
            self.connection_kwargs['socket_keepalive'] = socket_keepalive

        # "ssl_config" must be a dict with the keys:
        # 'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'
        ssl = self.get_config('REDIS_SSL_CONFIG') or config.get('ssl_config')
        if ssl:
            self.connection_kwargs.update(ssl)
            self.connection_kwargs['connection_class'] = redis.SSLConnection
        if url:
            self.connection_kwargs = self._params_from_url(url, self.connection_kwargs)

        # If we've received SSL parameters, check ssl_cert_reqs is valid. If set
        # via query string ssl_cert_reqs will be a string so convert it here
        if ('connection_class' in self.connection_kwargs and
                issubclass(self.connection_kwargs['connection_class'], redis.SSLConnection)):
            ssl_cert_reqs_missing = 'MISSING'
            ssl_string_to_constant = {'CERT_REQUIRED': CERT_REQUIRED,
                                      'CERT_OPTIONAL': CERT_OPTIONAL,
                                      'CERT_NONE': CERT_NONE,
                                      'required': CERT_REQUIRED,
                                      'optional': CERT_OPTIONAL,
                                      'none': CERT_NONE}
            ssl_cert_reqs = self.connection_kwargs.get('ssl_cert_reqs', ssl_cert_reqs_missing)
            ssl_cert_reqs = ssl_string_to_constant.get(ssl_cert_reqs, ssl_cert_reqs)
            if ssl_cert_reqs not in ssl_string_to_constant.values():
                raise ValueError(E_REDIS_SSL_CERT_REQS_MISSING_INVALID)

            if ssl_cert_reqs == CERT_OPTIONAL:
                logger.warning(W_REDIS_SSL_CERT_OPTIONAL)
            elif ssl_cert_reqs == CERT_NONE:
                logger.warning(W_REDIS_SSL_CERT_NONE)
            self.connection_kwargs['ssl_cert_reqs'] = ssl_cert_reqs

        self.url = url
        self._client = None
        self.connection_errors, _ = get_redis_error_classes() if get_redis_error_classes else ((), ())

    def _params_from_url(self, url, defaults):
        scheme, host, port, username, password, path, query = url_to_parts(url)
        connparams = dict(
            defaults, **dictfilter({
                'host': host, 'port': port, 'username': username,
                'password': password, 'db': query.pop('virtual_host', None)})
        )

        if scheme == 'socket':
            # use 'path' as path to the socket… in this case
            # the database number should be given in 'query'
            connparams.update({
                'connection_class': redis.UnixDomainSocketConnection,
                'path': '/' + path,
            })
            # host+port are invalid options when using this connection type.
            connparams.pop('host', None)
            connparams.pop('port', None)
            connparams.pop('socket_connect_timeout')
        else:
            connparams['db'] = path

        ssl_param_keys = ['ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile',
                          'ssl_cert_reqs']

        if scheme == 'redis':
            # If connparams or query string contain ssl params, raise error
            if (any(key in connparams for key in ssl_param_keys) or
                    any(key in query for key in ssl_param_keys)):
                raise ValueError(E_REDIS_SSL_PARAMS_AND_SCHEME_MISMATCH)

        if scheme == 'rediss':
            connparams['connection_class'] = redis.SSLConnection
            # The following parameters, if present in the URL, are encoded. We
            # must add the decoded values to connparams.
            for ssl_setting in ssl_param_keys:
                ssl_val = query.pop(ssl_setting, None)
                if ssl_val:
                    connparams[ssl_setting] = unquote(ssl_val)

        # db may be string and start with / like in kombu.
        db = connparams.get('db') or 0
        db = db.strip('/') if isinstance(db, str) else db
        connparams['db'] = int(db)

        for key, value in query.items():
            if key in redis.connection.URL_QUERY_ARGUMENT_PARSERS:
                query[key] = redis.connection.URL_QUERY_ARGUMENT_PARSERS[key](
                    value
                )

        # Query parameters override other parameters
        connparams.update(query)
        return connparams

    def exception_safe_to_retry(self, exc):
        if isinstance(exc, self.connection_errors):
            return True
        return False

    @cached_property
    def retry_policy(self):
        retry_policy = super().retry_policy
        if "retry_policy" in self._transport_options:
            retry_policy = retry_policy.copy()
            retry_policy.update(self._transport_options['retry_policy'])
        return retry_policy

    @cached_property
    def _transport_options(self):
        return self.get_config('TRANSPORT_OPTIONS', {})

    def _create_client(self, **params):
        return redis.StrictRedis(
            connection_pool=self._get_pool(**params),
        )

    def _get_pool(self, **params):
        return self.ConnectionPool(**params)

    @property
    def ConnectionPool(self):
        if self._ConnectionPool is None:
            self._ConnectionPool = redis.ConnectionPool
        return self._ConnectionPool

    @property
    def client(self):
        if self._client is None or not getattr(self._client, 'connection', None):
            self._client = self._create_client(**self.connection_kwargs)
        return self._client

    def close(self):
        if self._client is not None:
            self._client.close()
        if self._ConnectionPool is not None:
            self._ConnectionPool.disconnect()

    def ensure(self, fun, args, **policy):
        retry_policy = dict(self.retry_policy, **policy)
        max_retries = retry_policy.get('max_retries')
        return retry_over_time(
            fun, self.connection_errors, args, {},
            callback=partial(self.on_connection_error, max_retries),
        )

    def on_connection_error(self, max_retries, exc, intervals, retries):
        tts = next(intervals)
        logger.error(
            E_REDIS_LOST.strip(),
            retries, max_retries or 'Inf', humanize_seconds(tts, 'in '))
        if not self.client.ping():
            time.sleep(2)
            self.client.close()
        return tts

    def get(self, key):
        return self.ensure(self.client.get, args=(key, ))

    def mget(self, keys):
        return self.ensure(self.client.mget, args=(keys, ))

    def scan(self, pattern, count=1000):
        cursor = 0
        while True:
            cursor, keys = self.client.scan(cursor, match=pattern, count=1000)
            for key in keys:
                yield key
            if cursor == 0:
                break

    def set(self, key, value, **retry_policy):
        if isinstance(value, str) and len(value) > self._MAX_STR_VALUE_SIZE:
            raise BackendStoreError('value too large for Redis backend')
        return self.ensure(self._set, (key, value), **retry_policy)

    def _set(self, key, value):
        with self.client.pipeline() as pipe:
            if self.expires:
                pipe.setex(key, self.expires, value)
            else:
                pipe.set(key, value)
            pipe.publish(key, value)
            pipe.execute()

    def sadd(self, key, values):
        return self.ensure(self._sadd, args=(key, values))

    def _sadd(self, key, values):
        if isinstance(values, list):
            with self.client.pipeline() as pipe:
                pipe.multi()
                for value in values:
                    pipe.sadd(key, value)
        else:
            return self.client.sadd(key, values)

    def sget(self, key, count=1, pop=True):
        return self.ensure(self._sget, args=(key, count, pop))

    def _sget(self, key, count=1, pop=True):
        result = []
        if not pop:
            result = self.client.srandmember(key, count)
        else:
            set_len = self.client.scard(key)
            count = count if count <= set_len else set_len
            if count > 1:
                with self.client.pipeline() as pipe:
                    pipe.multi()
                    for _ in range(count):
                        pipe.spop(key)
                    result = pipe.execute()
            elif count:
                result.append(self.client.spop(key))
        return result

    def srem(self, key, values):
        return self.ensure(self._srem, args=(key, values))

    def _srem(self, key, values):
        if isinstance(values, list):
            with self.client.pipeline() as pipe:
                pipe.multi()
                for value in values:
                    pipe.srem(key, value)
                pipe.execute()
        else:
            self.client.srem(key, values)

    def zadd(self, key, values=None, scores=None, mapping=None):
        return self.ensure(self._zadd, args=(key, values, scores, mapping))

    def _zadd(self, key, values=None, scores=None, mapping=None):
        if not key:
            raise KeyError('Key must be a specific string, not blank.')
        if not mapping:
            if values and isinstance(values, list):
                scores = scores if isinstance(scores, list) else [scores] * len(values)
                if len(values) != len(scores):
                    raise ValueError("Can not map values with scores")
                mapping = dict(zip(values, scores))
            else:
                mapping = {values: scores}
        return self.client.zadd(key, mapping)

    def zget(self, key, count=1, pop=True):
        return self.ensure(self._zget, args=(key, count, pop))

    def _zget(self, key, count=1, pop=True):
        """less score, higher priority"""
        if not key:
            raise KeyError(f"Key must be a specific string, not blank.")
        start_pos = 0
        end_pos = count - 1 if count > 0 else count
        with self.client.pipeline() as pipe:
            pipe.multi()
            pipe.zrange(key, start_pos, end_pos)
            if pop:
                pipe.zremrangebyrank(key, start_pos, end_pos)
            results, *count = pipe.execute()
        return results

    def rpush(self, key, values):
        return self.ensure(self._rpush, args=(key, values))

    def _rpush(self, key, values):
        if isinstance(values, list):
            with self.client.pipeline() as pipe:
                pipe.multi()
                for value in values:
                    pipe.rpush(key, value)
                pipe.execute()
        else:
            return self.client.rpush(key, values)

    def rpop(self, key, count=1):
        return self.ensure(self._rpop, args=(key, count))

    def _rpop(self, key, count=1):
        result = []
        length = self.client.llen(key)
        count = count if count <= length else length
        if count > 1:
            with self.client.pipeline() as pipe:
                pipe.multi()
                for _ in range(count):
                    pipe.rpop(key)
                result = pipe.execute()
        elif count:
            result = self.client.rpop(key, count)
        return result

    def lpush(self, key, values):
        return self.ensure(self._lpush, args=(key, values))

    def _lpush(self, key, values):
        if isinstance(values, list):
            with self.client.pipeline() as pipe:
                pipe.multi()
                for value in values:
                    pipe.lpush(key, value)
                pipe.execute()
        else:
            return self.client.lpush(key, values)

    def lpop(self, key, count=1):
        return self.ensure(self._lpop, args=(key, count))

    def _lpop(self, key, count=1):
        result = None
        length = self.client.llen(key)
        count = count if count <= length else length
        if count > 1:
            with self.client.pipeline() as pipe:
                pipe.multi()
                for _ in range(count):
                    pipe.lpop(key)
                result = pipe.execute()
        elif count:
            result = self.client.lpop(key)
        return result

    def hset(self, key, field, value, expire=-1):
        with self.client.pipeline() as pipe:
            pipe.hset(key, field, value)
            if expire != -1:
                temp_expire = datetime.timedelta(days=0, seconds=expire)
                pipe.expire(key, temp_expire)
            pipe.execute()

    def hget(self, key, field, is_pop=False):
        if not is_pop:
            return self.client.hget(key, field)
        else:
            lua = """
            -- local key = KEYS[1]
            local field = ARGV[1]
            
            -- get values
            local datas = redis.call('hget', KEYS[1], field)
            
            -- delete values
            redis.call('hdel', KEYS[1], field)
            return datas
            """
            cmd = self.client.register_script(lua)
            res = cmd(keys=[key], args=[field])

            return res

    def hgetall(self, key):
        return self.client.hgetall(key)

    def hdel(self, key, *fields):
        self.client.hdel(key, *fields)

    def delete(self, keys):
        return self.ensure(self.client.delete, args=(keys, ))

    def incr(self, key):
        return self.client.incr(key)

    def expire(self, key, value):
        return self.ensure(self.client.expire, args=(key, value))

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return super().__reduce__(
            args, dict(kwargs, expires=self.expires, url=self.url))
