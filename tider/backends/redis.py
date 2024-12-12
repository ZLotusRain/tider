import time
import redis
import logging
import datetime

from tider.utils.functional import retry_over_time

E_REDIS_SSL_PARAMS_AND_SCHEME_MISMATCH = """
SSL connection parameters have been provided but the specified URL scheme \
is redis://. A Redis SSL connection URL should use the scheme rediss://.
"""

logger = logging.getLogger(__name__)


class RedisBackend:
    """
    redis://[:password]@host:port/db
    """

    #: Maximum number of connections in the pool.
    max_connections = None

    #: Maximal length of string value in Redis.
    #: 512 MB - https://redis.io/topics/data-types
    _MAX_STR_VALUE_SIZE = 536870912

    def __init__(self, url=None, max_connections=None, **connection_kwargs):
        _get = connection_kwargs.get

        self.url = url
        self.max_connections = max_connections or self.max_connections

        socket_timeout = _get('redis_socket_timeout')
        socket_connect_timeout = _get('redis_socket_connect_timeout')
        retry_on_timeout = _get('redis_retry_on_timeout')

        self.connection_kwargs = {
            'host': _get('host') or 'localhost',
            'port': _get('port') or 6379,
            'db': _get('redis_db') or _get('db') or _get('index') or 0,
            'password': _get('redis_password') or _get('auth') or _get('password'),
            'max_connections': self.max_connections,
            'socket_timeout': socket_timeout and float(socket_timeout),
            'retry_on_timeout': retry_on_timeout or False,
            'socket_connect_timeout':
                socket_connect_timeout and float(socket_connect_timeout),
            'encoding': _get('encoding') or 'utf-8',
            'encoding_errors': _get('encoding_errors') or 'replace'
        }

        self.client = self._create_client(**self.connection_kwargs)
    
    def _create_client(self, **params):
        if self.url:
            return redis.StrictRedis(
                connection_pool=redis.ConnectionPool.from_url(url=self.url, **params),
            )
        else:
            return redis.StrictRedis(
                connection_pool=self._get_pool(**params),
            )
    
    @staticmethod
    def _get_pool(**params):
        return redis.ConnectionPool(**params)

    def close(self):
        self.client.close()

    def _maybe_reconnect(self):
        if not self.client.ping():
            time.sleep(2)
            self.client.close()
            self.client = self._create_client(**self.connection_kwargs)

    def ensure(self, fun, *args, **kwargs):
        return retry_over_time(
            fun, Exception, args, kwargs,
            callback=self._maybe_reconnect
        )

    def sadd(self, key, values):
        return self.ensure(self._sadd, key, values)

    def _sadd(self, key, values):
        if isinstance(values, list):
            with self.client.pipeline() as pipe:
                pipe.multi()
                for value in values:
                    pipe.sadd(key, value)
        else:
            return self.client.sadd(key, values)

    def sget(self, key, count=1, pop=True):
        return self.ensure(self._sget, key, count, pop)

    def _sget(self, key, count=1, pop=True):
        result = []
        if not pop:
            result = self.client.srandmember(key, count)
        else:
            count = count if count <= self.get_set_length(key) else self.get_set_length(key)
            if count > 1:
                with self.client.pipeline() as pipe:
                    pipe.multi()
                    for _ in range(count):
                        pipe.spop(key)
                    result = pipe.execute()
            elif count:
                result.append(self.client.spop(key))
        return result

    def smembers(self, key):
        return self.ensure(self.client.smembers, key)

    def srem(self, key, values):
        return self.ensure(self._srem, key, values)

    def _srem(self, key, values):
        if isinstance(values, list):
            with self.client.pipeline() as pipe:
                pipe.multi()
                for value in values:
                    pipe.srem(key, value)
                pipe.execute()
        else:
            self.client.srem(key, values)

    def get_set_length(self, key):
        return self.client.scard(key)

    def zadd(self, key, values=None, scores=None, mapping=None):
        return self.ensure(self.zadd, key, values, scores, mapping)

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
        return self.ensure(self._zget, key, count, pop)

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

    def get_zset_length(self, key):
        return self.ensure(self._zset_length, key)

    def _zset_length(self, key):
        if not key:
            raise KeyError(f"Key must be a specific string, not blank.")
        return self.client.zcard(key)

    def rpush(self, key, values):
        return self.ensure(self._rpush, key, values)

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
        return self.ensure(self._rpop, key, count)

    def _rpop(self, key, count=1):
        result = []
        count = count if count <= self.get_list_length(key) else self.get_list_length(key)
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
        return self.ensure(self._lpush, key, values)

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
        return self.ensure(self._lpop, key, count)

    def _lpop(self, key, count=1):
        result = None
        count = count if count <= self.get_list_length(key) else self.get_list_length(key)
        if count > 1:
            with self.client.pipeline() as pipe:
                pipe.multi()
                for _ in range(count):
                    pipe.lpop(key)
                result = pipe.execute()
        elif count:
            result = self.client.lpop(key)
        return result

    def get_list_length(self, key):
        return self.client.llen(key)

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

    def delete(self, key):
        self.client.delete(key)

    def incr(self, key):
        return self.client.incr(key)

    def expire(self, key, value):
        return self.client.expire(key, value)

    def publish(self, channel, message):
        return self.ensure(self.client.publish, channel, message)

    def pubsub(self, **kwargs):
        return self.client.pubsub(**kwargs)
