import threading
from gevent import spawn_raw
from redis.lock import Lock

from tider.backends.redis import RedisBackend

__all__ = ('WatchDogLock', )


class RedisLock(RedisBackend):

    config_namespace = 'LOCK'


class WatchDogLock(Lock):

    name = 'tider-crawler-lock'

    def __init__(self, crawler, name=None, timeout=None, sleep=0, blocking=None,
                 blocking_timeout=None, thread_local=None, extend_timeout=None):
        self.crawler = crawler
        self._backend = RedisLock(crawler=crawler)
        name = name or self.crawler.settings.get('LOCK_NAME') or self.name
        timeout = timeout or self.crawler.settings.get('LOCK_TIMEOUT')
        sleep = sleep or self.crawler.settings.get('LOCK_SLEEP', 0.1)
        if blocking is None:
            blocking = self.crawler.settings.get('LOCK_BLOCKING', True)
        blocking_timeout = blocking_timeout or self.crawler.settings.get('LOCK_BLOCK_TIMEOUT')
        if thread_local is None:
            thread_local = self.crawler.settings.get('LOCK_THREAD_LOCAL', True)
        if extend_timeout is None:
            extend_timeout = self.crawler.settings.get('LOCK_EXTEND_TIMEOUT', True)
        super().__init__(redis=self._backend.client, name=name, timeout=timeout, sleep=sleep,
                         blocking=blocking, blocking_timeout=blocking_timeout, thread_local=thread_local)
        if timeout and extend_timeout:
            self.extend_timout = True
        else:
            self.extend_timout = False
        self._stop_extending = False

    def _watch(self):
        while not self._stop_extending:
            self.extend(additional_time=self.timeout, replace_ttl=False)
            self.crawler.sleep(self.timeout // 3)

    def _start_watch_dog(self):
        if getattr(self.crawler.Pool, 'is_green', False):
            t = threading.Thread(target=self._watch, daemon=True)
            t.start()
        else:
            spawn_raw(self._watch)

    def acquire(self, sleep=None, blocking=None, blocking_timeout=None, token=None) -> bool:
        result = super().acquire(sleep=sleep, blocking=blocking, blocking_timeout=blocking_timeout, token=token)
        if result and self.extend_timout:
            self._start_watch_dog()
        return result

    def do_release(self, expected_token: bytes) -> None:
        self._stop_extending = True
        return super().do_release(expected_token)

    def __del__(self):
        self._backend.close()
        self._stop_extending = True
