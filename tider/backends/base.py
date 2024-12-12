from datetime import timedelta

from kombu.utils.functional import LRUCache
from kombu.serialization import prepare_accept_content
from kombu.serialization import registry as serializer_registry


class _nulldict(dict):
    def ignore(self, *a, **kw):
        pass

    __setitem__ = update = setdefault = ignore


class Backend:

    def __init__(self, crawler, serializer=None, max_cached_results=None, accept=None,
                 expires=None, expires_type=None, url=None):
        self.crawler = crawler
        settings = self.crawler.settings
        self.serializer = serializer or settings.get('RESULT_SERIALIZER')
        (self.content_type,
         self.content_encoding,
         self.encoder) = serializer_registry._encoders[self.serializer]
        cmax = max_cached_results or settings.get('RESULT_CACHE_MAX')
        self._cache = _nulldict() if cmax == -1 else LRUCache(limit=cmax)

        self.expires = self.prepare_expires(expires, expires_type)

        # precedence: accept, conf.result_accept_content, conf.accept_content
        self.accept = settings.get("RESULT_ACCEPT_CONTENT") if accept is None else accept
        self.accept = settings.get("ACCEPT_CONTENT") if self.accept is None else self.accept
        self.accept = prepare_accept_content(self.accept)

        self.thread_safe = settings.get('RESULT_BACKEND_THREAD_SAFE', False)

        self.url = url

    def prepare_expires(self, value, exp_type=None):
        if value is None:
            value = self.crawler.settings.get('RESULT_EXPIRES')
        if isinstance(value, timedelta):
            value = value.total_seconds()
        if value is not None and exp_type:
            return exp_type(value)
        return value

    def save_stats(self, spider_id, spider_name, stats, schema=None):
        """Store the stats of an executed spider."""
        return self._save_stats(spider_id, spider_name, stats, schema)
