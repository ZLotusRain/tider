"""
Tider extension for collecting frame stats(from scrapy)
"""
import pprint
import logging

logger = logging.getLogger(__name__)


class StatsCollector:
    def __init__(self, crawler):
        self._dump = crawler.settings.getbool('STATS_DUMP')
        self._stats = {}

    def get_value(self, key, default=None):
        return self._stats.get(key, default)

    def get_stats(self):
        return self._stats

    def set_value(self, key, value):
        self._stats[key] = value

    def set_stats(self, stats):
        self._stats = stats

    def inc_value(self, key, count=1, start=0):
        d = self._stats
        d[key] = d.setdefault(key, start) + count

    def max_value(self, key, value):
        self._stats[key] = max(self._stats.setdefault(key, value), value)

    def min_value(self, key, value):
        self._stats[key] = min(self._stats.setdefault(key, value), value)

    def clear_stats(self):
        self._stats.clear()

    def close(self, spider, reason=None):
        ... if reason else ...
        if self._dump:
            logger.info("Dumping Tider stats:\n" + pprint.pformat(self._stats),
                        extra={'spider': spider})
        self._persist_stats(self._stats, spider)

    def _persist_stats(self, stats, spider):
        pass


class MemoryStatsCollector(StatsCollector):

    def __init__(self, crawler):
        super().__init__(crawler)
        self.spider_stats = {}

    def _persist_stats(self, stats, spider):
        self.spider_stats[spider.name] = stats


class DummyStatsCollector(StatsCollector):

    def get_value(self, key, default=None):
        return default

    def set_value(self, key, value):
        pass

    def set_stats(self, stats):
        pass

    def inc_value(self, key, count=1, start=0):
        pass

    def max_value(self, key, value):
        pass

    def min_value(self, key, value):
        pass


class InfluxStatsCollector:
    pass


class PrometheusStatsCollector:
    pass


class MongoStatsCollector:
    pass
