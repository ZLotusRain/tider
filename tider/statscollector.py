"""
Crawler extension for collecting frame stats(from scrapy).
"""
import pprint
from typing import List

from tider.mail import MailSender
from tider.utils.log import get_logger
from tider.exceptions import ImproperlyConfigured, HTTPError

logger = get_logger(__name__)


class StatsCollector:

    def __init__(self, crawler):
        self._dump = crawler.settings.getbool('STATS_DUMP')
        self._stats = {}
        self._failures = {}  # failed requests.
        self._errors = {}  # parsing errors.
        self._results = {}

    def get_value(self, key, default=None):
        return self._stats.get(key, default)

    def get_stats(self):
        if self._failures:
            self._stats.update(failures=self.get_failures())
        if self._errors:
            self._stats.update(errors=self.get_errors())
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

    def get_failures(self):
        return self._failures

    def add_failure(self, failure):
        try:
            failure.check_error()
        except Exception as e:
            if isinstance(e, HTTPError):
                reason = f"Bad status: {failure.status_code}"
            else:
                reason = str(e)
            if reason not in self._failures:
                self._failures[reason] = []
            if failure.request.url not in self._failures[reason]:
                self._failures[reason].append(failure.request.url)

    def get_errors(self):
        return self._errors

    def add_error(self, reason, request_or_response):
        if isinstance(reason, KeyError):
            reason = f"KeyError: {reason}"
        else:
            reason = str(reason)
        if reason not in self._errors:
            self._errors[reason] = []
        if request_or_response.url not in self._errors[reason]:
            self._errors[reason].append(request_or_response.url)

    def close_spider(self, spider, reason):
        if reason != 'finished':
            self._stats.update(reason=reason)
        if self._failures:
            self._stats.update(failures=self.get_failures())
        if self._errors:
            self._stats.update(errors=self.get_errors())
        if self._dump:
            logger.info("Dumping Tider stats:\n" + pprint.pformat(self._stats),
                        extra={'spider': spider})
        self._persist_stats(self._stats, spider)

    def _persist_stats(self, stats, spider):
        pass


class StatsMailer(StatsCollector):

    def __init__(self, crawler):
        super().__init__(crawler)
        recipients: List[str] = crawler.settings.getlist("STATSMAILER_RCPTS")
        if not recipients:
            raise ImproperlyConfigured
        self.recipients: List[str] = recipients
        self._mail = MailSender.from_settings(crawler.settings)

    def _persist_stats(self, stats, spider):
        body = f"{spider.name} stats\n\n"
        body += "\n".join(f"{k:<50} : {v}" for k, v in self.get_stats().items())
        self._mail.send(self.recipients, f"Tider stats for: {spider.name}", body)
        self._mail.close()


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
