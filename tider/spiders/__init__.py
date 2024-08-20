import logging
import weakref
from typing import Optional


class Spider:

    name: Optional[str] = None
    custom_settings: Optional[dict] = None

    def __init__(self, name=None, **kwargs):
        if name:
            self.name = name
        elif not self.name:
            self.name = self.__class__.__name__

        self.__dict__.update(kwargs)
        if not hasattr(self, 'start_urls'):
            self.start_urls = []

        if not hasattr(self, 'settings'):
            self.spider_config = {}
        else:
            self.spider_config = self.settings.get("SPIDER_CONFIG", {})
        self.kwargs = kwargs

    @property
    def logger(self):
        logger = logging.getLogger(self.name)
        return logging.LoggerAdapter(logger, {'spider': self})

    def log(self, message, level=logging.DEBUG, **kw):
        """Log the given message at the given log level

        This helper wraps a log call to the logger within the spider, but you
        can use it directly (e.g. Spider.logger.info('msg')) or use any other
        Python logger too.
        """
        self.logger.log(level, message, **kw)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider.initial(*args, **kwargs)
        return spider

    def _set_crawler(self, crawler):
        if isinstance(crawler, weakref.ProxyType):
            self.crawler = crawler
        else:
            self.crawler = weakref.proxy(crawler)
        self.settings = crawler.settings

    def start_requests(self, **kwargs):
        from tider.network.request import Request  # avoid conflicts with monkey patch
        for url in self.start_urls:
            yield Request(url, dup_check=False)

    def initial(self, *args, **kwargs):
        self.on_init(*args, **kwargs)

    def on_init(self, *args, **kwargs):
        pass

    def parse(self, response):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')

    def process(self, message):
        pass

    @classmethod
    def update_settings(cls, settings):
        settings.setdict(cls.custom_settings or {}, priority='spider')

    def close(self, reason=None):
        self.on_close(reason)
        del self.crawler

    def on_close(self, reason=None):
        pass

    def __str__(self):
        return f"<{type(self).__name__} {self.name!r} at 0x{id(self):0x}>"

    __repr__ = __str__
