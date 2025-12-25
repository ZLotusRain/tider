import logging
import warnings
from typing import Optional
from weakref import proxy

from tider import Response
from tider.utils.log import get_logger
from tider.network.user_agent import default_user_agent


class Spider:

    name: Optional[str] = None
    custom_settings: Optional[dict] = None

    default_ua: Optional[str] = None

    def __init__(self, name=None, **kwargs):
        if name is not None:
            self.name = name  # only affects node name in control if set here.
        elif not getattr(self, "name", None):
            warnings.warn(f"{type(self).__name__} doesn't have a name, use class name instead.")
            self.name = self.__class__.__name__

        self.meta = {"failures": [], "errors": []}  # unprocessed or uncaught
        self.meta.update(kwargs.pop('meta', {}))
        self.__dict__.update(kwargs)
        if not hasattr(self, 'start_urls'):
            self.start_urls = []
        self.default_ua = self.default_ua or default_user_agent()

    @property
    def logger(self) -> logging.LoggerAdapter:
        logger = get_logger(self.name)
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
        spider._initialize()
        return spider

    def _set_crawler(self, crawler):
        from tider.crawler.base import Crawler
        # self.crawler: Crawler | ProxyType[Crawler]
        # use proxy will use lower memory.
        self.crawler: Crawler = crawler  # type hint
        self.crawler = proxy(self.crawler)
        self.settings = crawler.settings

    def start_requests(self, **kwargs):
        from tider.network.request import Request  # avoid conflicts with monkey patch
        for url in self.start_urls:
            yield Request(url, dup_check=False)

    def _initialize(self):
        self.on_init()

    def on_init(self):
        pass

    def parse(self, response: Response):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')

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
