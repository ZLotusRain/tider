import logging
from typing import Optional
from weakref import proxy, ProxyType

from tider import Response


class Spider:

    name: Optional[str] = None
    custom_settings: Optional[dict] = None

    def __init__(self, name=None, **kwargs):
        if name is not None:
            self.name = name  # only affects node name in control if set here.
        elif not self.name:
            self.name = self.__class__.__name__

        self.__dict__.update(kwargs)
        if not hasattr(self, 'start_urls'):
            self.start_urls = []

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
    def from_tider(cls, tider, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_tider(tider)
        spider.initialize()
        return spider

    def _set_tider(self, tider):
        self.tider = tider if isinstance(tider, ProxyType) else proxy(tider)
        self.settings = tider.settings

    def start_requests(self, **kwargs):
        from tider.network.request import Request  # avoid conflicts with monkey patch
        for url in self.start_urls:
            yield Request(url, dup_check=False)

    def initialize(self):
        self.on_init()

    def on_init(self):
        pass

    def parse(self, response: Response):
        raise NotImplementedError(f'{self.__class__.__name__}.parse callback is not defined')

    def process(self, message):
        pass

    @classmethod
    def update_settings(cls, settings):
        settings.setdict(cls.custom_settings or {}, priority='spider')

    def close(self, reason=None):
        self.on_close(reason)
        del self.tider

    def on_close(self, reason=None):
        pass

    def __str__(self):
        return f"<{type(self).__name__} {self.name!r} at 0x{id(self):0x}>"

    __repr__ = __str__
