import pprint
import socket
import logging
import platform

from tider.spiders import Spider
from tider.core.engine import HeartEngine
from tider.settings import Settings, overridden_settings
from tider.utils.log import configure_logging
from tider.utils.decorators import cached_property
from tider.utils.misc import symbol_by_name, create_instance

logger = logging.getLogger(__name__)


def _evaluate_settings(settings):
    if isinstance(settings, dict) or settings is None:
        settings = Settings(settings)
    elif isinstance(settings, str):
        settings_module = settings
        settings = Settings()
        settings.setmodule(module=settings_module)
    if not isinstance(settings, Settings):
        raise ValueError(f"settings must be a `Settings|dict|str` instance or None, "
                         f"got {type(settings)}: {settings!r}")
    return settings.copy()


class Crawler:

    SYSTEM = platform.system()
    IS_macOS = SYSTEM == 'Darwin'
    IS_WINDOWS = SYSTEM == 'Windows'

    def __init__(self, spidercls, settings=None, spider_type='task'):
        if isinstance(spidercls, Spider):
            raise ValueError('The spidercls argument must be a class, not an object')

        settings = _evaluate_settings(settings)
        self.spidercls = spidercls
        self.settings = settings.copy()
        self.spidercls.update_settings(self.settings)
        self.settings.freeze()
        self._spider_type = spider_type  # task | worker | publisher

        configure_logging(self.settings, install_root_handler=True)
        d = dict(overridden_settings(self.settings))
        logger.info("Overridden settings:\n%(settings)s",
                    {'settings': pprint.pformat(d)})

        alarm_cls = symbol_by_name(self.settings['ALARM_CLASS'])
        self.alarm = create_instance(alarm_cls, settings=self.settings, crawler=self)
        self.stats = symbol_by_name(self.settings['STATS_CLASS'])(self)

        self.spider = None
        self.engine = None
        self.crawling = False

    @property
    def spider_type(self):
        return self._spider_type

    @property
    def spidername(self):
        return self.spider.name

    @cached_property
    def hostname(self):
        return socket.gethostname()

    def _create_spider(self, *args, **kwargs):
        return self.spidercls.from_tider(self, *args, **kwargs)

    def _create_engine(self):
        return HeartEngine(self)

    def crawl(self, *args, **kwargs):
        if self.crawling:
            raise RuntimeError("Crawling already taking place")
        self.crawling = True

        try:
            self.spider = self._create_spider(*args, **kwargs)
            self.engine = self._create_engine()
            self.engine.open_spider(self.spider)
            self.engine.start()
        except Exception:
            self.crawling = False
            if self.engine is not None:
                self.engine.close()
            raise
        else:
            self.stop()

    def stop(self):
        """Starts a graceful stop of the crawler."""
        if self.crawling:
            self.crawling = False
            self.engine.stop()


class CrawlerProcess:

    crawlers = property(
        lambda self: self._crawlers,
        doc="Set of :class:`crawlers <tider.crawler.Crawler>` started by "
            ":meth:`crawl` and managed by this class."
    )

    def __init__(self, settings=None, schema=None, spider_type='task', concurrency=0):
        self.settings = _evaluate_settings(settings)
        self._load_custom_default_settings()
        self.schema = schema

        self.spider_type = spider_type
        self.concurrency = concurrency   # concurrent workers
        self.spider_loader = self._get_spider_loader(self.settings, self.schema)

        self._crawlers = set()

    @staticmethod
    def _get_spider_loader(settings, schema):
        cls_path = settings.get('SPIDER_LOADER_CLASS')
        loader_cls = symbol_by_name(cls_path)
        return loader_cls(settings, schema)

    def _load_custom_default_settings(self):
        custom_default_settings_module = self.settings.get("CUSTOM_DEFAULT_SETTINGS_MODULE")
        if custom_default_settings_module:
            self.settings.setmodule(custom_default_settings_module, priority='default')
        if 'CUSTOM_DEFAULT_SETTINGS_MODULE' in self.settings:
            self.settings.delete("CUSTOM_DEFAULT_SETTINGS_MODULE")

    def update_setting(self, key, value, priority='project'):
        self.settings.set(key, value, priority)

    def create_crawler(self, crawler_or_spidercls):
        """
        Return a :class:`~tider.crawler.Crawler` object.

        * If ``crawler_or_spidercls`` is a Crawler, it is returned as-is.
        * If ``crawler_or_spidercls`` is a Spider subclass, a new Crawler
          is constructed for it.
        * If ``crawler_or_spidercls`` is a string, this function finds
          a spider with this name in a Tider project (using spider loader),
          then creates a Crawler instance for it.
        """
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                'The crawler_or_spidercls argument cannot be a spider object, '
                'it must be a spider class (or a Crawler object)')
        if isinstance(crawler_or_spidercls, Crawler):
            return crawler_or_spidercls
        return self._create_crawler(crawler_or_spidercls)

    def _create_crawler(self, spidercls):
        settings = self.settings.copy()
        if isinstance(spidercls, str):
            spidercls, spider_settings = self.spider_loader.load(spidercls)
            settings.update(spider_settings, priority='project')
        if settings.getbool('LOG_FILE_ENABLED') and not settings.get("LOG_FILE"):
            settings['LOG_FILE'] = f'{spidercls.name or spidercls.__name__}.log'
        return Crawler(spidercls, settings, spider_type=self.spider_type)

    def crawl(self, crawler_or_spidercls, *args, **kwargs):
        """
        Run a crawler with the provided arguments.

        :param crawler_or_spidercls: already created crawler, or a spider class
            or spider's name inside the project to create it
        :type crawler_or_spidercls: :class:`~tider.crawler.Crawler` instance,
            :class:`~tider.spiders.Spider` subclass or string

        :param args: arguments to initialize the spider

        :param kwargs: keyword arguments to initialize the spider
        """
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                'The crawler_or_spidercls argument cannot be a spider object, '
                'it must be a spider class (or a Crawler object)')
        if self.concurrency > 1:
            # # multiprocess will recreate the runtime environment
            from concurrent.futures.process import ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=self.concurrency) as executor:
                for _ in range(self.concurrency):
                    crawler = self.create_crawler(crawler_or_spidercls)
                    executor.submit(self._crawl, crawler, *args, **kwargs)
        else:
            crawler = self.create_crawler(crawler_or_spidercls)
            return self._crawl(crawler, *args, **kwargs)

    def _crawl(self, crawler, *args, **kwargs):
        self.crawlers.add(crawler)
        crawler.crawl(*args, **kwargs)
        self.crawlers.remove(crawler)

    def stop(self):
        for c in list(self.crawlers):
            c.stop()
