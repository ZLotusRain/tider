import os
import sys
import pprint
import logging
import threading
from uuid import uuid4
from operator import attrgetter

from kombu import Connection
from kombu.common import oid_from
from kombu.utils import cached_property

from tider import platforms
from tider._state import (_register_app, _deregister_app,
                          get_current_app, _set_current_app, set_default_app)
from tider.spiders import Spider
from tider.concurrency import get_implementation
from tider.settings import Settings, overridden_settings
from tider.utils.log import (
    configure_logging,
    get_tider_root_handler,
    install_tider_root_handler
)
from tider.utils.nodenames import gethostname, nodename
from tider.utils.misc import symbol_by_name, create_instance

__all__ = ('Tider',)

logger = logging.getLogger(__name__)

MP_MAIN_FILE = os.environ.get('MP_MAIN_FILE')


def uuid(_uuid=uuid4):
    """Generate unique id in UUID4 format.

    See Also:
        For now this is provided by :func:`uuid.uuid4`.
    """
    return str(_uuid())


def _unpickle(cls, kwargs):
    return cls(**kwargs)


def _unpickle_appattr(reverse_name, args):
    """Unpickle app."""
    # Given an attribute name and a list of args, gets
    # the attribute from the current app and calls it.
    return get_current_app()._rgetattr(reverse_name)(*args)


def appstr(app):
    """String used in __repr__ etc, to id app instances."""
    return f'{app.main or "__main__"} at {id(app):#x}'


def gen_spider_name(app, name, module_name, schema=None, spider_type='task'):
    """Generate task name from name/module pair."""
    module_name = module_name or '__main__'
    try:
        module = sys.modules[module_name]
    except KeyError:
        module = None

    if module is not None:
        module_name = module.__name__
        # - If the task module is used as the __main__ script
        # - we need to rewrite the module part of the task name
        # - to match App.main.
        if MP_MAIN_FILE and module.__file__ == MP_MAIN_FILE:
            # - see celery comment about :envvar:`MP_MAIN_FILE` above.
            module_name = '__main__'
    if module_name == '__main__' and app.main:
        module_name = app.main
    if spider_type != 'task':
        name = f"{name}[{spider_type}]"
    return '.'.join(p for p in (module_name, schema, name) if p)


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


class Tider:
    """Tider application.
    """

    SYSTEM = platforms.SYSTEM
    IS_macOS, IS_WINDOWS = platforms.IS_macOS, platforms.IS_WINDOWS

    #: Name of the `__main__` module.  Required for standalone scripts.
    #:
    #: If set this will be used instead of `__main__` when automatically
    #: generating task names.
    #: name: main.schema.spider[spider_type]@hostname
    main = None

    pool = 'threads'

    loader_cls = None
    engine_cls = 'tider.core.engine:HeartEngine'
    stats_cls = 'tider.statscollector:MemoryStatsCollector'
    alarm_cls = None
    control_cls = 'tider.core.control:Control'
    control_url = 'redis://localhost:6379/0'  # TODO set to None by default, use sleep in engine instead.

    def __init__(self, spider=None, spider_type='task', schema=None,
                 main=None, loader=None, stats=None, alarm=None, control=None,
                 set_as_current=True, settings=None, install_root_handler=True,
                 processes=0, pool=None, concurrency=None):

        self.spider = None
        self.main = main or self.main
        self._local = threading.local()

        if isinstance(spider, Spider):
            raise ValueError('The spider argument must be a class or str, not an object')

        self._spider_type = spider_type  # task | worker | publisher | consumer
        self.schema = schema
        self.settings = _evaluate_settings(settings)
        self.loader_cls = loader or self._get_spider_loader()
        self.set_as_current = set_as_current

        if isinstance(spider, str):  # spider unique name
            self.spidercls = self.spider_loader.load(spider)
        else:
            self.spidercls = spider
        # spidercls maybe None when using control or multi
        if self.spidercls is not None:
            # spider loader class will be ignored here
            self.spidercls.update_settings(self.settings)
            if self.settings.getbool('LOG_FILE_ENABLED') and not self.settings.get("LOG_FILE"):
                self.settings['LOG_FILE'] = f'{self.spidercls.name}.log'
        configure_logging(self.settings, install_root_handler)

        self.control_cls = control or self.control_cls
        self.alarm_cls = alarm or self._get_alarm()
        self.stats_cls = stats or self._get_stats()

        self.processes = processes  # concurrent workers
        self.pool = pool or self.settings.get('POOL') or self.pool
        if pool in ('eventlet', 'gevent'):
            self.is_green_pool = True
        else:
            self.is_green_pool = False
        self.concurrency = concurrency or self.settings.getint('CONCURRENCY')

        self._update_settings()

        if self.set_as_current:
            self.set_current()

        self.crawling = False
        self.on_init()
        _register_app(self)

    def on_init(self):
        """Optional callback called at init."""

    def set_current(self):
        """Make this the current app for this thread."""
        _set_current_app(self)

    def set_default(self):
        """Make this the default app for all threads."""
        set_default_app(self)

    def _get_spider_loader(self):
        return (
            self.settings.get('SPIDER_LOADER_CLASS') or
            self.loader_cls or
            'tider.spiderloader:SpiderLoader'
        )

    def _get_alarm(self):
        return (
            self.settings.get('ALARM_CLASS') or
            self.alarm_cls or
            'tider.alarm:Alarm'
        )

    def _get_stats(self):
        return (self.settings.get('STATS_CLASS') or
                self.alarm_cls)

    def _update_settings(self):
        self.settings.update({
            "ALARM_CLASS": self.alarm_cls,
            "STATS_CLASS": self.stats_cls,
            "SPIDER_LOADER_CLASS": self.loader_cls,
            "POOL": self.pool,
            "CONCURRENCY": self.concurrency
        }, priority='project')

    @property
    def thread_oid(self):
        """Per-thread unique identifier for this app."""
        try:
            return self._local.oid
        except AttributeError:
            self._local.oid = new_oid = oid_from(self, threads=True)
            return new_oid

    @property
    def spider_type(self):
        return self._spider_type

    @cached_property
    def stats(self):
        return symbol_by_name(self.stats_cls)(self)

    @cached_property
    def alarm(self):
        return create_instance(symbol_by_name(self.alarm_cls), tider=self)

    @cached_property
    def spider_loader(self):
        return symbol_by_name(self.loader_cls)(self.settings, self.schema)

    @property
    def spidername(self):
        return self.spider.name

    @cached_property
    def hostname(self):
        return gethostname()

    @property
    def nodename(self):
        name = self.spidername
        if self.spider_type == 'worker':
            name = f'{name}Worker{os.getpid()}'
        return nodename(name, self.hostname)

    def _rgetattr(self, path):
        return attrgetter(path)(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __repr__(self):
        return f'<{type(self).__name__} {appstr(self)}>'

    def subclass_with_self(self, Class, name=None, attribute='tider',
                           reverse=None, keep_reduce=False, **kw):
        """Subclass an tider-compatible class.

        Tider-compatible means that the class has a class attribute that
        provides the default tider it should use, for example:
        ``class Foo: tider = None``.

        Arguments:
            Class (type): The tider-compatible class to subclass.
            name (str): Custom name for the target class.
            attribute (str): Name of the attribute holding the app,
                Default is 'tider'.
            reverse (str): Reverse path to this object used for pickling
                purposes. For example, to get ``app.AsyncResult``,
                use ``"AsyncResult"``.
            keep_reduce (bool): If enabled a custom ``__reduce__``
                implementation won't be provided.
        """
        Class = symbol_by_name(Class)
        reverse = reverse if reverse else Class.__name__

        def __reduce__(self):
            return _unpickle_appattr, (reverse, self.__reduce_args__())

        attrs = dict(
            {attribute: self},
            __module__=Class.__module__,
            __doc__=Class.__doc__,
            **kw)
        if not keep_reduce:
            attrs['__reduce__'] = __reduce__

        return type(name or Class.__name__, (Class,), attrs)

    def connection_for_control(self, url=None, heartbeat=120, body_encoding='base64'):
        """Establish connection used for controlling.

         Returns:
            kombu.Connection: the lazy connection instance.
        """
        url = url or self.settings.get('CONTROL_URL') or self.control_url
        return self._connection(url, heartbeat=heartbeat, body_encoding=body_encoding)

    @staticmethod
    def _connection(url, heartbeat=None, body_encoding='base64'):
        return Connection(
            hostname=url,
            ssl=False,
            heartbeat=heartbeat,
            login_method=None,
            failover_strategy=None,
            transport_options={'body_encoding': body_encoding},
            connect_timeout=4
        )

    @cached_property
    def control(self):
        return symbol_by_name(self.control_cls)(tider=self)

    def autodiscover_spiders(self):
        return self.spider_loader.list()

    def __reduce__(self):
        return _unpickle, (self.__class__, self.__reduce_keys__())

    def __reduce_keys__(self):
        """Keyword arguments used to reconstruct the object when unpickling."""
        settings = self.settings.copy()
        settings.frozen = False
        return {
            'spider': self.spidercls,
            'spider_type': self._spider_type,
            'settings': settings,
            'schema': self.schema,  # spider class has already been loaded
            'install_root_handler': True,
            'loader': self.loader_cls,
            'control': self.control_cls,
            'alarm': self.alarm_cls,
            'stats': self.stats_cls,
            'pool': self.pool,
            'concurrency': self.concurrency,
            'processes': self.processes
        }

    @cached_property
    def engine(self):
        return self._create_engine()

    def _create_spider(self, *args, **kwargs):
        return self.spidercls.from_tider(self, *args, **kwargs)

    def _create_engine(self):
        return symbol_by_name(self.engine_cls)(tider=self)

    def create_pool(self, limit=None, **kwargs):
        limit = limit or self.concurrency
        return get_implementation(self.pool)(limit=limit, **kwargs)

    def crawl(self, *args, **kwargs):
        self.settings.freeze()
        d = dict(overridden_settings(self.settings))
        logger.info("Overridden settings:\n%(settings)s",
                    {'settings': pprint.pformat(d)})

        if self.processes > 1:
            from concurrent.futures import ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=self.processes) as executor:
                for _ in range(self.processes):
                    executor.submit(self._crawl, *args, **kwargs)
        else:
            return self._crawl(*args, **kwargs)

    def _crawl(self, *args, **kwargs):
        if get_tider_root_handler() is None:
            # re-initialize logging handlers in multiprocessing
            install_tider_root_handler(self.settings)
        if self.crawling:
            raise RuntimeError("Crawling already taking place")
        self.crawling = True

        try:
            self.spider = self._create_spider(*args, **kwargs)
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
        """Starts a graceful stop of the tider."""
        if self.crawling:
            self.crawling = False
            self.engine.stop()

    def close(self):
        """Clean up after the application.

        Only necessary for dynamically created apps, and you should
        probably use the :keyword:`with` statement instead.
        """

        _deregister_app(self)
