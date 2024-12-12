import sys
import threading
from datetime import datetime
from operator import attrgetter
from click.exceptions import Exit

from kombu import Connection
from kombu.clocks import LamportClock
from kombu.utils import cached_property

from tider import platforms
from tider.spiders import Spider
from tider._state import (_register_app, _deregister_app,
                          get_current_app, _set_current_app, set_default_app)
from tider.settings import Settings
from tider.utils.misc import symbol_by_name
from tider.utils.time import to_utc, timezone

__all__ = ('Tider',)


def _unpickle(cls, kwargs):
    return cls(**kwargs)


def _unpickle_appattr(reverse_name, args):
    """Unpickle app."""
    # noinspection PyProtectedMember
    # Given an attribute name and a list of args, gets
    # the attribute from the current app and calls it.
    return get_current_app()._rgetattr(reverse_name)(*args)


def appstr(app):
    """String used in __repr__ etc, to id app instances."""
    return f'{app.main or "__main__"} at {id(app):#x}'


class Tider:
    """Tider application.
    """

    SYSTEM = platforms.SYSTEM
    IS_macOS, IS_WINDOWS = platforms.IS_macOS, platforms.IS_WINDOWS

    #: Name of the `__main__` module.  Required for standalone scripts.
    #:
    #: If set this will be used instead of `__main__` when automatically
    #: generating task names.
    #: name: main.[schema.]spider[broker_type]@hostname
    main = None

    pool = 'threads'

    loader_cls = 'tider.spiderloader:SpiderLoader'
    control_cls = 'tider.crawler.control:Control'
    log_cls = 'tider.log:Logging'
    control_url = 'amqp://guest:guest@localhost//'

    crawlers = property(
        lambda self: self._crawlers,
        doc="Set of :class:`crawlers <tider.crawler.Crawler>` started by "
        ":meth:`crawl` and managed by this class.",
    )

    def __init__(self, main=None, loader=None, control=None, log=None, settings=None, set_as_current=True):

        self._local = threading.local()
        self.clock = LamportClock()

        self.main = main or self.main
        self.loader_cls = loader or self.loader_cls
        self.control_cls = control or self.control_cls
        self.log_cls = log or self.log_cls

        self.set_as_current = set_as_current

        self._config_source = settings
        self._conf = None

        if self.set_as_current:
            self.set_current()
        self._crawlers = set()
        # init broker to send message
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

    def _load_config(self):
        if isinstance(self._config_source, dict) or self._config_source is None:
            settings = Settings(self._config_source)
        elif isinstance(self._config_source, str):
            settings = Settings()
            settings.setmodule(module=self._config_source)
        else:
            settings = self._config_source
        if not isinstance(settings, Settings):
            raise ValueError(f"settings must be a `Settings|dict|str` instance or None, "
                             f"got {type(settings)}: {settings!r}")
        return settings.copy()

    @property
    def conf(self):
        """Current configuration."""
        if self._conf is None:
            self._conf = self._load_config()
        return self._conf

    @cached_property
    def spider_loader(self):
        return symbol_by_name(self.loader_cls)(self.conf.copy())

    @cached_property
    def log(self):
        """Logging: :class:`~@log`."""
        return symbol_by_name(self.log_cls)(app=self)

    # noinspection PyPep8Naming
    @cached_property
    def Crawler(self):
        return self.subclass_with_self('tider.crawler.base:Crawler', attribute='app')

    def _rgetattr(self, path):
        return attrgetter(path)(self)

    def load(self, spider: str, schema='default'):
        return self.spider_loader.load(spider, schema=schema)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __repr__(self):
        return f'<{type(self).__name__} {appstr(self)}>'

    def setup_security(self):
        pass

    # noinspection PyPep8Naming
    def subclass_with_self(self, Class, name=None, attribute='app',
                           reverse=None, keep_reduce=False, **kw):
        """Subclass a tider-compatible class.

        Tider-compatible means that the class has a class attribute that
        provides the default tider it should use, for example:
        ``class Foo: app = None``.

        Arguments:
            Class (type,str): The tider-compatible class to subclass.
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

    def connection_for_control(self, url=None, heartbeat=120, heartbeat_checkrate=3.0, **kwargs):
        """Establish connection used for controlling.

         Returns:
            kombu.Connection: the lazy connection instance.
        """
        return Connection(
            hostname=url or self.conf.get('CONTROL_URL'),
            ssl=False,
            heartbeat=heartbeat,
            heartbeat_checkrate=heartbeat_checkrate,
            login_method=None,
            failover_strategy=None,
            transport_options={},
            connect_timeout=4,
            **kwargs
        )

    @cached_property
    def control(self):
        return symbol_by_name(self.control_cls)(app=self)

    def autodiscover_spiders(self, schema=None):
        """
        Return a dict with the names and schemas of all spiders available
        in the project if no schema supplied else return a list with the names
        of all spiders available in the specified schema.
        """
        if schema:
            return self.spider_loader.list(schema=schema)
        return self.spider_loader.list_all()

    def __reduce__(self):
        return _unpickle, (self.__class__, self.__reduce_keys__())

    def __reduce_keys__(self):
        """Keyword arguments used to reconstruct the object when unpickling."""
        return {
            'main': self.main,
            'loader': self.loader_cls,
            'control': self.control_cls,
            'log': self.log_cls,
            'settings': self._config_source,
            'set_as_current': self.set_as_current,
        }

    def start(self, argv=None):
        """Run :program:`tider` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.
        """
        from tider.bin.tider import tider

        tider.params[0].default = self

        if argv is None:
            argv = sys.argv

        try:
            tider.main(args=argv, standalone_mode=False)
        except Exit as e:
            return e.exit_code
        finally:
            tider.params[0].default = None

    def crawl_main(self, argv=None):
        """Run :program:`tider crawl` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.
        """
        if argv is None:
            argv = sys.argv

        if 'crawl' not in argv:
            raise ValueError(
                "The crawl sub-command must be specified in argv.\n"
                "Use tider.start() to programmatically start other commands."
            )

        self.start(argv=argv)

    def crawl(self, crawler_or_spidercls, schema='default', *args, **kwargs):
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                "The spider argument cannot be a spider object, "
                "it must be a spider class (or a spider name)"
            )
        crawler = self.create_crawler(crawler_or_spidercls, schema)
        return self._crawl(crawler, *args, **kwargs)

    def _crawl(self, crawler, *args, **kwargs):
        self.crawlers.add(crawler)
        crawler.crawl(*args, **kwargs)
        self.crawlers.discard(crawler)

    def create_crawler(self, crawler_or_spidercls, schema='default'):
        """
        Return a :class:`~tider.crawler.Crawler` object.
        * If ``crawler_or_spidercls`` is a Crawler, it is returned as-is.
        * If ``spider`` is a Spider subclass, a new Crawler
          is constructed for it.
        * If ``spider`` is a string, this function finds
          a spider with this name in a Tider project (using spider loader),
          then creates a Crawler instance for it.
        """
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                "The crawler_or_spidercls argument cannot be a spider object, "
                "it must be a spider class (or a Crawler object)"
            )
        if isinstance(crawler_or_spidercls, self.Crawler):
            return crawler_or_spidercls
        return self._create_crawler(crawler_or_spidercls, schema)

    def _create_crawler(self, spidercls, schema='default'):
        schema = schema or 'default'
        if isinstance(spidercls, str):
            spidercls = self.load(spidercls, schema=schema)
        return self.Crawler(spidercls=spidercls, schema=schema)

    def stop(self):
        return [c.stop() for c in list(self.crawlers)]

    def close(self):
        """Clean up after the application.

        Only necessary for dynamically created apps, and you should
        probably use the :keyword:`with` statement instead.
        """
        self.stop()
        _deregister_app(self)

    def now(self):
        """Return the current time and date as a datetime."""
        now_in_utc = to_utc(datetime.utcnow())
        return now_in_utc.astimezone(self.timezone)

    def uses_utc_timezone(self):
        """Check if the application uses the UTC timezone."""
        return self.timezone == timezone.utc

    @cached_property
    def timezone(self):
        """Current timezone for this app.

        This is a cached property taking the time zone from the
        :setting:`timezone` setting.
        """
        conf = self.conf
        if not conf.get('TIMEZONE'):
            if conf.get('ENABLE_UTC'):
                return timezone.utc
            else:
                return timezone.local
        return timezone.get_timezone(conf.get('TIMEZONE'))
