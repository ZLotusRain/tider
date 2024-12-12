import os
import sys
import time
import psutil
import socket
import pprint
import platform as _platform
from typing import Optional, Type
from datetime import datetime
from functools import partial
from billiard.common import REMAP_SIGTERM
from billiard.process import current_process

from kombu.utils import cached_property
from kombu.utils.encoding import safe_str

from tider import Tider, __version__
from tider import platforms, signals
from tider.spiders import Spider
from tider.settings import Settings, overridden_settings
from tider.crawler import state
from tider.crawler.control import Pidbox
from tider.concurrency import get_implementation
from tider.platforms import EX_FAILURE, EX_OK, create_pidlock
from tider.network.user_agent import set_default_ua
from tider.utils.log import get_logger, in_sighandler, set_in_sighandler
from tider.utils.misc import symbol_by_name, build_from_crawler
from tider.utils.nodenames import gethostname, nodename, nodesplit, default_nodename

try:
    import resource
except ImportError:
    resource = None

logger = get_logger(__name__)

socket.setdefaulttimeout(10)

is_jython = sys.platform.startswith('java')
is_pypy = hasattr(sys, 'pypy_version_info')

BANNER = """\
{hostname} v{version}

{platform} {timestamp}

[overridden settings]
{settings}
"""


def safe_say(msg):
    print(f'\n{msg}', file=sys.__stderr__, flush=True)


class Extensions:

    def __init__(self, crawler, extensions=None):
        self._crawler = crawler
        self._extensions = {}
        extensions = extensions or {}
        for extension, path in extensions.items():
            self._load_extension(extension, path)

    def __getattr__(self, name):
        if name.startswith("_"):
            return super().__getattribute__(name)
        if name not in self._extensions:
            raise AttributeError("No extension named '{}'".format(name))
        return self._extensions[name]

    def __setattr__(self, name, value):
        if not name.startswith("_"):
            raise RuntimeError('Cannot set attribute')
        super().__setattr__(name, value)

    def _load_extension(self, name, path):
        cls = symbol_by_name(path)
        self._extensions[name] = build_from_crawler(cls, crawler=self._crawler)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(crawler=crawler, extensions=settings.getdict('CRAWLER_EXTENSIONS'))

    def close(self):
        for extension in self._extensions.values():
            if hasattr(extension, 'close'):
                extension.close()


class Crawler:

    app:  Optional[Tider] = None

    pidlock = None

    alarm_cls = 'tider.alarm:Alarm'
    engine_cls = 'tider.crawler.engine:HeartEngine'
    broker_cls = 'tider.crawler.broker:BrokersManager'

    #: contains the exit code if a :exc:`SystemExit` event is handled.
    exitcode = None

    def __init__(self, spidercls, schema='default', app=None, hostname=None, concurrency=None, pool_cls=None,
                 scheduler_cls=None, explorer_cls=None, broker_transport=None,
                 broker_wait_timeout=None, stats_cls=None, alarm_cls=None, debug=False,
                 loglevel=None, logfile=None, pidfile=None, purge=False, allow_duplicates=False):
        if isinstance(spidercls, Spider):
            raise ValueError("The spidercls argument must be a class, not an object")
        self.spidercls: Type[Spider] = spidercls
        self.schema = schema or 'default'

        self.app = app or self.app
        self._source_hostname = hostname
        self.startup_time = datetime.utcnow()
        custom_settings = spidercls.custom_settings or {}
        self.settings = Settings(defaults=[self.app.conf.copy(), custom_settings.copy()])
        self.pid = os.getpid()
        set_default_ua(self.settings.get('DEFAULT_USER_AGENT'))

        get, set_setting = self.settings.get, self.settings.set
        self.concurrency = concurrency or get("CONCURRENCY")
        set_setting("CONCURRENCY", self.concurrency, 'cmdline')
        self.pool_cls = pool_cls or get("POOL")
        set_setting("POOL", self.pool_cls, 'cmdline')
        self.scheduler_cls = scheduler_cls or get('SCHEDULER')
        set_setting("SCHEDULER", self.scheduler_cls, 'cmdline')
        self.explorer_cls = explorer_cls or get('EXPLORER')
        set_setting("EXPLORER", self.explorer_cls, 'cmdline')

        self.broker_transport = broker_transport or get('BROKER_TRANSPORT') or 'default'
        set_setting("BROKER_TRANSPORT", self.broker_transport, 'cmdline')
        self.broker_wait_timeout = broker_wait_timeout or get('BROKER_TIMEOUT')
        set_setting("BROKER_TIMEOUT", self.broker_wait_timeout, 'cmdline')

        self.stats_cls = stats_cls or get('STATS_CLASS')
        set_setting("STATS_CLASS", self.stats_cls, 'cmdline')
        self.alarm_cls = alarm_cls or get('ALARM_CLASS')
        set_setting("ALARM_CLASS", self.alarm_cls, 'cmdline')
        self.alarm_message_type = get('ALARM_MESSAGE_TYPE')
        set_setting("ALARM_MESSAGE_TYPE", self.alarm_message_type, 'cmdline')

        self.debug = debug
        logfile = logfile or get("LOG_FILE")
        self.logfile = os.path.join(get('LOG_DIRECTORY'), logfile) if logfile else logfile
        self.loglevel = loglevel or get("LOG_LEVEL")

        self.allow_duplicates = allow_duplicates or get('CRAWLER_ALLOW_DUPLICATES')

        self.pidfile = pidfile
        if not self.concurrency:
            self.concurrency = psutil.cpu_count()
        self.Pool = get_implementation(self.pool_cls)
        self.spider = None
        self.engine = None

        self.purge = purge

        self.pidbox = None
        self.connection = self.app.connection_for_control(url=get('CONTROL_URL'))
        self.crawling = False

    def on_start(self):
        if self.pidfile:
            self.pidlock = create_pidlock(self.pidfile)

    @cached_property
    def stats(self):
        return symbol_by_name(self.stats_cls)(self)

    @cached_property
    def alarm(self):
        return build_from_crawler(symbol_by_name(self.alarm_cls), crawler=self)

    @cached_property
    def broker(self):
        return symbol_by_name(self.broker_cls)(crawler=self,
                                               custom_transports=self.settings.getdict('BROKER_TRANSPORTS'))

    def _create_engine(self):
        return symbol_by_name(self.engine_cls)(crawler=self, on_spider_closed=self.stop)

    def _create_spider(self, *args, **kwargs):
        return self.spidercls.from_crawler(self, *args, **kwargs)

    def create_pool(self, limit=None, **kwargs):
        # don't use self.Pool here.
        pool_cls = get_implementation(self.pool_cls)
        limit = limit or self.concurrency
        return pool_cls(limit=limit, **kwargs)

    @cached_property
    def spidername(self):
        return self.spidercls.name

    @cached_property
    def extensions(self):
        return build_from_crawler(Extensions, crawler=self)

    def quick_alarm(self, message, message_type=None, extra_infos=None):
        message_type = message_type or self.alarm_message_type
        self.alarm.alarm(message, message_type, extra_infos)

    def _default_hostname(self, hostname):
        if not hostname:
            # [schema.]spider[.transport.broker_transport]@hostname
            name = self.spidername if self.schema == 'default' else f'{self.schema}.{self.spidername}'
            if self.broker_transport not in (None, 'default'):
                name = f'{name}.transport.{self.broker_transport}'
            hostname = nodename(name, gethostname())
        else:
            hostname = default_nodename(hostname)
        return hostname

    def get_hostname(self, hostname):
        hostname = self._default_hostname(hostname)
        # note: do not use self.connection here to avoid connection loss in pidbox.
        inspect = self.app.control.inspect(timeout=1.0)
        logger.info("Checking if hostname duplicates...")

        replies = inspect.sames(hostname)
        if replies:
            if self.allow_duplicates:
                return nodename(f'{nodesplit(hostname)[0]}.{str(os.getpid())}', nodesplit(hostname)[-1])
            logger.error(f"Duplicate hostname: {hostname}")
            raise RuntimeError("Can't crawl a duplicated spider.")
        return hostname

    @cached_property
    def hostname(self):
        return self.get_hostname(self._source_hostname)

    def rusage(self):
        if resource is None:
            raise NotImplementedError('rusage not supported by this platform')
        s = resource.getrusage(resource.RUSAGE_SELF)
        return {
            'utime': s.ru_utime,
            'stime': s.ru_stime,
            'maxrss': s.ru_maxrss,
            'ixrss': s.ru_ixrss,
            'idrss': s.ru_idrss,
            'isrss': s.ru_isrss,
            'minflt': s.ru_minflt,
            'majflt': s.ru_majflt,
            'nswap': s.ru_nswap,
            'inblock': s.ru_inblock,
            'oublock': s.ru_oublock,
            'msgsnd': s.ru_msgsnd,
            'msgrcv': s.ru_msgrcv,
            'nsignals': s.ru_nsignals,
            'nvcsw': s.ru_nvcsw,
            'nivcsw': s.ru_nivcsw,
        }

    def info(self):
        uptime = datetime.utcnow() - self.startup_time
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return {'pid': self.pid,
                'server_ip': ip,
                'clock': str(self.app.clock),
                'uptime': round(uptime.total_seconds())}

    def dump_stats(self):
        info = self.info()
        info.update(self.stats.get_stats())
        return info

    def maybe_sleep(self, seconds):
        getattr(self.Pool, 'is_green', False) and time.sleep(seconds)

    def send_task(self, task_id=None, task_type='message', args=None, kwargs=None, queue=None,
                  transport=None, **options):
        transport = transport or self.broker_transport
        return self.broker.send_task(task_id=task_id, task_type=task_type, args=args, kwargs=kwargs,
                                     queue=queue, transport=transport, **options)

    def crawl(self, *args, **kwargs):
        self.on_start()
        if self.crawling:
            raise RuntimeError("Crawling already taking place")
        self.crawling = True

        self.spider = self._create_spider(*args, **kwargs)

        install_crawler_term_handler(self)
        install_crawler_term_hard_handler(self)
        install_crawler_int_handler(self)

        get = self.settings.get
        self.app.log.setup(
            loglevel=self.loglevel,
            logfile=self.logfile,
            fmt=get('LOG_FORMAT'),
            redirect_stdouts=get('LOG_STDOUT'),
            redirect_level=get('LOG_STDOUT_LEVEL'),
            colorize=get("LOG_COLORIZE"),
            maxbytes=get('LOG_MAX_BYTES'),
            backup_count=get('LOG_BACKUP_COUNT'),
            encoding=get('LOG_ENCODING'),
            nodename=nodesplit(self._default_hostname(self._source_hostname))[0],
            debug=self.debug,
        )

        d = dict(overridden_settings(self.settings))
        d.pop('LOG_FILE', None)
        banner = BANNER.format(
            hostname=self.hostname,
            version=__version__,
            platform=safe_str(_platform.platform()),
            timestamp=datetime.now().replace(microsecond=0),
            settings=pprint.pformat(d),
        )
        logger.info(f"Crawler info:\n{banner}", {'banner': banner})

        self.settings.freeze()
        self.pidbox = Pidbox(self)

        try:
            self.engine = self._create_engine()
            self.pidbox.start()
            self.engine.start(self.spider, self.broker)
        except Exception:
            self.crawling = False
            self.exitcode = EX_FAILURE
            if self.engine is not None:
                self.engine.close()
            raise

    def stop(self, **_):
        """Starts a graceful stop of the crawler."""
        if self.crawling:
            self.crawling = False
            assert self.engine
            self.engine.stop()
        self.extensions.close()
        self.pidbox is not None and self.pidbox.shutdown()
        self.connection.release()


def _shutdown_handler(crawler, sig='TERM', how='Warm',
                      callback=None, exitcode=EX_OK):
    def _handle_request(*args):
        with in_sighandler():
            if current_process()._name == 'MainProcess':
                if callback:
                    callback(crawler)
                if how == 'Warm':
                    safe_say(f'crawler: {how} shutdown (MainProcess)')
                else:
                    safe_say(f'crawler: Forcing unclean {how} shutdown (MainProcess)')
                signals.crawler_shutting_down.send(
                    sender=crawler.hostname, sig=sig, how=how,
                    exitcode=exitcode,
                )
                setattr(state, {'Warm': 'should_stop',
                                'Cold': 'should_terminate'}[how], exitcode)
    _handle_request.__name__ = str(f'crawler_{how}')
    platforms.signals[sig] = _handle_request


if REMAP_SIGTERM == "SIGQUIT":
    install_crawler_term_handler = partial(
        _shutdown_handler, sig='SIGTERM', how='Cold', exitcode=EX_FAILURE,
    )
else:
    install_crawler_term_handler = partial(
        _shutdown_handler, sig='SIGTERM', how='Warm',
    )

if not is_jython:  # pragma: no cover
    install_crawler_term_hard_handler = partial(
        _shutdown_handler, sig='SIGQUIT', how='Cold',
        exitcode=EX_FAILURE,
    )
else:  # pragma: no cover
    install_crawler_term_handler = \
        install_crawler_term_hard_handler = lambda *a, **kw: None


def on_SIGINT(crawler):
    safe_say('crawler: Hitting Ctrl+C again will terminate all explorer and parser workers!')
    install_crawler_term_hard_handler(crawler, sig='SIGINT')


if not is_jython:  # pragma: no cover
    install_crawler_int_handler = partial(
        _shutdown_handler, sig='SIGINT', callback=on_SIGINT,
        exitcode=EX_FAILURE,
    )
else:  # pragma: no cover
    def install_crawler_int_handler(*args, **kwargs):
        pass


def _reload_current_crawler():
    platforms.close_open_fds([
        sys.__stdin__, sys.__stdout__, sys.__stderr__,
    ])
    os.execv(sys.executable, [sys.executable] + sys.argv)


def install_crawler_restart_handler(crawler, sig='SIGHUP'):

    def restart_crawler_sig_handler(*args):
        """Signal handler restarting the current python program."""
        set_in_sighandler(True)
        safe_say(f"Restarting crawler ({' '.join(sys.argv)})")
        import atexit
        atexit.register(_reload_current_crawler)
        from tider.crawler import state
        state.should_stop = EX_OK
    platforms.signals[sig] = restart_crawler_sig_handler


def install_HUP_not_supported_handler(crawler, sig='SIGHUP'):

    def warn_on_HUP_handler(signum, frame):
        with in_sighandler():
            safe_say('{sig} not supported: Restarting with {sig} is '
                     'unstable on this platform!'.format(sig=sig))
    platforms.signals[sig] = warn_on_HUP_handler
