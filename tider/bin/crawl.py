"""Program used to start a Tider crawler instance."""

import os
import sys
import click

from tider import concurrency as _concurrency
from tider.utils.log import get_logger
from tider.utils.nodenames import node_format
from tider.exceptions import SecurityError
from tider.platforms import EX_OK, EX_FAILURE, detached, maybe_drop_privileges
from tider.bin.base import TiderDaemonCommand, TiderOption, LogLevel

logger = get_logger(__name__)


class WorkersPool(click.Choice):
    """Workers pool option."""

    name = "pool"

    def __init__(self):
        """Initialize the workers pool option with the relevant choices."""
        super().__init__(_concurrency.get_available_pool_names())


WORKERS_POOL = WorkersPool()

T_FAKEFORK = os.environ.get('TIDER_FAKEFORK')


def detach(path, argv, logfile=None, pidfile=None, uid=None,
           gid=None, umask=None, workdir=None, fake=False, app=None,
           executable=None, hostname=None):
    """Detach program by argv."""
    fake = 1 if T_FAKEFORK else fake
    # `detached()` will attempt to touch the logfile to confirm that error
    # messages won't be lost after detaching stdout/err, but this means we need
    # to pre-format it rather than relying on `setup_logging_subsystem()` like
    # we can elsewhere.

    logfile = node_format(logfile, hostname)
    with detached(logfile, pidfile, uid, gid, umask, workdir, fake):
        # noinspection PyBroadException
        try:
            if executable is not None:
                path = executable
            os.execv(path, [path] + argv)
            return EX_OK
        except Exception:  # pylint: disable=broad-except
            if app is not None:
                app.log.setup_logging_subsystem(
                    'ERROR', logfile, hostname=hostname)
            logger.critical("Can't exec %r", ' '.join([path] + argv),
                            exc_info=True)
            return EX_FAILURE


@click.command(cls=TiderDaemonCommand,
               context_settings={'allow_extra_args': True})
@click.option('-s',
              '--spider',
              cls=TiderOption,
              help="Spider name",
              help_group="Global Options")
@click.option('-P',
              '--pool',
              default='threads',
              type=WORKERS_POOL,
              help="Pool implementation.")
@click.option('-c',
              '--concurrency',
              type=int,
              help="Number of child processes processing the queue.  "
                   "The default is the number of CPUs available"
                   " on your system.")
@click.option('-b',
              '--transport',
              default=None,
              help="Broker transport.")
@click.option('--broker-wait-timeout',
              default=None,
              help="Broker wait timeout.")
@click.option('-l',
              '--loglevel',
              default='INFO',
              type=LogLevel(),
              help="Logging level.")
@click.option('-D',
              '--detach',
              is_flag=True,
              default=False,
              help="Start crawler as a background process.")
@click.option('-n',
              '--hostname',
              type=str,
              help='Set custom hostname.')
@click.option('--debug',
              is_flag=True,
              default=False,
              help="Debug mode.")
@click.option('-dup',
              '--allow-duplicates',
              is_flag=True,
              default=False,
              help="Whether allow to start duplicated crawler.")
@click.option('--data-source',
              type=str,
              help='File which stores kwargs for crawling.')
@click.pass_context
def crawl(ctx, spider, hostname=None, transport=None, pool=None, concurrency=None,
          broker_wait_timeout=None, loglevel=None, logfile=None, uid=None, gid=None, pidfile=None,
          debug=False, allow_duplicates=False, data_source="", **kwargs):
    try:
        app = ctx.obj.app
        if data_source:
            transport = 'files'
        crawler = app.Crawler(
            app.load(spider or ctx.obj.spider, schema=ctx.obj.schema),
            schema=ctx.obj.schema,
            hostname=hostname,
            concurrency=concurrency,
            pool_cls=pool,
            data_source=data_source,
            broker_transport=transport,
            broker_wait_timeout=broker_wait_timeout,
            debug=debug, allow_duplicates=allow_duplicates,
            loglevel=loglevel, logfile=logfile, pidfile=pidfile,
        )

        if kwargs.get('detach', False):
            argv = ['-m', 'tider'] + sys.argv[1:]
            if '--detach' in argv:
                argv.remove('--detach')
            if '-D' in argv:
                argv.remove('-D')
            if "--uid" in argv:
                argv.remove('--uid')
            if "--gid" in argv:
                argv.remove('--gid')

            return detach(sys.executable,
                          argv,
                          logfile=crawler.logfile,
                          pidfile=pidfile,
                          uid=uid, gid=gid,
                          umask=kwargs.get('umask', None),
                          workdir=kwargs.get('workdir', None),
                          app=app,
                          executable=kwargs.get('executable', None),
                          hostname=crawler.hostname)
        maybe_drop_privileges(uid=uid, gid=gid)

        spider_kwargs = dict(x.split("=", 1) for x in ctx.args)
        if 'name' in spider_kwargs:
            spider_kwargs['name_'] = spider_kwargs.pop('name')
        crawler.crawl(**spider_kwargs)
        ctx.exit(crawler.exitcode)
    except SecurityError as e:
        ctx.obj.error(e.args[0])
        ctx.exit(1)
