"""Program used to start a Tider crawler instance.

.. program:: tider crawl

Examples
========

.. code-block:: console

    $ # Use default settings and spider custom settings.
    $ tider crawl foo

    $ # Use specific settings and spider custom settings.
    $ tider --settings settings_foo crawl foo
    
    $ # Find settings in project folder.
    $ tider --proj proj_foo crawl foo
"""

import os
import sys
import click
import logging
from tider.platforms import detached


logger = logging.getLogger(__name__)


def detach(path, argv, logfile=None, pidfile=None, uid=None,
           gid=None, umask=None, workdir=None, fake=False,
           executable=None):
    """Detach program by argv."""
    # `detached()` will attempt to touch the logfile to confirm that error
    # messages won't be lost after detaching stdout/err, but this means we need
    # to pre-format it rather than relying on `setup_logging_subsystem()` like
    # we can elsewhere.
    with detached(logfile, pidfile, uid, gid, umask, workdir, fake,
                  after_forkers=False):
        # noinspection PyBroadException
        try:
            if executable is not None:
                path = executable
            os.execv(path, [path] + argv)
        except Exception:  # pylint: disable=broad-except
            logger.critical("Can't exec %r", ' '.join([path] + argv),
                            exc_info=True)


@click.command(context_settings={'allow_extra_args': True})
@click.option('-s',
              '--spider')
@click.option('--producer',
              is_flag=True)
@click.option('--worker',
              is_flag=True)
@click.option('--worker-concurrency',
              type=int,
              default=1,
              help="Number of workers to consume from message queue")
@click.option('-D',
              '--detach',
              is_flag=True,
              default=False,
              help="Start crawler as a background process.")
@click.pass_context
def crawl(ctx, spider, uid=None, gid=None, pidfile=None, **kwargs):
    if not spider:
        raise click.UsageError("Unable to detect valid spider")

    is_worker = kwargs.get('worker')
    is_producer = kwargs.get('producer')
    if is_worker and is_producer:
        raise click.UsageError("Can not assign crawler different roles")

    crawler_role = 'crawler'
    if is_worker:
        crawler_role = 'worker'
    elif is_producer:
        crawler_role = 'producer'
    ctx.obj.update_setting('CRAWLER_ROLE', crawler_role, 'cmdline')
    ctx.obj.update_setting('WORKER_CONCURRENCY', kwargs['worker_concurrency'], 'cmdline')

    if kwargs.get('detach', False):
        argv = ['-m', 'tider'] + sys.argv[1:]
        if '--detach' in argv:
            argv.remove('--detach')
        if '-D' in argv:
            argv.remove('-D')
        return detach(sys.executable,
                      argv,
                      pidfile=pidfile,
                      uid=uid, gid=gid,
                      umask=kwargs.get('umask', None),
                      workdir=kwargs.get('workdir', None),
                      executable=kwargs.get('executable', None),
                      )
    ctx.obj.crawl(spider)
