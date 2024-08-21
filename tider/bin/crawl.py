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
import json
import logging

from tider.platforms import EX_OK, EX_FAILURE, detached
from tider.bin.base import TiderDaemonCommand


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
            return EX_OK
        except Exception:  # pylint: disable=broad-except
            logger.critical("Can't exec %r", ' '.join([path] + argv),
                            exc_info=True)
            return EX_FAILURE


@click.command(cls=TiderDaemonCommand,
               context_settings={'allow_extra_args': True})
@click.option('-n',
              '--name',
              type=str,
              help='Unique spider name.')
@click.option('-d',
              '--data',
              type=str,
              help='File which stores kwargs for crawling.')
@click.option('--delete',
              is_flag=True,
              default=False,
              help="Whether to delete the data file.")
@click.option('-D',
              '--detach',
              is_flag=True,
              default=False,
              help="Start tider as a background process.")
@click.pass_context
def crawl(ctx, name, data, delete, uid=None, gid=None, pidfile=None, **kwargs):
    if kwargs.get('detach', False):
        argv = ['-m', 'tider'] + sys.argv[1:]
        if '--detach' in argv:
            argv.remove('--detach')
        if '-D' in argv:
            argv.remove('-D')
        if ctx.obj.tider.settings.getbool('LOG_FILE_ENABLED'):
            logfile = ctx.obj.tider.settings.get('LOG_FILE')
            logdir = ctx.obj.tider.settings.get('LOG_DIRECTORY', '/')
            logfile = os.path.join(logdir, logfile)
        else:
            logfile = None
        return detach(sys.executable,
                      argv,
                      logfile=logfile,
                      pidfile=pidfile,
                      uid=uid, gid=gid,
                      umask=kwargs.get('umask', None),
                      workdir=kwargs.get('workdir', None),
                      executable=kwargs.get('executable', None),
                      )

    spider_kwargs = {}
    if data:
        with open(data, encoding='utf-8') as fo:
            spider_kwargs.update(json.load(fo))
        if delete:
            os.remove(data)
    spider_kwargs.update(dict(x.split("=", 1) for x in ctx.args))
    if 'name' in spider_kwargs:
        spider_kwargs['name_'] = spider_kwargs.pop('name')

    ctx.obj.tider.crawl(name=name, **spider_kwargs)
