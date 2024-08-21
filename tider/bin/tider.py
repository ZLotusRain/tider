"""Tider Command Line Interface."""
import os
import pathlib
try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points

import click
from click_didyoumean import DYMGroup
from click_plugins import with_plugins

from tider.settings import Settings
from tider.utils.misc import try_import
from tider.bin.crawl import crawl
from tider.bin.multi import multi
from tider.bin.bench import bench
from tider.bin.runspider import runspider
from tider.bin.control import control, inspect, status
from tider.bin.base import TiderSettings, LogLevel, WorkersPool, CLIContext


@with_plugins(entry_points().get('tider.commands', []))
@click.group(cls=DYMGroup, invoke_without_command=True)
@click.option('-s',
              '--spider',
              help="Spider name(identity)")
@click.option('--proj')
@click.option('--settings',
              type=TiderSettings())
@click.option('--schema')
@click.option('--publisher',
              is_flag=True)
@click.option('--worker',
              is_flag=True)
@click.option('--worker-concurrency',
              type=int,
              default=1,
              help="Number of workers to consume from message queue")
@click.option('-P',
              '--pool',
              default='threads',
              type=WorkersPool(),
              help="Pool implementation.")
@click.option('-c',
              '--concurrency',
              type=int,
              help="Number of child processes processing the queue.  "
                   "The default is the number of CPUs available"
                   " on your system.")
@click.option('-l',
              '--loglevel',
              default='INFO',
              type=LogLevel(),
              help="Logging level.")
@click.option('--workdir',
              type=pathlib.Path,
              callback=lambda _, __, wd: os.chdir(wd) if wd else None,
              is_eager=True)
@click.option('--version',
              is_flag=True)
@click.option('--egg',
              is_flag=True)
@click.pass_context
def tider(ctx, spider, proj, settings, schema, publisher, worker, worker_concurrency,
          pool, concurrency, loglevel, workdir, version, egg):
    """Tider command entrypoint."""
    if version:
        click.echo("In development...")
        ctx.exit()
    elif egg:
        click.echo("Star&Cloud")
        ctx.exit()
    elif ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        ctx.exit()

    presettings = Settings()
    module = try_import('src.settings.default_settings')
    module and presettings.setmodule(module, priority='default')
    if settings:
        presettings.setmodule(settings, priority='project')
    elif proj:
        presettings.setmodule(f'src.settings.settings_{proj}', priority='project')
    presettings.set("POOL", pool, "cmdline")
    if concurrency:
        presettings.set("CONCURRENCY", concurrency, "cmdline")
    presettings.set("LOG_LEVEL", loglevel, "cmdline")

    if worker and publisher:
        raise click.UsageError("Can not assign multiple spider types")
    spider_type = (worker and 'worker') or (publisher and 'publisher') or 'task'

    # if not settings, then use default settings
    # if not schema, then load all valid spiders in settings
    ctx.obj = CLIContext(spider, presettings, schema, spider_type, worker_concurrency, workdir)
    ctx.obj.finalize()


tider.add_command(crawl)
tider.add_command(multi)
tider.add_command(bench)
tider.add_command(control)
tider.add_command(inspect)
tider.add_command(status)
tider.add_command(runspider)


def main() -> int:
    """Start tider umbrella command.

    This function is the main entrypoint for the CLI.

    :return: The exit code of the CLI.
    """
    return tider(auto_envvar_prefix="TIDER")
