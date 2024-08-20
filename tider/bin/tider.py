"""Tider Command Line Interface."""

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points

import click
from click_didyoumean import DYMGroup
from click_plugins import with_plugins

from tider.bin.crawl import crawl
from tider.bin.multi import multi
from tider.bin.runspider import runspider
from tider.bin.base import TiderSettings, LogLevel, WorkersPool
from tider.settings import Settings
from tider.crawler import CrawlerProcess


@with_plugins(entry_points().get('tider.commands', []))
@click.group(cls=DYMGroup, invoke_without_command=True)
@click.option('--proj')
@click.option('--settings',
              type=TiderSettings())
@click.option('--schema')
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
@click.option('--version',
              is_flag=True)
@click.option('--egg',
              is_flag=True)
@click.pass_context
def tider(ctx, proj, settings, schema, pool, concurrency, loglevel, version, egg):
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

    if proj:
        module_path = f'src.settings.settings_{proj}'
    else:
        module_path = settings
    settings = Settings()
    settings.setmodule(module_path, priority="project")
    settings.set("POOL", pool, "cmdline")
    if concurrency:
        settings.set("CONCURRENCY", concurrency, "cmdline")
    settings.set("LOG_LEVEL", loglevel, "cmdline")

    # if not settings, then use default settings
    # if not schema, then load all valid spiders in settings
    ctx.obj = CrawlerProcess(settings, schema)


tider.add_command(crawl)
tider.add_command(multi)
tider.add_command(runspider)


def main() -> int:
    """Start tider umbrella command.

    This function is the main entrypoint for the CLI.

    :return: The exit code of the CLI.
    """
    return tider(auto_envvar_prefix="TIDER")
