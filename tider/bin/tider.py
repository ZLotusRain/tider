"""Tider Command Line Interface."""
import os
import pathlib
import traceback
try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points

import click
from click import ParamType
from click_didyoumean import DYMGroup
from click_plugins import with_plugins

from tider import Tider
from tider.settings import Settings
from tider.bin.crawl import crawl
from tider.bin.multi import multi
from tider.bin.bench import bench
from tider.bin.runspider import runspider
from tider.bin.control import control, inspect, status
from tider.bin.startproject import startproject
from tider.bin.base import TiderOption, CLIContext
from tider.utils.app import find_app


UNABLE_TO_LOAD_APP_MODULE_NOT_FOUND = click.style("""
Unable to load tider application.
The module {0} was not found.""", fg='red')

UNABLE_TO_LOAD_APP_ERROR_OCCURRED = click.style("""
Unable to load tider application.
While trying to load the module {0} the following error occurred:
{1}""", fg='red')

UNABLE_TO_LOAD_APP_APP_MISSING = click.style("""
Unable to load tider application.
{0}""")


def _get_default_app(src_dirname, proj, custom_settings):
    # settings
    # if not settings, then use default settings
    # if not schema, then load all valid spiders in settings
    settings = Settings()
    default_module_path = 'settings.default_settings'
    if src_dirname:
        default_module_path = f"{src_dirname}.{default_module_path}"
    settings.setmodule(default_module_path, priority='default')
    if custom_settings:
        settings.setmodule(custom_settings, priority='project')
    elif proj:
        proj_settings_path = f"settings.settings_{proj}"
        if src_dirname:
            proj_settings_path = f"{src_dirname}.{proj_settings_path}"
        settings.setmodule(proj_settings_path, priority='project')

    app = Tider(settings=settings)
    return app


class App(ParamType):
    """Application option."""

    name = "application"

    def convert(self, value, param, ctx):
        if value == 'default' and ctx is not None:
            return None
        # noinspection PyBroadException
        try:
            return find_app(value)
        except ModuleNotFoundError as e:
            if e.name != value:
                exc = traceback.format_exc()
                self.fail(
                    UNABLE_TO_LOAD_APP_ERROR_OCCURRED.format(value, exc)
                )
            self.fail(UNABLE_TO_LOAD_APP_MODULE_NOT_FOUND.format(e.name))
        except AttributeError as e:
            attribute_name = e.args[0].capitalize()
            self.fail(UNABLE_TO_LOAD_APP_APP_MISSING.format(attribute_name))
        except Exception:
            exc = traceback.format_exc()
            self.fail(
                UNABLE_TO_LOAD_APP_ERROR_OCCURRED.format(value, exc)
            )


class TiderSettings(ParamType):
    name = "settings"

    def convert(self, value, param, ctx):
        settings = Settings()
        settings.setmodule(module=value)
        return settings


APP = App()
TIDER_SETTINGS = TiderSettings()

try:
    plugins = entry_points().get('tider.commands', [])
except AttributeError:
    plugins = list(entry_points().select(name='tider.commands'))


@with_plugins(plugins)
@click.group(cls=DYMGroup, invoke_without_command=True)
@click.option('-A',
              '--app',
              envvar='APP',
              cls=TiderOption,
              type=APP,
              default='default',
              help_group="Global Options")
@click.option('--src',
              cls=TiderOption,
              default='src',
              help="Project implementation dirname",
              help_group="Global Options")
@click.option('--proj',
              cls=TiderOption,
              help_group="Global Options")
@click.option('--settings',
              cls=TiderOption,
              type=TIDER_SETTINGS,
              help_group="Global Options")
@click.option('--schema',
              cls=TiderOption,
              help_group="Global Options")
@click.option('-s',
              '--spider',
              cls=TiderOption,
              help="Spider name",
              help_group="Global Options")
@click.option('--workdir',
              cls=TiderOption,
              type=pathlib.Path,
              callback=lambda _, __, wd: os.chdir(wd) if wd else None,
              is_eager=True,
              help_group="Global Options")
@click.option('--version',
              cls=TiderOption,
              is_flag=True,
              help_group="Global Options")
@click.option('--egg',
              cls=TiderOption,
              is_flag=True,
              help_group="Global Options")
@click.pass_context
def tider(ctx, app, src, proj, settings, schema, spider, workdir, version, egg):
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

    if not app:
        app = _get_default_app(src_dirname=src,
                               proj=proj,
                               custom_settings=settings)
    ctx.obj = CLIContext(app=app, workdir=workdir, project=proj, schema=schema, spider=spider)


tider.add_command(crawl)
tider.add_command(multi)
tider.add_command(bench)
tider.add_command(control)
tider.add_command(inspect)
tider.add_command(status)
tider.add_command(runspider)
tider.add_command(startproject)


def main() -> int:
    """Start tider umbrella command.

    This function is the main entrypoint for the CLI.

    :return: The exit code of the CLI.
    """
    return tider(auto_envvar_prefix="TIDER")
