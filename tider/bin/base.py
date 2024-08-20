import click
import numbers
from click import ParamType

from tider import concurrency
from tider.settings import Settings


class TiderSettings(ParamType):

    name = "settings"

    def convert(self, value, param, ctx):
        settings_module = value
        value = Settings()
        value.setmodule(module=settings_module)
        return value


class LogLevel(click.Choice):
    """Log level option."""

    def __init__(self):
        """Initialize the log level option with the relevant choices."""
        super().__init__(('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL'))

    def convert(self, value, param, ctx):
        if isinstance(value, numbers.Integral):
            return value

        value = value.upper()
        value = super().convert(value, param, ctx)
        return value


class WorkersPool(click.Choice):
    """Workers pool option."""

    name = "pool"

    def __init__(self):
        """Initialize the workers pool option with the relevant choices."""
        super().__init__(concurrency.get_available_pool_names())
