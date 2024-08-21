import json
import click
import numbers
from pprint import pformat
from click import ParamType
from kombu.utils.objects import cached_property

from tider import Tider
from tider import concurrency
from tider.settings import Settings
from tider.utils.text import indent, str_to_list

try:
    from pygments import highlight
    from pygments.formatters import Terminal256Formatter
    from pygments.lexers import PythonLexer
except ImportError:
    def highlight(s, *args, **kwargs):
        """Placeholder function in case pygments is missing."""
        return s
    LEXER = Terminal256Formatter = None
    FORMATTER = PythonLexer = None
else:
    LEXER = PythonLexer()
    FORMATTER = Terminal256Formatter()


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


class CommaSeparatedList(ParamType):
    """Comma separated list argument."""

    name = "comma separated list"

    def convert(self, value, param, ctx):
        return str_to_list(value)


class TiderDaemonCommand(click.Command):
    def __init__(self, *args, **kwargs):
        """Initialize a Tider command with common daemon options."""
        super().__init__(*args, **kwargs)
        self.params.append(click.Option(('-f', '--logfile')))
        self.params.append(click.Option(('--pidfile',)))
        self.params.append(click.Option(('--uid',)))
        self.params.append(click.Option(('--gid',)))
        self.params.append(click.Option(('--umask',)))
        self.params.append(click.Option(('--executable',)))


class CLIContext:

    def __init__(self, spider, settings, schema=None, spider_type='task',
                 worker_concurrency=0, workdir=None):
        self.tider = None
        self.spider = spider
        self.settings = settings
        self.schema = schema
        self.spider_type = spider_type
        self.worker_concurrency = worker_concurrency
        self.no_color = False
        self.quiet = False
        self.workdir = workdir

    @cached_property
    def OK(self):
        return self.style("OK", fg="green", bold=True)

    @cached_property
    def ERROR(self):
        return self.style("ERROR", fg="red", bold=True)

    def style(self, message=None, **kwargs):
        if self.no_color:
            return message
        else:
            return click.style(message, **kwargs)

    def echo(self, message=None, **kwargs):
        if self.no_color:
            kwargs['color'] = False
            click.echo(message, **kwargs)
        else:
            click.echo(message, **kwargs)

    def pretty(self, n):
        if isinstance(n, list):
            return self.OK, self.pretty_list(n)
        if isinstance(n, dict):
            if 'ok' in n or 'error' in n:
                return self.pretty_dict_ok_error(n)
            else:
                s = json.dumps(n, sort_keys=True, indent=4)
                if not self.no_color:
                    s = highlight(s, LEXER, FORMATTER)
                return self.OK, s
        if isinstance(n, str):
            return self.OK, n
        return self.OK, pformat(n)

    def pretty_list(self, n):
        if not n:
            return '- empty -'
        return '\n'.join(
            f'{self.style("*", fg="white")} {item}' for item in n
        )

    def pretty_dict_ok_error(self, n):
        try:
            return (self.OK,
                    indent(self.pretty(n['ok'])[1], 4))
        except KeyError:
            pass
        return (self.ERROR,
                indent(self.pretty(n['error'])[1], 4))

    def say_chat(self, direction, title, body='', show_body=False):
        if direction == '<-' and self.quiet:
            return
        dirstr = not self.quiet and f'{self.style(direction, fg="white", bold=True)} ' or ''
        self.echo(f'{dirstr} {title}')
        if body and show_body:
            self.echo(body)

    def finalize(self):
        if self.tider is not None:
            raise RuntimeError('Tider already finalized')
        self.tider = Tider(spider=self.spider,
                           settings=self.settings,
                           schema=self.schema,
                           spider_type=self.spider_type,
                           processes=self.worker_concurrency)
        return self.tider


COMMA_SEPARATED_LIST = CommaSeparatedList()
