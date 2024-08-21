from typing import List, Any
from pprint import pformat


def str_to_list(s):
    # type: (str) -> List[str]
    """Convert string to list."""
    if isinstance(s, str):
        return s.split(',')
    return s


def indent(t, indent=0, sep='\n'):
    # type: (str, int, str) -> str
    """Indent text."""
    return sep.join(' ' * indent + p for p in t.split(sep))


def truncate(s, maxlen=128, suffix='...'):
    # type: (str, int, str) -> str
    """Truncate text to a maximum number of characters."""
    if maxlen and len(s) >= maxlen:
        return s[:maxlen].rsplit(' ', 1)[0] + suffix
    return s


def pluralize(n, text, suffix='s'):
    # type: (int, str, str) -> str
    """Pluralize term when n is greater than one."""
    if n != 1:
        return text + suffix
    return text


def pretty(value, width=80, nl_width=80, sep='\n', **kw):
    # type: (str, int, int, str, **Any) -> str
    """Format value for printing to console."""
    if isinstance(value, dict):
        return f'{{{sep} {pformat(value, 4, nl_width)[1:]}'
    elif isinstance(value, tuple):
        return '{}{}{}'.format(
            sep, ' ' * 4, pformat(value, width=nl_width, **kw),
        )
    else:
        return pformat(value, width=width, **kw)
