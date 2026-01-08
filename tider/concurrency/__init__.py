"""Pool implementation abstract factory, and alias definitions."""

from tider.utils.imports import symbol_by_name

__all__ = ('get_implementation', 'get_available_pool_names',)

ALIASES = {
    'eventlet': 'tider.concurrency.eventlet:TaskPool',
    'gevent': 'tider.concurrency.gevent:TaskPool'
}

try:
    import concurrent.futures  # noqa: F401
except ImportError:
    pass
else:
    ALIASES['threads'] = 'tider.concurrency.thread:TaskPool'


def get_implementation(cls):
    """Return pool implementation by name."""
    return symbol_by_name(cls, ALIASES)


def get_available_pool_names():
    """Return all available pool type names."""
    return tuple(ALIASES.keys())
