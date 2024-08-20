"""Broker implementation abstract factory, and alias definitions."""

from tider.utils.misc import symbol_by_name

__all__ = ('get_broker', 'get_available_broker_names',)

ALIASES = {
    'redis': 'tider.core.brokers.redis.RedisBroker'
}


def get_broker(cls):
    """Return pool implementation by name."""
    return symbol_by_name(cls, ALIASES)


def get_available_broker_names():
    """Return all available pool type names."""
    return tuple(ALIASES.keys())
