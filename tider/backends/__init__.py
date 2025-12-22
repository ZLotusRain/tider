"""Spider backends which store stats, results, errors, and failures."""

import sys
import types

from .base import Backend, KeyValueStoreBackend, State, backend_serializer_registry

from tider.utils.misc import symbol_by_name
from tider.exceptions import reraise, ImproperlyConfigured

__all__ = (
    'backend_serializer_registry', 'State',
    'Backend', 'KeyValueStoreBackend', 'by_name'
)


BACKEND_ALIASES = {
    'redis': 'tider.backends.redis:RedisBackend',
    'mongodb': 'tider.backends.mongodb:MongoBackend',
    'disabled': 'tider.backends.base:DisabledBackend',
}


def by_name(backend=None):
    """Get backend class by name/alias."""
    backend = backend or 'disabled'
    try:
        cls = symbol_by_name(backend, BACKEND_ALIASES)
    except ValueError as exc:
        reraise(ImproperlyConfigured, ImproperlyConfigured(
            "Unknown spider backend: {0!r}.".format(backend, exc)), sys.exc_info()[2])
    if isinstance(cls, types.ModuleType):
        raise ImproperlyConfigured("Unknown spider backend: {0!r}.".format(
            backend, 'is a Python module, not a backend class.'))
    return cls


def by_url(backend=None):
    """Get backend class by URL."""
    url = None
    if backend and '://' in backend:
        url = backend
        scheme, _, _ = url.partition('://')
        if '+' in scheme:
            backend, url = url.split('+', 1)
        else:
            backend = scheme
    return by_name(backend), url
