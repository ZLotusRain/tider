import warnings
import threading
from functools import wraps

from tider.exceptions import TiderDeprecationWarning


def deprecated(use_instead=None):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""

    def deco(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            message = f"Call to deprecated function {func.__name__}."
            if use_instead:
                message += f" Use {use_instead} instead."
            warnings.warn(message, category=TiderDeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)
        return wrapped

    if callable(use_instead):
        deco = deco(use_instead)
        use_instead = None
    return deco


def inthread(name=None, daemon=True):
    """Decorator to call a function in a thread and return a daemon
    thread
    """

    def deco(func):
        @wraps(func)
        def wrapped(*a, **kw):
            return threading.Thread(target=func, name=name, args=a, kwargs=kw, daemon=daemon)
        return wrapped
    return deco


# noinspection PyPep8Naming
class cached_property:
    """Cached property descriptor from kombu.

    Caches the return value of the get method on first call.

    Examples:
        .. code-block:: python

            @cached_property
            def connection(self):
                return Connection()

            @connection.setter  # Prepares stored value
            def connection(self, value):
                if value is None:
                    raise TypeError('Connection must be a connection')
                return value

            @connection.deleter
            def connection(self, value):
                # Additional action to do at del(self.attr)
                if value is not None:
                    print('Connection {0!r} deleted'.format(value)
    """

    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        self.__get = fget
        self.__set = fset
        self.__del = fdel
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            return obj.__dict__[self.__name__]
        except KeyError:
            value = obj.__dict__[self.__name__] = self.__get(obj)
            return value

    def __set__(self, obj, value):
        if obj is None:
            return self
        if self.__set is not None:
            value = self.__set(obj, value)
        obj.__dict__[self.__name__] = value

    def __delete__(self, obj, _sentinel=object()):
        if obj is None:
            return self
        value = obj.__dict__.pop(self.__name__, _sentinel)
        if self.__del is not None and value is not _sentinel:
            self.__del(obj, value)

    def setter(self, fset):
        return self.__class__(self.__get, fset, self.__del)

    def deleter(self, fdel):
        return self.__class__(self.__get, self.__set, fdel)
