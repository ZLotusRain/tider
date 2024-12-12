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
