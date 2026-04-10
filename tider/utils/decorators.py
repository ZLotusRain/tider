import warnings
import threading
from functools import wraps
from typing import Optional, Callable, TypeVar

from tider.exceptions import TiderDeprecationWarning

_F = TypeVar('_F', bound=Callable[..., None])


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


def inthread(name: Optional[str] = None, daemon: bool = True) -> Callable[[_F], Callable[..., threading.Thread]]:
    """Decorator to call a function in a thread and return a daemon thread."""

    def deco(func: _F) -> Callable[..., threading.Thread]:
        @wraps(func)
        def wrapped(*a, **kw) -> threading.Thread:
            return threading.Thread(target=func, name=name, args=a, kwargs=kw, daemon=daemon)
        return wrapped
    return deco
