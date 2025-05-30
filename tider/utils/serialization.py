from inspect import getmro
from itertools import takewhile
from .serialize import pickle_dumps, pickle_loads

from kombu.utils.encoding import safe_repr

# List of base classes we probably don't want to reduce to.
unwanted_base_classes = (Exception, BaseException, object)


def subclass_exception(name, parent, module):
    """Create new exception class."""
    return type(name, (parent,), {'__module__': module})


def find_pickleable_exception(exc, loads=pickle_loads, dumps=pickle_dumps):
    """Find first pickleable exception base class.

    With an exception instance, iterate over its super classes (by MRO)
    and find the first super exception that's pickleable.  It does
    not go below :exc:`Exception` (i.e., it skips :exc:`Exception`,
    :class:`BaseException` and :class:`object`).  If that happens
    you should use :exc:`UnpickleableException` instead.

    Arguments:
        exc (BaseException): An exception instance.
        loads: decoder to use.
        dumps: encoder to use

    Returns:
        Exception: Nearest pickleable parent exception class
            (except :exc:`Exception` and parents), or if the exception is
            pickleable it will return :const:`None`.
    """
    exc_args = getattr(exc, 'args', [])
    for supercls in itermro(exc.__class__, unwanted_base_classes):
        try:
            superexc = supercls(*exc_args)
            loads(dumps(superexc))
        except Exception:  # pylint: disable=broad-except
            pass
        else:
            return superexc


def itermro(cls, stop):
    return takewhile(lambda sup: sup not in stop, getmro(cls))


def create_exception_cls(name, module, parent=None):
    """Dynamically create an exception class."""
    if not parent:
        parent = Exception
    return subclass_exception(name, parent, module)


def ensure_serializable(items, encoder):
    """Ensure items will serialize.

    For a given list of arbitrary objects, return the object
    or a string representation, safe for serialization.

    Arguments:
        items (Iterable[Any]): Objects to serialize.
        encoder (Callable): Callable function to serialize with.
    """
    safe_exc_args = []
    for arg in items:
        try:
            encoder(arg)
            safe_exc_args.append(arg)
        except Exception:  # pylint: disable=broad-except
            safe_exc_args.append(safe_repr(arg))
    return tuple(safe_exc_args)


class UnpickleableExceptionWrapper(Exception):
    """Wraps unpickleable exceptions.

    Arguments:
        exc_module (str): See :attr:`exc_module`.
        exc_cls_name (str): See :attr:`exc_cls_name`.
        exc_args (Tuple[Any, ...]): See :attr:`exc_args`.

    Example:
        >>> def pickle_it(raising_function):
        ...     try:
        ...         raising_function()
        ...     except Exception as e:
        ...         exc = UnpickleableExceptionWrapper(
        ...             e.__class__.__module__,
        ...             e.__class__.__name__,
        ...             e.args,
        ...         )
        ...         pickle_dumps(exc)  # Works fine.
    """

    #: The module of the original exception.
    exc_module = None

    #: The name of the original exception class.
    exc_cls_name = None

    #: The arguments for the original exception.
    exc_args = None

    def __init__(self, exc_module, exc_cls_name, exc_args, text=None):
        safe_exc_args = ensure_serializable(
            exc_args, lambda v: pickle_loads(pickle_dumps(v))
        )
        self.exc_module = exc_module
        self.exc_cls_name = exc_cls_name
        self.exc_args = safe_exc_args
        self.text = text
        super().__init__(exc_module, exc_cls_name, safe_exc_args,
                         text)

    def restore(self):
        return create_exception_cls(self.exc_cls_name,
                                    self.exc_module)(*self.exc_args)

    def __str__(self):
        return self.text

    @classmethod
    def from_exception(cls, exc):
        res = cls(
            exc.__class__.__module__,
            exc.__class__.__name__,
            getattr(exc, 'args', []),
            safe_repr(exc)
        )
        if hasattr(exc, "__traceback__"):
            res = res.with_traceback(exc.__traceback__)
        return res


def get_pickleable_exception(exc):
    """Make sure exception is pickleable."""
    try:
        pickle_loads(pickle_dumps(exc))
    except Exception:  # pylint: disable=broad-except
        pass
    else:
        return exc
    nearest = find_pickleable_exception(exc)
    if nearest:
        return nearest
    return UnpickleableExceptionWrapper.from_exception(exc)


def get_pickled_exception(exc):
    """Reverse of :meth:`get_pickleable_exception`."""
    if isinstance(exc, UnpickleableExceptionWrapper):
        return exc.restore()
    return exc
