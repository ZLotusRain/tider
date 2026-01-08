import hashlib
from typing import Iterable

from tider.item import Item
from tider.utils.imports import symbol_by_name
from tider.utils.log import get_logger

logger = get_logger(__name__)

__all__ = (
    'is_iterable', 'arg_to_iter', 'str_to_list', 'unique_list',
    'evaluate_callable', 'try_copy', 'build_from_crawler',
    'md5sum', 'to_bytes',
)

_ITERABLE_SINGLE_VALUES = dict, Item, str, bytes


def is_iterable(o) -> bool:
    # hasattr(o, "__iter__")
    return isinstance(o, Iterable)


def arg_to_iter(arg):
    """Convert an argument to an iterable. The argument can be a None, single
    value, or an iterable.

    Exception: if arg is a dict, [arg] will be returned
    """
    if arg is None:
        return []
    elif not isinstance(arg, _ITERABLE_SINGLE_VALUES) and hasattr(arg, '__iter__'):
        return arg
    else:
        return [arg]


def str_to_list(s, sep=",", maxsplit=-1):
    """Convert string to list."""
    if isinstance(s, str):
        return s.split(sep, maxsplit=maxsplit)
    return s


def unique_list(list_, key=lambda x: x, prefer=lambda x: x):
    """efficient function to uniquify a list preserving item order"""
    seen = {}
    for item in list_:
        seenkey = key(item)
        if seenkey not in seen:
            seen[seenkey] = item
            continue
        existing = seen[seenkey]
        if not prefer(existing) and prefer(item):
            seen[seenkey] = item

    return list(seen.values())


def evaluate_callable(callback):
    if callable(callback):
        callback = callback
    elif isinstance(callback, str):
        callback = symbol_by_name(callback)
    else:
        callback = None
    return callback


def try_copy(obj):
    if isinstance(obj, (dict, list)):
        return type(obj)(obj)
    elif hasattr(obj, 'copy'):
        return obj.copy()
    raise TypeError(f"Not a copyable object, got: {type(obj)}")


def build_from_crawler(objcls, crawler, *args, **kwargs):
    """Construct a class instance using its ``from_crawler`` constructor.

    ``*args`` and ``**kwargs`` are forwarded to the constructor.

    Raises ``TypeError`` if the resulting instance is ``None``.
    """
    if hasattr(objcls, "from_crawler"):
        instance = objcls.from_crawler(crawler, *args, **kwargs)
        method_name = "from_crawler"
    elif hasattr(objcls, "from_settings"):
        instance = objcls.from_settings(crawler.settings, *args, **kwargs)
        method_name = "from_settings"
    else:
        instance = objcls(*args, **kwargs)
        method_name = "__new__"
    if instance is None:
        raise TypeError(f"{objcls.__qualname__}.{method_name} returned None")
    return instance


def md5sum(file):
    """Calculate the md5 checksum of a file-like object without reading its
    whole content in memory.

    >>> from io import BytesIO
    >>> md5sum(BytesIO(b'file content to hash'))
    '784406af91dd5a54fbb9c84c2236595a'
    """
    m = hashlib.md5()
    while True:
        d = file.read(8096)
        if not d:
            break
        m.update(d)
    return m.hexdigest()


def to_bytes(text, encoding=None, errors='strict'):
    """Return the binary representation of ``text``. If ``text``
    is already a bytes object, return it as-is."""
    if isinstance(text, bytes):
        return text
    if not isinstance(text, str):
        raise TypeError('to_bytes must receive a str or bytes '
                        f'object, got {type(text).__name__}')
    if encoding is None:
        encoding = 'utf-8'
    return text.encode(encoding, errors)
