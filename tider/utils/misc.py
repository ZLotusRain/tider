import hashlib
import importlib

from typing import Iterable
from pkgutil import iter_modules

from tider.item import Item
from tider.utils.log import get_logger

logger = get_logger(__name__)

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


def unique_list(list_, key=lambda x: x):
    """efficient function to uniquify a list preserving item order"""
    seen = set()
    result = []
    for item in list_:
        seenkey = key(item)
        if seenkey in seen:
            continue
        seen.add(seenkey)
        result.append(item)
    return result


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
    return obj


def load_object(path):
    """Load an object given its absolute object path, and return it.

    The object can be the import path of a class, function, variable or an
    instance.

    If ``path`` is not a string, but is a callable object, such as a class or
    a function, then return it as is.
    """

    if not isinstance(path, str):
        if callable(path):
            return path
        else:
            raise TypeError("Unexpected argument type, expected string "
                            f"or object, got: {type(path)}")

    try:
        dot = path.rindex('.')
    except ValueError:
        raise ValueError(f"Error loading object '{path}': not a full path")

    module, name = path[:dot], path[dot + 1:]
    mod = importlib.import_module(module)

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError(f"Module '{module}' doesn't define any object named '{name}'")

    return obj


def symbol_by_name(name, aliases=None, imp=None, package=None, sep='.', default=None):
    """Get symbol by qualified name.

    The name should be the full dot-separated path to the class::

        modulename.ClassName

    Example::

        tider.concurrency.processes.TaskPool
                                    ^- class name

    or using ':' to separate module and symbol::

        tider.concurrency.processes:TaskPool

    If `aliases` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    """
    aliases = {} if not aliases else aliases
    if imp is None:
        imp = importlib.import_module

    if not isinstance(name, str):
        return name  # already a class

    name = aliases.get(name) or name
    sep = ':' if ':' in name else sep
    module_name, _, cls_name = name.rpartition(sep)
    if not module_name:
        cls_name, module_name = None, package if package else cls_name
    try:
        try:
            module = imp(module_name, package=package)
        except ValueError as exc:
            logger.error(f"Couldn't import {name!r}: {exc}")
        else:
            return getattr(module, cls_name) if cls_name else module
    except (ImportError, AttributeError):
        if default is None:
            raise
    return default


def try_import(module, default=None):
    """Try to import and return module.

    Returns None if the module does not exist.
    """
    try:
        return importlib.import_module(module)
    except ImportError:
        return default


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


def walk_modules(path):
    """Loads a module and all its submodules from the given module path and
    returns them. If *any* module throws an exception while importing, that
    exception is thrown back.

    For example: walk_modules('tider.utils')
    """

    mods = []
    mod = importlib.import_module(path)
    mods.append(mod)
    if hasattr(mod, '__path__'):
        for _, subpath, ispkg in iter_modules(mod.__path__):
            fullpath = path + '.' + subpath
            if ispkg:
                mods += walk_modules(fullpath)
            else:
                submod = importlib.import_module(fullpath)
                mods.append(submod)
    return mods


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
