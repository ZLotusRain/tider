import os
import sys
from importlib import import_module, reload
from pkgutil import iter_modules
from contextlib import contextmanager

from tider.utils.log import get_logger

logger = get_logger(__name__)

__all__ = (
    'NotAPackage', 'qualname', 'instantiate', 'symbol_by_name',
    'walk_modules', 'cwd_in_path', 'find_module',
    'import_from_cwd', 'reload_from_cwd', 'module_file',
)


class NotAPackage(Exception):
    """Raised when importing a package, but it's not a package."""


def qualname(obj):
    """Return object name."""
    if not hasattr(obj, '__name__') and hasattr(obj, '__class__'):
        obj = obj.__class__
    q = getattr(obj, '__qualname__', None)
    if '.' not in q:
        q = '.'.join((obj.__module__, q))
    return q


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See Also:
        :func:`symbol_by_name`.
    """
    return symbol_by_name(name)(*args, **kwargs)


def symbol_by_name(name, aliases=None, imp=None, package=None, sep='.', default=None, raise_on_error=True):
    """Get symbol by qualified name.

    The name should be the full dot-separated path to the class::

        modulename.ClassName

    Example::

        tider.concurrency.gevent.TaskPool
                                 ^- class name

    or using ':' to separate module and symbol::

        tider.concurrency.gevent:TaskPool

    If `aliases` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    """
    aliases = {} if not aliases else aliases
    if imp is None:
        imp = import_module

    if not isinstance(name, str):
        if callable(name):
            return name  # already a class
        else:
            raise TypeError("Unexpected argument type, expected string "
                            f"or object, got: {type(name)}")

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
        if raise_on_error and default is None:
            raise
    return default


def walk_modules(path):
    """Loads a module and all its submodules from the given module path and
    returns them. If *any* module throws an exception while importing, that
    exception is thrown back.

    For example: walk_modules('tider.utils')
    """

    mods = []
    mod = import_module(path)
    try:
        mod = reload(mod)  # reload
    except (ImportError, ModuleNotFoundError):
        pass
    mods.append(mod)
    if hasattr(mod, '__path__'):
        for _, subpath, ispkg in iter_modules(mod.__path__):
            fullpath = path + '.' + subpath
            if ispkg:
                mods += walk_modules(fullpath)
            else:
                submod = import_module(fullpath)
                try:
                    submod = reload(submod)  # reload
                except (ImportError, ModuleNotFoundError):
                    pass
                mods.append(submod)
    return mods


@contextmanager
def cwd_in_path():
    """Context adding the current working directory to sys.path."""
    try:
        cwd = os.getcwd()
    except FileNotFoundError:
        cwd = None
    if not cwd:
        yield
    elif cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield cwd
        finally:
            try:
                sys.path.remove(cwd)
            except ValueError:  # pragma: no cover
                pass


def find_module(module, path=None, imp=None):
    """Version of :func:`imp.find_module` supporting dots."""
    if imp is None:
        imp = import_module
    with cwd_in_path():
        try:
            return imp(module)
        except ImportError:
            # Raise a more specific error if the problem is that one of the
            # dot-separated segments of the module name is not a package.
            if '.' in module:
                parts = module.split('.')
                for i, part in enumerate(parts[:-1]):
                    package = '.'.join(parts[:i + 1])
                    try:
                        mpart = imp(package)
                    except ImportError:
                        # Break out and re-raise the original ImportError
                        # instead.
                        break
                    try:
                        mpart.__path__
                    except AttributeError:
                        raise NotAPackage(package)
            raise


def import_from_cwd(module, imp=None, package=None):
    """Import module, temporarily including modules in the current directory.

    Modules located in the current directory has
    precedence over modules located in `sys.path`.
    """
    if imp is None:
        imp = import_module
    with cwd_in_path():
        return imp(module, package=package)


def reload_from_cwd(module, reloader=None):
    """Reload module (ensuring that CWD is in sys.path)."""
    if reloader is None:
        reloader = reload
    with cwd_in_path():
        return reloader(module)


def module_file(module):
    """Return the correct original file name of a module."""
    name = module.__file__
    return name[:-1] if name.endswith('.pyc') else name
