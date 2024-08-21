import os
import sys
from types import ModuleType
from contextlib import contextmanager
from importlib import import_module

from tider.utils.misc import symbol_by_name


@contextmanager
def cwd_in_path():
    """Context adding the current working directory to sys.path."""
    cwd = os.getcwd()
    if cwd in sys.path:
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


def import_from_cwd(module, imp=None, package=None):
    """Import module, temporarily including modules in the current directory.

    Modules located in the current directory has
    precedence over modules located in `sys.path`.
    """
    if imp is None:
        imp = import_module
    with cwd_in_path():
        return imp(module, package=package)


def find_app(app,  imp=import_from_cwd):
    """Find app by name."""
    from tider.base import Tider

    try:
        sym = symbol_by_name(app, imp=imp)
    except AttributeError:
        # last part was not an attribute, but a module
        sym = imp(app)
    if isinstance(sym, ModuleType) and ':' not in app:
        try:
            found = sym.app
            if isinstance(found, ModuleType):
                raise AttributeError()
        except AttributeError:
            try:
                found = sym.tider
                if isinstance(found, ModuleType):
                    raise AttributeError(
                        "attribute 'tider' is the tider module not the instance of tider")
            except AttributeError:
                if getattr(sym, '__path__', None):
                    try:
                        return find_app(
                            f'{app}.tider',
                            imp=imp,
                        )
                    except ImportError:
                        pass
                for suspect in vars(sym).values():
                    if isinstance(suspect, Tider):
                        return suspect
                raise
            else:
                return found
        else:
            return found
    return sym
