from types import ModuleType

from tider.utils.imports import symbol_by_name, import_from_cwd


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
