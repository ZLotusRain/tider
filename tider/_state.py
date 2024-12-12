import os
import sys
import threading
import weakref

from tider.local import Proxy

#: List of all app instances (weakrefs), mustn't be used directly.
_apps = weakref.WeakSet()

#: Global default app used when no current app.
default_app = None

#: Function returning the app provided or the default app if none.
#:
#: The environment variable :envvar:`TIDER_TRACE_APP` is used to
#: trace app leaks.  When enabled an exception is raised if there
#: is no active app.
app_or_default = None


class _TLS(threading.local):
    #: Apps with the :attr:`~tider.base.Tider.set_as_current` attribute
    #: sets this, so it will always contain the last instantiated app,
    #: and is the default app returned by :func:`app_or_default`.
    current_app = None


_tls = _TLS()


def set_default_app(app):
    """Set default app."""
    global default_app
    default_app = app


def _get_current_app():
    if default_app is None:
        #: creates the global fallback app instance.
        from tider.base import Tider
        set_default_app(Tider(
            main='default', set_as_current=False,
        ))
    return _tls.current_app or default_app


def _set_current_app(app):
    _tls.current_app = app


if os.environ.get('T_STRICT_APP'):  # pragma: no cover
    def get_current_app():
        """Return the current app."""
        raise RuntimeError('USES CURRENT APP')
elif os.environ.get('T_WARN_APP'):  # pragma: no cover
    def get_current_app():
        import traceback
        print('-- USES CURRENT_APP', file=sys.stderr)  # +
        traceback.print_stack(file=sys.stderr)
        return _get_current_app()
else:
    get_current_app = _get_current_app


#: Proxy to current app.
current_app = Proxy(get_current_app)


def _register_app(app):
    _apps.add(app)


def _deregister_app(app):
    _apps.discard(app)


def _get_active_apps():
    return _apps


def _app_or_default(app=None):
    if app is None:
        return get_current_app()
    return app


def _app_or_default_trace(app=None):  # pragma: no cover
    from traceback import print_stack
    try:
        from billiard.process import current_process
    except ImportError:
        current_process = None
    if app is None:
        if getattr(_tls, 'current_app', None):
            print('-- RETURNING TO CURRENT APP --')  # +
            print_stack()
            return _tls.current_app
        if not current_process or current_process()._name == 'MainProcess':
            raise Exception('DEFAULT APP')
        print('-- RETURNING TO DEFAULT APP --')      # +
        print_stack()
        return default_app
    return app


def enable_trace():
    """Enable tracing of app instances."""
    global app_or_default
    app_or_default = _app_or_default_trace


def disable_trace():
    """Disable tracing of app instances."""
    global app_or_default
    app_or_default = _app_or_default


if os.environ.get('TIDER_TRACE_APP'):  # pragma: no cover
    enable_trace()
else:
    disable_trace()
