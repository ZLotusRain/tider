import threading
from weakref import WeakSet


class _TLS(threading.local):
    #: Apps with the :attr:`~celery.app.base.BaseApp.set_as_current` attribute
    #: sets this, so it will always contain the last instantiated app,
    #: and is the default app returned by :func:`app_or_default`.
    current_tider = None
    workers = WeakSet()


_tls = _TLS()


def set_current_tider(tider):
    _tls.current_tider = tider


def get_current_tider():
    return _tls.current_tider


def add_worker(worker):
    _tls.workers.add(worker)
