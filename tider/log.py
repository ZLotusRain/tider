"""Logging configuration.

The Tider instances logging section: ``Tider.log``.

Sets up logging for the crawler and other programs,
redirects standard outs, colors log output, patches logging
related compatibility fixes, and so on.

Taken from: celery
"""
import logging
import os
import sys
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

from kombu.utils.encoding import set_default_encoding_file

from tider import signals
from tider.utils.nodenames import node_format
from tider.utils.log import (ColorFormatter, LoggingProxy, get_logger, get_multiprocessing_logger, mlevel,
                             reset_multiprocessing_logger)

__all__ = ('Logging', 'set_in_sighandler', 'in_sighandler')

_in_sighandler = False

MP_LOG = os.environ.get('MP_LOG', False)


def set_in_sighandler(value):
    """Set flag signifiying that we're inside a signal handler."""
    global _in_sighandler
    _in_sighandler = value


@contextmanager
def in_sighandler():
    """Context that records that we are in a signal handler."""
    set_in_sighandler(True)
    try:
        yield
    finally:
        set_in_sighandler(False)


class Logging:
    """Application logging setup (app.log)."""

    #: The logging subsystem is only configured once per process.
    #: setup_logging_subsystem sets this flag, and subsequent calls
    #: will do nothing.
    _setups = []

    def __init__(self, app):
        self.app = app
        self.loglevel = mlevel(logging.WARN)
        self.format = self.app.conf.get('LOG_FORMAT')
        self.colorize = self.app.conf.get('LOG_COLORIZE')

    def setup(self, loglevel=None, logfile=None, maxbytes=30*1024*1024, backup_count=1, encoding=None,
              fmt=None, redirect_stdouts=False, redirect_level='WARNING', colorize=None, hostname=None, debug=False):
        loglevel = mlevel(loglevel)
        self.setup_logging_subsystem(
            loglevel, logfile, maxbytes=maxbytes, backup_count=backup_count,
            encoding=encoding, fmt=fmt, colorize=colorize, hostname=hostname, debug=debug,
        )
        if redirect_stdouts:
            self.redirect_stdouts(redirect_level)
        # Route warnings through python logging
        if not sys.warnoptions:
            logging.captureWarnings(True)

    def redirect_stdouts(self, loglevel=None, name='tider.redirected'):
        self.redirect_stdouts_to_logger(
            get_logger(name), loglevel=loglevel
        )

    def setup_logging_subsystem(self, loglevel=None, logfile=None, fmt=None,
                                colorize=None, hostname=None, debug=False, **kwargs):
        if hostname in self._setups:
            return
        if logfile and hostname:
            logfile = node_format(logfile, hostname)
        Logging._setups.append(hostname)
        loglevel = mlevel(loglevel or self.loglevel)
        fmt = fmt or self.format
        colorize = self.supports_color(colorize, logfile)
        reset_multiprocessing_logger()
        receivers = signals.setup_logging.send(
            sender=None, loglevel=loglevel, logfile=logfile,
            format=format, colorize=colorize,
        )

        if not receivers:
            root = logging.getLogger()
            if len(root.handlers) == 1 and isinstance(root.handlers[0], logging.StreamHandler):
                # reset automatically configured root by logging.
                handler = root.handlers[0]
                root.removeHandler(handler)
                handler.close()

            if self.app.conf.get('LOG_HIJACK_ROOT'):
                root.handlers = []
                get_logger('tider').handlers = []
                get_logger('tider.crawler').handlers = []
                get_logger('tider.redirected').handlers = []

            # Configure root logger
            self._configure_logger(
                root, logfile, loglevel, fmt, colorize, debug=debug, **kwargs
            )

            # Configure the multiprocessing logger
            self._configure_logger(
                get_multiprocessing_logger(),
                logfile, loglevel if MP_LOG else logging.ERROR,
                fmt, colorize, debug=debug, **kwargs
            )

            signals.after_setup_logger.send(
                sender=None, logger=root,
                loglevel=loglevel, logfile=logfile,
                format=format, colorize=colorize,
            )

        try:
            stream = logging.getLogger().handlers[0].stream
        except (AttributeError, IndexError):
            pass
        else:
            set_default_encoding_file(stream)

        # This is a hack for multiprocessing's fork+exec, so that
        # logging before Process.run works.
        logfile_name = logfile if isinstance(logfile, str) else ''
        os.environ.update(_MP_FORK_LOGLEVEL_=str(loglevel),
                          _MP_FORK_LOGFILE_=logfile_name,
                          _MP_FORK_LOGFORMAT_=fmt)

    def _configure_logger(self, logger, logfile, loglevel,
                          fmt, colorize, debug=False, **kwargs):
        if logger is not None:
            self.setup_handlers(logger, logfile, fmt,
                                colorize, debug=debug, **kwargs)
            if loglevel:
                logger.setLevel(loglevel)

    def redirect_stdouts_to_logger(self, logger, loglevel=None,
                                   stdout=True, stderr=True):
        """Redirect :class:`sys.stdout` and :class:`sys.stderr` to logger.

        Arguments:
            logger (logging.Logger): Logger instance to redirect to.
            loglevel (int, str): The loglevel redirected message
                will be logged as.
            stdout (bool): Whether to redirect stdout to logger.
            stderr (bool): Whether to redirect stderr to logger.
        """
        proxy = LoggingProxy(logger, loglevel)
        if stdout:
            sys.stdout = proxy
        if stderr:
            sys.stderr = proxy
        return proxy

    def supports_color(self, colorize=None, logfile=None):
        colorize = self.colorize if colorize is None else colorize
        if self.app.IS_WINDOWS:
            # Windows does not support ANSI color codes.
            return False
        if colorize or colorize is None:
            # Only use color if there's no active log file
            # and stderr is an actual terminal.
            return logfile is None and sys.stderr.isatty()
        return colorize

    def setup_handlers(self, logger, logfile, fmt, colorize, debug=False,
                       formatter=ColorFormatter, maxbytes=0, backup_count=0, encoding=None):
        if self._is_configured(logger):
            return logger
        handler = self._detect_handler(logfile, maxbytes=maxbytes, backup_count=backup_count, encoding=encoding)
        handler.setFormatter(formatter(fmt, use_color=colorize))
        logger.addHandler(handler)

        if debug and isinstance(handler, RotatingFileHandler):
            handler = logging.StreamHandler()
            handler.setFormatter(formatter(fmt, use_color=True))
            logger.addHandler(handler)
        return logger

    def _detect_handler(self, logfile=None, maxbytes=0, backup_count=0, encoding=None):
        """Create handler from filename, an open stream or `None` (stderr)."""
        # TODO remove streamhandler if logfile
        logfile = sys.__stderr__ if logfile is None else logfile
        if hasattr(logfile, 'write'):
            return logging.StreamHandler(logfile)
        if isinstance(logfile, str):
            if not os.path.exists(os.path.dirname(logfile)):
                os.makedirs(os.path.dirname(logfile))
        return RotatingFileHandler(logfile, maxBytes=maxbytes, backupCount=backup_count, encoding=encoding or 'utf-8')

    def _has_handler(self, logger):
        return any(
            not isinstance(h, logging.NullHandler)
            for h in logger.handlers or []
        )

    def _is_configured(self, logger):
        return self._has_handler(logger) and not getattr(
            logger, '_rudimentary_setup', False)

    def get_default_logger(self, name='tider', **kwargs):
        return get_logger(name)

    def reset_setups(self):
        self._setups = []
