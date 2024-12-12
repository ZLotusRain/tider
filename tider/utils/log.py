import logging
import numbers
import sys
import threading
import traceback
from contextlib import contextmanager
from typing import AnyStr, Sequence  # noqa

from kombu.utils.encoding import safe_str

from tider.utils.term import colored

__all__ = (
    'ColorFormatter', 'LoggingProxy', 'base_logger',
    'set_in_sighandler', 'in_sighandler', 'get_logger',
    'get_crawler_logger', 'mlevel',
    'get_multiprocessing_logger', 'reset_multiprocessing_logger', 'LOG_LEVELS'
)

RESERVED_LOGGER_NAMES = {'tider', 'tider.crawler', 'crawler'}

LOG_LEVELS = dict(logging._nameToLevel)
LOG_LEVELS.update(logging._levelToName)
LOG_LEVELS.setdefault('FATAL', logging.FATAL)
LOG_LEVELS.setdefault(logging.FATAL, 'FATAL')


def _get_logger(logger_or_name: str) -> logging.Logger:
    """Get logger by name."""
    if isinstance(logger_or_name, str):
        logger = logging.getLogger(logger_or_name)
    else:
        logger = logger_or_name
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


base_logger = logger = _get_logger('tider')
_in_sighandler = False


def set_in_sighandler(value):
    """Set flag signifying  that we're inside a signal handler."""
    global _in_sighandler
    _in_sighandler = value


def iter_open_logger_fds():
    seen = set()
    loggers = (list(logging.Logger.manager.loggerDict.values()) +
               [logging.getLogger(None)])
    for l in loggers:
        try:
            for handler in l.handlers:
                try:
                    if handler not in seen:  # pragma: no cover
                        yield handler.stream
                        seen.add(handler)
                except AttributeError:
                    pass
        except AttributeError:  # PlaceHolder does not have handlers
            pass


@contextmanager
def in_sighandler():
    """Context that records that we are in a signal handler."""
    set_in_sighandler(True)
    try:
        yield
    finally:
        set_in_sighandler(False)


def logger_isa(l, p, max=1000):
    """Check whether l == p or l in p's parents"""
    this, seen = l, set()
    for _ in range(max):
        if this == p:
            return True
        else:
            if this in seen:
                raise RuntimeError(
                    f'Logger {l.name!r} parents recursive',
                )
            seen.add(this)
            this = this.parent
            if not this:
                break
    else:  # pragma: no cover
        raise RuntimeError(f'Logger hierarchy exceeds {max}')
    return False


def _using_logger_parent(parent_logger, logger_):
    """Set logger parent"""
    if not logger_isa(logger_, parent_logger):
        logger_.parent = parent_logger
    return logger_


def get_logger(name):
    """Get logger by name."""
    l = _get_logger(name)
    if logging.root not in (l, l.parent) and l is not base_logger:
        l = _using_logger_parent(base_logger, l)
    return l


crawler_logger = get_logger('tider.crawler')


def get_crawler_logger(name):
    """Get logger for crawler module by name."""
    if name in RESERVED_LOGGER_NAMES:
        raise RuntimeError(f'Logger name {name!r} is reserved!')
    return _using_logger_parent(crawler_logger, get_logger(name))


def mlevel(level):
    """Convert level name/int to log level."""
    if level and not isinstance(level, numbers.Integral):
        return LOG_LEVELS[level.upper()]
    return level


class ColorFormatter(logging.Formatter):
    """Logging formatter that adds colors based on severity."""

    #: Loglevel -> Color mapping.
    COLORS = colored().names
    colors = {
        'DEBUG': COLORS['blue'],
        'WARNING': COLORS['yellow'],
        'ERROR': COLORS['red'],
        'CRITICAL': COLORS['magenta'],
    }

    def __init__(self, fmt=None, use_color=True):
        super().__init__(fmt)
        self.use_color = use_color

    def formatException(self, ei):
        if ei and not isinstance(ei, tuple):
            ei = sys.exc_info()
        r = super().formatException(ei)
        return r

    def format(self, record):
        msg = super().format(record)
        color = self.colors.get(record.levelname)

        # reset exception info later for other handlers...
        einfo = sys.exc_info() if record.exc_info == 1 else record.exc_info

        if color and self.use_color:
            try:
                try:
                    if isinstance(msg, str):
                        return str(color(safe_str(msg)))
                    return safe_str(color(msg))
                except UnicodeDecodeError:  # pragma: no cover
                    return safe_str(msg)  # skip colors
            except Exception as exc:  # pylint: disable=broad-except
                prev_msg, record.exc_info, record.msg = (
                    record.msg, 1, '<Unrepresentable {!r}: {!r}>'.format(
                        type(msg), exc
                    ),
                )
                try:
                    return super().format(record)
                finally:
                    record.msg, record.exc_info = prev_msg, einfo
        else:
            return safe_str(msg)


class LoggingProxy:
    """Forward file object to :class:`logging.Logger` instance.

    Arguments:
        logger (~logging.Logger): Logger instance to forward to.
        loglevel (int, str): Log level to use when logging messages.
    """

    mode = 'w'
    name = None
    closed = False
    loglevel = logging.ERROR
    _thread = threading.local()

    def __init__(self, logger, loglevel=None):
        # pylint: disable=redefined-outer-name
        # Note that the logger global is redefined here, be careful changing.
        self.logger = logger
        self.loglevel = mlevel(loglevel or self.logger.level or self.loglevel)
        self._safewrap_handlers()

    def _safewrap_handlers(self):
        # Make the logger handlers dump internal errors to
        # :data:`sys.__stderr__` instead of :data:`sys.stderr` to circumvent
        # infinite loops.

        def wrap_handler(handler):                  # pragma: no cover

            class WithSafeHandleError(logging.Handler):

                def handleError(self, record):
                    try:
                        traceback.print_exc(None, sys.__stderr__)
                    except OSError:
                        pass    # see python issue 5971

            handler.handleError = WithSafeHandleError().handleError
        return [wrap_handler(h) for h in self.logger.handlers]

    def write(self, data):
        # type: (AnyStr) -> int
        """Write message to logging object."""
        if _in_sighandler:
            safe_data = safe_str(data)
            print(safe_data, file=sys.__stderr__)
            return len(safe_data)
        if getattr(self._thread, 'recurse_protection', False):
            # Logger is logging back to this file, so stop recursing.
            return 0
        if data and not self.closed:
            self._thread.recurse_protection = True
            try:
                safe_data = safe_str(data).rstrip('\n')
                if safe_data:
                    self.logger.log(self.loglevel, safe_data)
                    return len(safe_data)
            finally:
                self._thread.recurse_protection = False
        return 0

    def writelines(self, sequence):
        # type: (Sequence[str]) -> None
        """Write list of strings to file.

        The sequence can be any iterable object producing strings.
        This is equivalent to calling :meth:`write` for each string.
        """
        for part in sequence:
            self.write(part)

    def flush(self):
        # This object is not buffered so any :meth:`flush`
        # requests are ignored.
        pass

    def close(self):
        # when the object is closed, no write requests are
        # forwarded to the logging object anymore.
        self.closed = True

    def isatty(self):
        """Here for file support."""
        return False


class StreamLogger:
    """Fake file-like stream object that redirects writes to a logger instance

    Taken from:
        https://www.electricmonk.nl/log/2011/08/14/redirect-stdout-and-stderr-to-a-logger-in-python/
    """
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())

    def flush(self):
        for h in self.logger.handlers:
            h.flush()


class InterceptHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        try:
            import loguru
            super().__init__(level)
            self.logger = loguru.logger
        except ImportError:
            raise ImportError("Using log colorized on Windows, but the 'loguru' package is not installed.")

    def emit(self, record):
        # Retrieve context where the logging call occurred, this happens to be in the 6th frame upward
        logger_opt = self.logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


def get_multiprocessing_logger():
    """Return the multiprocessing logger."""
    try:
        from billiard import util
    except ImportError:  # pragma: no cover
        pass
    else:
        return util.get_logger()


def reset_multiprocessing_logger():
    """Reset multiprocessing logging setup."""
    try:
        from billiard import util
    except ImportError:  # pragma: no cover
        pass
    else:
        if hasattr(util, '_logger'):  # pragma: no cover
            util._logger = None


def current_process():
    try:
        from billiard import process
    except ImportError:
        pass
    else:
        return process.current_process()


def current_process_index(base=1):
    index = getattr(current_process(), 'index', None)
    return index + base if index is not None else index
