import os
import sys
import loguru
import logging
from logging.handlers import RotatingFileHandler

from tider.settings import Settings


class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Retrieve context where the logging call occurred, this happens to be in the 6th frame upward
        logger_opt = loguru.logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


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


def configure_logging(settings=None, install_root_handler=True):
    if not sys.warnoptions:
        # Route warnings through python logging
        logging.captureWarnings(True)

    if isinstance(settings, dict) or settings is None:
        settings = Settings(settings)

    if settings.getbool('LOG_STDOUT'):
        sys.stdout = StreamLogger(logging.getLogger('stdout'))

    if install_root_handler:
        install_tider_root_handler(settings)


def install_tider_root_handler(settings):
    global _tider_root_handler

    if (_tider_root_handler is not None
            and _tider_root_handler in logging.root.handlers):
        logging.root.removeHandler(_tider_root_handler)
    logging.root.setLevel(logging.NOTSET)
    _tider_root_handler = _get_handler(settings)
    logging.root.addHandler(_tider_root_handler)


def get_tider_root_handler():
    return _tider_root_handler


_tider_root_handler = None


def _get_handler(settings):
    formatter = logging.Formatter(fmt=settings.get("LOG_FORMAT"))

    filename = settings.get('LOG_FILE')
    if settings.getbool('LOG_FILE_ENABLED') and filename:
        directory = settings.get('LOG_DIRECTORY', '/')
        filename = os.path.join(directory, filename)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(settings.get('LOG_LEVEL'))
        logging.root.addHandler(stream_handler)
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        mode = 'a' if settings.getbool('LOG_FILE_APPEND') else 'w'
        encoding = settings.get('LOG_ENCODING')
        max_bytes = settings.get('LOG_MAX_BYTES')
        backup_count = settings.get('LOG_BACKUP_COUNT')
        handler = RotatingFileHandler(filename, mode=mode, maxBytes=max_bytes,
                                      backupCount=backup_count, encoding=encoding)
    elif settings.getbool("LOG_COLORIZE"):
        handler = InterceptHandler()
    elif settings.getbool('LOG_ENABLED'):
        # code below will turn log to stdout
        # handler.stream = sys.stdout
        handler = logging.StreamHandler()
    else:
        handler = logging.NullHandler()

    handler.setFormatter(formatter)
    handler.setLevel(settings.get('LOG_LEVEL'))
    return handler
