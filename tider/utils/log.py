import logging
import numbers
import sys
import time
import threading
import traceback
from contextlib import contextmanager
from typing import AnyStr, Sequence, Optional  # noqa

from kombu.utils.encoding import safe_str

from tider.utils.term import colored
from tider.utils.decorators import inthread

__all__ = (
    'ColorFormatter', 'LoggingProxy', 'OTLPHandler', 'get_otlp_handler',
    'base_logger', 'set_in_sighandler', 'in_sighandler',
    'mlevel', 'get_logger', 'get_crawler_logger',
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


class OTLPHandler(logging.Handler):
    """Logging handler that ships log records via gRPC using the OTLP protocol.

    Follows the OpenTelemetry gRPC exporter pattern: records are buffered and
    exported in a background thread with exponential-backoff retry on transient
    failures.  Compatible with any OTLP/gRPC receiver: Grafana Loki ≥ 2.9,
    OpenTelemetry Collector, Grafana Cloud, etc.

    Arguments:
        endpoint (str): gRPC ``host:port``, e.g. ``localhost:4317``.
            For Grafana Cloud use the regional OTLP endpoint.
        resource (dict): OpenTelemetry resource attributes sent with
            every batch.  At minimum set ``{"service.name": "myapp"}``.
        headers (dict): Per-call gRPC metadata, e.g.
            ``{"authorization": "Bearer <token>"}``.
        insecure (bool): Use ``insecure_channel`` (no TLS).  Set ``False``
            to enable TLS via ``ssl_channel_credentials``.
        compression: gRPC compression algorithm (``grpc.Compression.Gzip``
            etc.).  ``None`` disables compression.
        timeout (float): Per-RPC deadline in seconds.
        flush_interval (float): Background flush period in seconds.
        batch_size (int): Flush immediately when the buffer reaches this size.
        max_retries (int): Maximum retry attempts on retryable RPC errors.
        level (int): Minimum log level forwarded to Loki.
    """

    #: Default resource attributes merged with per-instance *resource*.
    default_resource: dict = {}

    #: Python log level → (OTLP SeverityNumber, severity_text).
    _severity_map = {
        logging.DEBUG: (5, 'DEBUG'),
        logging.INFO: (9, 'INFO'),
        logging.WARNING: (13, 'WARN'),
        logging.ERROR: (17, 'ERROR'),
        logging.CRITICAL: (21, 'FATAL'),
    }

    def __init__(
        self,
        endpoint: str = 'localhost:4317',
        resource: dict = None,
        headers: dict = None,
        insecure: bool = True,
        compression=None,
        timeout: float = 10.0,
        flush_interval: float = 1.0,
        batch_size: int = 512,
        max_retries: int = 5,
        level: int = logging.NOTSET,
    ):
        try:
            import grpc  # noqa: F401
            from opentelemetry.proto.collector.logs.v1 import (  # noqa: F401
                logs_service_pb2_grpc,
            )
        except ImportError as exc:
            raise ImportError(
                "LokiHandler requires 'grpcio' and 'opentelemetry-proto'. "
                f"Install with: pip install grpcio opentelemetry-proto  ({exc})"
            )
        import grpc

        super().__init__(level)
        self.endpoint = endpoint
        self.resource = dict(
            self.default_resource, **(resource or {})
        )
        self._headers = tuple((headers or {}).items())
        self._insecure = insecure
        self._compression = compression
        self._timeout = timeout
        self._max_retries = max_retries
        self._flush_interval = flush_interval
        self.batch_size = batch_size
        self._retryable_codes = frozenset({
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.ABORTED,
            grpc.StatusCode.OUT_OF_RANGE,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DATA_LOSS,
        })
        self._buffer: list = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._channel = None
        self._stub = None
        self._initialize_channel()
        self._flush_thread = self._flush_loop()
        self._flush_thread.start()

    def _initialize_channel(self) -> None:
        """Create (or recreate) the gRPC channel and LogsService stub."""
        import grpc
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2_grpc import (
            LogsServiceStub,
        )

        kwargs = {}
        if self._compression is not None:
            kwargs['compression'] = self._compression

        if self._insecure:
            self._channel = grpc.insecure_channel(self.endpoint, **kwargs)
        else:
            self._channel = grpc.secure_channel(
                self.endpoint, grpc.ssl_channel_credentials(), **kwargs
            )
        self._stub = LogsServiceStub(self._channel)

    @staticmethod
    def _kv(key: str, value: str):
        from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
        return KeyValue(key=key, value=AnyValue(string_value=value))

    def _translate_record(self, record: logging.LogRecord):
        """Convert a :class:`logging.LogRecord` to an OTLP *LogRecord* proto."""
        from opentelemetry.proto.common.v1.common_pb2 import AnyValue
        from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord as OTLPLogRecord

        severity_number, severity_text = self._severity_map.get(
            record.levelno, (0, record.levelname)
        )
        ts_ns = int(record.created * 1_000_000_000)
        attrs = [
            self._kv('log.logger', record.name),
            self._kv('log.filename', record.filename),
            self._kv('log.func_name', record.funcName),
            self._kv('log.lineno', str(record.lineno)),
        ]
        if record.exc_info:
            import traceback as _tb
            attrs.append(self._kv(
                'exception.stacktrace',
                ''.join(_tb.format_exception(*record.exc_info)),
            ))
        return OTLPLogRecord(
            time_unix_nano=ts_ns,
            observed_time_unix_nano=ts_ns,
            severity_number=severity_number,
            severity_text=severity_text,
            body=AnyValue(string_value=self.format(record)),
            attributes=attrs,
        )

    def _build_request(self, otlp_records: list):
        from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
            ExportLogsServiceRequest,
        )
        from opentelemetry.proto.common.v1.common_pb2 import InstrumentationScope
        from opentelemetry.proto.logs.v1.logs_pb2 import ResourceLogs, ScopeLogs
        from opentelemetry.proto.resource.v1.resource_pb2 import Resource

        resource = Resource(
            attributes=[self._kv(k, v) for k, v in self.resource.items()]
        )
        scope = InstrumentationScope(name='tider')
        scope_logs = ScopeLogs(scope=scope, log_records=otlp_records)
        resource_logs = ResourceLogs(resource=resource, scope_logs=[scope_logs])
        return ExportLogsServiceRequest(resource_logs=[resource_logs])

    def _send(self, otlp_records: list) -> None:
        import random
        import grpc

        request = self._build_request(otlp_records)
        deadline = time.time() + self._timeout

        for attempt in range(self._max_retries):
            try:
                self._stub.Export(
                    request,
                    metadata=self._headers,
                    timeout=deadline - time.time(),
                )
                return
            except grpc.RpcError as exc:
                code = exc.code()
                backoff = (2 ** attempt) * random.uniform(0.8, 1.2)

                if code == grpc.StatusCode.UNAVAILABLE and attempt == 0:
                    try:
                        self._channel.close()
                    except Exception:
                        pass
                    self._initialize_channel()

                if (
                    code not in self._retryable_codes
                    or attempt + 1 >= self._max_retries
                    or backoff > (deadline - time.time())
                    or self._stop_event.is_set()
                ):
                    self.handleError(None)
                    return

                self._stop_event.wait(backoff)

    @inthread(name='otlp-grpc-flush', daemon=True)
    def _flush_loop(self) -> None:
        """Background thread: flush the buffer on a fixed interval."""
        while not self._stop_event.wait(self._flush_interval):
            self.flush()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            otlp_record = self._translate_record(record)
            with self._lock:
                self._buffer.append(otlp_record)
                should_flush = len(self._buffer) >= self.batch_size
            if should_flush:
                self.flush()
        except Exception:
            self.handleError(record)

    def flush(self) -> None:
        """Drain the buffer and export all pending records via gRPC."""
        with self._lock:
            if not self._buffer:
                return
            records, self._buffer = self._buffer, []
        self._send(records)

    def close(self) -> None:
        """Stop the background thread, flush remaining records, and close the channel."""
        try:
            self._stop_event.set()
            self._flush_thread.join(timeout=self._timeout)
            self.flush()
            if self._channel is not None:
                self._channel.close()
        finally:
            super().close()


def get_otlp_handler(endpoint: str, resource: Optional[dict]) -> logging.Handler:
    """Setup OpenTelemetry logging and return a logging handler.

    Arguments:
        endpoint (str): gRPC ``host:port``, e.g. ``localhost:4317``.
            For Grafana Cloud use the regional OTLP endpoint.
        resource (dict): OpenTelemetry resource attributes sent with
            every batch.  At minimum set ``{"service.name": "myapp"}``.
    """
    try:
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import Resource
    except ImportError as exc:
        raise ImportError(
            "OTLP Handler requires 'opentelemetry-sdk' and 'opentelemetry-exporter-otlp-proto-grpc'. "
            f"Install with: pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc  ({exc})"
        )
    # Create Resource (service identification)
    resource = Resource.create(resource or {})

    # Create LoggerProvider
    logger_provider = LoggerProvider(resource=resource)
    set_logger_provider(logger_provider)

    # Create OTLP Log Exporter
    # Use insecure=True for internal gRPC endpoints usually
    otlp_exporter = OTLPLogExporter(
        endpoint=endpoint,
        insecure=True,
    )

    # Add BatchLogRecordProcessor
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))

    # Create logging handler
    handler = LoggingHandler(
        level=logging.DEBUG,
        logger_provider=logger_provider,
    )

    return handler


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
