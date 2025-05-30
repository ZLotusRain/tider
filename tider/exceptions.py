import sys
import contextlib
from click import ClickException
from typing import Iterator, Mapping, Type


ExceptionMapping = Mapping[Type[Exception], Type[Exception]]


def reraise(tp, value, tb=None):
    """Reraise exception."""
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


def raise_with_context(exc):
    exc_info = sys.exc_info()
    if not exc_info:
        raise exc
    elif exc_info[1] is exc:
        raise
    raise exc from exc_info[1]


@contextlib.contextmanager
def map_exceptions(mapping: ExceptionMapping) -> Iterator[None]:
    try:
        yield
    except Exception as exc:  # noqa: PIE786
        for from_exc, to_exc in mapping.items():
            if isinstance(exc, from_exc):
                raise to_exc(exc) from exc
        raise  # pragma: nocover


class SpiderShutdown(SystemExit):
    """Signals that the spider should perform a warm shutdown."""


class SpiderTerminate(SystemExit):
    """Signals that the spider should perform a cold shutdown."""


class TiderDeprecationWarning(Warning):
    """Warning category for deprecated features, since the default
    DeprecationWarning is silenced on Python 2.7+
    """


class TiderWarning(UserWarning):
    """Base class for all Tider warnings."""


class DuplicateSpiderWarning(TiderWarning):
    """Multiple spiders are using the same name."""


class SecurityWarning(TiderWarning):
    """Potential security issue found."""


class TiderCommandException(ClickException):
    """A general command exception which stores an exit code."""

    def __init__(self, message, exit_code):
        super().__init__(message=message)
        self.exit_code = exit_code


class TiderException(Exception):
    """Base class for all Tider errors."""


class ImproperlyConfigured(TiderException):
    """Tider is somehow improperly configured."""


class UnSupportedMethod(TiderException):
    """Raised when use the unsupported method in class."""


class SecurityError(TiderException):
    """Security related exception."""


class FileStoreError(TiderException):
    """Failed to persist file."""


class DownloadError(TiderException):
    pass


class InvalidRequest(DownloadError):
    pass


class HTTPError(DownloadError):
    """Base class for all http errors in tider."""


class ProxyError(DownloadError):
    """An error occurred while establishing a proxy connection."""


class InvalidProxy(ProxyError):
    """Try to connect to an invalid proxy."""


class ExclusiveProxy(ProxyError):
    """Try to connect to a exclusive proxy."""


class ProtocolError(DownloadError):
    """The protocol was violated."""


class SSLError(DownloadError):
    """An SSL error occurred."""


class TooManyRedirects(DownloadError):
    """Too many redirects."""


class InvalidURL(DownloadError):
    pass


class InvalidHeader(DownloadError):
    pass


class InvalidProxyURL(DownloadError):
    pass


class InvalidSchema(DownloadError):
    pass


class Timeout(DownloadError):
    pass


class PoolTimeout(Timeout):
    """
    compat for `httpcore.PoolTimeout`
    """


class WriteTimeout(Timeout):
    """
     compat for `httpcore.WriteError`
     """


class ReadTimeout(Timeout):
    pass


class ConnectTimeout(Timeout):
    pass


class ConnectionError(DownloadError):
    pass


class RetryError(DownloadError):
    pass


class ContentDecodingError(DownloadError):
    """
    Decoding of the response failed, due to a malformed encoding.
    """


class ResponseReadError(DownloadError):
    pass


class ResponseStreamConsumed(DownloadError):
    pass


class WgetError(DownloadError):
    def __init__(self, cmd, returncode=None, timeout=None, output=None, stderr=None):
        self.cmd = cmd
        self.returncode = returncode
        self.timeout = timeout
        self.output = output
        self.stderr = stderr

    def __str__(self):
        if self.returncode is not None:
            return f'Wget failed with returncode {self.returncode}): {self.cmd}'
        elif self.timeout is not None:
            return f'Wget commands {self.cmd} timed out after {self.timeout} seconds'
        else:
            return f'Wget failed with unknown error: {self.cmd}'

    @property
    def stdout(self):
        """Alias for output attribute, to match stderr"""
        return self.output

    @stdout.setter
    def stdout(self, value):
        # There's no obvious reason to set this, but allow it anyway so
        # .stdout is a transparent alias for .output
        self.output = value


class ReadError(DownloadError):
    """
    compat for `httpcore.ReadError`
    """


class WriteError(DownloadError):
    """
    compat for `httpcore.WriteError`
    """


class NetworkError(DownloadError):
    """
    compat for `httpcore.NetworkError`
    """


class UnsupportedProtocol(DownloadError):
    """
    compat for `httpcore.UnsupportedProtocol`
    """


class LocalProtocolError(DownloadError):
    """
    compat for `httpcore.LocalProtocolError`
    """


class RemoteProtocolError(DownloadError):
    """
    compat for `httpcore.RemoteProtocolError`
    """


class BackendError(TiderException):
    """An issue writing or reading to/from the backend."""


class BackendReadError(BackendError):
    """An issue reading from the backend."""


class BackendStoreError(BackendError):
    """An issue writing to the backend."""


class CrawlerRevokedError(TiderException):
    """The crawler has been revoked."""
