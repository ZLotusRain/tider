import sys
import ssl
import typing
import socket
import select
from functools import partial

from urllib3.exceptions import ConnectionError, ConnectTimeoutError, HTTPError

from tider.exceptions import map_exceptions


class ReadError(HTTPError):
    pass


class WriteError(HTTPError):
    pass


class WriteTimeout(HTTPError):
    pass


class ReadTimeout(HTTPError):
    pass


SOCKET_OPTION = typing.Union[
    typing.Tuple[int, int, int],
    typing.Tuple[int, int, typing.Union[bytes, bytearray]],
    typing.Tuple[int, int, None, int],
]


def is_socket_readable(sock: typing.Optional[socket.socket]) -> bool:
    """
    Return whether a socket, as identifed by its file descriptor, is readable.
    "A socket is readable" means that the read buffer isn't empty, i.e. that calling
    .recv() on it would immediately return some data.
    """
    # NOTE: we want check for readability without actually attempting to read, because
    # we don't want to block forever if it's not readable.

    # In the case that the socket no longer exists, or cannot return a file
    # descriptor, we treat it as being readable, as if it the next read operation
    # on it is ready to return the terminating `b""`.
    sock_fd = None if sock is None else sock.fileno()
    if sock_fd is None or sock_fd < 0:  # pragma: nocover
        return True

    # The implementation below was stolen from:
    # https://github.com/python-trio/trio/blob/20ee2b1b7376db637435d80e266212a35837ddcc/trio/_socket.py#L471-L478
    # See also: https://github.com/encode/httpcore/pull/193#issuecomment-703129316

    # Use select.select on Windows, and when poll is unavailable and select.poll
    # everywhere else. (E.g. When eventlet is in use. See #327)
    if (
        sys.platform == "win32" or getattr(select, "poll", None) is None
    ):  # pragma: nocover
        rready, _, _ = select.select([sock_fd], [], [], 0)
        return bool(rready)
    p = select.poll()
    p.register(sock_fd, select.POLLIN)
    return bool(p.poll(0))


class TLSinTLSStream:  # pragma: no cover
    """
    Because the standard `SSLContext.wrap_socket` method does
    not work for `SSLSocket` objects, we need this class
    to implement TLS stream using an underlying `SSLObject`
    instance in order to support TLS on top of TLS.
    """

    # Defined in RFC 8449
    TLS_RECORD_SIZE = 16384

    def __init__(
        self,
        sock: socket.socket,
        ssl_context: ssl.SSLContext,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ):
        self._sock = sock
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()

        self.ssl_obj = ssl_context.wrap_bio(
            incoming=self._incoming,
            outgoing=self._outgoing,
            server_hostname=server_hostname,
        )

        self._sock.settimeout(timeout)
        self._perform_io(self.ssl_obj.do_handshake)

    def _perform_io(self, func: typing.Callable[..., typing.Any]):
        ret = None

        while True:
            errno = None
            try:
                ret = func()
            except (ssl.SSLWantReadError, ssl.SSLWantWriteError) as e:
                errno = e.errno

            self._sock.sendall(self._outgoing.read())

            if errno == ssl.SSL_ERROR_WANT_READ:
                buf = self._sock.recv(self.TLS_RECORD_SIZE)

                if buf:
                    self._incoming.write(buf)
                else:
                    self._incoming.write_eof()
            if errno is None:
                return ret

    def read(self, max_bytes: int, timeout: typing.Optional[float] = None) -> bytes:
        exc_map = {socket.timeout: ReadTimeout, OSError: ReadError}
        with map_exceptions(exc_map):
            self._sock.settimeout(timeout)
            return typing.cast(
                bytes, self._perform_io(partial(self.ssl_obj.read, max_bytes))
            )

    def write(self, buffer: bytes, timeout: typing.Optional[float] = None) -> None:
        exc_map = {socket.timeout: WriteTimeout, OSError: WriteError}
        with map_exceptions(exc_map):
            self._sock.settimeout(timeout)
            while buffer:
                nsent = self._perform_io(partial(self.ssl_obj.write, buffer))
                buffer = buffer[nsent:]

    def close(self) -> None:
        self._sock.close()

    def start_tls(self, ssl_context: ssl.SSLContext, server_hostname=None, timeout=None):
        raise NotImplementedError()

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return self.ssl_obj
        if info == "client_addr":
            return self._sock.getsockname()
        if info == "server_addr":
            return self._sock.getpeername()
        if info == "socket":
            return self._sock
        if info == "is_readable":
            return is_socket_readable(self._sock)


class Stream:
    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock

    def read(self, max_bytes: int, timeout: typing.Optional[float] = None) -> bytes:
        exc_map = {socket.timeout: ReadTimeout, OSError: ReadError}
        with map_exceptions(exc_map):
            self._sock.settimeout(timeout)
            return self._sock.recv(max_bytes)

    def write(self, buffer: bytes, timeout: typing.Optional[float] = None) -> None:
        if not buffer:
            return

        exc_map = {socket.timeout: WriteTimeout, OSError: WriteError}
        with map_exceptions(exc_map):
            while buffer:
                self._sock.settimeout(timeout)
                n = self._sock.send(buffer)
                buffer = buffer[n:]

    def close(self) -> None:
        self._sock.close()

    def start_tls(self, ssl_context: ssl.SSLContext, server_hostname=None, timeout=None):
        exc_map = {socket.timeout: ConnectTimeoutError, OSError: ConnectionError}
        with map_exceptions(exc_map):
            try:
                if isinstance(self._sock, ssl.SSLSocket):  # pragma: no cover
                    # If the underlying socket has already been upgraded
                    # to the TLS layer (i.e. is an instance of SSLSocket),
                    # we need some additional smarts to support TLS-in-TLS.
                    return TLSinTLSStream(
                        self._sock, ssl_context, server_hostname, timeout
                    )
                else:
                    self._sock.settimeout(timeout)
                    sock = ssl_context.wrap_socket(
                        self._sock, server_hostname=server_hostname
                    )
            except Exception as exc:  # pragma: nocover
                self.close()
                raise exc
        return Stream(sock)

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object" and isinstance(self._sock, ssl.SSLSocket):
            return self._sock._sslobj  # type: ignore
        if info == "client_addr":
            return self._sock.getsockname()
        if info == "server_addr":
            return self._sock.getpeername()
        if info == "socket":
            return self._sock
        if info == "is_readable":
            return is_socket_readable(self._sock)


def create_stream(
    host: str,
    port: int,
    timeout: typing.Optional[float] = None,
    local_address: typing.Optional[str] = None,
    socket_options: typing.Optional[typing.Iterable[SOCKET_OPTION]] = None
) -> Stream:
    # Note that we automatically include `TCP_NODELAY`
    # in addition to any other custom socket options.
    if socket_options is None:
        socket_options = []  # pragma: no cover
    address = (host, port)
    source_address = None if local_address is None else (local_address, 0)
    exc_map = {socket.timeout: ConnectTimeoutError, OSError: ConnectionError}

    with map_exceptions(exc_map):
        sock = socket.create_connection(
            address,
            timeout,
            source_address=source_address,
        )
        for option in socket_options:
            sock.setsockopt(*option)  # pragma: no cover
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return Stream(sock)
