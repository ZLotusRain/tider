import ssl
import sys
import threading
from base64 import b64encode
from types import TracebackType
from typing import Iterator, Iterable, List, Optional, Type

from urllib3.util import Timeout
from urllib3.exceptions import TimeoutError, EmptyPoolError

from .models import (
    Origin,
    URL,
    HTTP2Response,
    enforce_bytes,
    enforce_url,
    enforce_headers,
)
from .stream import SOCKET_OPTION
from .context import create_ssl_context
from .connection import HTTP2Connection, TunnelHTTPConnection, ConnectionNotAvailable

__all__ = ("HTTP2ConnectionPool", "HTTPProxy", "is_same_host")

_Default = Timeout(connect=5.0, read=5.0)


def port_or_default(url: URL):
    if url.port is not None:
        return url.port
    return {"http": 80, "https": 443}.get(url.scheme)


def is_same_host(url, request_url):
    if request_url.startswith('/'):
        return True
    url = enforce_url(url, name="url")
    request_url = enforce_url(request_url, name='request_url')
    return (
        url.scheme == request_url.scheme
        and url.host == request_url.host
        and port_or_default(url) == port_or_default(request_url)
    )


class PoolTimeout(TimeoutError):
    pass


class Event:
    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self) -> None:
        self._event.set()

    def wait(self, timeout: Optional[float] = None):
        if timeout == float("inf"):  # pragma: no cover
            timeout = None
        return self._event.wait(timeout=timeout)


class ThreadLock:
    """
    This is a threading-only lock for no-I/O contexts.

    In the sync case `ThreadLock` provides thread locking.
    In the async case `AsyncThreadLock` is a no-op.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def __enter__(self) -> "ThreadLock":
        self._lock.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        self._lock.release()


class PoolRequest:
    def __init__(self, url: URL):
        self.url = url
        self.connection = None
        self._connection_acquired = Event()

    def assign_to_connection(self, connection):
        self.connection = connection
        self._connection_acquired.set()

    def clear_connection(self):
        self.connection = None
        self._connection_acquired = Event()

    def wait_for_connection(self, timeout: Optional[float] = None) -> Optional[HTTP2Connection]:
        if self.connection is None:
            if not self._connection_acquired.wait(timeout=timeout):
                return None
        return self.connection

    def is_queued(self) -> bool:
        return self.connection is None


class HTTP2ConnectionPool:
    """
    A connection pool for making HTTP/2 requests.
    """

    def __init__(
        self,
        verify=False,
        cert=None,
        trust_env=True,
        maxsize: Optional[int] = 10,
        max_keepalive_connections: Optional[int] = None,
        keepalive_expiry: Optional[float] = None,
        retries: int = 0,
        socket_options: Optional[Iterable[SOCKET_OPTION]] = None,
        **_,
    ) -> None:
        """
        A connection pool for making HTTP/2 requests.

        Parameters:
            maxsize: The maximum number of concurrent HTTP connections that
                the pool should allow. Any attempt to send a request on a pool that
                would exceed this amount will block until a connection is available.
            max_keepalive_connections: The maximum number of idle HTTP connections
                that will be maintained in the pool.
            keepalive_expiry: The duration in seconds that an idle HTTP connection
                may be maintained for before being expired from the pool.
            retries: The maximum number of retries when trying to establish a
                connection.
            socket_options: Socket options that have to be included
             in the TCP socket when the connection was established.
        """
        # http or https
        self._ssl_context = create_ssl_context(verify=verify, cert=cert, trust_env=trust_env, http2=True)

        self._max_connections = (
            sys.maxsize if maxsize is None else maxsize
        )
        self._max_keepalive_connections = (
            sys.maxsize
            if max_keepalive_connections is None
            else max_keepalive_connections
        )
        self._max_keepalive_connections = min(
            self._max_connections, self._max_keepalive_connections
        )

        self._keepalive_expiry = keepalive_expiry
        self.retries = retries

        self._socket_options = socket_options

        # The mutable state on a connection pool is the queue of incoming requests,
        # and the set of connections that are servicing those requests.
        self._connections = []
        self._requests: List[PoolRequest] = []

        # We only mutate the state of the connection pool within an 'optional_thread_lock'
        # context. This holds a threading lock unless we're running in async mode,
        # in which case it is a no-op.
        self._optional_thread_lock = ThreadLock()

    def create_connection(self, origin: Origin):
        return HTTP2Connection(
            origin=origin,
            ssl_context=self._ssl_context,
            keepalive_expiry=self._keepalive_expiry,
            retries=self.retries,
            socket_options=self._socket_options,
        )

    @property
    def connections(self):
        """
        Return a list of the connections currently in the pool.
        """
        return list(self._connections)

    def urlopen(self, method, url: str, body=None, headers=None, timeout=_Default, pool_timeout=None,
                preload_content=True, **response_kw):
        """
        Send an HTTP request, and return an HTTP response.
        """
        url = URL(url=url)
        pool_timeout = pool_timeout or timeout.connect_timeout

        with self._optional_thread_lock:
            # Add the incoming request to our request queue.
            pool_request = PoolRequest(url)
            self._requests.append(pool_request)

        try:
            while True:
                with self._optional_thread_lock:
                    # Assign incoming requests to available connections,
                    # closing or creating new connections as required.
                    closing = self._assign_requests_to_connections()
                self._close_connections(closing)

                # Wait until this request has an assigned connection.
                connection = pool_request.wait_for_connection(timeout=pool_timeout)
                if not connection:
                    raise EmptyPoolError(
                        self,
                        "No more http2 connections are available within time limit."
                    )

                try:
                    # Send the request on the assigned connection.
                    response = connection.urlopen(method=method, url=pool_request.url,
                                                  body=body, headers=headers, timeout=timeout)
                except ConnectionNotAvailable:
                    # In some cases a connection may initially be available to
                    # handle a request, but then become unavailable.
                    #
                    # In this case we clear the connection and try again.
                    pool_request.clear_connection()
                else:
                    break  # pragma: nocover

        except BaseException as exc:
            with self._optional_thread_lock:
                # For any exception or cancellation we remove the request from
                # the queue, and then re-assign requests to connections.
                self._requests.remove(pool_request)
                closing = self._assign_requests_to_connections()

            self._close_connections(closing)
            raise exc from None

        # Return the response. Note that in this case we still have to manage
        # the point at which the response is closed.
        return HTTP2Response(
            status=response.status,
            headers=response.headers,
            body=PoolByteStream(stream=response.stream, pool_request=pool_request, pool=self),
            version="HTTP/2",
            request_url=url.url,
            request_method=method,
            preload_content=preload_content,
            **response_kw
        )

    def _assign_requests_to_connections(self):
        """
        Manage the state of the connection pool, assigning incoming
        requests to connections as available.

        Called whenever a new request is added or removed from the pool.

        Any closing connections are returned, allowing the I/O for closing
        those connections to be handled seperately.
        """
        closing_connections = []

        # First we handle cleaning up any connections that are closed,
        # have expired their keep-alive, or surplus idle connections.
        for connection in list(self._connections):
            if connection.is_closed():
                # log: "removing closed connection"
                self._connections.remove(connection)
            elif connection.has_expired():
                # log: "closing expired connection"
                self._connections.remove(connection)
                closing_connections.append(connection)
            elif (
                connection.is_idle()
                and len([connection.is_idle() for connection in self._connections])
                > self._max_keepalive_connections
            ):
                # log: "closing idle connection"
                self._connections.remove(connection)
                closing_connections.append(connection)

        # Assign queued requests to connections.
        queued_requests = [request for request in self._requests if request.is_queued()]
        for pool_request in queued_requests:
            origin = pool_request.url.origin
            available_connections = [
                connection
                for connection in self._connections
                if connection.can_handle_request(origin) and connection.is_available()
            ]
            idle_connections = [
                connection for connection in self._connections if connection.is_idle()
            ]

            # There are three cases for how we may be able to handle the request:
            #
            # 1. There is an existing connection that can handle the request.
            # 2. We can create a new connection to handle the request.
            # 3. We can close an idle connection and then create a new connection
            #    to handle the request.
            if available_connections:
                # log: "reusing existing connection"
                connection = available_connections[0]
                pool_request.assign_to_connection(connection)
            elif len(self._connections) < self._max_connections:
                # log: "creating new connection"
                connection = self.create_connection(origin)
                self._connections.append(connection)
                pool_request.assign_to_connection(connection)
            elif idle_connections:
                # log: "closing idle connection"
                connection = idle_connections[0]
                self._connections.remove(connection)
                closing_connections.append(connection)
                # log: "creating new connection"
                connection = self.create_connection(origin)
                self._connections.append(connection)
                pool_request.assign_to_connection(connection)

        return closing_connections

    def _close_connections(self, closing):
        # Close connections which have been removed from the pool.
        for connection in closing:
            connection.close()

    def close(self) -> None:
        # Explicitly close the connection pool.
        # Clears all existing requests and connections.
        with self._optional_thread_lock:
            closing_connections = list(self._connections)
            self._connections = []
        self._close_connections(closing_connections)

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        with self._optional_thread_lock:
            request_is_queued = [request.is_queued() for request in self._requests]
            connection_is_idle = [
                connection.is_idle() for connection in self._connections
            ]

            num_active_requests = request_is_queued.count(False)
            num_queued_requests = request_is_queued.count(True)
            num_active_connections = connection_is_idle.count(False)
            num_idle_connections = connection_is_idle.count(True)

        requests_info = (
            f"Requests: {num_active_requests} active, {num_queued_requests} queued"
        )
        connection_info = (
            f"Connections: {num_active_connections} active, {num_idle_connections} idle"
        )

        return f"<{class_name} [{requests_info} | {connection_info}]>"


class PoolByteStream:
    def __init__(
        self,
        stream,
        pool_request: PoolRequest,
        pool,
    ) -> None:
        self._stream = stream
        self._pool_request = pool_request
        self._pool = pool
        self._closed = False

    def __iter__(self) -> Iterator[bytes]:
        try:
            for part in self._stream:
                yield part
        except BaseException as exc:
            self.close()
            raise exc from None

    def read(self, amt=None):
        return self._stream.read(amt)

    def read1(self, amt=None):
        return self._stream.read1(amt)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if hasattr(self._stream, "close"):
                self._stream.close()

            with self._pool._optional_thread_lock:
                self._pool._requests.remove(self._pool_request)
                closing = self._pool._assign_requests_to_connections()

            self._pool._close_connections(closing)

    @property
    def closed(self) -> bool:
        return self._closed


def build_auth_header(username: bytes, password: bytes) -> bytes:
    userpass = username + b":" + password
    return b"Basic " + b64encode(userpass)


class HTTPProxy(HTTP2ConnectionPool):
    """
    A connection pool that sends requests via an HTTP proxy.
    """

    def __init__(
        self,
        proxy_url,
        proxy_auth=None,
        proxy_headers=None,
        verify=False,
        cert=None,
        trust_env=True,
        proxy_ssl_context: Optional[ssl.SSLContext] = None,
        maxsize: Optional[int] = 10,
        max_keepalive_connections: Optional[int] = None,
        keepalive_expiry: Optional[float] = None,
        retries: int = 0,
        socket_options: Optional[Iterable[SOCKET_OPTION]] = None,
        **_,
    ) -> None:
        """
        A connection pool for making HTTP requests.

        Parameters:
            proxy_url: The URL to use when connecting to the proxy server.
                For example `"http://127.0.0.1:8080/"`.
            proxy_auth: Any proxy authentication as a two-tuple of
                (username, password). May be either bytes or ascii-only str.
            proxy_headers: Any HTTP headers to use for the proxy requests.
                For example `{"Proxy-Authorization": "Basic <username>:<password>"}`.
            proxy_ssl_context: The same as `ssl_context`, but for a proxy server rather than a remote origin.
            maxsize: The maximum number of concurrent HTTP connections that
                the pool should allow. Any attempt to send a request on a pool that
                would exceed this amount will block until a connection is available.
            max_keepalive_connections: The maximum number of idle HTTP connections
                that will be maintained in the pool.
            keepalive_expiry: The duration in seconds that an idle HTTP connection
                may be maintained for before being expired from the pool.
            retries: The maximum number of retries when trying to establish
                a connection.
        """
        super().__init__(
            verify=verify,
            cert=cert,
            trust_env=trust_env,
            maxsize=maxsize,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry,
            retries=retries,
            socket_options=socket_options,
        )

        self._proxy_url = enforce_url(proxy_url, name="proxy_url")
        if (
            self._proxy_url.scheme == b"http" and proxy_ssl_context is not None
        ):  # pragma: no cover
            raise RuntimeError(
                "The `proxy_ssl_context` argument is not allowed for the http scheme"
            )

        self._proxy_ssl_context = proxy_ssl_context
        self._proxy_headers = enforce_headers(proxy_headers, name="proxy_headers")
        if proxy_auth is not None:
            username = enforce_bytes(proxy_auth[0], name="proxy_auth")
            password = enforce_bytes(proxy_auth[1], name="proxy_auth")
            authorization = build_auth_header(username, password)
            self._proxy_headers = [
                (b"Proxy-Authorization", authorization)
            ] + self._proxy_headers

    def create_connection(self, origin: Origin):
        if origin.scheme == b"http":
            raise RuntimeError("Use HTTP/1.1 connections for HTTP requests.")
        return TunnelHTTPConnection(
            proxy_origin=self._proxy_url.origin,
            proxy_headers=self._proxy_headers,
            remote_origin=origin,
            ssl_context=self._ssl_context,
            proxy_ssl_context=self._proxy_ssl_context,
            keepalive_expiry=self._keepalive_expiry,
        )
