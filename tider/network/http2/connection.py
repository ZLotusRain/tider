import io
import ssl
import enum
import time
import socket
import typing
import certifi
import itertools
import collections
from typing import (
    Optional,
    Iterable,
    Iterator,
    Union,
    List,
    Sequence,
    Tuple
)

import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings

from urllib3.util import Timeout
from urllib3.exceptions import ProtocolError, ConnectTimeoutError, ProxyError, HTTPError

from .stream import SOCKET_OPTION, Stream, create_stream
from .models import (Origin, URL, enforce_bytes,
                     enforce_headers, enforce_stream, Lock, Semaphore)
from tider.network.compat import ExpirableHTTPSConnection

_Default = Timeout(connect=5.0, read=5.0)

RETRIES_BACKOFF_FACTOR = 0.5  # 0s, 0.5s, 1s, 2s, 4s, etc.

__all__ = ("HTTP2Connection", "TunnelHTTPConnection", "ConnectionNotAvailable")


def exponential_backoff(factor: float) -> Iterator[float]:
    """
    Generate a geometric sequence that has a ratio of 2 and starts with 0.

    For example:
    - `factor = 2`: `0, 2, 4, 8, 16, 32, 64, ...`
    - `factor = 3`: `0, 3, 6, 12, 24, 48, 96, ...`
    """
    yield 0
    for n in itertools.count():
        yield factor * 2 ** n


def default_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.load_verify_locations(certifi.where())
    return context


def has_body_headers(headers) -> bool:
    return any(
        k.lower() == b"content-length" or k.lower() == b"transfer-encoding"
        for k, v in headers
    )


class StreamEnded(HTTPError):
    pass


class HostLostError(HTTPError):
    pass


class ConnectionNotAvailable(HTTPError):
    pass


class BytesQueueBuffer:
    """Memory-efficient bytes buffer

    To return decoded data in read() and still follow the BufferedIOBase API, we need a
    buffer to always return the correct amount of bytes.

    This buffer should be filled using calls to put()

    Our maximum memory usage is determined by the sum of the size of:

     * self.buffer, which contains the full data
     * the largest chunk that we will copy in get()

    The worst case scenario is a single chunk, in which case we'll make a full copy of
    the data inside get().
    """

    def __init__(self) -> None:
        self.buffer: typing.Deque[bytes] = collections.deque()
        self._size: int = 0

    def __len__(self) -> int:
        return self._size

    def put(self, data: bytes) -> None:
        self.buffer.append(data)
        self._size += len(data)

    def get(self, n: int) -> bytes:
        if n == 0:
            return b""
        elif not self.buffer:
            raise RuntimeError("buffer is empty")
        elif n < 0:
            raise ValueError("n should be > 0")

        fetched = 0
        ret = io.BytesIO()
        while fetched < n:
            remaining = n - fetched
            chunk = self.buffer.popleft()
            chunk_length = len(chunk)
            if remaining < chunk_length:
                left_chunk, right_chunk = chunk[:remaining], chunk[remaining:]
                ret.write(left_chunk)
                self.buffer.appendleft(right_chunk)
                self._size -= remaining
                break
            else:
                ret.write(chunk)
                self._size -= chunk_length
            fetched += chunk_length

            if not self.buffer:
                break

        return ret.getvalue()

    def get_all(self) -> bytes:
        buffer = self.buffer
        if not buffer:
            assert self._size == 0
            return b""
        if len(buffer) == 1:
            result = buffer.pop()
        else:
            ret = io.BytesIO()
            ret.writelines(buffer.popleft() for _ in range(len(buffer)))
            result = ret.getvalue()
        self._size = 0
        return result


class Response:
    def __init__(
        self,
        status: int,
        *,
        headers=None,
        content=None,
        version=None,
        request_url=None,
        request_method=None,
        network_stream=None
    ):
        """
        Simple response to collect data from http/2 server
        """
        self.status: int = status
        self.headers = enforce_headers(headers, name="headers")
        self.stream: Union[Iterable[bytes]] = enforce_stream(content, name="content")
        self.version = version
        self.request_url = request_url
        self.request_method = request_method
        self.network_stream = network_stream


class HTTPConnectionState(enum.IntEnum):
    ACTIVE = 1
    IDLE = 2
    CLOSED = 3


class H2Connection:
    READ_NUM_BYTES = 64 * 1024
    CONFIG = h2.config.H2Configuration(validate_inbound_headers=False)

    def __init__(
        self,
        origin: Origin,
        stream: Stream,
        keepalive_expiry: typing.Optional[float] = None,
    ):
        self._origin = origin
        self._network_stream = stream
        self._keepalive_expiry: typing.Optional[float] = keepalive_expiry
        self._h2_state = h2.connection.H2Connection(config=self.CONFIG)
        self._state = HTTPConnectionState.IDLE
        self._expire_at: typing.Optional[float] = None
        self._request_count = 0
        self._init_lock = Lock()
        self._state_lock = Lock()
        self._read_lock = Lock()
        self._write_lock = Lock()
        self._sent_connection_init = False
        self._used_all_stream_ids = False
        self._connection_error = False

        # Mapping from stream ID to response stream events.
        self._events: typing.Dict[
            int,
            List[typing.Union[
                h2.events.ResponseReceived,
                h2.events.DataReceived,
                h2.events.StreamEnded,
                h2.events.StreamReset,
            ],
            ]] = {}

        # Connection terminated events are stored as state since
        # we need to handle them for all streams.
        self._connection_terminated: typing.Optional[
            h2.events.ConnectionTerminated
        ] = None

        self._read_exception: typing.Optional[Exception] = None
        self._write_exception: typing.Optional[Exception] = None

    def urlopen(self, method, url: URL, body=None, headers=None, timeout=_Default, **_):
        method = enforce_bytes(method, name='method')
        headers = enforce_headers(headers, name='headers')

        with self._state_lock:
            if self._state in (HTTPConnectionState.ACTIVE, HTTPConnectionState.IDLE):
                self._request_count += 1
                self._expire_at = None
                self._state = HTTPConnectionState.ACTIVE
            else:
                raise ConnectionNotAvailable("HTTP2 Connection not available.")

        with self._init_lock:
            if not self._sent_connection_init:
                try:
                    self._send_connection_init(timeout)
                except BaseException as exc:
                    self.close()
                    raise exc

                self._sent_connection_init = True

                # Initially start with just 1 until the remote server provides
                # its max_concurrent_streams value
                self._max_streams = 1

                local_settings_max_streams = (
                    self._h2_state.local_settings.max_concurrent_streams
                )
                self._max_streams_semaphore = Semaphore(local_settings_max_streams)

                for _ in range(local_settings_max_streams - self._max_streams):
                    self._max_streams_semaphore.acquire()

        self._max_streams_semaphore.acquire()

        try:
            stream_id = self._h2_state.get_next_available_stream_id()
            self._events[stream_id] = []
        except h2.exceptions.NoAvailableStreamIDError:  # pragma: nocover
            self._used_all_stream_ids = True
            self._request_count -= 1
            raise ConnectionNotAvailable("HTTP2 Connection not available.")

        try:
            self._send_request_headers(method, url, headers, timeout, stream_id=stream_id)
            self._send_request_body(headers, body, timeout, stream_id=stream_id)
            status, headers = self._receive_response(timeout, stream_id=stream_id)

            return Response(
                status=status,
                headers=headers,
                content=H2ConnectionByteStream(self, timeout, stream_id=stream_id),
                version="HTTP/2",
                request_url=url.url,
                request_method=method,
                network_stream=self._network_stream
            )
        except BaseException as exc:  # noqa: PIE786
            self._response_closed(stream_id=stream_id)

            if isinstance(exc, h2.exceptions.ProtocolError):
                # One case where h2 can raise a protocol error is when a
                # closed frame has been seen by the state machine.
                #
                # This happens when one stream is reading, and encounters
                # a GOAWAY event. Other flows of control may then raise
                # a protocol error at any point they interact with the 'h2_state'.
                #
                # In this case we'll have stored the event, and should raise
                # it as a RemoteProtocolError.
                if self._connection_terminated:  # pragma: nocover
                    raise ProtocolError(self._connection_terminated)
                # If h2 raises a protocol error in some other state then we
                # must somehow have made a protocol violation.
                raise ProtocolError(exc)  # pragma: nocover

            raise exc

    def _send_connection_init(self, timeout) -> None:
        """
        The HTTP/2 connection requires some initial setup before we can start
        using individual request/response streams on it.
        """
        # Need to set these manually here instead of manipulating via
        # __setitem__() otherwise the H2Connection will emit SettingsUpdate
        # frames in addition to sending the undesired defaults.
        self._h2_state.local_settings = h2.settings.Settings(
            client=True,
            initial_values={
                # Disable PUSH_PROMISE frames from the server since we don't do anything
                # with them for now.  Maybe when we support caching?
                h2.settings.SettingCodes.ENABLE_PUSH: 0,
                # These two are taken from h2 for safe defaults
                h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 100,
                h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE: 65536,
            },
        )

        # Some websites (*cough* Yahoo *cough*) balk at this setting being
        # present in the initial handshake since it's not defined in the original
        # RFC despite the RFC mandating ignoring settings you don't know about.
        del self._h2_state.local_settings[
            h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL
        ]

        self._h2_state.initiate_connection()
        self._h2_state.increment_flow_control_window(2 ** 24)
        self._write_outgoing_data(timeout)

    # Sending the request...

    def _send_request_headers(self, method, url: URL, headers, timeout, stream_id: int) -> None:
        """
        Send the request headers to a given stream ID.
        """
        end_stream = not has_body_headers(headers)
        # In HTTP/2 the ':authority' pseudo-header is used instead of 'Host'.
        # In order to gracefully handle HTTP/1.1 and HTTP/2 we always require
        # HTTP/1.1 style headers, and map them appropriately if we end up on
        # an HTTP/2 connection.
        try:
            authority = [v for k, v in headers if k.lower() == b"host"][0]
        except IndexError:
            authority = url.host

        headers = [
            (b":method", method),
            (b":authority", authority),
            (b":scheme", url.scheme),
            (b":path", url.target),
        ] + [
            (k.lower(), v)
            for k, v in headers
            if k.lower()
            not in (
                b"host",
                b"transfer-encoding",
             )
        ]

        self._h2_state.send_headers(stream_id, headers, end_stream=end_stream)
        self._h2_state.increment_flow_control_window(2 ** 24, stream_id=stream_id)
        self._write_outgoing_data(timeout)

    def _send_request_body(self, headers, body, timeout, stream_id: int) -> None:
        """
        Iterate over the request body sending it to a given stream ID.
        """
        if not has_body_headers(headers):
            return
        stream = enforce_stream(body, name='body')
        for data in stream:
            self._send_stream_data(timeout, stream_id, data)
        self._send_end_stream(timeout, stream_id)

    def _send_stream_data(
            self, timeout, stream_id: int, data: bytes
    ) -> None:
        """
        Send a single chunk of data in one or more data frames.
        """
        while data:
            max_flow = self._wait_for_outgoing_flow(timeout, stream_id)
            chunk_size = min(len(data), max_flow)
            chunk, data = data[:chunk_size], data[chunk_size:]
            self._h2_state.send_data(stream_id, chunk)
            self._write_outgoing_data(timeout)

    def _send_end_stream(self, timeout, stream_id: int) -> None:
        """
        Send an empty data frame on on a given stream ID with the END_STREAM flag set.
        """
        self._h2_state.end_stream(stream_id)
        self._write_outgoing_data(timeout)

    # Receiving the response...

    def _receive_response(
            self, timeout, stream_id: int
    ) -> typing.Tuple[int, typing.List[typing.Tuple[bytes, bytes]]]:
        """
        Return the response status code and headers for a given stream ID.
        """
        while True:
            event = self._receive_stream_event(timeout, stream_id)
            if isinstance(event, h2.events.ResponseReceived):
                break

        status_code = 200
        headers = []
        for k, v in event.headers:
            if k == b":status":
                status_code = int(v.decode("ascii", errors="ignore"))
            elif not k.startswith(b":"):
                headers.append((k, v))

        return status_code, headers

    def _receive_response_body(self, timeout, stream_id: int):
        """
        Iterator that returns the bytes of the response body for a given stream ID.
        """
        event = self._receive_stream_event(timeout, stream_id)
        if isinstance(event, h2.events.DataReceived):
            amount = event.flow_controlled_length
            self._h2_state.acknowledge_received_data(amount, stream_id)
            self._write_outgoing_data(timeout)
            return event.data
        elif isinstance(event, h2.events.StreamEnded):
            raise StreamEnded()

    def _receive_stream_event(
            self, timeout, stream_id: int
    ) -> typing.Union[
        h2.events.ResponseReceived, h2.events.DataReceived, h2.events.StreamEnded
    ]:
        """
        Return the next available event for a given stream ID.

        Will read more data from the network if required.
        """
        while not self._events.get(stream_id):
            self._receive_events(timeout, stream_id)
        event = self._events[stream_id].pop(0)
        if isinstance(event, h2.events.StreamReset):
            raise ProtocolError(event)
        return event

    def _receive_events(
            self, timeout, stream_id: typing.Optional[int] = None
    ) -> None:
        """
        Read some data from the network until we see one or more events
        for a given stream ID.
        """
        with self._read_lock:
            if self._connection_terminated is not None:
                last_stream_id = self._connection_terminated.last_stream_id
                if stream_id and last_stream_id and stream_id > last_stream_id:
                    self._request_count -= 1
                    raise ConnectionNotAvailable("HTTP2 Connection not available.")
                raise ProtocolError(self._connection_terminated)

            # This conditional is a bit icky. We don't want to block reading if we've
            # actually got an event to return for a given stream. We need to do that
            # check *within* the atomic read lock. Though it also need to be optional,
            # because when we call it from `_wait_for_outgoing_flow` we *do* want to
            # block until we've available flow control, event when we have events
            # pending for the stream ID we're attempting to send on.
            if stream_id is None or not self._events.get(stream_id):
                events = self._read_incoming_data(timeout)
                for event in events:
                    if isinstance(event, h2.events.RemoteSettingsChanged):
                        self._receive_remote_settings_change(event)

                    elif isinstance(
                        event,
                        (
                            h2.events.ResponseReceived,
                            h2.events.DataReceived,
                            h2.events.StreamEnded,
                            h2.events.StreamReset,
                        ),
                    ):
                        if event.stream_id in self._events:
                            self._events[event.stream_id].append(event)

                    elif isinstance(event, h2.events.ConnectionTerminated):
                        self._connection_terminated = event

        self._write_outgoing_data(timeout)

    def _receive_remote_settings_change(self, event: h2.events.RemoteSettingsChanged) -> None:
        max_concurrent_streams = event.changed_settings.get(
            h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS
        )
        if max_concurrent_streams:
            new_max_streams = min(
                max_concurrent_streams.new_value,
                self._h2_state.local_settings.max_concurrent_streams,
            )
            if new_max_streams and new_max_streams != self._max_streams:
                while new_max_streams > self._max_streams:
                    self._max_streams_semaphore.release()
                    self._max_streams += 1
                while new_max_streams < self._max_streams:
                    self._max_streams_semaphore.acquire()
                    self._max_streams -= 1

    def _response_closed(self, stream_id: int) -> None:
        self._max_streams_semaphore.release()
        del self._events[stream_id]
        with self._state_lock:
            if self._connection_terminated and not self._events:
                self.close()

            elif self._state == HTTPConnectionState.ACTIVE and not self._events:
                self._state = HTTPConnectionState.IDLE
                if self._keepalive_expiry is not None:
                    now = time.monotonic()
                    self._expire_at = now + self._keepalive_expiry
                if self._used_all_stream_ids:  # pragma: nocover
                    self.close()

    def close(self) -> None:
        # Note that this method unilaterally closes the connection, and does
        # not have any kind of locking in place around it.
        self._h2_state.close_connection()
        self._state = HTTPConnectionState.CLOSED
        self._network_stream.close()

    # Wrappers around network read/write operations...

    def _read_incoming_data(self, timeout) -> typing.List[h2.events.Event]:
        timeout = timeout.read_timeout

        if self._read_exception is not None:
            raise self._read_exception  # pragma: nocover

        try:
            data = self._network_stream.read(self.READ_NUM_BYTES, timeout)
            if data == b"":
                raise ProtocolError("Server disconnected")
        except Exception as exc:
            # If we get a network error we should:
            #
            # 1. Save the exception and just raise it immediately on any future reads.
            #    (For example, this means that a single read timeout or disconnect will
            #    immediately close all pending streams. Without requiring multiple
            #    sequential timeouts.)
            # 2. Mark the connection as errored, so that we don't accept any other
            #    incoming requests.
            self._read_exception = exc
            self._connection_error = True
            raise exc

        events: typing.List[h2.events.Event] = self._h2_state.receive_data(data)

        return events

    def _write_outgoing_data(self, timeout) -> None:
        timeout = timeout.read_timeout  # TODO write timeout

        with self._write_lock:
            data_to_send = self._h2_state.data_to_send()

            if self._write_exception is not None:
                raise self._write_exception  # pragma: nocover

            try:
                self._network_stream.write(data_to_send, timeout)
            except Exception as exc:  # pragma: nocover
                # If we get a network error we should:
                #
                # 1. Save the exception and just raise it immediately on any future write.
                #    (For example, this means that a single write timeout or disconnect will
                #    immediately close all pending streams. Without requiring multiple
                #    sequential timeouts.)
                # 2. Mark the connection as errored, so that we don't accept any other
                #    incoming requests.
                self._write_exception = exc
                self._connection_error = True
                raise exc

    # Flow control...

    def _wait_for_outgoing_flow(self, timeout, stream_id: int) -> int:
        """
        Returns the maximum allowable outgoing flow for a given stream.

        If the allowable flow is zero, then waits on the network until
        WindowUpdated frames have increased the flow rate.
        https://tools.ietf.org/html/rfc7540#section-6.9
        """
        local_flow: int = self._h2_state.local_flow_control_window(stream_id)
        max_frame_size: int = self._h2_state.max_outbound_frame_size
        flow = min(local_flow, max_frame_size)
        while flow == 0:
            self._receive_events(timeout)
            local_flow = self._h2_state.local_flow_control_window(stream_id)
            max_frame_size = self._h2_state.max_outbound_frame_size
            flow = min(local_flow, max_frame_size)
        return flow

    # Interface for connection pooling...

    def can_handle_request(self, origin: Origin) -> bool:
        return origin == self._origin

    def is_available(self) -> bool:
        return (
            self._state != HTTPConnectionState.CLOSED
            and not self._connection_error
            and not self._used_all_stream_ids
            and not (
                self._h2_state.state_machine.state
                == h2.connection.ConnectionState.CLOSED
            )
        )

    def has_expired(self) -> bool:
        now = time.monotonic()
        return self._expire_at is not None and now > self._expire_at

    def is_idle(self) -> bool:
        return self._state == HTTPConnectionState.IDLE

    def is_closed(self) -> bool:
        return self._state == HTTPConnectionState.CLOSED

    def info(self) -> str:
        origin = str(self._origin)
        return (
            f"{origin!r}, HTTP/2, {self._state.name}, "
            f"Request Count: {self._request_count}"
        )

    def __repr__(self):
        class_name = self.__class__.__name__
        origin = str(self._origin)
        return (
            f"<{class_name} [{origin!r}, {self._state.name}, "
            f"Request Count: {self._request_count}]>"
        )

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


class H2ConnectionByteStream:
    def __init__(
            self, connection: H2Connection, timeout, stream_id: int
    ) -> None:
        self._connection = connection
        self._timeout = timeout
        self._stream_id = stream_id
        self._closed = False
        self._buffer = BytesQueueBuffer()

        self._read = False
        self._stream_consumed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        if self._stream_consumed:
            raise RuntimeError(
                "Attempted to call '__iter__' more than once or after read(amt=None)."
            )
        self._stream_consumed = True
        if self._read:
            raise RuntimeError(
                "Attempted to call '__iter__' after read."
            )
        try:
            while True:
                try:
                    chunk = self._connection._receive_response_body(
                        timeout=self._timeout, stream_id=self._stream_id
                    )
                    if chunk is not None:
                        yield chunk
                except StreamEnded:
                    break
        except BaseException as exc:
            # If we get an exception while streaming the response,
            # we want to close the response (and possibly the connection)
            # before raising that exception.
            self.close()
            raise exc

    def read(self, amt=None) -> bytes:
        if self._stream_consumed:
            raise RuntimeError(
                "The stream has already been consumed."
            )
        self._read = True
        if amt is not None:
            while len(self._buffer) < amt:
                try:
                    chunk = self._connection._receive_response_body(
                        timeout=self._timeout, stream_id=self._stream_id
                    )
                    self._buffer.put(chunk)
                except StreamEnded:
                    self._stream_consumed = True
                    break
            return self._buffer.get(amt)
        else:
            while True:
                try:
                    chunk = self._connection._receive_response_body(
                        timeout=self._timeout, stream_id=self._stream_id
                    )
                    self._buffer.put(chunk)
                except StreamEnded:
                    self._stream_consumed = True
                    break
            return self._buffer.get_all()

    def read1(self, amt=None):
        """
        Returns any data that is available on the buffer even if it less than amt,
        if no data is available and amt is > 0, then it makes at most one read() call
        to the underlying IO object.
        """
        if self._stream_consumed:
            raise RuntimeError(
                "The stream has already been consumed."
            )
        self._read = True
        if len(self._buffer) > 0:
            if amt is None:
                return self._buffer.get_all()
            return self._buffer.get(amt)

        if amt == 0:
            return b""

        return self.read(amt)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._connection._response_closed(stream_id=self._stream_id)

    @property
    def closed(self) -> bool:
        return self._closed


class HTTP2Connection:
    """Manage HTTP/2 connection."""

    def __init__(
            self,
            origin,
            ssl_context: Optional[ssl.SSLContext] = None,
            retries: int = 0,
            keepalive_expiry: Optional[float] = None,
            local_address: Optional[str] = None,
            socket_options: Optional[Iterable[SOCKET_OPTION]] = None,
    ):
        self._origin = origin
        self._ssl_context = ssl_context
        self._keepalive_expiry = keepalive_expiry
        self._retries = retries
        self._local_address = local_address

        self._connection = None
        self._connect_failed: bool = False
        self._request_lock = Lock()
        self._socket_options = socket_options

    def urlopen(self, method, url: URL, body=None, headers=None, timeout=_Default, sni_hostname=None, **_):
        if not self.can_handle_request(url.origin):
            raise RuntimeError(
                f"Attempted to send request to {url.origin} on connection to {self._origin}"
            )

        try:
            with self._request_lock:
                if self._connection is None:
                    stream = self._connect(timeout.connect_timeout, sni_hostname)
                    self._connection = H2Connection(
                        origin=self._origin,
                        stream=stream,
                        keepalive_expiry=self._keepalive_expiry,
                    )
        except BaseException as exc:
            self._connect_failed = True
            raise exc

        return self._connection.urlopen(method, url, body, headers, timeout=timeout)

    def _connect(self, timeout=None, sni_hostname=None) -> Stream:
        retries_left = self._retries
        delays = exponential_backoff(factor=RETRIES_BACKOFF_FACTOR)

        while True:
            try:
                kwargs = {
                    "host": self._origin.host.decode("ascii"),
                    "port": self._origin.port,
                    "timeout": timeout,
                    "socket_options": self._socket_options,
                }
                stream = create_stream(**kwargs)

                if self._origin.scheme in (b"https", b"wss"):
                    ssl_context = (
                        default_ssl_context()
                        if self._ssl_context is None
                        else self._ssl_context
                    )
                    alpn_protocols = ["http/1.1", "h2"]
                    ssl_context.set_alpn_protocols(alpn_protocols)

                    kwargs = {
                        "ssl_context": ssl_context,
                        "server_hostname": sni_hostname
                                           or self._origin.host.decode("ascii"),
                        "timeout": timeout,
                    }
                    stream = stream.start_tls(**kwargs)
                return stream
            except (ConnectionNotAvailable, ConnectTimeoutError):
                if retries_left <= 0:
                    raise
                retries_left -= 1
                delay = next(delays)
                time.sleep(delay)

    def can_handle_request(self, origin: Origin) -> bool:
        return origin == self._origin

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()

    def is_available(self) -> bool:
        if self._connection is None:
            # If HTTP/2 support is enabled, and the resulting connection could
            # end up as HTTP/2 then we should indicate the connection as being
            # available to service multiple requests.
            return (
                self._origin.scheme == b"https"
                and not self._connect_failed
            )
        return self._connection.is_available()

    def has_expired(self) -> bool:
        if self._connection is None:
            return self._connect_failed
        return self._connection.has_expired()

    def is_idle(self) -> bool:
        if self._connection is None:
            return self._connect_failed
        return self._connection.is_idle()

    def is_closed(self) -> bool:
        if self._connection is None:
            return self._connect_failed
        return self._connection.is_closed()

    def info(self) -> str:
        if self._connection is None:
            return "CONNECTION FAILED" if self._connect_failed else "CONNECTING"
        return self._connection.info()

    def __repr__(self):
        return f"<{self.__class__.__name__} [{self.info()}]>"

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


def merge_headers(
        default_headers: Optional[Sequence[Tuple[bytes, bytes]]] = None,
        override_headers: Optional[Sequence[Tuple[bytes, bytes]]] = None,
) -> List[Tuple[bytes, bytes]]:
    """
    Append default_headers and override_headers, de-duplicating if a key exists
    in both cases.
    """
    default_headers = [] if default_headers is None else list(default_headers)
    override_headers = [] if override_headers is None else list(override_headers)
    has_override = set(key.lower() for key, value in override_headers)
    default_headers = [
        (key, value)
        for key, value in default_headers
        if key.lower() not in has_override
    ]
    return default_headers + override_headers


class TunnelHTTPConnection:
    def __init__(
        self,
        proxy_origin: Origin,
        remote_origin: Origin,
        ssl_context: Optional[ssl.SSLContext] = None,
        proxy_ssl_context: Optional[ssl.SSLContext] = None,
        proxy_headers: Optional[Sequence[Tuple[bytes, bytes]]] = None,
        keepalive_expiry: Optional[float] = None,
        socket_options: Optional[Iterable[SOCKET_OPTION]] = None,
    ):
        self._connection = ExpirableHTTPSConnection(
            host=proxy_origin.host.decode("ascii"),
            port=proxy_origin.port,
            ssl_context=proxy_ssl_context,
        )

        self._proxy_origin = proxy_origin
        self._remote_origin = remote_origin
        self._ssl_context = ssl_context
        self._proxy_ssl_context = proxy_ssl_context
        self._proxy_headers = enforce_headers(proxy_headers, name="proxy_headers")
        self._socket_options = socket_options

        self._keepalive_expiry = keepalive_expiry
        self._connect_lock = Lock()
        self._connected = False

    def _prepare_proxy(self, conn: ExpirableHTTPSConnection, timeout=None, sni_hostname=None):
        """
        Establishes a tunnel connection through HTTP CONNECT.

        Tunnel connection is established early because otherwise httplib would
        improperly set Host: header to proxy's IP:port.
        """

        proxy_headers = {'Host': self._remote_origin.host.decode('ascii'), 'Accept': '*/*'}
        proxy_headers.update({k.decode('ascii'): v.decode('ascii') for k, v in self._proxy_headers})
        conn.set_tunnel(self._remote_origin.host.decode('ascii'), self._remote_origin.port, proxy_headers)

        if self._proxy_origin.scheme == "https":
            conn.tls_in_tls_required = True

        kwargs = {
            "host": self._proxy_origin.host.decode("ascii"),
            "port": self._proxy_origin.port,
            "timeout": timeout,
            "socket_options": self._socket_options,
        }
        stream = create_stream(**kwargs)

        if self._proxy_origin.scheme in (b"https", b"wss"):
            ssl_context = (
                default_ssl_context()
                if self._proxy_ssl_context is None
                else self._proxy_ssl_context
            )
            alpn_protocols = ["http/1.1", "h2"]
            ssl_context.set_alpn_protocols(alpn_protocols)

            kwargs = {
                "ssl_context": ssl_context,
                "server_hostname": sni_hostname or self._proxy_origin.host.decode("ascii"),
                "timeout": timeout,
            }
            stream = stream.start_tls(**kwargs)

        conn.sock = stream._sock
        conn._tunnel()
        return stream

    def urlopen(self, method, url, body=None, headers=None, timeout=_Default, **_) -> Response:
        connect_timeout = timeout.connect_timeout

        with self._connect_lock:
            if not self._connected:
                try:
                    stream = self._prepare_proxy(self._connection, connect_timeout)
                except (ConnectTimeoutError, socket.timeout, OSError) as e:
                    raise ProxyError('Cannot connect to proxy.', error=e)
                finally:
                    self._connection.sock = None  # don't close sock.
                    self._connection = self._connection.close()

                # Upgrade the stream to SSL
                ssl_context = (
                    default_ssl_context()
                    if self._ssl_context is None
                    else self._ssl_context
                )
                alpn_protocols = ["http/1.1", "h2"]
                ssl_context.set_alpn_protocols(alpn_protocols)

                kwargs = {
                    "ssl_context": ssl_context,
                    "server_hostname": self._remote_origin.host.decode("ascii"),
                    "timeout": connect_timeout,
                }

                stream = stream.start_tls(**kwargs)

                self._connection = H2Connection(
                    origin=self._remote_origin,
                    stream=stream,
                    keepalive_expiry=self._keepalive_expiry,
                )

                self._connected = True
        return self._connection.urlopen(method, url, body, headers, timeout)

    def can_handle_request(self, origin: Origin) -> bool:
        return origin == self._remote_origin

    def close(self) -> None:
        self._connection.close()

    def info(self) -> str:
        return self._connection.info()

    def is_available(self) -> bool:
        return self._connection.is_available()

    def has_expired(self) -> bool:
        return self._connection.has_expired()

    def is_idle(self) -> bool:
        return self._connection.is_idle()

    def is_closed(self) -> bool:
        if self._connection is None:
            return True
        return self._connection.is_closed()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} [{self.info()}]>"
