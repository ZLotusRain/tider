import httpcore
import contextlib
import email.message
from requests.exceptions import (
    Timeout,
    ConnectTimeout,
    ReadTimeout,
    ConnectionError,
    ProxyError
)

from tider.exceptions import (
    WriteTimeout,
    PoolTimeout,
    NetworkError,
    ReadError,
    WriteError,
    UnsupportedProtocol,
    ProtocolError,
    LocalProtocolError,
    RemoteProtocolError
)
from tider.utils.decoders import _get_decoder
from tider.utils.collections import BytesSlicer


@contextlib.contextmanager
def map_httpcore_exceptions():
    try:
        yield
    except Exception as exc:  # noqa: PIE-786
        mapped_exc = None

        for from_exc, to_exc in HTTPCORE_EXC_MAP.items():
            if not isinstance(exc, from_exc):
                continue
            # We want to map to the most specific exception we can find.
            # Eg if `exc` is an `httpcore.ReadTimeout`, we want to map to
            # `httpx.ReadTimeout`, not just `httpx.TimeoutException`.
            if mapped_exc is None or issubclass(to_exc, mapped_exc):
                mapped_exc = to_exc

        if mapped_exc is None:  # pragma: no cover
            raise

        message = str(exc)
        raise mapped_exc(message) from exc


HTTPCORE_EXC_MAP = {
    httpcore.TimeoutException: Timeout,
    httpcore.ConnectTimeout: ConnectTimeout,
    httpcore.ReadTimeout: ReadTimeout,
    httpcore.WriteTimeout: WriteTimeout,
    httpcore.PoolTimeout: PoolTimeout,
    httpcore.NetworkError: NetworkError,
    httpcore.ConnectError: ConnectionError,
    httpcore.ReadError: ReadError,
    httpcore.WriteError: WriteError,
    httpcore.ProxyError: ProxyError,
    httpcore.UnsupportedProtocol: UnsupportedProtocol,
    httpcore.ProtocolError: ProtocolError,
    httpcore.LocalProtocolError: LocalProtocolError,
    httpcore.RemoteProtocolError: RemoteProtocolError,
}


class _CookieCompatResponse:

    def __init__(self, headers):
        self.msg = email.message.Message()
        for k, v in headers.items():
            self.msg[k] = v


class MockHTTPResponse:

    def __init__(self, body, headers, decode_content=False):
        self._stream = body
        self.headers = headers
        self._original_response = _CookieCompatResponse(headers)

        self.decode_content = decode_content
        self._decoder = None
        self._closed = False

    def _get_content_decoder(self):
        if self._decoder is not None:
            return self._decoder

        content_encodings = self.headers.get("content-encoding", "").lower().split(',')
        if len(content_encodings) > 1:
            self._decoder = _get_decoder(content_encodings)
        else:
            self._decoder = _get_decoder(content_encodings[0])

        return self._decoder

    def _flush_decoder(self) -> bytes:
        """
        Flushes the decoder. Should only be called if the decoder is actually
        being used.
        """
        if self._decoder:
            return self._decoder.decode(b"") + self._decoder.flush()
        return b""

    def read(self, decode_content):
        return b"".join(self.stream(decode_content))

    def stream(self, amt=2 ** 16, decode_content=None):
        decoder = self._get_content_decoder()
        slicer = BytesSlicer(chunk_size=amt)
        decode_content = decode_content or self.decode_content
        with map_httpcore_exceptions():
            for raw_bytes in self.iter_raw():
                decoded = decoder.decode(raw_bytes) if decode_content else raw_bytes
                for chunk in slicer.slice(decoded):
                    yield chunk
            if decode_content:
                decoded = decoder.flush()
                for chunk in slicer.slice(decoded):
                    yield chunk  # pragma: no cover
            for chunk in slicer.flush():
                yield chunk

    def iter_raw(self, chunk_size=None):
        """
        A byte-iterator over the raw response content.
        """

        slicer = BytesSlicer(chunk_size=chunk_size)

        for raw_stream_bytes in self._stream:
            for chunk in slicer.slice(raw_stream_bytes):
                yield chunk

        for chunk in slicer.flush():
            yield chunk

        self.close()

    def release_conn(self):
        return self.close()

    def close(self):
        if not self._closed and self._stream is not None:
            self._stream.close()
        self._closed = True
