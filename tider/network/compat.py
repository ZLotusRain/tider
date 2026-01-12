from __future__ import annotations

import collections
import inspect
import io
import typing
import warnings
import zlib
import sys
import time
import queue
import http.client
import email.message
from typing import Union

try:
    try:
        import brotlicffi as brotli  # type: ignore[import-not-found]
    except ImportError:
        import brotli  # type: ignore[import-not-found]
except ImportError:
    brotli = None

from urllib.parse import urlparse, urlunparse
from requests.utils import to_native_string

from http.client import HTTPException as HTTPException
from urllib3.response import HTTPResponse as _HTTPResponse
from urllib3._collections import RecentlyUsedContainer as _RecentlyUsedContainer
from urllib3.connection import HTTPConnection, HTTPSConnection, BaseSSLError
from urllib3.connectionpool import HTTPConnectionPool as _HTTPConnectionPool
from urllib3.connectionpool import HTTPSConnectionPool as _HTTPSConnectionPool
from urllib3.exceptions import (
    HTTPError,
    ClosedPoolError,
    EmptyPoolError,
    PoolError,
    DecodeError,
    DependencyWarning,
    IncompleteRead,
    InvalidChunkLength,
    ProtocolError,
)
from urllib3.util.response import is_fp_closed
from urllib3.util.connection import is_connection_dropped

from tider.utils.log import get_logger

__all__ = ('CookieCompatRequest', 'CookieCompatResponse', 'RecentlyUsedContainer',
           'HTTPConnectionPool', 'HTTPSConnectionPool', 'ExpirableHTTPSConnection',
           'ExpirableHTTPConnection', 'close_expired_connections')

urllib3_log = get_logger('urllib3.connectionpool')


class CookieCompatRequest:
    """
    Wraps a `Request` instance up in a compatibility interface suitable
    for use with `CookieJar` operations.

    The original request object is read-only.
    """

    def __init__(self, request):
        self._r = request
        self._new_headers = {}

    def get_host(self):
        return urlparse(self._r.url).netloc

    def get_origin_req_host(self):
        return self.get_host()

    def get_full_url(self):
        # Only return the response's URL if the user hadn't set the Host
        # header(host of url may differ from host in headers,
        # (e.g https://www.bing.com/  Host: cn.bing.com))
        if not self._r.headers.get("Host"):
            return self._r.url
        # If they did set it, retrieve it and reconstruct the expected domain
        host = to_native_string(self._r.headers["Host"], encoding="utf-8")
        parsed = urlparse(self._r.url)
        # Reconstruct the URL as we expect it
        return urlunparse(
            [
                parsed.scheme,
                host,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            ]
        )

    def is_unverifiable(self):
        return True

    def has_header(self, name):
        return name in self._r.headers or name in self._new_headers

    def get_header(self, name, default=None):
        return self._r.headers.get(name, self._new_headers.get(name, default))

    def add_unredirected_header(self, name, value):
        self._new_headers[name] = value

    def get_new_headers(self):
        return self._new_headers

    @property
    def unverifiable(self):
        return self.is_unverifiable()

    @property
    def origin_req_host(self):
        return self.get_origin_req_host()


class CookieCompatResponse:
    """
    Wraps a `Response` instance up in a compatibility interface suitable
    for use with `CookieJar` operations.
    """

    def __init__(self, response):
        self._headers = response.headers

    def info(self) -> email.message.Message:
        info = email.message.Message()
        for key, value in self._headers.items():
            # Note that setting `info[key]` here is an "append" operation,
            # not a "replace" operation.
            # https://docs.python.org/3/library/email.compat32-message.html#email.message.Message.__setitem__
            info[key] = value
        return info


class RecentlyUsedContainer(_RecentlyUsedContainer):

    def __init__(self, maxsize=10, dispose_func=None, clean_func=None):
        super().__init__(maxsize=maxsize, dispose_func=dispose_func)
        self.clean_func = clean_func

    def __getitem__(self, key):
        # Re-insert the item if item has not expired, moving it to the end of the eviction line.
        with self.lock:
            item = self._container.pop(key)
            if hasattr(item, 'has_expired') and item.has_expired():
                if self.dispose_func:
                    self.dispose_func(item)
                raise KeyError(key)
            self._container[key] = item
            return item

    def clean_up(self):
        if not self.clean_func:
            return
        # don't use lock here to avoid stuck.
        for key in list(iter(self._container.keys())):
            item = self.get(key)
            if item is None:
                continue
            self.clean_func(item)


class ExpirableConnection:

    def __init__(self, keepalive_expiry=None):
        self._expire_at = None
        self._keepalive_expiry = keepalive_expiry

    def activate(self):
        # don't need lock because the connection
        # can only be fetched by one request at a time.
        self._expire_at = None

    def deactivate(self):
        if not self._keepalive_expiry or self._keepalive_expiry < 0:
            return
        if self._expire_at is not None:
            return
        now = time.monotonic()
        self._expire_at = now + self._keepalive_expiry

    def has_expired(self):
        now = time.monotonic()
        keepalive_expired = self._expire_at is not None and now > self._expire_at
        return keepalive_expired

    if sys.version_info < (3, 11, 9) or ((3, 12) <= sys.version_info < (3, 12, 3)):
        # Taken from python/cpython#100986 which was backported in 3.11.9 and 3.12.3.
        # When using connection_from_host, host will come without brackets.
        def _wrap_ipv6(self, ip: bytes) -> bytes:
            if b":" in ip and ip[0] != b"["[0]:
                return b"[" + ip + b"]"
            return ip

        if sys.version_info < (3, 11, 9):
            # `_tunnel` copied from 3.11.13 backporting
            # https://github.com/python/cpython/commit/0d4026432591d43185568dd31cef6a034c4b9261
            # and https://github.com/python/cpython/commit/6fbc61070fda2ffb8889e77e3b24bca4249ab4d1
            def _tunnel(self) -> None:
                _MAXLINE = http.client._MAXLINE  # type: ignore[attr-defined]
                connect = b"CONNECT %s:%d HTTP/1.0\r\n" % (  # type: ignore[str-format]
                    self._wrap_ipv6(self._tunnel_host.encode("ascii")),  # type: ignore[union-attr]
                    self._tunnel_port,
                )
                headers = [connect]
                for header, value in self._tunnel_headers.items():  # type: ignore[attr-defined]
                    headers.append(f"{header}: {value}\r\n".encode("latin-1"))
                headers.append(b"\r\n")
                # Making a single send() call instead of one per line encourages
                # the host OS to use a more optimal packet size instead of
                # potentially emitting a series of small packets.
                self.send(b"".join(headers))
                del headers

                response = self.response_class(self.sock, method=self._method)  # type: ignore[attr-defined]
                try:
                    (version, code, message) = response._read_status()  # type: ignore[attr-defined]

                    if code != http.HTTPStatus.OK:
                        self.close()
                        raise OSError(
                            f"Tunnel connection failed: {code} {message.strip()}"
                        )
                    while True:
                        line = response.fp.readline(_MAXLINE + 1)
                        if len(line) > _MAXLINE:
                            raise http.client.LineTooLong("header line")
                        if not line:
                            # for sites which EOF without sending a trailer
                            break
                        if line in (b"\r\n", b"\n", b""):
                            break

                        if self.debuglevel > 0:
                            print("header:", line.decode())
                finally:
                    # https://github.com/urllib3/urllib3/pull/3252
                    response.close()

        elif (3, 12) <= sys.version_info < (3, 12, 3):
            # `_tunnel` copied from 3.12.11 backporting
            # https://github.com/python/cpython/commit/23aef575c7629abcd4aaf028ebd226fb41a4b3c8
            def _tunnel(self) -> None:  # noqa: F811
                connect = b"CONNECT %s:%d HTTP/1.1\r\n" % (  # type: ignore[str-format]
                    self._wrap_ipv6(self._tunnel_host.encode("idna")),  # type: ignore[union-attr]
                    self._tunnel_port,
                )
                headers = [connect]
                for header, value in self._tunnel_headers.items():  # type: ignore[attr-defined]
                    headers.append(f"{header}: {value}\r\n".encode("latin-1"))
                headers.append(b"\r\n")
                # Making a single send() call instead of one per line encourages
                # the host OS to use a more optimal packet size instead of
                # potentially emitting a series of small packets.
                self.send(b"".join(headers))
                del headers

                response = self.response_class(self.sock, method=self._method)  # type: ignore[attr-defined]
                try:
                    (version, code, message) = response._read_status()  # type: ignore[attr-defined]

                    self._raw_proxy_headers = http.client._read_headers(response.fp)  # type: ignore[attr-defined]

                    if self.debuglevel > 0:
                        for header in self._raw_proxy_headers:
                            print("header:", header.decode())

                    if code != http.HTTPStatus.OK:
                        self.close()
                        raise OSError(
                            f"Tunnel connection failed: {code} {message.strip()}"
                        )

                finally:
                    response.close()


class ExpirableHTTPConnection(ExpirableConnection, HTTPConnection):

    def __init__(self, *args, keepalive_expiry=60, **kwargs):
        HTTPConnection.__init__(self, *args, **kwargs)
        ExpirableConnection.__init__(self, keepalive_expiry=keepalive_expiry)


class ExpirableHTTPSConnection(ExpirableConnection, HTTPSConnection, HTTPConnection):

    def __init__(self, *args, keepalive_expiry=60, **kwargs):
        HTTPSConnection.__init__(self, *args, **kwargs)
        ExpirableConnection.__init__(self, keepalive_expiry=keepalive_expiry)


class ContentDecoder:
    def decompress(self, data: bytes, max_length: int = -1) -> bytes:
        raise NotImplementedError()

    @property
    def has_unconsumed_tail(self) -> bool:
        raise NotImplementedError()

    def flush(self) -> bytes:
        raise NotImplementedError()


class DeflateDecoder(ContentDecoder):
    def __init__(self) -> None:
        self._first_try = True
        self._first_try_data = b""
        self._unfed_data = b""
        self._obj = zlib.decompressobj()

    def decompress(self, data: bytes, max_length: int = -1) -> bytes:
        data = self._unfed_data + data
        self._unfed_data = b""
        if not data and not self._obj.unconsumed_tail:
            return data
        original_max_length = max_length
        if original_max_length < 0:
            max_length = 0
        elif original_max_length == 0:
            # We should not pass 0 to the zlib decompressor because 0 is
            # the default value that will make zlib decompress without a
            # length limit.
            # Data should be stored for subsequent calls.
            self._unfed_data = data
            return b""

        # Subsequent calls always reuse `self._obj`. zlib requires
        # passing the unconsumed tail if decompression is to continue.
        if not self._first_try:
            return self._obj.decompress(
                self._obj.unconsumed_tail + data, max_length=max_length
            )

        # First call tries with RFC 1950 ZLIB format.
        self._first_try_data += data
        try:
            decompressed = self._obj.decompress(data, max_length=max_length)
            if decompressed:
                self._first_try = False
                self._first_try_data = b""
            return decompressed
        # On failure, it falls back to RFC 1951 DEFLATE format.
        except zlib.error:
            self._first_try = False
            self._obj = zlib.decompressobj(-zlib.MAX_WBITS)
            try:
                return self.decompress(
                    self._first_try_data, max_length=original_max_length
                )
            finally:
                self._first_try_data = b""

    @property
    def has_unconsumed_tail(self) -> bool:
        return bool(self._unfed_data) or (
            bool(self._obj.unconsumed_tail) and not self._first_try
        )

    def flush(self) -> bytes:
        return self._obj.flush()


class GzipDecoderState:
    FIRST_MEMBER = 0
    OTHER_MEMBERS = 1
    SWALLOW_DATA = 2


class GzipDecoder(ContentDecoder):
    def __init__(self) -> None:
        self._obj = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._state = GzipDecoderState.FIRST_MEMBER
        self._unconsumed_tail = b""

    def decompress(self, data: bytes, max_length: int = -1) -> bytes:
        ret = bytearray()
        if self._state == GzipDecoderState.SWALLOW_DATA:
            return bytes(ret)

        if max_length == 0:
            # We should not pass 0 to the zlib decompressor because 0 is
            # the default value that will make zlib decompress without a
            # length limit.
            # Data should be stored for subsequent calls.
            self._unconsumed_tail += data
            return b""

        # zlib requires passing the unconsumed tail to the subsequent
        # call if decompression is to continue.
        data = self._unconsumed_tail + data
        if not data and self._obj.eof:
            return bytes(ret)

        while True:
            try:
                ret += self._obj.decompress(
                    data, max_length=max(max_length - len(ret), 0)
                )
            except zlib.error:
                previous_state = self._state
                # Ignore data after the first error
                self._state = GzipDecoderState.SWALLOW_DATA
                self._unconsumed_tail = b""
                if previous_state == GzipDecoderState.OTHER_MEMBERS:
                    # Allow trailing garbage acceptable in other gzip clients
                    return bytes(ret)
                raise

            self._unconsumed_tail = data = (
                self._obj.unconsumed_tail or self._obj.unused_data
            )
            if max_length > 0 and len(ret) >= max_length:
                break

            if not data:
                return bytes(ret)
            # When the end of a gzip member is reached, a new decompressor
            # must be created for unused (possibly future) data.
            if self._obj.eof:
                self._state = GzipDecoderState.OTHER_MEMBERS
                self._obj = zlib.decompressobj(16 + zlib.MAX_WBITS)

        return bytes(ret)

    @property
    def has_unconsumed_tail(self) -> bool:
        return bool(self._unconsumed_tail)

    def flush(self) -> bytes:
        return self._obj.flush()


if brotli is not None:

    class BrotliDecoder(ContentDecoder):
        # Supports both 'brotlipy' and 'Brotli' packages
        # since they share an import name. The top branches
        # are for 'brotlipy' and bottom branches for 'Brotli'
        def __init__(self) -> None:
            self._obj = brotli.Decompressor()
            if hasattr(self._obj, "decompress"):
                setattr(self, "_decompress", self._obj.decompress)
            else:
                setattr(self, "_decompress", self._obj.process)

        # Requires Brotli >= 1.2.0 for `output_buffer_limit`.
        def _decompress(self, data: bytes, output_buffer_limit: int = -1) -> bytes:
            raise NotImplementedError()

        def decompress(self, data: bytes, max_length: int = -1) -> bytes:
            try:
                if max_length > 0:
                    return self._decompress(data, output_buffer_limit=max_length)
                else:
                    return self._decompress(data)
            except TypeError:
                # Fallback for Brotli/brotlicffi/brotlipy versions without
                # the `output_buffer_limit` parameter.
                warnings.warn(
                    "Brotli >= 1.2.0 is required to prevent decompression bombs.",
                    DependencyWarning,
                )
                return self._decompress(data)

        @property
        def has_unconsumed_tail(self) -> bool:
            try:
                return not self._obj.can_accept_more_data()
            except AttributeError:
                return False

        def flush(self) -> bytes:
            if hasattr(self._obj, "flush"):
                return self._obj.flush()  # type: ignore[no-any-return]
            return b""


try:
    if sys.version_info >= (3, 14):
        from compression import zstd
    else:
        from backports import zstd
except ImportError:
    HAS_ZSTD = False
else:
    HAS_ZSTD = True

    class ZstdDecoder(ContentDecoder):
        def __init__(self) -> None:
            self._obj = zstd.ZstdDecompressor()

        def decompress(self, data: bytes, max_length: int = -1) -> bytes:
            if not data and not self.has_unconsumed_tail:
                return b""
            if self._obj.eof:
                data = self._obj.unused_data + data
                self._obj = zstd.ZstdDecompressor()
            part = self._obj.decompress(data, max_length=max_length)
            length = len(part)
            data_parts = [part]
            # Every loop iteration is supposed to read data from a separate frame.
            # The loop breaks when:
            #   - enough data is read;
            #   - no more unused data is available;
            #   - end of the last read frame has not been reached (i.e.,
            #     more data has to be fed).
            while (
                self._obj.eof
                and self._obj.unused_data
                and (max_length < 0 or length < max_length)
            ):
                unused_data = self._obj.unused_data
                if not self._obj.needs_input:
                    self._obj = zstd.ZstdDecompressor()
                part = self._obj.decompress(
                    unused_data,
                    max_length=(max_length - length) if max_length > 0 else -1,
                )
                if part_length := len(part):
                    data_parts.append(part)
                    length += part_length
                elif self._obj.needs_input:
                    break
            return b"".join(data_parts)

        @property
        def has_unconsumed_tail(self) -> bool:
            return not (self._obj.needs_input or self._obj.eof) or bool(
                self._obj.unused_data
            )

        def flush(self) -> bytes:
            if not self._obj.eof:
                raise DecodeError("Zstandard data is incomplete")
            return b""


class MultiDecoder(ContentDecoder):
    """
    From RFC7231:
        If one or more encodings have been applied to a representation, the
        sender that applied the encodings MUST generate a Content-Encoding
        header field that lists the content codings in the order in which
        they were applied.
    """

    def __init__(self, modes: str) -> None:
        self._decoders = [_get_decoder(m.strip()) for m in modes.split(",")]

    def flush(self) -> bytes:
        return self._decoders[0].flush()

    def decompress(self, data: bytes, max_length: int = -1) -> bytes:
        if max_length <= 0:
            for d in reversed(self._decoders):
                data = d.decompress(data)
            return data

        ret = bytearray()
        # Every while loop iteration goes through all decoders once.
        # It exits when enough data is read or no more data can be read.
        # It is possible that the while loop iteration does not produce
        # any data because we retrieve up to `max_length` from every
        # decoder, and the amount of bytes may be insufficient for the
        # next decoder to produce enough/any output.
        while True:
            any_data = False
            for d in reversed(self._decoders):
                data = d.decompress(data, max_length=max_length - len(ret))
                if data:
                    any_data = True
                # We should not break when no data is returned because
                # next decoders may produce data even with empty input.
            ret += data
            if not any_data or len(ret) >= max_length:
                return bytes(ret)
            data = b""

    @property
    def has_unconsumed_tail(self) -> bool:
        return any(d.has_unconsumed_tail for d in self._decoders)


def _get_decoder(mode: str) -> ContentDecoder:
    if "," in mode:
        return MultiDecoder(mode)

    # According to RFC 9110 section 8.4.1.3, recipients should
    # consider x-gzip equivalent to gzip
    if mode in ("gzip", "x-gzip"):
        return GzipDecoder()

    if brotli is not None and mode == "br":
        return BrotliDecoder()

    if HAS_ZSTD and mode == "zstd":
        return ZstdDecoder()

    return DeflateDecoder()


class BytesQueueBuffer:
    """Memory-efficient bytes buffer

    To return decoded data in read() and still follow the BufferedIOBase API, we need a
    buffer to always return the correct amount of bytes.

    This buffer should be filled using calls to put()

    Our maximum memory usage is determined by the sum of the size of:

     * self.buffer, which contains the full data
     * the largest chunk that we will copy in get()
    """

    def __init__(self) -> None:
        self.buffer: typing.Deque[Union[bytes, memoryview[bytes]]] = collections.deque()
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

        if len(self.buffer[0]) == n and isinstance(self.buffer[0], bytes):
            self._size -= n
            return self.buffer.popleft()

        fetched = 0
        ret = io.BytesIO()
        while fetched < n:
            remaining = n - fetched
            chunk = self.buffer.popleft()
            chunk_length = len(chunk)
            if remaining < chunk_length:
                chunk = memoryview(chunk)
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
            if isinstance(result, memoryview):
                result = result.tobytes()
        else:
            ret = io.BytesIO()
            ret.writelines(buffer.popleft() for _ in range(len(buffer)))
            result = ret.getvalue()
        self._size = 0
        return result


class HTTPResponse(_HTTPResponse):

    CONTENT_DECODERS = ["gzip", "x-gzip", "deflate"]
    if brotli is not None:
        CONTENT_DECODERS += ["br"]
    if HAS_ZSTD:
        CONTENT_DECODERS += ["zstd"]
    REDIRECT_STATUSES = [301, 302, 303, 307, 308]

    DECODER_ERROR_CLASSES: tuple[type[Exception], ...] = (IOError, zlib.error)
    if brotli is not None:
        DECODER_ERROR_CLASSES += (brotli.error,)

    if HAS_ZSTD:
        DECODER_ERROR_CLASSES += (zstd.ZstdError,)

    def __init__(self, **kwargs):
        self._has_decoded_content = False
        super().__init__(**kwargs)
        if not hasattr(self, "_decoded_buffer "):
            # Used to return the correct amount of bytes for partial read()s
            self._decoded_buffer = BytesQueueBuffer()

    def drain_conn(self) -> None:
        """
        Read and discard any remaining HTTP response data in the response connection.

        Unread data in the HTTPResponse connection blocks the connection from being released back to the pool.
        """
        try:
            self.read(
                # Do not spend resources decoding the content unless
                # decoding has already been initiated.
                decode_content=self._has_decoded_content,
            )
        except (HTTPError, OSError, BaseSSLError, HTTPException):
            pass

    def _init_decoder(self) -> None:
        """
        Set-up the _decoder attribute if necessary.
        """
        # Note: content-encoding value should be case-insensitive, per RFC 7230
        # Section 3.2
        content_encoding = self.headers.get("content-encoding", "").lower()
        if self._decoder is None:
            if content_encoding in self.CONTENT_DECODERS:
                self._decoder = _get_decoder(content_encoding)
            elif "," in content_encoding:
                encodings = [
                    e.strip()
                    for e in content_encoding.split(",")
                    if e.strip() in self.CONTENT_DECODERS
                ]
                if encodings:
                    self._decoder = _get_decoder(content_encoding)

    def _decode(self, data: bytes, decode_content, flush_decoder, max_length=None) -> bytes:
        """
        Decode the data passed in and potentially flush the decoder.
        """
        if not decode_content:
            if self._has_decoded_content:
                raise RuntimeError(
                    "Calling read(decode_content=False) is not supported after "
                    "read(decode_content=True) was called."
                )
            return data

        if max_length is None or flush_decoder:
            max_length = -1

        try:
            if self._decoder:
                data = self._decoder.decompress(data, max_length=max_length)
                self._has_decoded_content = True
        except self.DECODER_ERROR_CLASSES as e:
            content_encoding = self.headers.get("content-encoding", "").lower()
            raise DecodeError(
                "Received response with content-encoding: %s, but "
                "failed to decode it." % content_encoding,
                e,
            ) from e
        if flush_decoder:
            data += self._flush_decoder()

        return data

    def _raw_read(self, amt=None, *, read1=False) -> bytes:
        """
        Reads `amt` of bytes from the socket.
        """
        if self._fp is None:
            return None  # type: ignore[return-value]

        fp_closed = getattr(self._fp, "closed", False)

        with self._error_catcher():
            signature = inspect.signature(self._fp_read)
            if 'read1' in signature.parameters:
                data = self._fp_read(amt, read1=read1) if not fp_closed else b""
            else:
                data = self._fp_read(amt) if not fp_closed else b""
            if amt is not None and amt != 0 and not data:
                # Platform-specific: Buggy versions of Python.
                # Close the connection when no data is returned
                #
                # This is redundant to what httplib/http.client _should_
                # already do.  However, versions of python released before
                # December 15, 2012 (http://bugs.python.org/issue16298) do
                # not properly close the connection in all cases. There is
                # no harm in redundantly calling close.
                self._fp.close()
                if (
                    self.enforce_content_length
                    and self.length_remaining is not None
                    and self.length_remaining != 0
                ):
                    # This is an edge case that httplib failed to cover due
                    # to concerns of backward compatibility. We're
                    # addressing it here to make sure IncompleteRead is
                    # raised during streaming, so all calls with incorrect
                    # Content-Length are caught.
                    raise IncompleteRead(self._fp_bytes_read, self.length_remaining)
            elif read1 and (
                (amt != 0 and not data) or self.length_remaining == len(data)
            ):
                # All data has been read, but `self._fp.read1` in
                # CPython 3.12 and older doesn't always close
                # `http.client.HTTPResponse`, so we close it here.
                # See https://github.com/python/cpython/issues/113199
                self._fp.close()

        if data:
            self._fp_bytes_read += len(data)
            if self.length_remaining is not None:
                self.length_remaining -= len(data)
        return data

    def read(self, amt=None, decode_content=None, cache_content=False) -> bytes:
        """
        Similar to :meth:`http.client.HTTPResponse.read`, but with two additional
        parameters: ``decode_content`` and ``cache_content``.

        :param amt:
            How much of the content to read. If specified, caching is skipped
            because it doesn't make sense to cache partial content as the full
            response.

        :param decode_content:
            If True, will attempt to decode the body based on the
            'content-encoding' header.

        :param cache_content:
            If True, will save the returned data such that the same result is
            returned despite of the state of the underlying file object. This
            is useful if you want the ``.data`` property to continue working
            after having ``.read()`` the file object. (Overridden if ``amt`` is
            set.)
        """
        self._init_decoder()
        if decode_content is None:
            decode_content = self.decode_content

        if amt and amt < 0:
            # Negative numbers and `None` should be treated the same.
            amt = None
        elif amt is not None:
            cache_content = False

            if self._decoder and self._decoder.has_unconsumed_tail:
                decoded_data = self._decode(
                    b"",
                    decode_content,
                    flush_decoder=False,
                    max_length=amt - len(self._decoded_buffer),
                )
                self._decoded_buffer.put(decoded_data)
            if len(self._decoded_buffer) >= amt:
                return self._decoded_buffer.get(amt)

        data = self._raw_read(amt)

        flush_decoder = amt is None or (amt != 0 and not data)

        if (
            not data
            and len(self._decoded_buffer) == 0
            and not (self._decoder and self._decoder.has_unconsumed_tail)
        ):
            return data

        if amt is None:
            data = self._decode(data, decode_content, flush_decoder)
            if cache_content:
                self._body = data
        else:
            # do not waste memory on buffer when not decoding
            if not decode_content:
                if self._has_decoded_content:
                    raise RuntimeError(
                        "Calling read(decode_content=False) is not supported after "
                        "read(decode_content=True) was called."
                    )
                return data

            decoded_data = self._decode(
                data,
                decode_content,
                flush_decoder,
                max_length=amt - len(self._decoded_buffer),
            )
            self._decoded_buffer.put(decoded_data)

            while len(self._decoded_buffer) < amt and data:
                # TODO make sure to initially read enough data to get past the headers
                # For example, the GZ file header takes 10 bytes, we don't want to read
                # it one byte at a time
                data = self._raw_read(amt)
                decoded_data = self._decode(
                    data,
                    decode_content,
                    flush_decoder,
                    max_length=amt - len(self._decoded_buffer),
                )
                self._decoded_buffer.put(decoded_data)
            data = self._decoded_buffer.get(amt)

        return data

    def read1(self, amt=None, decode_content=None,) -> bytes:
        """
        Similar to ``http.client.HTTPResponse.read1`` and documented
        in :meth:`io.BufferedReader.read1`, but with an additional parameter:
        ``decode_content``.

        :param amt:
            How much of the content to read.

        :param decode_content:
            If True, will attempt to decode the body based on the
            'content-encoding' header.
        """
        if decode_content is None:
            decode_content = self.decode_content
        if amt and amt < 0:
            # Negative numbers and `None` should be treated the same.
            amt = None
        # try and respond without going to the network
        if self._has_decoded_content:
            if not decode_content:
                raise RuntimeError(
                    "Calling read1(decode_content=False) is not supported after "
                    "read1(decode_content=True) was called."
                )
            if (
                self._decoder
                and self._decoder.has_unconsumed_tail
                and (amt is None or len(self._decoded_buffer) < amt)
            ):
                decoded_data = self._decode(
                    b"",
                    decode_content,
                    flush_decoder=False,
                    max_length=(
                        amt - len(self._decoded_buffer) if amt is not None else None
                    ),
                )
                self._decoded_buffer.put(decoded_data)
            if len(self._decoded_buffer) > 0:
                if amt is None:
                    return self._decoded_buffer.get_all()
                return self._decoded_buffer.get(amt)
        if amt == 0:
            return b""

        data = self._raw_read(amt, read1=True)
        if not decode_content or data is None:
            return data

        self._init_decoder()
        while True:
            flush_decoder = not data
            decoded_data = self._decode(
                data, decode_content, flush_decoder, max_length=amt
            )
            self._decoded_buffer.put(decoded_data)
            if decoded_data or flush_decoder:
                break
            data = self._raw_read(8192, read1=True)

        if amt is None:
            return self._decoded_buffer.get_all()
        return self._decoded_buffer.get(amt)

    def stream(self, amt=2 ** 16, decode_content=None) -> typing.Generator[bytes]:
        """
        A generator wrapper for the read() method. A call will block until
        ``amt`` bytes have been read from the connection or until the
        connection is closed.

        :param amt:
            How much of the content to read. The generator will return up to
            much data per iteration, but may return less. This is particularly
            likely when using compressed data. However, the empty string will
            never be returned.

        :param decode_content:
            If True, will attempt to decode the body based on the
            'content-encoding' header.
        """
        if self.chunked and self.supports_chunked_reads():
            yield from self.read_chunked(amt, decode_content=decode_content)
        else:
            while (
                not is_fp_closed(self._fp)
                or len(self._decoded_buffer) > 0
                or (self._decoder and self._decoder.has_unconsumed_tail)
            ):
                data = self.read(amt=amt, decode_content=decode_content)

                if data:
                    yield data

    def _update_chunk_length(self) -> None:
        # First, we'll figure out length of a chunk and then
        # we'll try to read it from socket.
        if self.chunk_left is not None:
            return None
        line = self._fp.fp.readline()  # type: ignore[union-attr]
        line = line.split(b";", 1)[0]
        try:
            self.chunk_left = int(line, 16)
        except ValueError:
            self.close()
            if line:
                # Invalid chunked protocol response, abort.
                raise InvalidChunkLength(self, line) from None
            else:
                # Truncated at start of next chunk
                raise ProtocolError("Response ended prematurely") from None


class FullPoolError(PoolError):
    """Raised when we try to add a connection to a full pool in blocking mode."""


class ConnectionPool:

    _expire_at = None
    _keepalive_expiry = 60

    # https://github.com/urllib3/urllib3/security/advisories/GHSA-2xpw-w6gg-jr37
    # https://github.com/urllib3/urllib3/commit/c19571de34c47de3a766541b041637ba5f716ed7
    # https://nvd.nist.gov/vuln/detail/CVE-2025-66471
    ResponseCls = HTTPResponse

    def activate(self):
        # don't need lock because the connection
        # can only be fetched by one request at a time.
        self._expire_at = None

    def deactivate(self):
        if not self._keepalive_expiry or self._keepalive_expiry < 0:
            return
        if self._expire_at is not None:
            return
        now = time.monotonic()
        self._expire_at = now + self._keepalive_expiry

    def has_expired(self):
        now = time.monotonic()
        keepalive_expired = self._expire_at is not None and now > self._expire_at
        return keepalive_expired

    def _get_conn(self, timeout=None):
        """
        Get a connection. Will return a pooled connection if one is available.

        If no connections are available and :prop:`.block` is ``False``, then a
        fresh connection is returned.

        :param timeout:
            Seconds to wait before giving up and raising
            :class:`urllib3.exceptions.EmptyPoolError` if the pool is empty and
            :prop:`.block` is ``True``.
        """
        conn = None

        if self.pool is None:
            raise ClosedPoolError(self, "Pool is closed.")

        try:
            conn = self.pool.get(block=self.block, timeout=timeout)
            if conn and conn.has_expired():
                conn.close()  # connection won't be dropped even closed.
        except AttributeError:  # self.pool is None
            raise ClosedPoolError(self, "Pool is closed.") from None  # Defensive:

        except queue.Empty:
            if self.block:
                raise EmptyPoolError(
                    self,
                    "Pool reached maximum size and no more connections are allowed.",
                )
            pass  # Oh well, we'll create a new connection then

        # If this is a persistent connection, check if it got disconnected
        if conn and is_connection_dropped(conn):
            urllib3_log.debug("Resetting dropped connection: %s", self.host)
            conn.close()
            if getattr(conn, "auto_open", 1) == 0:
                # This is a proxied connection that has been mutated by
                # http.client._tunnel() and cannot be reused (since it would
                # attempt to bypass the proxy)
                conn = None

        self.activate()
        conn and conn.activate()
        return conn or self._new_conn()

    def _put_conn(self, conn):
        """
        Put a connection back into the pool.

        :param conn:
            Connection object for the current host and port as returned by
            :meth:`._new_conn` or :meth:`._get_conn`.

        If the pool is already full, the connection is closed and discarded
        because we exceeded maxsize. If connections are discarded frequently,
        then maxsize should be increased.

        If the pool is closed, then the connection will be closed and discarded.
        """
        conn and conn.deactivate()

        if self.pool is not None:
            try:
                self.pool.put(conn, block=False)
                if self.pool.full():
                    # maybe not retrieved again.
                    self.deactivate()
                return  # Everything is dandy, done.
            except AttributeError:
                # self.pool is None.
                self.deactivate()
            except queue.Full:
                # Connection never got put back into the pool, close it.
                if conn:
                    conn.close()
                self.deactivate()

                if self.block:
                    # This should never happen if you got the conn from self._get_conn
                    raise FullPoolError(
                        self,
                        "Pool reached maximum size and no more connections are allowed.",
                    ) from None

                urllib3_log.warning(
                    "Connection pool is full, discarding connection: %s. Connection pool size: %s",
                    self.host,
                    self.pool.qsize(),
                )

        # Connection never got put back into the pool, close it.
        if conn:
            conn.close()


class HTTPConnectionPool(ConnectionPool, _HTTPConnectionPool):

    ConnectionCls = ExpirableHTTPConnection


class HTTPSConnectionPool(ConnectionPool, _HTTPSConnectionPool, _HTTPConnectionPool):

    ConnectionCls = ExpirableHTTPSConnection


def close_expired_connections(
    connectionpool: Union[HTTPConnectionPool, HTTPSConnectionPool]
):
    # the connection pool(LIFO) will not be used before cleaned up if with container lock.
    try:
        # avoid deque mutated during iteration
        for idx in range(len(connectionpool.pool.queue)):
            conn = connectionpool.pool.queue[idx]
            if conn and conn.has_expired():
                conn.close()  # no need to remove from pool.
    except (AttributeError, IndexError):
        # pool is None:
        pass
