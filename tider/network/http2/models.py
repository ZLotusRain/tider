import io
import threading
from typing import (Any, Union, Optional,
                    List, Tuple, Mapping, Sequence,
                    Iterable, Iterator, AsyncIterator)

from http import HTTPStatus
from urllib.parse import urlparse
from urllib3._collections import HTTPHeaderDict
from urllib3.util.response import is_fp_closed
from urllib3.exceptions import HTTPError

from tider.network.http2.decoders import (
    ContentDecoder,
    MultiDecoder,
    SUPPORTED_DECODERS,
    IdentityDecoder
)


def _get_reason_phrase(status_code):
    for status in HTTPStatus:
        if int(status_code) == status:
            return status.phrase
    return ""


def enforce_stream(value: Union[bytes, Iterable[bytes], None], *, name: str) -> Union[Iterable[bytes]]:
    if value is None:
        return ByteStream(b"")
    elif isinstance(value, bytes):
        return ByteStream(value)
    return value


def enforce_bytes(value: Union[bytes, str], *, name: str) -> bytes:
    """
    Any arguments that are ultimately represented as bytes can be specified
    either as bytes or as strings.

    However we enforce that any string arguments must only contain characters in
    the plain ASCII range. chr(0)...chr(127). If you need to use characters
    outside that range then be precise, and use a byte-wise argument.
    """
    if isinstance(value, str):
        try:
            return value.encode("ascii")
        except UnicodeEncodeError:
            raise TypeError(f"{name} strings may not include unicode characters.")
    elif isinstance(value, bytes):
        return value

    seen_type = type(value).__name__
    raise TypeError(f"{name} must be bytes or str, but got {seen_type}.")


def enforce_url(value: Union["URL", bytes, str], *, name: str) -> "URL":
    """
    Type check for URL parameters.
    """
    if isinstance(value, (bytes, str)):
        return URL(value)
    elif isinstance(value, URL):
        return value

    seen_type = type(value).__name__
    raise TypeError(f"{name} must be a URL, bytes, or str, but got {seen_type}.")


def enforce_headers(value=None, *, name: str) -> List[Tuple[bytes, bytes]]:
    """
    Convienence function that ensure all items in request or response headers
    are either bytes or strings in the plain ASCII range.
    """
    if value is None:
        return []
    elif isinstance(value, Mapping):
        return [
            (
                enforce_bytes(k, name="header name"),
                enforce_bytes(v, name="header value"),
            )
            for k, v in value.items()
        ]
    elif isinstance(value, Sequence):
        return [
            (
                enforce_bytes(k, name="header name"),
                enforce_bytes(v, name="header value"),
            )
            for k, v in value
        ]

    seen_type = type(value).__name__
    raise TypeError(
        f"{name} must be a mapping or sequence of two-tuples, but got {seen_type}."
    )


class ByteStream:
    """
    A container for non-streaming content, and that supports both sync and async
    stream iteration.
    """

    def __init__(self, content: bytes) -> None:
        self._content = content

    def __iter__(self) -> Iterator[bytes]:
        yield self._content

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._content

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} [{len(self._content)} bytes]>"


class Lock:
    """
    This is a standard lock.

    In the sync case `Lock` provides thread locking.
    In the async case `AsyncLock` provides async locking.
    """

    def __init__(self):
        self._lock = threading.Lock()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        self._lock.release()


class Semaphore:
    def __init__(self, bound: int) -> None:
        self._semaphore = threading.Semaphore(value=bound)

    def acquire(self) -> None:
        self._semaphore.acquire()

    def release(self) -> None:
        self._semaphore.release()


class BytesSlicer:

    def __init__(self, chunk_size=None):
        self._buffer = io.BytesIO()
        self._chunk_size = chunk_size

    def slice(self, content):
        if self._chunk_size is None:
            # get all
            return [content] if content else []

        self._buffer.write(content)
        if self._buffer.tell() >= self._chunk_size:
            value = self._buffer.getvalue()
            # slice
            chunks = [value[i: i+self._chunk_size]
                      for i in range(0, len(value), self._chunk_size)]
            if len(chunks[-1]) == self._chunk_size:
                # every chunk can be sliced to chunk size
                self._buffer.seek(0)
                self._buffer.truncate()
                return chunks
            else:
                # use flush to get the last chunk
                self._buffer.seek(0)
                self._buffer.write(chunks[-1])
                self._buffer.truncate()
                return chunks[:-1]
        else:
            return []

    def flush(self):
        value = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate()
        return [value] if value else []


class Origin:
    def __init__(self, scheme: bytes, host: bytes, port: int) -> None:
        self.scheme = scheme
        self.host = host
        self.port = port

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Origin)
            and self.scheme == other.scheme
            and self.host == other.host
            and self.port == other.port
        )

    def __str__(self) -> str:
        scheme = self.scheme.decode("ascii")
        host = self.host.decode("ascii")
        port = str(self.port)
        return f"{scheme}://{host}:{port}"


class URL:
    """
    Represents the URL against which an HTTP request may be made.
    """

    def __init__(
        self,
        url: Union[bytes, str] = "",
        *,
        scheme: Union[bytes, str] = b"",
        host: Union[bytes, str] = b"",
        port: Optional[int] = None,
        target: Union[bytes, str] = b"",
    ) -> None:
        """
        Parameters:
            url: The complete URL as a string or bytes.
            scheme: The URL scheme as a string or bytes.
                Typically either `"http"` or `"https"`.
            host: The URL host as a string or bytes. Such as `"www.example.com"`.
            port: The port to connect to. Either an integer or `None`.
            target: The target of the HTTP request. Such as `"/items?search=red"`.
        """
        if url:
            parsed = urlparse(enforce_bytes(url, name="url"))
            self.scheme = parsed.scheme
            self.host = parsed.hostname or b""
            self.port = parsed.port
            self.target = (parsed.path or b"/") + (
                b"?" + parsed.query if parsed.query else b""
            )
        else:
            self.scheme = enforce_bytes(scheme, name="scheme")
            self.host = enforce_bytes(host, name="host")
            self.port = port
            self.target = enforce_bytes(target, name="target")

        self._url = url

    @property
    def url(self):
        return self._url

    @property
    def origin(self) -> Origin:
        default_port = {
            b"http": 80,
            b"https": 443,
            b"ws": 80,
            b"wss": 443,
            b"socks5": 1080,
        }[self.scheme]
        return Origin(
            scheme=self.scheme, host=self.host, port=self.port or default_port
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, URL)
            and other.scheme == self.scheme
            and other.host == self.host
            and other.port == self.port
            and other.target == self.target
        )

    def __bytes__(self) -> bytes:
        if self.port is None:
            return b"%b://%b%b" % (self.scheme, self.host, self.target)
        return b"%b://%b:%d%b" % (self.scheme, self.host, self.port, self.target)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(scheme={self.scheme!r}, "
            f"host={self.host!r}, port={self.port!r}, target={self.target!r})"
        )


def parse_h2_headers(headers: List[Tuple[bytes]], encoding=None):
    if encoding is None:
        for enc in ["ascii", "utf-8"]:
            for key, value in headers:
                try:
                    key.decode(enc)
                    value.decode(enc)
                except UnicodeDecodeError:
                    break
            else:
                # The else block runs if 'break' did not occur, meaning
                # all values fitted the encoding.
                encoding = enc
                break
        else:
            # The ISO-8859-1 encoding covers all 256 code points in a byte,
            # so will never raise decode errors.
            encoding = "iso-8859-1"
    return {item[0].decode(encoding): item[1].decode(encoding) for item in headers}


class HTTP2Response:
    CONTENT_DECODERS = list(SUPPORTED_DECODERS.keys())
    REDIRECT_STATUSES = [301, 302, 303, 307, 308]

    def __init__(self, body, headers: List[Tuple[bytes]], status, version, reason=None,
                 preload_content=True, decode_content=True, request_method=None, request_url=None, retries=None, **_):
        self.headers = HTTPHeaderDict(parse_h2_headers(headers))
        self.status = status
        self.version = version
        if reason is None:
            reason = _get_reason_phrase(status)
        self.reason = reason
        self.decode_content = decode_content
        self._has_decoded_content = False
        self.request_method = request_method
        self._request_url = request_url
        self.retries = retries

        # HTTP2 will ignore `Transfer-Encoding: chunked`.
        self.chunk = False
        self._decoder = None

        self._body = None
        self._fp = None
        self._fp_bytes_read = 0

        if body and isinstance(body, (str, bytes)):
            self._body = body

        if hasattr(body, "read"):
            self._fp = body  # type: ignore[assignment]

        self.chunk_left = None

        # If requested, preload the body.
        if preload_content and not self._body:
            self._body = self.read(decode_content=decode_content)

        self._stream_consumed = False

    @staticmethod
    def _normalize_headers(headers):
        h = []
        for k, v in headers:
            if not isinstance(k, bytes):
                k = k.encode("ascii")
            if not isinstance(v, bytes):
                v = v.encode("ascii")
            h.append((k, v))
        return h

    def decode_headers(self, headers):
        h = {}
        headers = self._normalize_headers(headers)
        for encoding in ["ascii", "utf-8", "iso-8859-1"]:
            for key, value in headers:
                try:
                    k = key.decode(encoding)
                    v = value.decode(encoding)
                    if k in h:
                        h[k] += f", {v}"
                    else:
                        h[k] = v
                except UnicodeDecodeError:
                    break
            else:
                break
        return h

    # Compatibility method for http.cookiejar
    def info(self) -> HTTPHeaderDict:
        return self.headers

    def geturl(self):
        return self.url

    # Compatibility methods for `io` module
    def readinto(self, b: bytearray) -> int:
        temp = self.read(len(b))
        if len(temp) == 0:
            return 0
        else:
            b[: len(temp)] = temp
            return len(temp)

    def get_redirect_location(self):
        """
        Should we redirect and where to?

        :returns: Truthy redirect location string if we got a redirect status
            code and valid location. ``None`` if redirect status and no
            location. ``False`` if not a redirect status code.
        """
        if self.status in self.REDIRECT_STATUSES:
            return self.headers.get("location")
        return False

    def release_conn(self):
        pass

    def drain_conn(self):
        """
        Read and discard any remaining HTTP2 response data in the response connection.
        """
        try:
            self.read()
        except (HTTPError, OSError):
            pass

    @property
    def data(self) -> bytes:
        if self._body:
            return self._body  # type: ignore[return-value]

        if self._fp:
            return self.read(cache_content=True)

        return None  # type: ignore[return-value]

    def isclosed(self) -> bool:
        return is_fp_closed(self._fp)

    def tell(self) -> int:
        """
        Obtain the number of bytes pulled over the wire so far. May differ from
        the amount of content returned by :meth:``http2.models.HTTP2Response.read``
        if bytes are encoded on the wire (e.g, compressed).
        """
        return self._fp_bytes_read

    def _get_content_decoder(self) -> ContentDecoder:
        """
        Returns a decoder instance which can be used to decode the raw byte
        content, depending on the Content-Encoding used in the response.
        """
        if self._decoder is None:
            decoders: list[ContentDecoder] = []
            values = self.headers.get("content-encoding", "").lower().split(',')
            for value in values:
                value = value.strip().lower()
                try:
                    decoder_cls = SUPPORTED_DECODERS[value]
                    decoders.append(decoder_cls())
                except KeyError:
                    continue

            if len(decoders) == 1:
                self._decoder = decoders[0]
            elif len(decoders) > 1:
                self._decoder = MultiDecoder(children=decoders)
            else:
                self._decoder = IdentityDecoder()

        return self._decoder

    def read(self, amt=None, decode_content=None, cache_content=False):
        decoder = self._get_content_decoder()
        if decode_content is None:
            decode_content = self.decode_content

        if amt is not None:
            cache_content = False

        data = self._fp.read(amt)
        self._fp_bytes_read += len(data)

        slicer = BytesSlicer(chunk_size=amt)
        decoded = decoder.decode(data) if decode_content else data
        data = b"".join(chunk for chunk in slicer.slice(decoded))

        if decode_content:
            decoded = decoder.flush()
            data += b"".join(chunk for chunk in slicer.slice(decoded))
        data += b"".join(chunk for chunk in slicer.flush())

        if amt is None:
            if cache_content:
                self._body = data

        return data

    def read1(self, amt=None, decode_content=None, cache_content=False):
        decoder = self._get_content_decoder()
        if decode_content is None:
            decode_content = self.decode_content

        if amt is not None:
            cache_content = False

        data = self._fp.read1(amt)
        self._fp_bytes_read += len(data)

        slicer = BytesSlicer(chunk_size=amt)
        decoded = decoder.decode(data) if decode_content else data
        data = b"".join(chunk for chunk in slicer.slice(decoded))

        if decode_content:
            decoded = decoder.flush()
            data += b"".join(chunk for chunk in slicer.slice(decoded))
        data += b"".join(chunk for chunk in slicer.flush())

        if amt is None:
            if cache_content:
                self._body = data

        return data

    def stream(self, amt=2 ** 16, decode_content=None):
        """
        A generator wrapper for the read() method. A call will block until
        ``amt`` bytes have been read from the connection or until the
        connection is closed.
        """
        decoder = self._get_content_decoder()
        if decode_content is None:
            decode_content = self.decode_content

        slicer = BytesSlicer(chunk_size=amt)
        decode_content = decode_content or self.decode_content
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
        for raw_stream_bytes in self._fp:
            self._fp_bytes_read += len(raw_stream_bytes)
            for chunk in slicer.slice(raw_stream_bytes):
                yield chunk

        for chunk in slicer.flush():
            yield chunk

        self.close()

    # Overrides from io.IOBase
    def readable(self) -> bool:
        return True

    def close(self) -> None:
        if not self.closed and self._fp:
            self._fp.close()

    @property
    def closed(self) -> bool:
        if self._fp is None:
            return True
        elif hasattr(self._fp, "isclosed"):
            return self._fp.isclosed()
        elif hasattr(self._fp, "closed"):
            return self._fp.closed
        else:
            return True

    def fileno(self) -> int:
        if self._fp is None:
            raise OSError("HTTP2Response has no file to get a fileno from")
        elif hasattr(self._fp, "fileno"):
            return self._fp.fileno()
        else:
            raise OSError(
                "The file-like object this HTTP2Response is wrapped "
                "around has no file descriptor"
            )

    def flush(self) -> None:
        if (
            self._fp is not None
            and hasattr(self._fp, "flush")
            and not getattr(self._fp, "closed", False)
        ):
            return self._fp.flush()

    def read_chunked(self, **_):
        raise RuntimeError("HTTP/2 doesn't support chunked transfer.")

    @property
    def url(self):
        """
        Returns the URL that was the source of this response.
        If the request that generated this response redirected, this method
        will return the final redirect location.
        """
        return self._request_url

    @url.setter
    def url(self, url: str) -> None:
        self._request_url = url

    def __iter__(self):
        buffer: list[bytes] = []
        for chunk in self.stream(decode_content=True):
            if b"\n" in chunk:
                chunks = chunk.split(b"\n")
                yield b"".join(buffer) + chunks[0] + b"\n"
                for x in chunks[1:-1]:
                    yield x + b"\n"
                if chunks[-1]:
                    buffer = [chunks[-1]]
                else:
                    buffer = []
            else:
                buffer.append(chunk)
        if buffer:
            yield b"".join(buffer)
