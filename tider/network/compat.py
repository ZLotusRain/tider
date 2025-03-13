import sys
import time
import queue
import http.client
import email.message
from typing import Union

from urllib.parse import urlparse, urlunparse
from requests.utils import to_native_string

from urllib3._collections import RecentlyUsedContainer as _RecentlyUsedContainer
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool as _HTTPConnectionPool
from urllib3.connectionpool import HTTPSConnectionPool as _HTTPSConnectionPool
from urllib3.exceptions import ClosedPoolError, EmptyPoolError, PoolError
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

    if sys.version_info < (3, 11, 4):

        def _tunnel(self) -> None:
            _MAXLINE = http.client._MAXLINE  # type: ignore[attr-defined]
            connect = b"CONNECT %s:%d HTTP/1.0\r\n" % (  # type: ignore[str-format]
                self._tunnel_host.encode("ascii"),  # type: ignore[union-attr]
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
                    raise OSError(f"Tunnel connection failed: {code} {message.strip()}")
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


class ExpirableHTTPConnection(ExpirableConnection, HTTPConnection):

    def __init__(self, *args, keepalive_expiry=60, **kwargs):
        HTTPConnection.__init__(self, *args, **kwargs)
        ExpirableConnection.__init__(self, keepalive_expiry=keepalive_expiry)


class ExpirableHTTPSConnection(ExpirableConnection, HTTPSConnection, HTTPConnection):

    def __init__(self, *args, keepalive_expiry=60, **kwargs):
        HTTPSConnection.__init__(self, *args, **kwargs)
        ExpirableConnection.__init__(self, keepalive_expiry=keepalive_expiry)


class FullPoolError(PoolError):
    """Raised when we try to add a connection to a full pool in blocking mode."""


class ConnectionPool:

    _expire_at = None
    _keepalive_expiry = 60

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
