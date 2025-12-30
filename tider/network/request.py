import time
import hashlib
import inspect
from weakref import WeakKeyDictionary
from w3lib.url import canonicalize_url
from typing import Optional

from requests import PreparedRequest as _PreparedRequest
from requests.exceptions import (
    InvalidJSONError,
    InvalidURL,
    MissingSchema,
)
from requests.structures import CaseInsensitiveDict
from requests.cookies import RequestsCookieJar, cookiejar_from_dict, merge_cookies

from tider.network.proxy import Proxy
from tider.exceptions import InvalidRequest
from tider.utils.log import get_logger
from tider.utils.serialize import pickle_loads
from tider.utils.network import copy_cookie_jar
from tider.utils.curl import curl_to_request_kwargs
from tider.utils.misc import symbol_by_name, to_bytes

logger = get_logger(__name__)

REQUEST_PREPARE_ERRORS = (InvalidJSONError, InvalidURL, MissingSchema, ValueError)


_fingerprint_cache = WeakKeyDictionary()


def request_from_dict(d, spider=None):
    request_cls = symbol_by_name(d["_class"]) if "_class" in d else Request
    if d.get("callback"):
        if spider:
            d["callback"] = _get_method(spider, d["callback"])
        else:
            d["callback"] = pickle_loads(d["callback"])
    if d.get("errback"):
        if spider:
            d["errback"] = _get_method(spider, d["errback"])
        else:
            d["errback"] = pickle_loads(d["errback"])
    return request_cls(**d)


class PreparedRequest(_PreparedRequest):

    def __init__(self, body=None, auth=None, body_position=None):
        #: HTTP verb to send to the server.
        self.method = None
        #: HTTP URL to send the request to.
        self.url = None
        self.auth = auth
        #: dictionary of HTTP headers.
        self.headers: Optional[CaseInsensitiveDict] = None
        # The `CookieJar` used to create the Cookie header will be stored here
        # after prepare_cookies is called
        self._cookies = None
        #: request body to send to the server.
        self.body = body
        #: integer denoting starting position of a readable file-like body.
        self._body_position = body_position

    def prepare(
        self,
        method=None,
        url=None,
        headers=None,
        files=None,
        data=None,
        params=None,
        cookies=None,
        json=None,
        **_
    ):
        """Prepares the entire request with the given parameters."""
        try:
            self.prepare_method(method)
            self.prepare_url(url, params)
            headers = headers or {}
            # Remove keys that are set to None.
            none_keys = [k for (k, v) in headers.items() if v is None]
            for key in none_keys:
                del headers[key]
            self.prepare_headers(headers)
            self.prepare_cookies(cookies)
            if self.body is None:
                self.prepare_body(data, files, json)
            # Note that prepare_auth must be last to enable authentication schemes
            # such as OAuth to work on a fully prepared request.
            self.prepare_auth(self.auth, url)
        except REQUEST_PREPARE_ERRORS as e:
            raise InvalidRequest('Failed to prepare request.') from e

    @property
    def cookies(self):
        return copy_cookie_jar(self._cookies)

    def copy_with(self, method=None, url=None, headers=None, files=None, data=None,
                  params=None, auth=None, cookies=None, json=None, body=None, body_position=None):
        body_position = None if body is None else body_position
        p = PreparedRequest(auth=auth, body=body, body_position=body_position)
        p.method = method.upper() if method is not None else self.method

        try:
            if url is not None:
                p.prepare_url(url, params)
            else:
                p.url = self.url

            if headers is not None:
                p.prepare_headers(headers)
            else:
                p.headers = self.headers.copy()

            cookies = cookies if cookies is not None else self.cookies
            if cookies is not None:
                p.headers.pop('Cookie', None)  # avoid cookies can't be updated to empty.
                p.prepare_cookies(cookies)

            if p.body is None:
                if data or files or json:
                    p.prepare_body(data, files, json)
                else:
                    p.body = self.body
            if auth is not None:
                p.prepare_auth(auth, p.url)
        except REQUEST_PREPARE_ERRORS as e:
            raise InvalidRequest('Failed to copy request with specific arguments.') from e
        return p

    def copy(self):
        p = PreparedRequest(body=self.body, auth=self.auth)
        p.method = self.method
        p.url = self.url
        p.headers = self.headers.copy() if self.headers is not None else None
        p._cookies = copy_cookie_jar(self._cookies)
        p._body_position = self._body_position
        return p

    def to_dict(self):
        cookies = self.cookies.get_dict() if isinstance(self.cookies, RequestsCookieJar) else self.cookies
        return {
            'method': self.method,
            'url': self.url,
            'auth': self.auth,
            'headers': dict(self.headers),
            'cookies': cookies,
            'body': self.body,
            'body_position': self._body_position,
        }


class Request:
    """Represents an HTTP request."""

    INTERNAL_ATTRIBUTES = ('encoding', 'meta', 'cb_kwargs',)

    _REQUEST_KWARGS = ("method", "url", "headers", "params", "body",
                       "data", "json", "files", "cookies", "auth", "body_position")

    def __init__(self, url, callback=None, cb_kwargs=None, errback=None, encoding=None, priority=0,
                 meta=None, dup_check=False, allow_redirects=True, timeout=None, raise_for_status=True,
                 ignored_status_codes=(400, 412, 521), stream=None, verify=None, cert=None, http2=False,
                 proxies=None, proxy_schema=0, proxy_params=None, max_retries=5, max_parse_times=1, delay=0,
                 downloader=None, impersonate=None, prepared=None, session_cookies=None, **request_kwargs):

        self._encoding = encoding
        url = url.strip()
        if prepared is not None:
            self._prepared = prepared.copy()
        else:
            self._prepared = self._prepare(url=url, **request_kwargs)

        if not isinstance(session_cookies, RequestsCookieJar):
            session_cookies = cookiejar_from_dict(session_cookies)
        self.session_cookies = merge_cookies(session_cookies, self.cookies)

        self.callback = callback
        self.errback = errback

        # change objects in source meta and cb_kwargs still works.
        self._meta = dict(meta) if meta else None
        self._cb_kwargs = dict(cb_kwargs) if cb_kwargs else None  # use `dict` to create a new dict

        if not isinstance(priority, int):
            raise TypeError(f"Request priority not an integer: {priority!r}")
        self.priority = priority
        self.dup_check = dup_check

        self.allow_redirects = allow_redirects
        self.timeout = timeout if timeout is not None else 10
        self.raise_for_status = raise_for_status
        self.ignored_status_codes = ignored_status_codes or tuple()
        self.stream = stream
        self.verify = verify
        self.cert = cert
        self.http2 = http2

        self._proxy = Proxy(proxies)
        self.proxy_schema = proxy_schema
        self.proxy_params = dict(proxy_params) if proxy_params else {}

        self.impersonate = impersonate
        self.delay = delay
        if delay and delay > 0:
            self.meta["explore_after"] = time.monotonic() + delay  # add or update when using replace.
        self.max_retries = max_retries
        self.max_parse_times = max_parse_times

        self.downloader = downloader

    @property
    def encoding(self):
        return self._encoding

    @property
    def prepared(self):
        return self._prepared

    @property
    def meta(self):
        if self._meta is None:
            self._meta = {}
        return self._meta

    @property
    def cb_kwargs(self):
        if self._cb_kwargs is None:
            self._cb_kwargs = {}
        return self._cb_kwargs

    def _prepare(
        self,
        method=None,
        url=None,
        headers=None,
        body=None,
        files=None,
        data=None,
        params=None,
        auth=None,
        cookies=None,
        json=None,
        **_,
    ):
        if not method and not (files or data or json):
            method = 'GET'
        if not method and (files or data or json):
            method = 'POST'
        p = PreparedRequest(body=body, auth=auth)
        p.prepare(
            method=method,
            url=url,
            headers=headers,
            files=files,
            data=data,
            params=params,
            cookies=cookies,
            json=json
        )
        return p

    @property
    def method(self):
        return self.prepared.method

    @property
    def url(self):
        return self.prepared.url

    @property
    def body(self):
        return self.prepared.body

    @property
    def cookies(self):
        # can't change cookies here.
        return self.prepared.cookies

    @property
    def headers(self):
        return self.prepared.headers

    @property
    def proxy(self) -> Proxy:
        return self._proxy

    @property
    def proxies(self) -> dict:
        try:
            return dict(self.proxy.proxies)
        except AttributeError:
            return {}

    @property
    def selected_proxy(self) -> str:
        return self.proxy.select_proxy(self.url)

    def update_proxy(self, proxy: Proxy):
        # discard disposable proxy when switching to a new proxy.
        self._proxy and self._proxy.unbind(self)
        self._proxy = proxy

    def invalidate_proxy(self):
        # invalidate a fulfill proxy.
        if self.proxies:
            self.proxy.invalidate()

    @classmethod
    def from_curl(cls, curl_command: str, ignore_unknown_options: bool = True, **kwargs):
        """Create a Request object from a string containing a `cURL
        <https://curl.haxx.se/>`_ command. It populates the HTTP method, the
        URL, the headers, the cookies and the body. It accepts the same
        arguments as the :class:`Request` class, taking preference and
        overriding the values of the same arguments contained in the cURL
        command.

        Unrecognized options are ignored by default. To raise an error when
        finding unknown options call this method by passing
        ``ignore_unknown_options=False``.
        """
        request_kwargs = curl_to_request_kwargs(curl_command, ignore_unknown_options)
        headers = {}
        raw_headers = request_kwargs.get("headers", [])
        for each in raw_headers:
            headers[each[0]] = each[1]
        cookies = request_kwargs.get("cookies")
        request_kwargs.update({"headers": headers, "cookies": cookies})
        kwargs.update(request_kwargs)
        return cls(**kwargs)

    def __str__(self):
        return "<Request [{0} {1}]>".format(self.method, self.url)

    __repr__ = __str__

    def __lt__(self, other):
        """compare priority."""
        if isinstance(other, Request):
            return self.priority < other.priority
        return NotImplemented("Can't compare `Request` with other type")

    def copy(self):
        # the source request may not be collected by system
        # because the callback kwargs mustn't use deep copy,
        # and will be passed between requests,
        # otherwise the promise value will be changed.
        return self.replace()

    def replace(self, *args, **kwargs):
        """Create a new Request with the same attributes except for those given new values"""
        for x in (*self.__dict__.keys(), *self.INTERNAL_ATTRIBUTES):
            if x.startswith("_"):
                continue
            value = getattr(self, x)
            kwargs.setdefault(x, value)
        prepared = kwargs.pop('prepared', None)
        if not prepared:
            prepared = self.prepared
            request_kwargs = {x: kwargs[x] for x in self._REQUEST_KWARGS if x in kwargs}
            if request_kwargs:
                prepared = self.prepared.copy_with(**request_kwargs)
        kwargs['prepared'] = prepared
        kwargs['url'] = prepared.url
        cls = kwargs.pop('cls', self.__class__)
        request = cls(*args, **kwargs)
        request.update_proxy(self.proxy)
        return request

    def fingerprint(self, include_headers=None, keep_fragments=False):
        headers = dict(include_headers or {})
        headers = tuple(to_bytes(h.lower()) for h in sorted(headers))

        cache = _fingerprint_cache.setdefault(self, {})
        cache_key = (headers, keep_fragments)
        if cache_key not in cache:
            fp = hashlib.sha1()
            fp.update(to_bytes(canonicalize_url(self.url, keep_fragments=keep_fragments)))
            for hdr in headers:
                if hdr not in self.prepared.headers:
                    continue
                fp.update(to_bytes(hdr))
                fp.update(to_bytes(headers[hdr]))
            fp.update(to_bytes(self.prepared.body))

            cache[cache_key] = fp.hexdigest()
        return cache[cache_key]

    def to_dict(self, spider=None) -> dict:
        """Return a dictionary containing the Request's data."""
        # callback or errback either be a string or None object or must be bound to a spider
        d = {
            "callback": _find_method(spider, self.callback) if callable(self.callback) else self.callback,
            "errback": _find_method(spider, self.errback) if callable(self.errback) else self.errback
        }
        for attr in (*self.__dict__.keys(), *self.INTERNAL_ATTRIBUTES):
            if attr.startswith("_"):
                continue
            value = getattr(self, attr)
            if attr == 'prepared':
                d.update(value.to_dict())
            else:
                if isinstance(value, tuple):
                    value = list(value)
                elif isinstance(value, RequestsCookieJar):
                    value = value.get_dict()
                d.setdefault(attr, value)
        if type(self) is not Request:
            d["_class"] = self.__module__ + '.' + self.__class__.__name__
        return d

    def close(self):
        if self._proxy is not None:
            self._proxy.unbind(self)
            self._proxy = None
        # don't use clear to avoid destroy cb_kwargs in promise
        self._cb_kwargs = {}
        self._meta = {}
        self._prepared = None
        del self.callback, self.errback, self._cb_kwargs


def _find_method(obj, func):
    """Helper function for Request.to_dict"""
    # Only instance methods contain ``__func__``
    if obj and hasattr(func, '__func__'):
        members = inspect.getmembers(obj, predicate=inspect.ismethod)
        for name, obj_func in members:
            # We need to use __func__ to access the original function object because instance
            # method objects are generated each time attribute is retrieved from instance.
            #
            # Reference: The standard type hierarchy
            # https://docs.python.org/3/reference/datamodel.html
            if obj_func.__func__ is func.__func__:
                return name
    elif _is_static_method(obj.__class__, getattr(func, '__name__', str(func))):
        return getattr(func, '__name__', str(func))
    raise ValueError(f"Function {func} is not an instance method in: {obj}")


def _get_method(obj, name):
    """Helper function for request_from_dict"""
    name = str(name)
    try:
        return getattr(obj, name)
    except AttributeError:
        raise ValueError(f"Method {name!r} not found in: {obj}")


def _is_static_method(klass, attr, value=None):
    """Test if a value of a class is static method.
    example::
        class MyClass(object):
            @staticmethod
            def method():
                ...
    :param klass: the class
    :param attr: attribute name
    :param value: attribute value
    """
    if value is None:
        value = getattr(klass, attr)
    assert getattr(klass, attr) == value

    for cls in inspect.getmro(klass):
        if inspect.isroutine(value):
            if attr in cls.__dict__:
                binded_value = cls.__dict__[attr]
                if isinstance(binded_value, staticmethod):
                    return True
    return False
