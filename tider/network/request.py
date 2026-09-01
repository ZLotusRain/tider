import time
import hashlib
import inspect
from http.cookiejar import CookieJar
from urllib.parse import urlunparse
from weakref import WeakKeyDictionary
from w3lib.url import canonicalize_url

from requests.exceptions import (
    InvalidURL,
    MissingSchema,
)
from requests.structures import CaseInsensitiveDict
from requests.cookies import (
    RequestsCookieJar,
    cookiejar_from_dict,
    merge_cookies,
)
from requests.utils import (
    check_header_validity,
    requote_uri,
)
from urllib3.exceptions import LocationParseError
from urllib3.util import parse_url

from tider.network.proxy import NO_PROXY_FIELDS, Proxy
from tider.network.prepared import PreparedRequest
from tider.exceptions import InvalidRequest
from tider.utils.log import get_logger
from tider.utils.serialize import pickle_loads
from tider.utils.cookies import copy_cookie_jar
from tider.utils.curl import curl_to_request_kwargs
from tider.utils.imports import symbol_by_name
from tider.utils.misc import to_bytes

logger = get_logger(__name__)

_fingerprint_cache = WeakKeyDictionary()
# Sentinel for omitted proxy arguments. ``None`` means "do not override" in replace().
_UNSET = object()


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


def _normalize_method(method, has_payload=False):
    if not method:
        method = 'POST' if has_payload else 'GET'
    if isinstance(method, bytes):
        method = method.decode('ascii')
    return str(method).upper()


def _normalize_url(url):
    if isinstance(url, bytes):
        url = url.decode('utf-8')
    else:
        url = str(url)
    url = url.lstrip()

    # Explorer consumes non-HTTP schemes without a network transport.
    if ':' in url and not url.lower().startswith('http'):
        return url

    try:
        scheme, auth, host, port, path, query, fragment = parse_url(url)
    except LocationParseError as e:
        raise InvalidURL(*e.args)
    if not scheme:
        raise MissingSchema(
            f"Invalid URL {url!r}: No scheme supplied. "
            f"Perhaps you meant https://{url}?"
        )
    if not host:
        raise InvalidURL(f"Invalid URL {url!r}: No host supplied")

    if not host.isascii():
        try:
            import idna
            host = idna.encode(host, uts46=True).decode('utf-8')
        except (UnicodeError, idna.IDNAError):
            raise InvalidURL('URL has an invalid label.')
    elif host.startswith(('*', '.')):
        raise InvalidURL('URL has an invalid label.')

    netloc = auth or ''
    if netloc:
        netloc += '@'
    netloc += host
    if port:
        netloc += f':{port}'
    path = path or '/'

    return requote_uri(urlunparse((scheme, netloc, path, '', query, fragment)))


def _normalize_headers(headers):
    normalized = CaseInsensitiveDict()
    for name, value in dict(headers or {}).items():
        if value is None:
            continue
        check_header_validity((name, value))
        if isinstance(name, bytes):
            name = name.decode('ascii')
        normalized[str(name)] = value
    return normalized


class Request:
    """Represents an HTTP request."""

    _REQUEST_PARAMS = (
        "method", "params", "data", "json", "headers", "cookies",
        "files", "auth", "timeout", "allow_redirects", "proxies", "stream", "verify", "cert"
    )

    INTERNAL_ATTRIBUTES = ('encoding', 'meta', 'cb_kwargs',)

    def __init__(self, url, callback=None, cb_kwargs=None, errback=None, encoding=None, priority=0,
                 meta=None, dup_check=False, allow_redirects=True, timeout=None, raise_for_status=True,
                 ignored_status_codes=(400, 412, 521), stream=None, verify=None, cert=None, http2=False,
                 proxies=_UNSET, proxy_schema=_UNSET, proxy_params=_UNSET, max_retries=5, max_parse_times=1,
                 delay=0, downloader=None, impersonate=None, session_cookies=None, **request_kwargs):
        try:
            self.url = _normalize_url(url)
            self.headers = _normalize_headers(request_kwargs.get('headers'))
        except (InvalidURL, MissingSchema, ValueError) as e:
            raise InvalidRequest('Failed to build request.') from e

        self.data = request_kwargs.get('data')
        self.files = request_kwargs.get('files')
        self.json = request_kwargs.get('json')
        has_payload = bool(self.files or self.data or self.json)
        self.method = _normalize_method(request_kwargs.get('method'), has_payload=has_payload)
        self.params = request_kwargs.get('params')

        auth = request_kwargs.get('auth')
        if isinstance(auth, list) and len(auth) == 2:
            auth = tuple(auth)
        self.auth = auth

        cookies = request_kwargs.get('cookies')
        if isinstance(cookies, CookieJar):
            self._cookies = copy_cookie_jar(cookies)
        else:
            self._cookies = cookiejar_from_dict(cookies)

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
        self.stream = stream
        self.verify = verify
        self.cert = cert
        self.http2 = http2
        self.raise_for_status = raise_for_status
        self.ignored_status_codes = ignored_status_codes or tuple()
        self._encoding = encoding

        # Keep explicit empty proxies distinguishable from omitted proxies.
        self._proxies_explicit = proxies is not _UNSET and proxies is not None
        proxies = None if proxies is _UNSET else proxies
        proxy_schema = 0 if proxy_schema is _UNSET or proxy_schema is None else proxy_schema
        proxy_params = None if proxy_params is _UNSET else proxy_params
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
    def cookies(self):
        return copy_cookie_jar(self._cookies)

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

    def prepare(self, headers=None, cookies=None, auth=_UNSET):
        """Build an attempt-local transport snapshot."""
        if headers is None:
            headers = self.headers
        if cookies is None:
            cookies = self.cookies
        if auth is _UNSET:
            auth = self.auth
        return PreparedRequest.from_request(self, headers=headers, cookies=cookies, auth=auth)

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
    def proxies_explicit(self) -> bool:
        return self._proxies_explicit

    @property
    def selected_proxy(self) -> str:
        return self.proxy.select_proxy(self.url)

    def _update_proxy(self, proxy: Proxy, proxies_explicit=None):
        old_proxy = self._proxy
        if old_proxy is not None and old_proxy is not proxy:
            old_proxy.unbind(self)
        self._proxy = proxy
        if proxies_explicit is not None:
            self._proxies_explicit = proxies_explicit

    def bind_proxy(self, proxy: Proxy, proxies_explicit=None):
        # discard disposable proxy when switching to a new proxy.
        proxy.bind(self)
        self._update_proxy(proxy, proxies_explicit=proxies_explicit)

    def inherit_proxy(self, request, proxies_explicit=None):
        # Inherit proxy by moving the reserved lease from parent request.
        proxy = request.proxy
        proxy.transfer_lease(request, self)
        # transfer_lease() already binds this request.
        self._update_proxy(proxy, proxies_explicit=proxies_explicit)

    def invalidate_proxy(self, force=False):
        # invalidate a fulfill proxy.
        if not self.proxies:
            return
        # Force is used by user-requested proxy reset.
        # Explicit proxies can fallback to proxy pool only when a valid schema exists.
        fallback_to_pool = self.proxy_schema not in NO_PROXY_FIELDS
        if self.proxies_explicit and not (force or fallback_to_pool):
            return
        self.proxy.invalidate()
        if self.proxies_explicit and fallback_to_pool:
            # Let the next build_request_proxy() select one from proxy pool.
            self._proxies_explicit = False

    @classmethod
    def blank(cls, callback=None, cb_kwargs=None, priority=0, meta=None):
        return cls(url="data:,", callback=callback, cb_kwargs=cb_kwargs, priority=priority, meta=meta, raise_for_status=False)

    @classmethod
    def about_blank(cls, callback=None, cb_kwargs=None, priority=0, meta=None):
        return cls(url="about:blank", callback=callback, cb_kwargs=cb_kwargs, priority=priority, meta=meta, raise_for_status=False)

    @classmethod
    def from_curl(cls, curl_command: str, ignore_unknown_options: bool = True, **kwargs):
        """Create a Request object from a string containing a `cURL
        <https://curl.haxx.se/>`_ command. It populates the HTTP method, the
        URL, the headers, the cookies and the request data. It accepts the same
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

    def copy(self, bind_proxy=True):
        # the source request may not be collected by system
        # because the callback kwargs mustn't use deep copy,
        # and will be passed between requests,
        # otherwise the promise value will be changed.
        return self.replace(bind_proxy=bind_proxy)

    def replace(self, *args, **kwargs):
        """Create a new Request with the same attributes except for those given new values"""
        bind_proxy = kwargs.pop('bind_proxy', True)
        overridden_keys = set(kwargs)
        proxy_keys = ('proxies', 'proxy_schema', 'proxy_params')
        proxy_overridden = any(kwargs.get(x) is not None for x in proxy_keys)
        proxies_arg = kwargs.get('proxies', _UNSET)
        proxies_overridden = proxies_arg is not _UNSET and proxies_arg is not None
        for x in proxy_keys:
            if x in kwargs and kwargs[x] is None:
                kwargs.pop(x)
        for x in (
            *self.__dict__.keys(),
            *self._REQUEST_PARAMS,
            *self.INTERNAL_ATTRIBUTES,
        ):
            if x.startswith("_"):
                continue
            value = getattr(self, x)
            kwargs.setdefault(x, value)

        # Accessing ``self.proxies`` cannot preserve whether an empty mapping
        # was explicitly supplied.  Keep implicit proxy-pool requests implicit
        # when copying/replacing them so ``proxy_schema`` is still resolved by
        # the explorer.
        if not proxies_overridden and not self.proxies_explicit:
            kwargs.pop('proxies', None)

        reset_cookies = (
            'meta' in overridden_keys
            and (kwargs.get('meta') or {}).get('merge_cookies') is False
        )
        if reset_cookies:
            if 'cookies' not in overridden_keys:
                kwargs['cookies'] = {}
            if 'session_cookies' not in overridden_keys:
                kwargs['session_cookies'] = {}
        else:
            kwargs.setdefault('cookies', self.cookies)
        payload_replaced = overridden_keys.intersection(('data', 'files', 'json'))
        if 'headers' not in overridden_keys and payload_replaced:
            headers = kwargs['headers'].copy()
            headers.pop('Content-Length', None)
            headers.pop('Transfer-Encoding', None)
            headers.pop('Content-Type', None)
            kwargs['headers'] = headers
        cls = kwargs.pop('cls', self.__class__)
        request = cls(*args, **kwargs)
        if bind_proxy and not proxy_overridden:
            # Keep manual proxy fallback behavior when inheriting the proxy lease.
            request.bind_proxy(self.proxy, proxies_explicit=self.proxies_explicit)
        return request

    def fingerprint(self, include_headers=None, keep_fragments=False):
        headers = tuple(
            (name.decode('ascii') if isinstance(name, bytes) else str(name)).lower()
            for name in sorted(dict(include_headers or {}))
        )

        cache = _fingerprint_cache.setdefault(self, {})
        cache_key = (headers, keep_fragments)
        if cache_key not in cache:
            fp = hashlib.sha1()
            fp.update(to_bytes(canonicalize_url(self.url, keep_fragments=keep_fragments)))
            for hdr in headers:
                if hdr not in self.headers:
                    continue
                fp.update(to_bytes(hdr))
                fp.update(to_bytes(self.headers[hdr]))
            for name in ('params', 'data', 'files', 'json', 'auth'):
                value = getattr(self, name)
                if value:
                    fp.update(to_bytes(repr(value)))

            cache[cache_key] = fp.hexdigest()
        return cache[cache_key]

    def to_dict(self, spider=None, encode_body=False) -> dict:
        """Return a dictionary containing the Request's data."""
        # callback or errback either be a string or None object or must be bound to a spider
        d = {
            "callback": _find_method(spider, self.callback) if callable(self.callback) else self.callback,
            "errback": _find_method(spider, self.errback) if callable(self.errback) else self.errback
        }
        for attr in (
            *self.__dict__.keys(),
            *self._REQUEST_PARAMS,
            *self.INTERNAL_ATTRIBUTES,
        ):
            value = getattr(self, attr)
            if attr.startswith("_"):
                continue
            else:
                if isinstance(value, tuple):
                    value = list(value)
                elif isinstance(value, RequestsCookieJar):
                    value = value.get_dict()
                elif isinstance(value, CaseInsensitiveDict):
                    value = dict(value)
                d.setdefault(attr, value)
        if not self.proxies_explicit:
            d.pop('proxies', None)
        d['cookies'] = self.cookies.get_dict()
        if type(self) is not Request:
            d["_class"] = self.__module__ + '.' + self.__class__.__name__
        return d

    def unbind_proxy(self):
        proxy, self._proxy = self._proxy, None
        if proxy is not None:
            proxy.unbind(self)

    def close(self):
        self.unbind_proxy()

        self.params = None
        self.data = None
        self.json = None
        self.files = None
        self.auth = None

        self.headers.clear()
        self._cookies.clear()

        self._meta = {}
        # Drop references without mutating callback kwargs shared with promises.
        self.__dict__.pop('callback', None)
        self.__dict__.pop('errback', None)
        self.__dict__.pop('_cb_kwargs', None)


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
