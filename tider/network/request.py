import shlex
import inspect
import logging
import hashlib
from weakref import WeakKeyDictionary
from w3lib.url import canonicalize_url

from tider.network.proxy import Proxy
from tider.network.user_agent import get_random_ua
from tider.utils.serialize import pickle_loads
from tider.utils.curl import curl_to_request_kwargs
from tider.utils.misc import symbol_by_name, to_bytes


logger = logging.getLogger(__name__)


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


def get_multipart_boundary_from_content_type(content_type):
    if not content_type or not content_type.startswith(b"multipart/form-data"):
        return None
    # parse boundary according to
    # https://www.rfc-editor.org/rfc/rfc2046#section-5.1.1
    if b";" in content_type:
        for section in content_type.split(b";"):
            if section.strip().lower().startswith(b"boundary="):
                return section.strip()[len(b"boundary="):].strip(b'"')
    return None


class Request:
    """Represents an HTTP request."""

    _REQUEST_KWARGS = ("method", "params", "headers", "cookies", "data", "json",
                       "files", "content", "auth", "timeout", "allow_redirects",
                       "proxies", "stream", "verify", "cert")

    def __init__(self, url, callback=None, cb_kwargs=None, errback=None, priority=1, encoding=None,
                 meta=None, dup_check=False, raise_for_status=True, ignored_status_codes=(400, 521),
                 proxy_schema=0, proxy_params=None, max_retries=5, max_parse_times=1, delay=0,
                 broadcast=False, impersonate=None, **kwargs):

        self.url = url
        self.callback = callback
        self.cb_kwargs = dict(cb_kwargs) if cb_kwargs else {}  # use `dict` to create a new dict
        self.errback = errback

        self.encoding = encoding
        self.meta = dict(meta) if meta else {}
        self.dup_check = dup_check
        self.raise_for_status = raise_for_status
        self.ignored_status_codes = ignored_status_codes or tuple()

        if not isinstance(priority, int):
            raise TypeError(f"Request priority not an integer: {priority!r}")
        self.priority = priority
        self.proxy_schema = proxy_schema
        self.proxy_params = dict(proxy_params) if proxy_params else {}
        self.proxy = Proxy(**kwargs.pop('proxies', {}))

        self.max_retry_times = kwargs.pop("max_retry_times", None) or max_retries
        self.max_parse_times = max_parse_times
        self.delay = delay
        self.broadcast = broadcast
        self.impersonate = impersonate

        self.request_kwargs = {}
        self.init_request_kwargs(**kwargs)

    def init_request_kwargs(self, **kwargs):
        self.proxy.active()
        # deepcopy may optimize memory
        self.request_kwargs = {k: kwargs[k] for k in kwargs if k in self._REQUEST_KWARGS}
        if 'data' in kwargs or 'json' in kwargs:
            default_method = "POST"
        else:
            default_method = "GET"
        self.request_kwargs.setdefault("method", default_method)
        self.request_kwargs['method'] = str(self.request_kwargs['method']).upper()
        if self.meta.get('random_ua'):
            headers = self.request_kwargs.get("headers", {})
            headers["User-Agent"] = get_random_ua()
            self.request_kwargs["headers"] = headers
        self.request_kwargs.setdefault("timeout", 10)

    def fetch_proxies(self):
        """
        Call this method will increase the proxy's used_times.
        """
        return self.proxy.fetch()

    def update_proxy(self, proxy):
        if proxy:
            self.request_kwargs.setdefault('verify', False)
        proxy.activate()
        self.proxy and self.proxy.maybe_discard()
        self.proxy = proxy

    def forbid_proxy(self):
        self.proxy and self.proxy.forbid()

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

    @property
    def method(self):
        return self.request_kwargs.get("method")

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
        for x in self.__dict__:
            value = getattr(self, x)
            if x == "request_kwargs":
                [kwargs.setdefault(k, value[k]) for k in value]
            else:
                kwargs.setdefault(x, value)
        cls = kwargs.pop('cls', self.__class__)
        return cls(*args, **kwargs)

    def fingerprint(self, include_headers=None, keep_fragments=False):
        headers = dict(include_headers or {})
        headers = tuple(to_bytes(h.lower()) for h in sorted(headers))

        cache = _fingerprint_cache.setdefault(self, {})
        cache_key = (headers, keep_fragments)
        if cache_key not in cache:
            fp = hashlib.sha1()
            fp.update(to_bytes(canonicalize_url(self.url, keep_fragments=keep_fragments)))
            for hdr in headers:
                if hdr not in self.request_kwargs.get('headers', {}):
                    continue
                fp.update(to_bytes(hdr))
                fp.update(to_bytes(headers[hdr]))
            for arg in ["params", "data", "files", "auth", "cert", "json"]:
                if self.request_kwargs.get(arg):
                    fp.update(to_bytes(str(self.request_kwargs[arg])))

            cache[cache_key] = fp.hexdigest()
        return cache[cache_key]

    def to_dict(self, spider=None) -> dict:
        """Return a dictionary containing the Request's data."""
        # callback or errback either be a string or None object or must be bound to a spider
        d = {
            "callback": _find_method(spider, self.callback) if callable(self.callback) else self.callback,
            "errback": _find_method(spider, self.errback) if callable(self.errback) else self.errback
        }
        for attr in self.__dict__:
            value = getattr(self, attr)
            if attr == 'request_kwargs':
                [d.setdefault(k, value[k]) for k in value]
            else:
                d.setdefault(attr, value)
        if type(self) is not Request:
            d["_class"] = self.__module__ + '.' + self.__class__.__name__
        return d

    def to_wget(self, output, quiet=True):
        wget_cmd = 'wget'
        if quiet:
            wget_cmd += ' -q'
        wget_cmd += ' --tries=1'
        if not self.request_kwargs.get("verify"):
            wget_cmd += ' --no-check-certificate'

        timeout = self.request_kwargs['timeout']
        if isinstance(timeout, tuple):
            timeout = timeout[-1]
        wget_cmd += f' --timeout={timeout}'

        method = self.method.upper()
        if method == "POST":
            data = self.request_kwargs.get("data") or self.request_kwargs.get("json", "")
            wget_cmd += f' --post-data={data}'
        ua = self.request_kwargs.get("headers", {}).get('User-Agent')
        if ua:
            wget_cmd += f' -U "{ua}"'
        if self.proxy.get('http'):
            wget_cmd += f' -e "http_proxy={self.proxy["http"]}"'
        if self.proxy.get('https'):
            wget_cmd += f' -e "https_proxy={self.proxy["https"]}"'
        wget_cmd += f' -O {output} {self.url}'
        return shlex.split(wget_cmd)

    def close(self):
        self.proxy and self.proxy.maybe_discard()
        # don't use clear to avoid break cb_kwargs in promise
        self.cb_kwargs = {}
        self.meta = {}
        self.request_kwargs.clear()
        del self.callback, self.errback


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
