import copy
import email.message
from urllib.parse import urlparse, urlunparse

from requests.cookies import cookiejar_from_dict
from requests.utils import to_native_string


def cookiejar_from_str(s: str):
    if not s:
        return None
    d = {}
    for each in s.split(';'):
        each = each.strip()
        if '=' not in each:
            continue
        k, v = each.split('=', maxsplit=1)
        d[k] = v
    return cookiejar_from_dict(d)


def copy_cookie_jar(jar):
    if jar is None:
        return None

    if hasattr(jar, "copy"):
        # We're dealing with an instance of RequestsCookieJar
        return jar.copy()
    # We're dealing with a generic CookieJar instance
    new_jar = copy.copy(jar)
    new_jar.clear()
    for cookie in jar:
        new_jar.set_cookie(copy.copy(cookie))
    return new_jar


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


def extract_cookies_to_jar(jar, request, response):
    """Extract the cookies from the response into a CookieJar.

    :param jar: cookielib.CookieJar (not necessarily a RequestsCookieJar)
    :param request: prepared request
    :param response: urllib3.HTTPResponse-like object
    """
    req = CookieCompatRequest(request)
    res = CookieCompatResponse(response)
    jar.extract_cookies(res, req)
