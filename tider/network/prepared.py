from http.cookiejar import CookieJar

from requests import PreparedRequest as _PreparedRequest
from requests.exceptions import InvalidJSONError, InvalidURL, MissingSchema
from requests.cookies import cookiejar_from_dict, get_cookie_header
from requests.utils import rewind_body as _rewind_body

from tider.exceptions import InvalidRequest
from tider.utils.cookies import copy_cookie_jar


_UNSET = object()
_PREPARE_ERRORS = (
    InvalidJSONError, InvalidURL, MissingSchema, TypeError, ValueError,
)


class PreparedRequest(_PreparedRequest):
    """Transport snapshot for one download attempt."""

    def __init__(self, body=None, auth=None, body_position=_UNSET):
        super().__init__()
        self.body = body
        self.auth = auth
        self._body_position = (
            self._get_body_position(body)
            if body_position is _UNSET else body_position
        )

    @staticmethod
    def _get_body_position(body):
        if getattr(body, 'tell', None) is None:
            return None
        try:
            return body.tell()
        except OSError:
            # Preserve requests' failed-tell sentinel so redirects fail
            # explicitly instead of sending a partial body.
            return object()

    @classmethod
    def from_request(cls, request, headers=None, cookies=None, auth=_UNSET):
        if headers is None:
            headers = request.headers
        if cookies is None:
            cookies = request.cookies
        if auth is _UNSET:
            auth = request.auth

        try:
            prepared = cls(body=None, auth=auth)
            prepared.prepare_method(request.method)
            if request.url == 'about:blank' or request.url.startswith('data:'):
                prepared.url = request.url
            else:
                prepared.prepare_url(request.url, request.params)
            prepared.prepare_headers(headers)
            # Regenerate Cookie from the merged jar instead of retaining an
            # earlier logical-request header.
            prepared.headers.pop('Cookie', None)
            prepared.prepare_cookies(cookies)
            prepared.prepare_body(request.data, request.files, request.json)
            prepared.prepare_auth(auth, prepared.url)
            prepared.prepare_content_length(prepared.body)
            prepared.rewind()
            return prepared
        except _PREPARE_ERRORS as e:
            raise InvalidRequest('Failed to prepare download attempt.') from e

    @property
    def cookies(self):
        return copy_cookie_jar(self._cookies)

    def rewind(self):
        """Restore a replayable body to its logical starting position."""
        if not isinstance(self._body_position, int):
            return
        try:
            position = self.body.tell()
        except (AttributeError, OSError):
            position = None
        if position != self._body_position:
            _rewind_body(self)

    def copy_with(self, method=_UNSET, url=_UNSET, headers=_UNSET,
                  cookies=_UNSET, auth=_UNSET, body=_UNSET, params=_UNSET,
                  body_position=_UNSET, apply_auth=True):
        body_supplied = body is not _UNSET
        body = self.body if not body_supplied else body
        if body_position is _UNSET:
            body_position = (
                self._body_position
                if not body_supplied or body is self.body else None
            )

        if headers is _UNSET:
            headers = self.headers
            if body_supplied and body is not self.body:
                headers = headers.copy()
                headers.pop('Content-Length', None)
                headers.pop('Transfer-Encoding', None)

        method = self.method if method is _UNSET or method is None else method
        url = self.url if url is _UNSET or url is None else url
        auth = self.auth if auth is _UNSET else auth
        cookies = self.cookies if cookies is _UNSET else cookies

        copied = type(self)(body=body, auth=auth, body_position=body_position)
        if isinstance(method, bytes):
            method = method.decode('ascii')
        copied.method = str(method).upper()
        if params is _UNSET:
            copied.url = url
        else:
            copied.prepare_url(url, params)
        copied.headers = type(self.headers)(headers or {})
        copied.headers.pop('Cookie', None)
        copied._cookies = copied._coerce_cookies(cookies)
        cookie_header = get_cookie_header(copied._cookies, copied)
        if cookie_header is not None:
            copied.headers['Cookie'] = cookie_header
        if apply_auth:
            copied.prepare_auth(auth, copied.url)
        copied.prepare_content_length(copied.body)
        return copied

    @staticmethod
    def _coerce_cookies(cookies):
        if isinstance(cookies, CookieJar):
            return cookies
        return cookiejar_from_dict(cookies)

    def copy(self):
        return self.copy_with(apply_auth=False)
