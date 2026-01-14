import re
import json
import codecs
import mimetypes
import charset_normalizer
from bs4 import BeautifulSoup, Comment
from html import unescape as html_unescape
from urllib.parse import urljoin, unquote
from typing import Optional, Callable

from requests.models import REDIRECT_STATI
from requests.structures import CaseInsensitiveDict
from requests.cookies import cookiejar_from_dict, merge_cookies, RequestsCookieJar
from requests.utils import guess_json_utf

from tider import Request
from tider.utils.log import get_logger
from tider.utils.network import extract_cookies_to_jar, guess_encoding_from_headers
from tider.exceptions import DownloadError, HTTPError, ResponseReadError, ResponseStreamConsumed

logger = get_logger(__name__)

SPACE_CHARACTERS = ["&nbsp;", "&ensp;", "&emsp;", "&thinsp;",
                    "&zwnj;", "&zwj;", "&#x0020;", "&#x0009;",
                    "&#x000A;", "&#x000D;", "&#12288;"]
# remove the special characters which may affect the construct of etree in html codes
# remove control characters, full list see: https://zh.wikipedia.org/wiki/%E6%8E%A7%E5%88%B6%E5%AD%97%E7%AC%A6
SPECIAL_CHARACTERS = ["[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", "|".join(SPACE_CHARACTERS)]
SPECIAL_CHARACTER_PATTERNS = [re.compile(special_character) for special_character in SPECIAL_CHARACTERS]

CONTENT_CHUNK_SIZE = 10 * 1024

mimetypes.add_type('application/x-rar-compressed', '.rar')


def iter_slices(content, slice_length):
    """Iterate over slices of a string."""
    pos = 0
    if slice_length is None or slice_length <= 0:
        slice_length = len(content)
    while pos < len(content):
        yield content[pos: pos + slice_length]
        pos += slice_length


def stream_decode_response_unicode(iterator, encoding=None):
    """Stream decodes an iterator."""

    if encoding is None:
        yield from iterator
        return

    decoder = codecs.getincrementaldecoder(encoding)(errors="replace")
    for chunk in iterator:
        rv = decoder.decode(chunk)
        if rv:
            yield rv
    rv = decoder.decode(b"", final=True)
    if rv:
        yield rv


class Response:

    __slots__ = (
        'request', 'status_code', 'url', 'headers', 'encoding',
        'cookies', 'reason', 'raw', 'version', 'elapsed', '_error',
        '_redirect_target', '_content', '_content_consumed',
        '_cached_text', '_cached_selector', 'on_consumed',
    )

    def __init__(self, request: Optional[Request] = None, on_consumed: Optional[Callable] = None):
        # Final URL location of Response.
        self.url = None

        self.status_code = None
        self.headers = CaseInsensitiveDict()
        self.cookies = cookiejar_from_dict({})
        self.reason = None
        self.version = None

        self.elapsed = 0
        self.encoding = None

        # bound request
        self.request = request
        self._redirect_target = None

        self._content: Optional[bool, bytes] = False
        self._content_consumed = False
        self._cached_text = None
        self._cached_selector = None

        self.raw = None
        self.on_consumed = on_consumed
        self._error = None

    @classmethod
    def dummy(cls, content=None, text=None, url=None, request=None, encoding=None):
        response = cls(request)
        response.url = url or 'about:blank'
        response._content_consumed = True
        response._content = None
        if content:
            response._content = content
        elif text:
            response._cached_text = text
        response.encoding = encoding or 'utf-8'
        return response

    @classmethod
    def from_origin_resp(cls, resp, request):
        response = cls(request)

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = getattr(resp, "status", None) or getattr(resp, "status_code", None)  # for compat.

        # Make headers case-insensitive.
        response.headers = CaseInsensitiveDict(getattr(resp, "headers", {}))

        # Set encoding.
        response.encoding = guess_encoding_from_headers(response.headers)
        if response.encoding:
            response.encoding = response.encoding.split(',')[0].strip()
        if response.encoding == 'yaml.null':
            response.encoding = 'utf-8'
        version = getattr(resp, 'version', None) or getattr(resp, 'http_version', None)  # for compat.
        if isinstance(version, int) and version == 11:
            response.version = "HTTP/1.1"
        elif isinstance(version, int) and version == 10:
            response.version = "HTTP/1.0"
        elif version:
            response.version = version
        else:
            response.version = "HTTP/?"
        response.raw = resp
        response.reason = getattr(resp, 'reason', None)
        response.url = request.url

        # Add new cookies from the server.
        extract_cookies_to_jar(response.cookies, request.prepared, resp)
        # response.downloader = downloader
        return response

    @property
    def is_redirect(self):
        return "location" in self.headers and self.status_code in REDIRECT_STATI

    @property
    def redirect_target(self):
        if not self.is_redirect:
            return None
        if not self._redirect_target:
            location = self.headers["location"]
            # Currently the underlying http module on py3 decode headers
            # in latin1, but empirical evidence suggests that latin1 is very
            # rarely used with non-ASCII characters in HTTP headers.
            # It is more likely to get UTF8 header rather than latin1.
            # This causes incorrect handling of UTF8 encoded location headers.
            # To solve this, we re-encode the location in latin1.
            location = location.encode("latin1")
            self._redirect_target = location.decode("utf8")
        return self._redirect_target

    def raise_for_status(self):
        """Raises :class:`HTTPError`, if one occurred."""

        http_error_msg = ""
        if isinstance(self.reason, bytes):
            # We attempt to decode utf-8 first because some servers
            # choose to localize their reason strings. If the string
            # isn't utf-8, we fall back to iso-8859-1 for all other
            # encodings.
            try:
                reason = self.reason.decode("utf-8")
            except UnicodeDecodeError:
                reason = self.reason.decode("iso-8859-1")
        else:
            reason = self.reason

        if 400 <= self.status_code < 500:
            http_error_msg = (
                f"{self.status_code} Client Error: {reason} for url: {self.url}"
            )

        elif 500 <= self.status_code < 600:
            http_error_msg = (
                f"{self.status_code} Server Error: {reason} for url: {self.url}"
            )

        elif self.status_code >= 600:
            http_error_msg = (
                f"{self.status_code} Custom Error: {reason} for url: {self.url}"
            )

        if http_error_msg:
            raise HTTPError(http_error_msg)

    def check_error(self):
        """Raise and remove error if error exists."""
        if self._error is not None:
            # don't clear error here to avoid resource leak.
            # error, self._error = self._error, None
            raise self._error

    def check_length(self):
        """Can't use this with iter_content."""
        content_length = self.headers.get('Content-Length')
        if len(self.content) < content_length:
            raise ResponseReadError('Expect to read more bytes.')

    def __iter__(self):
        """Allows you to use a response as an iterator."""
        return self.iter_content(128)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        if self._error:
            return '<Failed Response [%s]>' % self._error
        if self.status_code:
            return '<Response [%s]>' % self.status_code
        return f'<Dummy Response at {id(self):#x}>'

    @property
    def ok(self):
        """Returns True if :attr:`status_code` is less than 400, False if not.

        This attribute checks if the status code of the response is between
        400 and 600 to see if there was a client error or a server error. If
        the status code is between 200 and 400, this will return True. This
        is **not** a check to see if the response code is ``200 OK``.
        """
        try:
            self.raise_for_status()
        except (TypeError, HTTPError):
            return False
        return True

    @property
    def failed(self):
        return self._error is not None

    @property
    def cb_kwargs(self):
        try:
            return self.request.cb_kwargs
        except AttributeError:
            raise AttributeError(
                "Response.cb_kwargs not available, this response "
                "is not tied to any request"
            )

    @property
    def meta(self):
        try:
            return self.request.meta
        except AttributeError:
            raise AttributeError(
                "Response.meta not available, this response "
                "is not tied to any request"
            )

    @property
    def proxy(self):
        return self.request.proxy if self.request else None

    @property
    def is_html(self):
        return "text/html" in self.headers.get("Content-Type", "")

    def fail(self, error):
        if self._error is not None:
            raise RuntimeError("Can't assign an error to an already failed response")
        if not isinstance(error, DownloadError):
            error = DownloadError(error)
        self._error = error

    def urljoin(self, url):
        """Join this Response's url with a possible relative url to form an
        absolute interpretation of the latter."""
        return urljoin(self.url, url)

    @property
    def filename(self):
        content_disposition = self.headers.get('Content-Disposition', '')
        # 'attachment;filename=%E6%A0%87%E9%A1%B9%E4.pdf'
        # "attachment;filename*=utf-8'zh_cn'%E6%8B%9B%E6%A0%87%E6%96%87%E4%BB%B6%E6%AD%A3%E6%96%87.pdf"
        # 'attachment; filename="æ\x88\x90äº¤å\x85¬å\x91\x8a.pdf"'
        filename = list(filter(lambda x: "file" in x, content_disposition.split(";")))
        filename = filename[0] if filename else ""
        filename = unquote(filename.split("=")[-1].replace("utf-8'zh_cn'", '')).strip('"').strip("'")
        return filename.strip()

    @property
    def filetype(self):
        if self.meta.get('filetype', ''):
            return self.meta['filetype']

        content_type = self.headers.get('Content-Type', '')
        filetype = list(filter(lambda x: x.startswith("."), content_type.split(";")))
        filetype = filetype[0] if filetype else ""

        for each in content_type.split(';'):
            if not each or each.lower().startswith("charset="):
                continue
            if each == 'application/ms-download':
                filetype = filetype or 'pdf'
            elif each == 'image/webp':
                filetype = filetype or 'png'
            elif each in ('application/octet-stream', 'application/x-msdownload'):
                continue
            else:
                filetype = mimetypes.guess_extension(each) or filetype
            if filetype:
                return filetype.split('.', maxsplit=1)[-1]
        return self.filename.split(".", maxsplit=1)[-1]

    @property
    def apparent_encoding(self):
        return charset_normalizer.detect(self.content)["encoding"]

    def read(self, ignore_errors=False) -> Optional[bytes]:
        """Read and return the response content."""
        try:
            return self.content
        except DownloadError:
            if not ignore_errors:
                raise

    def iter_content(self, chunk_size=1, decode_unicode=False):
        def generate():
            # noinspection PyBroadException
            try:
                # Special case for urllib3.
                if hasattr(self.raw, "stream"):
                    yield from self.raw.stream(chunk_size, decode_content=True)
                elif self.raw is not None:
                    # Standard file-like object.
                    while True:
                        chunk = self.raw.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk

                self._content_consumed = True
            except Exception as e:  # don't catch BaseException to avoid trigger `Exception ignored in xxx`
                self.fail(ResponseReadError(e))
                self.check_error()
            finally:
                self.maybe_on_consumed()

        if self._content_consumed and isinstance(self._content, bool):
            raise ResponseStreamConsumed()
        elif chunk_size is not None and not isinstance(chunk_size, int):
            raise TypeError("chunk_size must be an int, it is instead a %s." % type(chunk_size))

        # simulate reading small chunks of the content
        if self._content_consumed:
            chunks = iter_slices(self._content, chunk_size)
        else:
            chunks = generate()
        if decode_unicode:
            chunks = stream_decode_response_unicode(chunks, self.encoding)

        return chunks

    @property
    def content(self):
        """Content of the response, in bytes."""
        if self._content is False:
            # Read the contents.
            if self._content_consumed:
                raise RuntimeError("The content for this response was already consumed")
            if self.status_code == 0 or self.raw is None:
                self._content = None
            else:
                self._content = b"".join(self.iter_content(CONTENT_CHUNK_SIZE)) or b""
        self._content_consumed = True
        self.maybe_on_consumed()
        # don't need to release the connection; that's been handled by urllib3
        # since we exhausted the data.
        return self._content

    def maybe_on_consumed(self):
        if hasattr(self, 'on_consumed') and self.on_consumed:
            self.on_consumed(response=self)
            del self.on_consumed

    @property
    def text(self):
        if self._cached_text is not None:
            return self._cached_text

        if not self.content:
            return ""

        encoding = self.encoding or self.apparent_encoding
        # Decode unicode from given encoding.
        try:
            text = self.content.decode(encoding, errors="replace")
        except (LookupError, TypeError):
            # A LookupError is raised if the encoding was not found which could
            # indicate a misspelling or similar mistake.
            #
            # A TypeError can be raised if encoding is None
            #
            # So we try blindly encoding.
            text = str(self.content, errors='replace')

        self._cached_text = text
        return self._cached_text

    def clean_text(self, shield_a=False, remove_input=True, remove_comment=False, remove_base64=True):
        text = self.text
        for special_character_pattern in SPECIAL_CHARACTER_PATTERNS:
            text = special_character_pattern.sub("", text)
        text = html_unescape(text).replace("<!DOCTYPE html>", "")
        if remove_comment:
            # comment bug
            text = re.sub(r'<!--[.\s\S]*?-->', '', text)

        soup = None
        invalid_tags = []
        # noinspection PyBroadException
        try:
            soup = BeautifulSoup(text, "lxml")
            for tag in soup.find_all(recursive=True):
                for each in tag.contents or []:
                    if isinstance(each, Comment) and remove_comment:
                        each.extract()
                if isinstance(tag, Comment) and remove_comment:
                    tag.extract()
                if tag.name in ('style', 'script', 'input'):
                    if tag.name != 'input' or remove_input:
                        tag.extract()
                if shield_a and tag.name == 'a':
                    tag.name = 'p'
                if tag.name == 'div' and not tag.find_all(recursive=False) and not tag.get_text().strip():
                    tag.extract()
                if tag.attrs.get("href"):
                    if tag['href'] == "javascript:void(0);":
                        tag.attrs.pop("href")
                        if tag.name in ('iframe', 'img', 'a'):
                            invalid_tags.append(tag)
                    elif not tag['href'].startswith(("javascript", 'ftp:', 'window.')):
                        try:
                            tag['href'] = self.urljoin(tag['href'])
                        except ValueError:
                            pass
                if tag.get("src"):
                    if remove_base64 and tag["src"].startswith("data:image"):
                        tag.extract()
                    else:
                        tag["src"] = self.urljoin(tag["src"])
                if tag.attrs.get("class"):
                    if "el-loading-mask" in tag.attrs["class"]:
                        tag.attrs["class"].remove("el-loading-mask")
                style = tag.attrs.pop("style", "").replace(' ', '')
                if 'display:none' in style:
                    invalid_tags.append(tag)
                tag.attrs.pop("rel", None)
            for tag in invalid_tags:
                tag.extract()
            invalid_tags[:] = []
            text = str(soup)
        finally:
            soup and soup.clear(decompose=True)
        return text

    def json(self, **kwargs):
        if not self.encoding and self.content and len(self.content) > 3:
            encoding = guess_json_utf(self.content)
            if encoding is not None:
                try:
                    return json.loads(self.content.decode(encoding), **kwargs)
                except UnicodeDecodeError:
                    # Wrong UTF codec detected; usually because it's not UTF-8
                    # but some other 8-bit codec.  This is an RFC violation,
                    # and the server didn't bother to tell us what codec *was*
                    # used.
                    pass
        return json.loads(self.text, **kwargs)

    @property
    def selector(self):
        if self._cached_selector is None:
            from tider.selector import Selector
            self._cached_selector = Selector(text=self.text)
        return self._cached_selector

    def xpath(self, query, **kwargs):
        return self.selector.xpath(query, **kwargs)

    def css(self, query):
        return self.selector.css(query)

    def jmespath(self, query):
        return self.selector.jmespath(query)

    def soup(self, features="html.parser", parse_only=None, **kwargs):
        return BeautifulSoup(self.text, features=features, parse_only=parse_only, **kwargs)

    def re(self, regex, replace_entities=True, flags=re.S):
        return self.selector.re(regex, replace_entities, flags)

    def retry(self, reset_proxy=True, keep_cookies=False, **kwargs):
        # bug in old version:
        # callback must be specified if using retry in promise,
        # otherwise the callback maybe `Promise._then` which will
        # cause fatal error like recursion error.
        if reset_proxy:
            self.request.invalidate_proxy()

        meta = dict(self.meta)
        meta.update(kwargs.pop('meta', {}))
        meta.pop('promise_node', None)
        meta.pop('promise_then', None)
        parse_times = meta.get('parse_times', 0) + 1
        meta.update(retry_times=0, parse_times=parse_times)

        headers = dict(self.request.headers or {})
        headers.update(kwargs.pop('headers', {}))
        if not keep_cookies:
            headers.pop('Cookie', None)
            meta['merge_cookies'] = False
        # request will be closed after parsed, so using `replace`
        # to create a new request.
        request = self.request.replace(meta=meta, dup_check=False, headers=headers, **kwargs)
        return request

    def follow(self, url, callback=None, errback=None, cb_kwargs=None,
               headers=None, cookies=None, proxies=None, proxy_schema=None, **kwargs):
        h = self.request.headers.copy()
        h.pop('Cookie', None)
        h.update(headers or {})

        # may contain cookies from redirects in request.session_cookies.
        session_cookies = merge_cookies(RequestsCookieJar(), self.request.session_cookies)
        # # copy cookies to avoid conflict with brother requests.
        session_cookies = merge_cookies(session_cookies, self.cookies)
        # user cookies will override the cookies in request tree.
        session_cookies = merge_cookies(session_cookies, cookies)

        url = self.urljoin(url)
        # cookies here will override cookies in session.
        yield self.request.replace(
            url=url,
            callback=callback,
            errback=errback,
            cb_kwargs=cb_kwargs,
            headers=h,
            cookies=session_cookies,
            proxies=proxies,
            proxy_schema=proxy_schema,
            **kwargs
        )

    def follow_all(self, urls, callback=None, cb_kwargs=None, **kwargs):
        if not hasattr(urls, '__iter__'):
            raise TypeError("'urls' argument must be an iterable")
        return (
            self.follow(
                url=url,
                callback=callback,
                cb_kwargs=cb_kwargs,
                **kwargs
            )
            for url in urls
        )

    def _clear_caches(self):
        self._cached_text = None
        if self._cached_selector:
            # root created by etree.fromstring
            self._cached_selector.clear()
        self._cached_selector = None

        # break references.
        self.headers = None
        self.cookies = None

    def close(self):
        # maybe not read when using stream or in some specific case.
        self.maybe_on_consumed()
        raw, self.raw = self.raw, None
        if getattr(raw, "release_conn", None):
            # special for urllib3.
            if not self._content_consumed or self.failed:
                # also includes HEAD requests.
                raw.close()  # close connection and http.client.HTTPResponse if exists.
            # drain_conn will read and discard any remaining HTTP response data in the response connection,
            # if connection pool is closed, the connection will be closed.
            raw.release_conn()  # break refs
        elif getattr(raw, "close", None):
            try:
                raw.close()
            except AttributeError:
                # using curl_cffi and Python version < 3.8
                pass
        self._clear_caches()
        if self._error:
            self._error.__traceback__ = None
        self._error = None
        self.request = None
