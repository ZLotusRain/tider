import re
import json
import charset_normalizer
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from urllib3.exceptions import (
    DecodeError,
    ProtocolError,
    ReadTimeoutError,
    SSLError,
)
from requests.models import REDIRECT_STATI
from requests.cookies import cookiejar_from_dict
from requests.structures import CaseInsensitiveDict
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    ContentDecodingError,
    StreamConsumedError,
    HTTPError
)
from requests.exceptions import SSLError as RequestsSSLError
from requests.utils import iter_slices, stream_decode_response_unicode, guess_json_utf

from tider import Request


SPACE_CHARACTERS = ["&nbsp;", "&ensp;", "&emsp;", "&thinsp;",
                    "&zwnj;", "&zwj;", "&#x0020;", "&#x0009;",
                    "&#x000A;", "&#x000D;", "&#12288;"]
# html 源码中的特殊字符，需要删掉，否则会影响etree的构建
# 移除控制字符 全部字符列表 https://zh.wikipedia.org/wiki/%E6%8E%A7%E5%88%B6%E5%AD%97%E7%AC%A6
SPECIAL_CHARACTERS = ["[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]", "|".join(SPACE_CHARACTERS)]
SPECIAL_CHARACTER_PATTERNS = [re.compile(special_character) for special_character in SPECIAL_CHARACTERS]

CONTENT_CHUNK_SIZE = 10 * 1024


class Response:

    __slots__ = (
        'request', 'status_code', 'url', 'headers', 'encoding',
        'cookies', 'reason', 'raw', 'version', 'elapsed', 'history', '_next',
        '_redirect_target', '_success', '_content', '_content_consumed',
        '_cached_text', '_cached_selector'
    )

    def __init__(self, request=None):
        # bound request
        self.request = request

        self.status_code = None

        # Final URL location of Response.
        self.url = None
        self.headers = CaseInsensitiveDict()
        self.encoding = None
        self.cookies = cookiejar_from_dict({})
        self.reason = None
        self.version = None

        self.elapsed = 0
        self.history = []
        self._next = None

        self._redirect_target = None
        self._success = True
        self._content = False
        self._content_consumed = False
        self._cached_text = None
        self._cached_selector = None

        self.raw = None

    @classmethod
    def dummy(cls, text, request=None, **kwargs):
        resp = cls(request)
        kwargs.setdefault('encoding', 'utf-8')
        resp._cached_text = text
        for key, value in kwargs.items():
            setattr(resp, key, value)
        return resp

    @property
    def is_redirect(self):
        """True if this Response is a well-formed HTTP redirect that could have
        been processed automatically (by :meth:`Session.resolve_redirects`).
        """
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
            # encodings. (See PR #3538)
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

        if http_error_msg:
            raise HTTPError(http_error_msg)

    def __iter__(self):
        """Allows you to use a response as an iterator."""
        return self.iter_content(128)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        if self.ok:
            return '<Response [%s]>' % self.status_code
        else:
            return '<Failure [%s]>' % self.reason

    @property
    def ok(self):
        return self._success

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
    def is_html(self):
        return "text/html" in self.headers.get("Content-Type", "")

    def urljoin(self, url):
        """Join this Response's url with a possible relative url to form an
        absolute interpretation of the latter."""
        return urljoin(self.url, url)

    @property
    def filename(self):
        filename = ''
        content_disposition = self.headers.get('Content-Disposition', '')
        if content_disposition:
            # 'attachment;filename=%E6%A0%87%E9%A1%B9%E4.pdf'
            # "attachment;filename*=utf-8'zh_cn'%E6%8B%9B%E6%A0%87%E6%96%87%E4%BB%B6%E6%AD%A3%E6%96%87.pdf"
            # 'attachment; filename="æ\x88\x90äº¤å\x85¬å\x91\x8a.pdf"'
            filename = list(filter(lambda x: "file" in x, content_disposition.split(";")))
            filename = filename[0] if filename else ""
            filename = unquote(filename.split("=")[-1].replace("utf-8'zh_cn'", '')).strip('"').strip("'")
        return filename.strip()

    @property
    def filetype(self):
        filetype = self.meta.get('filetype') or self.filename.split(".")[-1]
        if not filetype:
            content_type = self.headers.get('Content-Type', '')
            maybe_filetype = list(filter(lambda x: x.startswith("."), content_type.split(";")))
            if maybe_filetype:
                filetype = maybe_filetype[0].split(".")[-1].strip('"').strip("'")
            elif "application/msword" in content_type:
                filetype = "doc"
            elif "application/ms-download" in content_type:
                filetype = "pdf"
            elif "application/pdf" in content_type:
                filetype = "pdf"
            elif content_type == "application/x-rar-compressed":
                filetype = "rar"
        return filetype

    def fail(self, reason):
        self.reason = reason
        self._success = False

    @property
    def apparent_encoding(self):
        return charset_normalizer.detect(self.content)["encoding"]

    def read(self) -> bytes:
        """
        Read and return the response content.
        """
        return self.content

    def iter_content(self, chunk_size=1, decode_unicode=False):
        def generate():
            # Special case for urllib3.
            if hasattr(self.raw, "stream"):
                try:
                    yield from self.raw.stream(chunk_size, decode_content=True)
                except ProtocolError as e:
                    raise ChunkedEncodingError(e)
                except DecodeError as e:
                    raise ContentDecodingError(e)
                except ReadTimeoutError as e:
                    raise ConnectionError(e)
                except SSLError as e:
                    raise RequestsSSLError(e)
            elif self.raw is not None:
                # Standard file-like object.
                while True:
                    chunk = self.raw.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

            self._content_consumed = True

        if self._content_consumed and isinstance(self._content, bool):
            raise StreamConsumedError()
        elif chunk_size is not None and not isinstance(chunk_size, int):
            raise TypeError("chunk_size must be an int, it is instead a %s." % type(chunk_size))

        # simulate reading small chunks of the content
        reused_chunks = iter_slices(self._content, chunk_size)

        stream_chunks = generate()

        chunks = reused_chunks if self._content_consumed else stream_chunks

        if decode_unicode:
            chunks = stream_decode_response_unicode(chunks, self)

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
        # don't need to release the connection; that's been handled by urllib3
        # since we exhausted the data.
        return self._content

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

        for special_character_pattern in SPECIAL_CHARACTER_PATTERNS:
            text = special_character_pattern.sub("", text)
        self._cached_text = text
        return self._cached_text

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

    def soup(self, features="html.parser", parse_only=None, **kwargs):
        return BeautifulSoup(self.text, features=features, parse_only=parse_only, **kwargs)

    def re(self, regex, replace_entities=True, flags=re.S):
        return self.selector.re(regex, replace_entities, flags)

    def retry(self, reset_proxy=True, **kwargs):
        # old version error:
        # callback must be specified if using retry in promise,
        # otherwise the callback maybe `Promise._then` which will
        # cause fatal error like recursion error.
        # request will be closed after retry, so using `replace`
        # to create a new request
        parse_times = self.meta.get('parse_times', 0) + 1
        meta = dict(self.meta)
        meta.pop('promise_node', None)
        meta.pop('promise_then', None)
        meta.update(retry_times=0, parse_times=parse_times)
        if reset_proxy:
            self.request.forbid_proxy()
        request = self.request.replace(priority=self.request.priority+1,
                                       meta=meta, dup_check=False, **kwargs)
        return request

    def follow(self, url, callback=None, cb_kwargs=None, **kwargs):
        url = self.urljoin(url)
        return Request(
            url=url,
            callback=callback,
            cb_kwargs=cb_kwargs,
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
        self._cached_selector = None

    def close(self):
        raw, self.raw = self.raw, None
        if raw is not None:
            raw.close()
        if getattr(raw, "release_conn", None):
            raw.release_conn()
        self._clear_caches()
        self.request and self.request.close()
        self.request = None


class DummyResponse:
    def __init__(self, url, text, encoding=None):
        self.url = url
        self.text = text
        self.encoding = encoding
        self._cached_selector = None

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

    def close(self):
        self.text = None
        self._cached_selector = None
