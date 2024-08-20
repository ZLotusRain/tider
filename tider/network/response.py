import os
import re
import json
import charset_normalizer
from bs4 import BeautifulSoup
from contextlib import suppress
from urllib.parse import urljoin, unquote
from requests.structures import CaseInsensitiveDict
from requests.utils import iter_slices, stream_decode_response_unicode

from tider.network.request import Request


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
        'request', 'status_code', 'url', 'headers',
        'encoding', 'cookies', 'reason', 'raw', 'elapsed', '_success',
        '_cached_soup', '_cached_content', '_cached_text', '_cached_selector',

    )

    def __init__(self, request=None):
        self.request = request
        self.status_code = None
        self.url = ''
        self.headers = CaseInsensitiveDict()
        self.encoding = None
        self.cookies = None
        self.reason = None
        self.raw = None

        self.elapsed = 0
        self._success = True
        self._cached_content = None
        self._cached_text = None
        self._cached_selector = None

    def _charset_from_content_type(self):
        content_type = self.headers.get("Content-Type", "")
        charset = list(filter(lambda x: "charset" in x, content_type.split(";")))
        return charset[0].split("=")[-1] if charset else None

    @classmethod
    def dummy(cls, text, request=None, **kwargs):
        resp = cls(request)
        resp._cached_text = text
        for key, value in kwargs.items():
            setattr(resp, key, value)
        return resp

    def __iter__(self):
        """Allows you to use a response as an iterator."""
        return self.iter_content(128)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self):
        return '<Response [%s]>' % self.status_code

    @property
    def ok(self):
        return self._success

    @property
    def stored(self):
        return self.path and os.path.exists(self.path)

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
    def path(self):
        return self.meta.get('download_path', '')

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
        return filename

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
        if self.raw is not None:
            self.raw.content
        self._success = False

    @property
    def apparent_encoding(self):
        return charset_normalizer.detect(self.content)["encoding"]

    def iter_content(self, chunk_size=1, decode_unicode=False):
        if chunk_size is not None and not isinstance(chunk_size, int):
            raise TypeError("chunk_size must be an int, it is instead a %s." % type(chunk_size))
        chunks = iter([])
        if self.stored or self._cached_content is not None:
            chunks = iter_slices(self.content, chunk_size)
            if decode_unicode:
                chunks = stream_decode_response_unicode(chunks, self)
        elif self.raw is not None:
            chunks = self.raw.iter_content(chunk_size, decode_unicode)
        return chunks

    @property
    def content(self):
        """Content of the response, in bytes."""
        if self._cached_content is None:
            if self.stored:
                with open(self.path, "rb+") as fo:
                    self._cached_content = fo.read()
            elif self.raw is not None:
                self._cached_content = self.raw.content
            else:
                self._cached_content = b''
        return self._cached_content

    @property
    def text(self):
        if self._cached_text is not None:
            return self._cached_text

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

    def retry(self, **kwargs):
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
        meta.update(retry_times=0, parse_times=parse_times, reset_proxy=True)
        request = self.request.replace(priority=self.request.priority+1,
                                       meta=meta, dup_check=False)
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
        if self.stored:
            with suppress(Exception):
                os.remove(self.path)
        self._cached_content = None
        self._cached_text = None
        self._cached_selector = None

    def close(self):
        raw, self.raw = self.raw, None
        if raw is not None:
            raw.close()
        self._clear_caches()
        self.request and self.request.close()
        self.headers = None
        self.cookies = None
        del self.request, raw


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
