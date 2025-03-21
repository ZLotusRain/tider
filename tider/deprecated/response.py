import os
import re
import charset_normalizer
from typing import Optional
from bs4 import BeautifulSoup
from contextlib import suppress
from urllib.parse import urljoin, unquote
from requests.exceptions import HTTPError
from requests.models import Response as BaseResponse

from tider.selector import Selector
from tider.network.request import Request


SPACE_CHARACTERS = ["&nbsp;", "&ensp;", "&emsp;", "&thinsp;",
                    "&zwnj;", "&zwj;", "&#x0020;", "&#x0009;",
                    "&#x000A;", "&#x000D;", "&#12288;"]
# html 源码中的特殊字符，需要删掉，否则会影响etree的构建
# 移除控制字符 全部字符列表 https://zh.wikipedia.org/wiki/%E6%8E%A7%E5%88%B6%E5%AD%97%E7%AC%A6
SPECIAL_CHARACTERS = ['[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '|'.join(SPACE_CHARACTERS)]
SPECIAL_CHARACTER_PATTERNS = [re.compile(special_character) for special_character in SPECIAL_CHARACTERS]

CONTENT_CHUNK_SIZE = 10 * 1024
ITER_CHUNK_SIZE = 512


class Response(BaseResponse):

    def __init__(self, request=None):
        super(Response, self).__init__()

        self._content_consumed = False
        self.encoding = None
        self.path = None
        self.request: Optional[Request] = request
        if request:
            self.path = request.meta.get('download_path', '')
            self.encoding = request.encoding or self.encoding
        self._success = True
        self._cached_text = None
        self._cached_selector = None

    def update_request(self, request):
        self.request = request
        self.path = request.meta.get('download_path', '')
        self.encoding = request.encoding or self.encoding

    def _charset_from_content_type(self):
        content_type = self.headers.get("Content-Type", "")
        charset = list(filter(lambda x: "charset" in x, content_type.split(";")))
        return charset[0].split("=")[-1] if charset else None

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
            filename = unquote(filename.split("=")[-1].replace("utf-8'zh_cn'", ''))
        return filename

    @property
    def filetype(self):
        filetype = self.meta.get('filetype') or self.filename.split(".")[-1]
        if not filetype:
            content_type = self.headers.get('Content-Type', '')
            maybe_filetype = list(filter(lambda x: x.startswith("."), content_type.split(";")))
            if maybe_filetype:
                filetype = maybe_filetype[0].split(".")[-1]
            elif "application/msword" in content_type:
                filetype = "doc"
            elif "application/ms-download" in content_type:
                filetype = "pdf"
            elif "application/pdf" in content_type:
                filetype = "pdf"
            elif content_type == "application/x-rar-compressed":
                filetype = "rar"
        return filetype

    def fail(self, reason=None):
        self.reason = reason or self.reason
        self._success = False

    @property
    def apparent_encoding(self):
        return charset_normalizer.detect(self.content)["encoding"]

    def iter_content(self, chunk_size=1, decode_unicode=False):
        if self._content is False and self.stored:
            with open(self.path, "rb+") as fo:
                self._content = fo.read()
                self._content_consumed = True
        return super(Response, self).iter_content(chunk_size, decode_unicode)

    @property
    def content(self):
        """Content of the response, in bytes."""

        if self._content is False:
            # Read the contents.
            if self._content_consumed:
                raise RuntimeError("The content for this response was already consumed")

            if self.stored:
                with open(self.path, "rb+") as fo:
                    self._content = fo.read()
            elif self.status_code == 0 or self.raw is None:
                self._content = None
            else:
                content_list = []
                for chunk in self.iter_content(CONTENT_CHUNK_SIZE):
                    content_list.append(chunk)
                self._content = b''.join(content_list)

        self._content_consumed = True
        # don't need to release the connection; that's been handled by urllib3
        # since we exhausted the data.
        return self._content

    @property
    def text(self):
        if self._cached_text is not None:
            return self._cached_text
        text = super(Response, self).text
        for special_character_pattern in SPECIAL_CHARACTER_PATTERNS:
            text = special_character_pattern.sub("", text)
        self._cached_text = text
        return self._cached_text

    def raise_for_status(self):
        if self.status_code is None:
            return

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

    @property
    def selector(self):
        if self._cached_selector is None:
            self._cached_selector = Selector(text=self.text)
        return self._cached_selector

    def xpath(self, query, **kwargs):
        return self.selector.xpath(query, **kwargs)

    def css(self, query):
        return self.selector.css(query)

    def soup(self, features="html.parser", builder=None, parse_only=None, from_encoding=None,
             exclude_encodings=None, **kwargs):
        return BeautifulSoup(self.text, features=features, builder=builder, parse_only=parse_only,
                             from_encoding=from_encoding, exclude_encodings=exclude_encodings, **kwargs)

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
        self._content = None
        self._cached_text = None
        self._cached_selector = None

    def close(self):
        self.request and self.request.close()
        raw, self.raw = self.raw, None
        if raw is not None:
            raw.close()
        if hasattr(raw, 'release_conn'):
            raw.release_conn()
            setattr(raw, '_pool', None)
        self._clear_caches()
