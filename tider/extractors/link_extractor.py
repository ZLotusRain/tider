import re
import operator
from functools import partial
from typing import Union, Iterable

from lxml import etree
from parsel.csstranslator import HTMLTranslator
from urllib.parse import urljoin, urlparse
from w3lib.html import strip_html5_whitespace
from w3lib.url import safe_url_string

from tider.utils.misc import arg_to_iter, unique_list
from tider.utils.url import url_is_from_any_domain


# from lxml/src/lxml/html/__init__.py
XHTML_NAMESPACE = "http://www.w3.org/1999/xhtml"

_collect_string_content = etree.XPath("string()")

_RegexT = Union[str, re.Pattern[str]]
_RegexOrSeveralT = Union[_RegexT, Iterable[_RegexT]]


def _matches(url, regexs):
    return any(r.search(url) for r in regexs)


def _is_valid_url(url):
    return url.split('://', 1)[0] in {'http', 'https', 'file', 'ftp'}


def _nons(tag):
    if isinstance(tag, str):
        if tag[0] == '{' and tag[1:len(XHTML_NAMESPACE) + 1] == XHTML_NAMESPACE:
            return tag.split('}')[-1]
    return tag


def _identity(x):
    return x


class LinkExtractor:

    _csstranslator = HTMLTranslator()

    def __init__(self, allow=(), deny=(), allow_domains=(), deny_domains=(), restrict_xpaths=(),
                 tags=('a', 'area'), attrs=('href',), on_extract=None, unique=False,
                 deny_extensions=None, restrict_css=(), strip=True):
        self.tags, self.attrs = set(arg_to_iter(tags)), set(arg_to_iter(attrs))
        self.scan_tags = partial(operator.contains, self.tags)
        self.scan_attrs = partial(operator.contains, self.attrs)
        self.on_extract = on_extract if callable(on_extract) else _identity
        self.unique = unique
        self.strip = strip

        self.allow_res = self._compile_regexes(allow)
        self.deny_res = self._compile_regexes(deny)

        self.allow_domains = set(arg_to_iter(allow_domains))
        self.deny_domains = set(arg_to_iter(deny_domains))

        self.restrict_xpaths = tuple(arg_to_iter(restrict_xpaths))
        self.restrict_xpaths += tuple(
            map(self._csstranslator.css_to_xpath, arg_to_iter(restrict_css))
        )
        self.deny_extensions = {"." + e if not e.startswith('.') else e for e in arg_to_iter(deny_extensions)}

    @staticmethod
    def _compile_regexes(value: Union[_RegexOrSeveralT, None]) -> list[re.Pattern[str]]:
        return [
            x if isinstance(x, re.Pattern) else re.compile(x)
            for x in arg_to_iter(value)
        ]

    def _iter_links(self, document):
        for element in document.iter(etree.Element):
            if not self.scan_tags(_nons(element.tag)):
                continue
            attribs = element.attrib
            for attrib in attribs:
                if not self.scan_attrs(attrib):
                    continue
                yield element, attrib, attribs[attrib]

    def _link_allowed(self, link):
        if not _is_valid_url(link["url"]):
            return False
        if self.allow_res and not _matches(link["url"], self.allow_res):
            return False
        if self.deny_res and _matches(link["url"], self.deny_res):
            return False
        parsed_url = urlparse(link["url"])
        if self.allow_domains and not url_is_from_any_domain(parsed_url, self.allow_domains):
            return False
        if self.deny_domains and url_is_from_any_domain(parsed_url, self.deny_domains):
            return False
        if self.deny_extensions and link['url'].endswith(tuple(self.deny_extensions)):
            return False
        return True

    def _process_links(self, links):
        """ Normalize and filter extracted links

        The subclass should override it if necessary
        """
        links = [x for x in links if self._link_allowed(x)]
        return self._deduplicate_if_needed(links)

    def _deduplicate_if_needed(self, links):
        if self.unique:
            return unique_list(links, key=lambda link: link["url"])
        return links

    def _extract_links(self, selector, response_url, response_encoding):
        links = []
        # hacky way to get the underlying lxml parsed document
        for el, attr, attr_val in self._iter_links(selector.root):
            # pseudo lxml.html.HtmlElement.make_links_absolute(base_url)
            try:
                if self.strip:
                    attr_val = strip_html5_whitespace(attr_val)
                attr_val = urljoin(response_url, attr_val)
            except ValueError:
                continue  # skipping bogus links
            else:
                url = self.on_extract(attr_val)
                if url is None or 'javascript:void(0)' in url:
                    continue
            try:
                url = safe_url_string(url, encoding=response_encoding)
            except ValueError:
                pass
            url = urljoin(response_url, url)
            if el.attrib.get("title"):
                title = el.attrib.get("title")
            else:
                title = _collect_string_content(el) or ""
            links.append({"title": str(title), "url": url})
        return self._deduplicate_if_needed(links)

    def extract_links(self, response):
        # {"title": "", "url": ""}
        all_links = []

        if self.restrict_xpaths:
            docs = [
                sub_doc
                for x in self.restrict_xpaths
                for sub_doc in response.xpath(x)
            ]
        else:
            docs = [response.selector]
        response_url = response.url
        encoding = response.encoding or response.apparent_encoding
        for doc in docs:
            links = self._extract_links(doc, response_url, encoding)
            all_links.extend(self._process_links(links))
        return unique_list(all_links, key=lambda link: link["url"])
