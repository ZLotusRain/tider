import re
from functools import partial
from enum import StrEnum
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


def rel_has_nofollow(rel) -> bool:
    """Return True if link rel attribute has nofollow type"""
    return rel is not None and "nofollow" in rel.replace(",", " ").split()


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


def _contains(container, x):
    return x.lower() in container


class FiletypeCategory(StrEnum):

    Archives = 'Archives'
    BitmapImages = 'Bitmap images'
    DigitalCameraRAWPhotos = 'Digital camera RAW photos'
    VectorGraphics = 'Vector graphics'
    FontFiles = 'Font files'
    AudioFiles = 'Audio files'
    VideoFiles = 'Video files'
    OfficeSuites = 'Office suites'
    Documents = 'Documents'
    SimpleTextFiles = 'Simple text files'
    PossiblyDangerousFiles = 'Possibly dangerous files'


class FileExtExtractor:
    # Extension refs:
    # 1.https://www.file-extensions.org
    # 2.https://support.microsoft.com/en-us/windows/common-file-name-extensions-in-windows-da4a4430-8e76-89c5-59f7-1cdbbc75cb01

    COMMON_EXTENSIONS = {
        FiletypeCategory.Archives: {
            '7z', '7zip', 'bz2', 'rar', 'tar', 'tar.gz', 'xz', 'zip'
        },
        FiletypeCategory.BitmapImages: {
            'bmp', 'cpt', 'dds', 'dib', 'dng', 'gif', 'ico', 'icon',
            'jpeg', 'jpg', 'pcx', 'pic', 'png', 'psd', 'psdx', 'raw',
            'tif', 'tiff', 'webp'
        },
        FiletypeCategory.DigitalCameraRAWPhotos: {
            'arw', 'crw', 'dcr', 'dng', 'pcd', 'ptx', 'rw2'
        },
        FiletypeCategory.VectorGraphics: {
            'ai', 'cdr', 'csh', 'cls', 'drw', 'odg', 'svg', 'svgz', 'swf'
        },
        FiletypeCategory.FontFiles: {
            'eot', 'otf', 'ttc', 'ttf', 'woff'
        },
        FiletypeCategory.AudioFiles: {
            'mp3', 'wma', 'ogg', 'wav', 'ra', 'aac', 'mid', 'au', 'aiff',
        },
        FiletypeCategory.VideoFiles: {
            '264', '3g2', '3gp', 'asf', 'asx', 'avi',
            'bik', 'dat', 'h264', 'mov', 'mp4', 'mpg', 'qt', 'rm', 'swf', 'wmv',
            'm4a', 'm4v', 'flv', 'webm', 'mpeg', 'f4v', 'rmvb', 'vob', 'mkv',
        },
        FiletypeCategory.OfficeSuites: {
            'xls', 'xlsm', 'xlsx', 'xltm', 'xltx', 'potm', 'potx', 'ppt', 'pptm', 'pptx', 'pps',
            'doc', 'docb', 'docm', 'docx', 'dotm', 'dotx', 'odt', 'ods', 'odg', 'odp', 'xps'
        },
        FiletypeCategory.Documents: {
            'abw', 'pdf', 'djvu ', 'epub', 'mht', 'pages', 'vsd',
        },
        FiletypeCategory.SimpleTextFiles: {
            'txt', 'csv', 'xml', 'rb'
        },
        FiletypeCategory.PossiblyDangerousFiles: {
            'exe', 'sys', 'com', 'bat', 'bin', 'rss',
            'dmg', 'iso', 'apk', 'ipa', 'chm', 'class',
            'dll', 'drv', 'jar', 'js', 'lnk', 'ocx', 'msi',
            'pcx', 'scr', 'sh', 'shs', 'vbs', 'vxd', 'wmf', 'py'
        },
    }

    NETWORK_RELATED = {
        'com', 'org', 'net', 'int', 'edu', 'gov', 'mil',
        'arpa', 'cn', 'xyz', 'info', 'icu', 'top', 'cc',
        'de', 'app', 'pub', 'do', 'action', 'sg',
        'htm', 'html', 'jhtml', 'shtml', 'asp', 'aspx', 'jsp', 'jspx', 'php', 'css', 'js',
    }

    def __init__(self, ignored_types=(FiletypeCategory.PossiblyDangerousFiles, ), guess=True, includes=(), excludes=()):
        self.ignored_types = ignored_types
        self.guess = guess
        self._excludes = set(excludes)
        self._includes = set(includes)

    @property
    def valid_extensions(self):
        valid_extensions = set() | self._includes
        for t, extensions in self.COMMON_EXTENSIONS.items():
            if t in self.ignored_types:
                continue
            valid_extensions = valid_extensions | extensions
        valid_extensions.difference_update(self._excludes)
        return valid_extensions

    def extract(self, source, source_type='text'):
        if source_type not in ('text', 'url'):
            source_type = 'text'
        source = source.strip().lower()
        dots_num = source.count('.')  # whether dot in extension
        # consider extensions like 'tar.gz' and maybe valid extension itself
        splits = [source.split('.', maxsplit=num)[-1] for num in range(dots_num + 1)]
        for each in splits:
            if each in self.valid_extensions:
                return each

        ext = ""
        suffix = source.split(".")[-1]
        if not self.guess or "." not in source:
            ext = ""
        elif suffix in self._excludes:
            ext = ""
        elif (source_type.lower() == 'url' or source.startswith(('http', 'www'))) and suffix in self.NETWORK_RELATED:
            ext = ""
        elif suffix.isdigit():
            ext = ""
        elif re.findall(r"[\u4e00-\u9fa5 \\/\[\]-_]+", suffix):
            ext = ""
        elif len(suffix) <= 7 and suffix.isalnum():
            ext = suffix
        return ext


class Link:
    __slots__ = ["ext", "tag", "nofollow", "alt", "text", "title", "url"]

    def __init__(
        self, url: str, tag: str, title: str = "", text: str = "", alt: str = "", ext: str = "", nofollow: bool = False
    ):
        if not isinstance(url, str):
            got = url.__class__.__name__
            raise TypeError(f"Link urls must be str objects, got {got}")
        self.url: str = url
        self.tag: str = tag
        self.title: str = title
        self.text: str = text
        self.alt: str = alt
        self.ext: str = ext
        self.nofollow: bool = nofollow

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            raise KeyError(item)

    def __setitem__(self, key, value):
        if key not in self.__slots__:
            raise KeyError(key)
        setattr(self, key, value)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Link):
            raise NotImplementedError
        return (
            self.url == other.url
            and self.tag == other.tag
            and self.title == other.title
            and self.text == other.text
            and self.alt == other.alt
            and self.ext == other.ext
            and self.nofollow == other.nofollow
        )

    def __hash__(self) -> int:
        return (
            hash(self.url) ^ hash(self.tag) ^ hash(self.title) ^ hash(self.text) ^ hash(self.ext) ^ hash(self.nofollow)
        )

    def __repr__(self) -> str:
        return (
            f"Link(url={self.url!r}, title={self.title!r}, text={self.text!r}, "
            f"ext={self.ext!r}, nofollow={self.nofollow!r})"
        )

    def to_dict(self):
        return {
            "url": self.url,
            "title": self.title,
            "text": self.text,
            "ext": self.ext,
            "nofollow": self.nofollow,
        }


class LinkExtractor:

    _csstranslator = HTMLTranslator()

    file_hints = {'附件', '下载', '文件', 'download', 'file', '附件下载'}

    def __init__(self, allow=(), deny=(), allow_domains=(), deny_domains=(), restrict_xpaths=(),
                 tags=('a', 'area'), attrs=('href',), on_extract=None, unique=False,
                 allow_titles=(), deny_titles=(), deny_extensions=None, restrict_css=(),
                 file_only=False, ignored_file_types=(FiletypeCategory.PossiblyDangerousFiles,), guess_extension=True,
                 include_extensions=(), file_hints=(), file_tags=('img', ), strip=True):
        tags = [tag.lower() for tag in arg_to_iter(tags)]
        attrs = [attr.lower() for attr in arg_to_iter(attrs)]
        self.tags, self.attrs = set(tags), set(attrs)
        self.scan_tags = partial(_contains, self.tags)
        self.scan_attrs = partial(_contains, self.attrs)
        self.on_extract = on_extract if callable(on_extract) else _identity
        self.unique = unique
        self.strip = strip

        self.allow_res = self._compile_regexes(allow)
        self.deny_res = self._compile_regexes(deny)
        self.allow_titles = self._compile_regexes(allow_titles)
        self.deny_titles = self._compile_regexes(deny_titles)

        self.allow_domains = set(arg_to_iter(allow_domains))
        self.deny_domains = set(arg_to_iter(deny_domains))

        self.restrict_xpaths = tuple(arg_to_iter(restrict_xpaths))
        self.restrict_xpaths += tuple(
            map(self._csstranslator.css_to_xpath, arg_to_iter(restrict_css))
        )

        self.file_only = file_only
        self.file_hints = self.file_hints | set(arg_to_iter(file_hints))
        self.file_tags = set(arg_to_iter(file_tags))
        exclude_extensions = {e.split('.', maxsplit=1)[-1] if e.startswith('.') else e for e in arg_to_iter(deny_extensions)}
        self.ext_extractor = FileExtExtractor(ignored_types=ignored_file_types, guess=guess_extension,
                                              includes=include_extensions, excludes=exclude_extensions)
        self.deny_extensions = {"." + e for e in arg_to_iter(exclude_extensions)}

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
        if self.allow_titles and not _matches(" ".join((link["title"], link['text'])), self.allow_titles):
            return False
        if self.deny_titles and _matches(" ".join((link["title"], link['text'])), self.deny_titles):
            return False
        parsed_url = urlparse(link["url"])
        if self.allow_domains and not url_is_from_any_domain(parsed_url, self.allow_domains):
            return False
        if self.deny_domains and url_is_from_any_domain(parsed_url, self.deny_domains):
            return False
        if self.deny_extensions and link['url'].endswith(tuple(self.deny_extensions)):
            return False
        if self.file_only:
            if link['url'].endswith(tuple(self.ext_extractor.NETWORK_RELATED)):
                return False
            if link['ext'] or link['title'] in self.file_hints or link['tag'] in self.file_tags:
                # some file links don't end with extensions.
                return True
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
            return unique_list(links, key=lambda link: link["url"], prefer=lambda link: link["title"] or link["text"])
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
                if url is None or 'javascript:void(0)' in url or url == 'javascript:;':
                    continue
            try:
                response_encoding = response_encoding or 'utf-8'
                url = safe_url_string(url, encoding=response_encoding)
            except (ValueError, TypeError):
                pass
            except LookupError:
                try:
                    url = safe_url_string(url, encoding='utf-8')
                except (ValueError, TypeError):
                    pass
            url = urljoin(response_url, url)
            text = str(_collect_string_content(el)) or ""
            if el.attrib.get("title"):
                title = el.attrib.get("title")
            else:
                title = text
            alt = el.attrib.get("alt") or ""
            ext = self.ext_extractor.extract(title) or self.ext_extractor.extract(url, source_type='url')
            links.append(Link(url=url, tag=el.tag, title=str(title).strip(), text=text, alt=alt, ext=ext,  nofollow=rel_has_nofollow(el.get("rel"))))
        return links

    def extract(self, response):
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
        if self.unique:
            return unique_list(all_links, key=lambda link: link["url"], prefer=lambda link: link["title"] or link["text"])
        return all_links

    extract_links = extract
