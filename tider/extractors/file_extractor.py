import re
from typing import Optional, List
from functools import partial

from tider.extractors.link_extractor import LinkExtractor
from tider.utils.misc import arg_to_iter
from tider.utils.url import url_has_any_extension


IGNORED_EXTENSIONS = ['exe', 'bin', 'rss', 'dmg', 'iso', 'apk']
IGNORED_EXTENSIONS.extend([ext.upper() for ext in IGNORED_EXTENSIONS])

COMMON_EXTENSIONS = [
    # archives
    '7z', '7zip', 'bz2', 'rar', 'tar', 'tar.gz', 'xz', 'zip',
    # images
    'mng', 'pct', 'bmp', 'gif', 'jpg', 'jpeg', 'png', 'pst', 'psp', 'tif',
    'tiff', 'ai', 'drw', 'dxf', 'eps', 'ps', 'svg', 'cdr', 'ico',
    # audio
    'mp3', 'wma', 'ogg', 'wav', 'ra', 'aac', 'mid', 'au', 'aiff',
    # video
    '3gp', 'asf', 'asx', 'avi', 'mov', 'mp4', 'mpg', 'qt', 'rm', 'swf', 'wmv',
    'm4a', 'm4v', 'flv', 'webm',
    # office suites
    'xls', 'xlsx', 'ppt', 'pptx', 'pps', 'doc', 'docx', 'odt', 'ods', 'odg',
    'odp',
    # other
    'pdf', "txt", "csv", "fyzf", "qdncf", "qdnzb", "xml", "gzzf",
    "gef", "zbqd", 'zbj', 'jszs'
]
COMMON_EXTENSIONS.extend([ext.upper() for ext in COMMON_EXTENSIONS])

NETWORK_EXTENSIONS = ["cn", "com", "net", "xyz", "info", "icu", "top", "cc", "de", "app", "pub", "org",
                      "htm", "html", "jhtml", "shtml", "asp", "aspx", "jsp", "jspx", "php", 'css', "js",
                      "do", "action", "sg"]
NETWORK_EXTENSIONS.extend([ext.upper() for ext in NETWORK_EXTENSIONS])

MEDIA_EXTENSIONS = ['avi', 'mp4', 'wmv', 'mpeg', 'm4v', 'mov', 'asf', 'flv', 'f4v',
                    'rmvb', 'rm', '3gp', 'vob', 'mkv']


def is_common_filetype(s: str):
    extensions = COMMON_EXTENSIONS + IGNORED_EXTENSIONS
    return s.endswith(tuple(extensions))


def maybe_valid_ext(text: str = "", suffix: str = "", ignored: Optional[List] = None):
    """return valid file extension if exists"""
    ext = ""
    suffix = suffix.strip() or text.strip().split(".")[-1] if "." in text else suffix
    suffix = suffix.lower()
    if ignored and suffix in ignored:
        ext = ""
    elif suffix in NETWORK_EXTENSIONS:
        ext = ""
    elif suffix.isdigit():
        ext = ""
    elif re.findall(r"[\u4e00-\u9fa5 \\/\[\]-_]+", suffix):
        ext = ""
    elif (len(suffix) <= 7 and suffix.isalnum()) or is_common_filetype(suffix):
        ext = suffix
    return ext


def maybe_file_description(text, desc=("附件", "下载")):
    return any(map(lambda x: x in text, desc))


class FileExtractor(LinkExtractor):
    def __init__(self, deny_extensions=(), append=True, **kwargs):
        if append:
            deny_extensions = deny_extensions + tuple(IGNORED_EXTENSIONS)
        self.deny_extensions = {'.' + e for e in arg_to_iter(deny_extensions)}
        kwargs.setdefault('unique', True)
        super().__init__(**kwargs)

    def _extension_allowed(self, link):
        if url_has_any_extension(link["url"], self.deny_extensions):
            return False
        if url_has_any_extension(link["title"], self.deny_extensions):
            return False
        _maybe_valid_ext = partial(maybe_valid_ext, suffix="")
        if not any(map(_maybe_valid_ext, (link["title"], link["url"]))):
            if maybe_file_description(link['title']):
                return True
            return False
        return True

    def _process_links(self, links):
        """ Normalize and filter extracted links

        The subclass should override it if necessary
        """
        links = [x for x in links if self._link_allowed(x) and self._extension_allowed(x)]
        _maybe_valid_ext = partial(maybe_valid_ext, suffix="")
        for idx, link in enumerate(links):
            ext = _maybe_valid_ext(link["url"]) or _maybe_valid_ext(link["title"])
            file_name = link["title"] or f"附件{idx}"
            link.update({"file_name": file_name, "ext": ext})
        return self._deduplicate_if_needed(links)
