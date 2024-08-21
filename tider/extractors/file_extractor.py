import re
import logging

from tider.extractors.link_extractor import LinkExtractor

logger = logging.getLogger(__name__)


class FileExtExtractor:

    # https://support.microsoft.com/en-us/windows/common-file-name-extensions-in-windows-da4a4430-8e76-89c5-59f7-1cdbbc75cb01
    URL = "https://www.file-extensions.org"

    COMMON_EXTENSIONS = {
        'Archives': {
            '7z', '7zip', 'bz2', 'rar', 'tar', 'tar.gz', 'xz', 'zip'
        },
        'Bitmap images': {
            'bmp', 'cpt', 'dds', 'dib', 'dng', 'gif', 'ico', 'icon',
            'jpeg', 'jpg', 'pcx', 'pic', 'png', 'psd', 'psdx', 'raw',
            'tif', 'tiff', 'webp'
        },
        'Digital camera RAW photos': {
            'arw', 'crw', 'dcr', 'dng', 'pcd', 'ptx', 'rw2'
        },
        'Vector graphics': {
            'ai', 'cdr', 'csh', 'cls', 'drw', 'odg',
            'svg', 'svgz', 'swf'
        },
        'Font files': {
            'eot', 'otf', 'ttc', 'ttf', 'woff'
        },
        'Audio files': {
            'mp3', 'wma', 'ogg', 'wav', 'ra', 'aac', 'mid', 'au', 'aiff',
        },
        'Video files': {
            '264', '3g2', '3gp', 'asf', 'asx', 'avi',
            'bik', 'dat', 'h264', 'mov', 'mp4', 'mpg', 'qt', 'rm', 'swf', 'wmv',
            'm4a', 'm4v', 'flv', 'webm', 'mpeg', 'f4v', 'rmvb', 'vob', 'mkv',
        },
        'Microsoft Office files': {
            'xls', 'xlsx', 'ppt', 'pptx', 'pps', 'doc', 'docx',
            'odt', 'ods', 'odg', 'odp',
        },
        'Documents': {
            'abw', 'pdf', 'djvu ', 'dotm', 'epub', 'mht', 'pages', 'vsd', 'xps'
        },
        'Simple text files': {
            'txt', 'csv', 'xml'
        },
        'Possibly dangerous files': {
            'exe', 'sys', 'com', 'bat', 'bin', 'rss',
            'dmg', 'iso', 'apk', 'ipa', 'chm', 'class',
            'dll', 'drv', 'jar', 'js', 'lnk', 'ocx',
            'pcx', 'scr', 'shs', 'vbs', 'vxd', 'wmf'
        },
    }

    NETWORK_RELATED = {
        'com', 'org', 'net', 'int', 'edu', 'gov', 'mil', 'arpa',
        'cn', 'xyz', 'info', 'icu', 'top', 'cc', 'de', 'app',
        'pub', 'do', 'action', 'sg', 'htm', 'html', 'jhtml', 'shtml', 'asp',
        'aspx', 'jsp', 'jspx', 'php', 'css', 'js'
    }

    def __init__(self, allow_untrusted=False, guess=True, update_online=False, includes=(), excludes=()):
        self.untrusted_categories = ('Possibly dangerous files', )
        self.allow_untrusted = allow_untrusted
        self.guess = guess
        if update_online:
            self._update_online()
        self._excludes = set(excludes)
        self._includes = set(includes)

    @property
    def valid_extensions(self):
        common_extensions = self.COMMON_EXTENSIONS.copy()
        if not self.allow_untrusted:
            [common_extensions.pop(key) for key in self.untrusted_categories]
        valids = set()
        for each in common_extensions.values():
            valids = valids | each
        valids = valids | self._includes
        for ex in list(self._excludes):
            valids.discard(ex)
        return valids

    @property
    def categories(self):
        return list(self.COMMON_EXTENSIONS.keys())

    def _update_online(self):
        pass

    def get_extensions(self, category=None):
        return self.COMMON_EXTENSIONS.get(category, set())

    def extract(self, source, source_type='text'):
        # source_type: text | url
        source = source.strip().lower()
        dots_num = source.count('.')
        splits = []
        # whether dot in extension
        for num in range(dots_num + 1):
            splits.append(source.split('.', maxsplit=num)[-1])
        for each in splits:
            if each in self.valid_extensions:
                return each
        if not self.guess or not splits:
            return ""
        # don't consider extensions like 'tar.gz'
        ext = ""
        suffix = source.split(".")[-1]
        if suffix in self._excludes:
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


class FileExtractor(LinkExtractor):
    """Extract file links from response.

    :param deny_extensions: (tuple) a list of strings containing extensions that
                            should be ignored when extracting links
    :param ignore_untrusted: (bool) ignore UNTRUSTED_EXTENSIONS if set to True
    """
    def __init__(self, allow_untrusted=False, guess=True, update_online=False,
                 includes=(), excludes=(), hints=('附件', '下载', '文件', 'download', 'file'),
                 unique=True, **kwargs):
        super().__init__(unique=unique, **kwargs)

        self.ext_extractor = FileExtExtractor(allow_untrusted, guess, update_online, includes, excludes)
        self.hints = hints

    def _extension_allowed(self, link):
        title, url = link['title'], link['url']
        ext_from_title = self.ext_extractor.extract(title, 'text')
        ext_from_url = self.ext_extractor.extract(url, 'url')
        if not ext_from_url and not ext_from_title:
            if title in self.hints and not url.endswith(tuple(self.ext_extractor.NETWORK_RELATED)):
                return True
            return False
        link['ext'] = ext_from_title or ext_from_url
        return True

    def _process_links(self, links):
        """ Normalize and filter extracted links

        The subclass should override it if necessary
        """
        links = [x for x in links if self._link_allowed(x) and self._extension_allowed(x)]
        return self._deduplicate_if_needed(links)
