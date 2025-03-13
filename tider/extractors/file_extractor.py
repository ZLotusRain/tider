import re

from tider.extractors.link_extractor import LinkExtractor
from tider.utils.log import get_logger

logger = get_logger(__name__)


class FileExtExtractor:
    # Extension refs:
    # https://www.file-extensions.org
    # https://support.microsoft.com/en-us/windows/common-file-name-extensions-in-windows-da4a4430-8e76-89c5-59f7-1cdbbc75cb01

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
        'Office suites': {
            'xls', 'xlsm', 'xlsx', 'xltm', 'xltx', 'potm', 'potx', 'ppt', 'pptm', 'pptx', 'pps',
            'doc', 'docb', 'docm', 'docx', 'dotm', 'dotx', 'odt', 'ods', 'odg', 'odp', 'xps'
        },
        'Documents': {
            'abw', 'pdf', 'djvu ', 'epub', 'mht', 'pages', 'vsd',
        },
        'Simple text files': {
            'txt', 'csv', 'xml', 'rb'
        },
        'Possibly dangerous files': {
            'exe', 'sys', 'com', 'bat', 'bin', 'rss',
            'dmg', 'iso', 'apk', 'ipa', 'chm', 'class',
            'dll', 'drv', 'jar', 'js', 'lnk', 'ocx', 'msi',
            'pcx', 'scr', 'sh', 'shs', 'vbs', 'vxd', 'wmf', 'py'
        },
    }

    NETWORK_RELATED = {
        'com', 'org', 'net', 'int', 'edu', 'gov', 'mil', 'arpa',
        'cn', 'xyz', 'info', 'icu', 'top', 'cc', 'de', 'app',
        'pub', 'do', 'action', 'sg', 'htm', 'html', 'jhtml', 'shtml', 'asp',
        'aspx', 'jsp', 'jspx', 'php', 'css', 'js',
    }

    def __init__(self, ignored_types=('Possibly dangerous files', ), guess=True, includes=(), excludes=()):
        self.ignored_types = ignored_types
        self.guess = guess
        self._excludes = set(excludes)
        self._includes = set(includes)

    @property
    def valid_extensions(self):
        valid_extensions = set()
        for t, extensions in self.COMMON_EXTENSIONS.items():
            if t in self.ignored_types:
                continue
            valid_extensions = valid_extensions | extensions
        valid_extensions = valid_extensions | self._includes
        for ex in list(self._excludes):
            valid_extensions.discard(ex)
        return valid_extensions

    def extract(self, source, source_type='text'):
        # source_type: text | url
        source = source.strip().lower()
        dots_num = source.count('.')
        splits = [source]  # maybe valid extension itself.
        # whether dot in extension
        for num in range(dots_num + 1):
            splits.append(source.split('.', maxsplit=num)[-1])
        # consider extensions like 'tar.gz'
        for each in splits:
            if each in self.valid_extensions:
                return each
        if not self.guess:
            return ""

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
    """Extract file links from response."""

    DEFAULT_FILE_HINTS = ('附件', '下载', '文件', 'download', 'file', '附件下载')

    def __init__(self, ignored_types=('Possibly dangerous files', ), guess=True,
                 includes=(), excludes=(), hints=None, unique=True, **kwargs):
        super().__init__(unique=unique, **kwargs)
        self.ext_extractor = FileExtExtractor(ignored_types, guess, includes, excludes)
        self.hints = hints or self.DEFAULT_FILE_HINTS

    def _extension_allowed(self, link):
        title, url = link['title'], link['url']
        link['ext'] = self.ext_extractor.extract(title, source_type='text')
        if not link['ext']:
            link['ext'] = self.ext_extractor.extract(url, source_type='url')
        if not link['ext']:
            if title.strip() in self.hints and not url.endswith(tuple(self.ext_extractor.NETWORK_RELATED)):
                # some file links don't end with extensions.
                return True
            return False
        return True

    def _process_links(self, links):
        """ Normalize and filter extracted links

        The subclass should override it if necessary
        """
        links = [x for x in links if self._link_allowed(x) and self._extension_allowed(x)]
        return self._deduplicate_if_needed(links)
