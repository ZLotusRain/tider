import os
from io import BytesIO
from collections import defaultdict

from tider.utils.misc import md5sum


class FSFilesStore:
    def __init__(self, basedir):
        if '://' in basedir:
            basedir = basedir.split('://', 1)[1]
        self.basedir = basedir
        self._mkdir(self.basedir)
        self.created_directories = defaultdict(set)

    @classmethod
    def from_settings(cls, settings):
        basedir = settings["FS_BASEDIR"]
        return cls(basedir)

    def persist_file(self, path, buf, **kwargs):
        absolute_path = self._get_filesystem_path(path)
        self._mkdir(os.path.dirname(absolute_path), kwargs.get("info"))
        with open(absolute_path, 'wb+') as f:
            if hasattr(buf, "iter_content"):
                for chunk in buf.iter_content(chunk_size=100*1024):
                    f.write(chunk)
            elif hasattr(buf, "content"):
                f.write(buf.content)
            elif isinstance(buf, BytesIO):
                f.write(buf.getvalue())
            else:
                f.write(buf)
        return absolute_path

    def stat_file(self, path, **_):
        absolute_path = self._get_filesystem_path(path)
        try:
            last_modified = os.path.getmtime(absolute_path)
        except os.error:
            return {}

        with open(absolute_path, 'rb') as f:
            checksum = md5sum(f)

        return {'last_modified': last_modified, 'checksum': checksum}

    def _get_filesystem_path(self, path):
        path_comps = path.split('/')
        return os.path.join(self.basedir, *path_comps)

    def _mkdir(self, dirname, domain=None):
        seen = self.created_directories[domain] if domain else set()
        if dirname not in seen:
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            seen.add(dirname)
