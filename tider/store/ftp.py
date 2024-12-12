import hashlib
from io import BytesIO
from ftplib import FTP
from urllib.parse import urlparse

from tider.network import Response
from tider.utils.ftp import ftp_store_file


class FTPFilesStore:

    FTP_USERNAME = None
    FTP_PASSWORD = None
    USE_ACTIVE_MODE = None

    def __init__(self, uri):
        if not uri.startswith("ftp://"):
            raise ValueError(f"Incorrect URI scheme in {uri}, expected 'ftp'")
        u = urlparse(uri)
        self.port = u.port
        self.host = str(u.hostname)
        self.port = int(u.port or 21)
        self.username = u.username or self.FTP_USERNAME
        self.password = u.password or self.FTP_PASSWORD
        self.basedir = u.path.rstrip('/')

    @classmethod
    def from_settings(cls, settings):
        uri = settings['FTP_URI']
        return cls(uri)

    def persist_file(self, path, buf, **_):
        path = f'{self.basedir}/{path}'
        if isinstance(buf, Response):
            buf = buf.content
        elif isinstance(buf, bytes):
            buf = BytesIO(buf)
        return ftp_store_file(
            path=path, file=buf,
            host=self.host, port=self.port, username=self.username,
            password=self.password, use_active_mode=self.USE_ACTIVE_MODE
        )

    def stat_file(self, path, **_):
        def _stat_file(p):
            # noinspection PyBroadException
            try:
                ftp = FTP()
                ftp.connect(self.host, self.port)
                ftp.login(self.username, self.password)
                if self.USE_ACTIVE_MODE:
                    ftp.set_pasv(False)
                file_path = f"{self.basedir}/{p}"
                last_modified = float(ftp.voidcmd(f"MDTM {file_path}")[4:].strip())
                m = hashlib.md5()
                ftp.retrbinary(f'RETR {file_path}', m.update)
                return {'last_modified': last_modified, 'checksum': m.hexdigest()}
            # The file doesn't exist
            except Exception:
                return {}
        return _stat_file(path)
