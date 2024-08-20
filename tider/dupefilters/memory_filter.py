import os
import logging

from tider.dupefilters import BaseDupeFilter


class MemoryDupeFilter(BaseDupeFilter):
    def __init__(self, path=None):
        self.file = None
        self.fingerprints = set()
        self.logger = logging.getLogger(__name__)
        if path:
            self.file = open(os.path.join(path, 'requests.seen'), 'a+')
            self.file.seek(0)
            self.fingerprints.update(x.rstrip() for x in self.file)

    @classmethod
    def from_settings(cls, settings):
        path = settings["REQUEST_FILTER_JOB_DIR"]
        if path and not os.path.exists(path):
            os.makedirs(path)
        return cls(path=path)

    def request_seen(self, request) -> bool:
        fp = request.fingerprint
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)
        if self.file:
            self.file.write(fp + '\n')
        return False

    def close(self, reason):
        if self.file:
            self.file.close()

    def log(self, request):
        msg = "Filtered duplicate request: %(request)s"
        args = {'request': request}
        self.logger.debug(msg, args)
