import os

from tider.utils.log import get_logger
from tider.dupefilters import BaseDupeFilter


class MemoryDupeFilter(BaseDupeFilter):

    _first_log = True

    def __init__(self, path=None, debug=False):
        self.file = None
        self.fingerprints = set()
        self.logger = get_logger(__name__)
        if path:
            self.file = open(os.path.join(path, 'requests.seen'), 'a+')
            self.file.seek(0)
            self.fingerprints.update(x.rstrip() for x in self.file)
        self.debug = debug

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool("DUPEFILTER_DEBUG")
        path = settings["DUPEFILTER_JOB_DIR"]
        if path and not os.path.exists(path):
            os.makedirs(path)
        return cls(path=path, debug=debug)

    def request_seen(self, request) -> bool:
        fp = request.fingerprint()
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)
        if self.file:
            self.file.write(fp + '\n')
        return False

    def close(self, reason):
        if self.file:
            self.file.close()

    def log(self, request, spider):
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            args = {"request": request}
            self.logger.debug(msg, args, extra={"spider": spider})
        elif self._first_log:
            msg = (
                "Filtered duplicate request: %(request)s"
                " - no more duplicates will be shown"
                " (see DUPFILTER_DEBUG to show all duplicates)"
            )
            self.logger.debug(msg, {"request": request}, extra={"spider": spider})
            self._first_log = False

        spider.crawler.stats.inc_value("dupefilter/filtered")
