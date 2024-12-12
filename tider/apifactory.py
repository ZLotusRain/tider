import logging

from tider.network.request import Request


logger = logging.getLogger(__name__)


class ApiFactory:

    API = ""
    DEFAULT_RESULT_FIELD = "api_result"

    def __call__(self, *args, **kwargs):
        return self.factory(*args, **kwargs)

    def factory(self, api_kwargs, callback, cb_kwargs=None, errback=None, **kwargs):
        """build Request according to api request kwargs"""
        kw = self.prepare_request_kwargs(**api_kwargs)
        kw.update(**dict(kwargs))
        return Request(url=self.API, callback=callback, errback=errback,
                       cb_kwargs=cb_kwargs, **kw)

    def prepare_request_kwargs(self, **kwargs):
        raise NotImplementedError

    def parse(self, response):
        raise NotImplementedError
