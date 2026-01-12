from tider.network.request import Request


class ApiFactory:

    API = ""
    DEFAULT_RESULT_FIELD = "api_result"

    def __call__(self, *args, **kwargs):
        return self.factory(*args, **kwargs)

    def factory(self, api_kwargs, callback, cb_kwargs=None, errback=None, **kwargs):
        """build Request according to api request kwargs"""
        request_params = self.prepare_request_kwargs(**api_kwargs)
        request_params.update(**dict(kwargs))
        meta = request_params.setdefault('meta', {})
        meta.setdefault('save_unhandled_failure', False)
        return Request(url=self.API, callback=callback, errback=errback,
                       cb_kwargs=cb_kwargs, **request_params)

    def prepare_request_kwargs(self, **kwargs):
        raise NotImplementedError

    def parse(self, response):
        raise NotImplementedError
