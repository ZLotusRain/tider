import threading

from tider import Tider
from tider.spiders import Spider
from tider.crawler.explorer import Explorer
from tider.settings import Settings
from tider.utils.decorators import inthread


class RequestsSpider(Spider):

    name = 'requests'


class Session:

    def __init__(self, config=None, clear_idles=False):
        self._config_source = config
        self.settings = self._load_config()
        self.app = Tider(settings=self.settings)
        self._crawler = self.app.create_crawler(RequestsSpider)
        self.explorer = Explorer(crawler=self._crawler)
        self._stopped = threading.Event()
        self._clear_thread = None
        if clear_idles:
            self._clear_thread = self._clear_idle_conns()
            self._clear_thread.start()

    def _load_config(self):
        if isinstance(self._config_source, dict) or self._config_source is None:
            settings = Settings(dict(self._config_source or {}))
        elif isinstance(self._config_source, str):
            settings = Settings()
            settings.setmodule(module=self._config_source)
        else:
            settings = self._config_source.copy()
        if not isinstance(settings, Settings):
            raise ValueError(f"settings must be a `Settings|dict|str` instance or None, "
                             f"got {type(settings)}: {settings!r}")
        return settings

    @inthread(name="ClearThread")
    def _clear_idle_conns(self):
        while not self._stopped.is_set():
            self.explorer.clear_idle_conns()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._stopped.set()
        if self._clear_thread is not None:
            self._clear_thread.join()
        self.explorer.close(reason='finished')
        self._crawler.close()
        self.app.close()

    def request(self, method, url, params=None, data=None, headers=None, cookies=None,
                files=None, auth=None, timeout=None, allow_redirects=True, proxies=None,
                stream=None, verify=None, cert=None, json=None, raise_for_status=False,
                proxy_schema=0, proxy_params=None, delay=0, max_retries=0, meta=None,
                encoding=None, ignored_status_codes=(400, 412, 521), http2=False, downloader=None,
                impersonate=None, session_cookies=None):
        return self.explorer.try_explore(
            method, url,
            params=params, data=data, headers=headers, cookies=cookies,
            files=files, auth=auth, timeout=timeout, allow_redirects=allow_redirects,
            proxies=proxies, proxy_schema=proxy_schema, proxy_params=proxy_params,
            stream=stream, verify=verify, cert=cert, json=json,
            raise_for_status=raise_for_status, delay=delay, max_retries=max_retries,
            meta=meta, encoding=encoding, ignored_status_codes=ignored_status_codes,
            http2=http2, downloader=downloader, impersonate=impersonate, session_cookies=session_cookies
        )


def request(method, url, proxy_schema=0, proxy_params=None, delay=0, max_retries=0, meta=None,
            encoding=None, ignored_status_codes=(400, 412, 521), http2=False, downloader=None,
            impersonate=None, session_cookies=None, **kwargs):
    with Session() as session:
        return session.request(
            method, url, proxy_schema=proxy_schema, proxy_params=proxy_params,
            delay=delay, max_retries=max_retries, meta=meta, encoding=encoding,
            ignored_status_codes=ignored_status_codes, http2=http2,
            downloader=downloader, impersonate=impersonate, session_cookies=session_cookies, **kwargs
        )


def get(url, params=None, **kwargs):
    """Sends a GET request."""
    return request("get", url, params=params, **kwargs)


def options(url, **kwargs):
    """Sends an OPTIONS request."""
    return request("options", url, **kwargs)


def head(url, **kwargs):
    """Sends a HEAD request."""
    kwargs.setdefault("allow_redirects", False)
    return request("head", url, **kwargs)


def post(url, data=None, json=None, **kwargs):
    """Sends a POST request."""
    return request("post", url, data=data, json=json, **kwargs)


def put(url, data=None, **kwargs):
    """Sends a PUT request."""
    return request("put", url, data=data, **kwargs)


def patch(url, data=None, **kwargs):
    """Sends a PATCH request."""
    return request("patch", url, data=data, **kwargs)


def delete(url, **kwargs):
    """Sends a DELETE request."""
    return request("delete", url, **kwargs)
