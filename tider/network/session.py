import os
import threading
from pathlib import Path
from typing import Mapping, Optional

from requests.utils import (
    get_environ_proxies,
    get_netrc_auth,
    to_key_val_list
)
from requests.cookies import (
    merge_cookies,
    cookiejar_from_dict,
    RequestsCookieJar
)
from requests.structures import CaseInsensitiveDict

from tider import Request, Response
from tider.network.proxy import Proxy
from tider.network.user_agent import default_user_agent
from tider.utils.log import get_logger
from tider.utils.network import cookiejar_from_str, extract_cookies_to_jar
from tider.utils.misc import symbol_by_name, build_from_crawler
from tider.exceptions import ProxyError

logger = get_logger(__name__)


def default_headers():
    """
    :rtype: requests.structures.CaseInsensitiveDict
    """
    return CaseInsensitiveDict({
        'User-Agent': default_user_agent(),
        'Accept-Encoding': ', '.join(('gzip', 'deflate')),
        'Accept': '*/*',
        'Connection': 'keep-alive',
    })


def merge_setting(request_setting, session_setting, dict_class=CaseInsensitiveDict):
    """Determines appropriate setting for a given request, taking into account
    the explicit setting on that request, and the setting in the session. If a
    setting is a dictionary, they will be merged together using `dict_class`
    """

    if session_setting is None:
        return request_setting

    if request_setting is None:
        return session_setting

    # Bypass if not a dictionary (e.g. verify)
    if not (
        isinstance(session_setting, Mapping) and isinstance(request_setting, Mapping)
    ):
        return request_setting

    merged_setting = dict_class(to_key_val_list(session_setting))
    merged_setting.update(to_key_val_list(request_setting))

    # Remove keys that are set to None. Extract keys first to avoid altering
    # the dictionary during iteration.
    none_keys = [k for (k, v) in merged_setting.items() if v is None]
    for key in none_keys:
        del merged_setting[key]

    return merged_setting


def get_ca_bundle_from_env() -> Optional[str]:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        return os.environ.get("REQUESTS_CA_BUNDLE")
    if "CURL_CA_BUNDLE" in os.environ:
        return os.environ.get("CURL_CA_BUNDLE")
    if "SSL_CERT_FILE" in os.environ:
        ssl_file = Path(os.environ["SSL_CERT_FILE"])
        if ssl_file.is_file():
            return str(ssl_file)
    if "SSL_CERT_DIR" in os.environ:
        ssl_path = Path(os.environ["SSL_CERT_DIR"])
        if ssl_path.is_dir():
            return str(ssl_path)
    return None


class Session:

    DEFAULT_REDIRECT_LIMIT = 30
    DEFAULT_DOWNLOADERS = {
        'default': 'tider.network.downloaders:HTTPDownloader',
        'wget': 'tider.network.downloaders:WgetDownloader',
        'impersonate': 'tider.network.downloaders:ImpersonateDownloader',
        'playwright': 'tider.network.downloaders:PlaywrightDownloader',
    }

    __attrs__ = [
        "headers",
        "cookies",
        "auth",
        "proxies",
        "verify",
        "cert",
        "_downloaders",
        "stream",
        "trust_env",
        "max_redirects",
    ]

    def __init__(self, crawler=None, headers=None, auth=None, proxies=None, stream=False, cookies=None,
                 verify=False, cert=None, trust_env=True, redirect_limit=None, downloaders=None):
        self._crawler = crawler
        self.headers = default_headers()
        if headers is not None:
            self.headers.update(headers)
        self.auth = auth
        self.proxies = dict(proxies) if proxies else {}
        self.stream = stream
        self.verify = verify
        self.cert = cert
        self.cookies = cookiejar_from_dict(cookies)

        #: Trust environment settings for proxy configuration, default
        #: authentication and similar.
        self.trust_env = trust_env

        self.max_redirects = redirect_limit or self.DEFAULT_REDIRECT_LIMIT
        self._load_lock = threading.Lock()

        self._downloaders = {}
        self._detected_downloaders = self.DEFAULT_DOWNLOADERS.copy()
        self._detected_downloaders.update(dict(downloaders) if downloaders else {})
        for downloader in self._detected_downloaders:
            self._load_downloader(downloader, skip_lazy=True)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            headers=crawler.settings.get('EXPLORER_SESSION_HEADERS'),
            auth=crawler.settings.get('EXPLORER_SESSION_AUTH'),
            proxies=crawler.settings.get('EXPLORER_SESSION_PROXIES'),
            stream=crawler.settings.getbool('EXPLORER_SESSION_STREAM'),
            cookies=crawler.settings.get('EXPLORER_SESSION_COOKIES'),
            verify=crawler.settings.get('EXPLORER_SESSION_VERIFY'),
            cert=crawler.settings.get('EXPLORER_SESSION_CERT'),
            trust_env=crawler.settings.get('EXPLORER_SESSION_TRUST_ENV'),
            redirect_limit=crawler.settings.get("EXPLORER_SESSION_REDIRECT_LIMIT"),
            downloaders=crawler.settings.getdict("EXPLORER_DOWNLOADERS"),
        )

    def _get_downloader(self, downloader: str):
        """
        Lazy-load the downloader only on the first request for that downloader.
        """
        if downloader in self._downloaders:
            return self._downloaders[downloader]
        if downloader not in self._detected_downloaders:
            return None

        return self._load_downloader(downloader)

    def _load_downloader(self, downloader, skip_lazy=False):
        with self._load_lock:
            # avoid loading the same downloader multi times.
            if downloader in self._downloaders:
                return self._downloaders[downloader]
            path = self._detected_downloaders[downloader]
            # noinspection PyBroadException
            try:
                dhcls = symbol_by_name(path)
                if skip_lazy and getattr(dhcls, "lazy", True):
                    return None
                dh = build_from_crawler(
                    dhcls,
                    self._crawler,
                )
            except Exception:
                logger.error(
                    'Loading "%(clspath)s" for downloader "%(downloader)s"',
                    {"clspath": path, "downloader": downloader},
                    exc_info=True,
                )
                return None
            else:
                self._downloaders[downloader] = dh
                return dh

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def merge_environment_settings(self, request: Request):
        """
        Check the environment and merge it with some settings.

        :rtype: dict
        """
        # Gather clues from the surrounding environment.
        if self.trust_env:
            if request.proxy_schema not in ('no_proxy', 'dummy', 0):
                new_proxies = dict(request.proxies or {})
                # Set environment's proxies.
                # use no_proxy to avoid using proxy for specific domain.
                no_proxy = new_proxies.get("no_proxy")
                env_proxies = get_environ_proxies(request.url, no_proxy=no_proxy)
                for (k, v) in env_proxies.items():
                    new_proxies.setdefault(k, v)
                new_proxies = merge_setting(new_proxies, self.proxies)

                new_proxy = Proxy(new_proxies)
                if new_proxy.select_proxy(request.url) != request.selected_proxy:
                    # replace with environment proxies.
                    # if the proxy has already been discarded, a ProxyError will be raised.
                    request.update_proxy(new_proxy)

            # Look for requests environment configuration
            # and be compatible with cURL.
            if request.verify is True or request.verify is None:
                request.verify = (
                    get_ca_bundle_from_env()
                    or request.verify
                )

        # Merge all the kwargs.
        request.stream = merge_setting(request.stream, self.stream)
        request.verify = merge_setting(request.verify, self.verify)
        request.cert = merge_setting(request.cert, self.cert)

    def download_request(self, request: Request):
        downloader = request.downloader
        if request.impersonate and not downloader:
            downloader = 'impersonate'
        downloader = self._get_downloader(downloader or 'default')
        if not downloader:
            raise RuntimeError(f"Can't load downloader `{downloader}`")

        request.prepared.prepare_headers(merge_setting(request.headers, self.headers))

        h_cookie = request.headers.pop("Cookie", None)
        cookies = request.cookies or cookiejar_from_str(h_cookie)
        # Merge with session cookies
        merged_cookies = merge_cookies(
            merge_cookies(RequestsCookieJar(), self.cookies), cookies
        )
        request.prepared.prepare_cookies(merged_cookies)

        auth = request.prepared.auth
        if self.trust_env and not auth and not self.auth:
            auth = get_netrc_auth(request.url)
            auth = merge_setting(auth, self.auth)
            request.prepared.auth = auth
            request.prepared.prepare_auth(auth)

        try:
            # raise ProxyError if proxy is invalid
            self.merge_environment_settings(request)
            request.proxy.connect()
        except ProxyError as e:
            response = Response(request)
            response.fail(e)
        else:
            response = downloader.download_request(request, trust_env=self.trust_env)
            if request.method.upper() == 'HEAD':
                # avoid using stream and HEAD together
                response.read()
            request.proxy.disconnect()
        if response.raw:
            extract_cookies_to_jar(request.session_cookies, request.prepared, response.raw)
        return response

    def close(self):
        for downloader in self._downloaders.values():
            downloader.close()

    def close_expired_connections(self):
        # avoid dict changed.
        for downloader in dict(self._downloaders).values():
            downloader.close_expired_connections()
