import warnings

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None
try:
    from DrissionPage import Chromium, ChromiumOptions
except ImportError:
    Chromium = ChromiumOptions = None

from requests.structures import CaseInsensitiveDict

from tider import Request, Response
from tider.utils.time import preferred_clock
from tider.utils.log import get_logger
from tider.utils.network import extract_cookies_to_jar, guess_encoding_from_headers
from tider.exceptions import ImproperlyConfigured

logger = get_logger(__name__)


class BrowserResponse:

    def __init__(self, content: str, encoding: str = 'utf-8', http_version: str = 'HTTP/?',
                 status_code=None, headers=None, cookies=None):
        self._content = content
        self._encoding = encoding
        self.http_version = http_version
        self.status_code = status_code
        self.headers = headers or {}
        self.cookies = cookies or {}

    def stream(self, chunk_size=None, **_):
        """
        iterate streaming content chunk by chunk in bytes.
        """
        if chunk_size:
            warnings.warn("chunk_size is ignored, there is no way to tell browser that.")
        yield self._content.encode(encoding=self._encoding)


class BrowserDownloader:

    lazy = True

    def close_expired_connections(self):
        pass

    def close(self):
        pass

    def build_response(self, request, resp):
        response = Response(request)

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = getattr(resp, "status_code", None)

        # Make headers case-insensitive.
        response.headers = CaseInsensitiveDict(getattr(resp, "headers", {}))

        # Set encoding.
        response.encoding = guess_encoding_from_headers(response.headers)
        if isinstance(resp.http_version, int) and resp.http_version == 11:
            response.version = "HTTP/1.1"
        elif isinstance(resp.http_version, int) and resp.http_version == 2:
            response.version = "HTTP/2"
        elif resp.http_version:
            response.version = resp.http_version
        else:
            response.version = "HTTP/?"
        response.raw = resp
        response.reason = response.raw.reason
        response.url = request.url

        # Add new cookies from the server.
        extract_cookies_to_jar(response.cookies, request.prepared, resp)

        return response


class PlaywrightDownloader(BrowserDownloader):

    def download_request(self, request: Request, session_cookies=None, **_):
        if not sync_playwright:
            raise ImproperlyConfigured(
                'You need to install the playwright library to use PlaywrightDownloader.'
            )

        browser_type = request.meta.get('browser_type', 'chromium')
        if browser_type not in ('chromium', 'firefox', 'webkit'):
            raise ImproperlyConfigured("Unsupported browser type: %s" % browser_type)
        # Channel can be "chrome", "msedge", "chrome-beta", "msedge-beta" or "msedge-dev".
        browser_channel = request.meta.get('browser_channel', 'chromium')  # default to headless
        if browser_channel not in ('chromium', 'chrome', 'msedge', 'chrome-beta', 'msedge-beta', 'msedge-dev'):
            raise ImproperlyConfigured("Unsupported browser channel: %s" % browser_channel)
        screenshot_path = request.meta.get('browser_screenshot_path')
        init_script = request.meta.get('browser_init_script')
        init_script_path = request.meta.get('browser_init_script_path')
        device = request.meta.get('browser_device')

        http_version = "HTTP/?"
        response_headers = {}
        status_code = None

        def _get_http_version(browser_request):
            nonlocal http_version
            http_version = browser_request.http_version()

        def _get_response_info(browser_response):
            nonlocal response_headers, status_code
            status_code = browser_response.status
            response_headers.update(browser_response.headers)

        try:
            with sync_playwright() as p:
                request.proxy.connect()
                proxy = request.selected_proxy
                browser = getattr(p, browser_type).launch(channel=browser_channel,
                                                          headless=True,
                                                          chromium_sandbox=False,
                                                          proxy={'server': proxy})
                context_config = {}
                if device:
                    context_config.update(p.devices[device])
                context = browser.new_context(**context_config)
                page = context.new_page(ignore_https_errors=True)
                if init_script or init_script_path:
                    page.add_initial_script(script=init_script, path=init_script_path)
                page.set_extra_http_headers(request.headers)
                page.on("request", _get_http_version)
                page.on("response", _get_response_info)
                # Start time (approximately) of the request
                start = preferred_clock()

                page.goto(request.url, timeout=request.timeout)
                if screenshot_path:
                    page.screenshot(path=screenshot_path)
                html = page.content()
                cookies = {}
                context_cookies = context.cookies()
                for cookie in context_cookies:
                    cookies[cookie['name']] = cookie['value']
                resp = BrowserResponse(html, http_version=http_version, headers=response_headers,
                                       status_code=status_code, cookies=cookies)

                # Total elapsed time of the request (approximately)
                elapsed = preferred_clock() - start
                page.close()
                context.close()
                browser.close()

                response = self.build_response(request, resp)
                response.elapsed = elapsed
                extract_cookies_to_jar(session_cookies, request.prepared, resp)
                return response
        except Exception as e:
            response = Response(request)
            if not response.failed:  # maybe already failed in response.read().
                response.fail(error=e)
            return response
        finally:
            request.proxy.disconnect()


class DrissionPageDownloader(BrowserDownloader):

    def download_request(self, request: Request, session_cookies=None, **_):
        if not Chromium:
            raise ImproperlyConfigured(
                'You need to install the DrissionPage library to use DrissionPageDownloader.'
            )

        browser_ini_path = request.meta.get('browser_ini_path')
        browser_channel = request.meta.get('browser_channel', 'chromium')  # default to headless
        if browser_channel not in ('chromium', 'chrome', 'msedge', 'chrome-beta', 'msedge-beta', 'msedge-dev'):
            raise ImproperlyConfigured("Unsupported browser channel: %s" % browser_channel)
        screenshot_path = request.meta.get('browser_screenshot_path')

        http_version = "HTTP/?"
        response_headers = {}
        status_code = None

        request.proxy.connect()
        browser = tab = None
        try:
            co = ChromiumOptions(ini_path=browser_ini_path)
            co.no_imgs(True).mute(True)
            co.incognito().headless().auto_port()
            timeout = request.timeout
            co.set_timeouts(base=timeout, page_load=timeout, script=timeout).set_retry(times=0)
            proxy = request.selected_proxy
            co.set_argument('--no-sandbox').set_proxy(proxy)
            ua = request.headers.get('User-Agent')
            if ua:
                co.set_user_agent(user_agent=ua)
            browser = Chromium(addr_or_opts=co, session_options=False)
            tab = browser.latest_tab
            cookies = request.headers.get('Cookie') or request.cookies
            if cookies:
                tab.set.cookies(cookies=cookies)

            # Start time (approximately) of the request
            start = preferred_clock()
            tab.get(url=request.url, show_errmsg=True, timeout=timeout)
            if screenshot_path:
                tab.get_screenshot(path=screenshot_path, full_page=True)
            html = tab.html
            cookies = tab.cookies().as_dict()
            resp = BrowserResponse(html, http_version=http_version, headers=response_headers,
                                   status_code=status_code, cookies=cookies)
            # Total elapsed time of the request (approximately)
            elapsed = preferred_clock() - start

            response = self.build_response(request, resp)
            response.elapsed = elapsed
            extract_cookies_to_jar(session_cookies, request.prepared, resp)
            return response
        except Exception as e:
            response = Response(request)
            if not response.failed:  # maybe already failed in response.read().
                response.fail(error=e)
            return response
        finally:
            request.proxy.disconnect()
            tab and tab.close()
            browser and browser.quit()
