import pathlib
import os.path
import warnings
import functools
from threading import RLock
from collections import defaultdict

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    sync_playwright = PlaywrightTimeoutError = None
try:
    from DrissionPage import Chromium, ChromiumOptions
except ImportError:
    Chromium = ChromiumOptions = None

from tider import Request, Response
from tider.utils.time import preferred_clock
from tider.utils.url import parse_url_host
from tider.utils.crypto import set_md5
from tider.utils.log import get_logger
from tider.exceptions import ImproperlyConfigured

__all__ = ('PlaywrightDownloader', 'DrissionPageDownloader')

logger = get_logger(__name__)

# https://ygp.gdzwfw.gov.cn/ggzy-portal/#/440300/new/jygg/v3/A?noticeId=4403003C522b6560fdfae04cc0bd72fca3fa55fc9f&projectCode=E4403000004a00377005&bizCode=3C52
INIT_SCRIPT = """
    window.elementsCreatedByVue = [];
    window.elementsCreatedByDOM = [];
    window._hooked = []
    
    document._createElement = document.createElement;
    document.createElement = function (name) {
        ele = document._createElement(name);
        window.elementsCreatedByDOM.push(ele);
        return ele;
    }
    
    function delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    };
    
    waitFiveSeconds = async function() {
        console.log("Starting to wait for 5 seconds...");
        await delay(5000);
        console.log("5 seconds have passed!");
        return "Completed";
    };
    
    function getResourceStats() {
         return window.performance.getEntries("resource") || [];
    }
    
    let lastResources = getResourceStats();
    
    window.checkResourcesDone = function () {
        let resources = getResourceStats();
        if (resources.length != lastResources.length) {
            lastResources = getResourceStats();
            return false;
        }
        else if (resources[resources.length - 1].name != lastResources[lastResources.length - 1].name) {
            lastResources = getResourceStats();
            return false;
        }
        else if (resources[resources.length - 1].duration != lastResources[lastResources.length - 1].duration) {
            lastResources = getResourceStats();
            return false;
        }
        else {
            return true;
        }
    }
    
    createHidden = function(name) {
        if (window._hooked.includes(name)) { return; }
        window._hooked.push(name); 
        let newInput = document.createElement('input');
        newInput.type = 'hidden'
        if (name != "") {
            newInput.id = `foo-tider-${name}`;
        } else { newInput.id = 'foo-tider'; }
        
        newInput.value = 'DOM_LOAD_HOOK'
        let lastElement = (document?.body || document?.head || document).lastElementChild;
        lastElement.insertAdjacentElement('afterend', newInput);
    }
    
    Element.prototype._attachShadow = Element.prototype.attachShadow;
    Element.prototype.attachShadow = function() {
        const shadowDiv = document.createElement('div');
        this.appendChild(shadowDiv);
        return shadowDiv;
    };
"""

# https://sinopharmzc.com/#/trade-info-detail?id=1320886088940199937&noticeType=1
STYLE_PATCH = """
    // document.body.scrollHeight = documentElement.scrollHeight = scrollingElement.scrollHeight
    () => {
        const styleSheets = document.styleSheets;
        for (let i = 0; i < styleSheets.length; i++) {
            try {
                const rules = styleSheets[i].cssRules;
                for (let j = 0; j < rules.length; j++) {
                    if (rules[j].selectorText === 'body') {
                        rules[j].style.height = '';
                        rules[j].style.width = '';
                    }
                }
            } catch (e) {
                // Some style sheets may not be accessible due to cross-domain or other reasons
            }
        }
    };
    
    // window.scrollTo(0, document.body.scrollHeight);
    document.body.style.removeProperty('height');
    document.body.style.removeProperty('width');
    
    document.documentElement.style.height = 'auto';
    document.documentElement.style.width = 'auto';
    document.body.style.height = 'auto';
    document.body.style.width = 'auto';
    document.body.style.overflow = 'visible';
"""

VUE_DOM_LOAD_HOOK = """
(this || window).waitFiveSeconds().then(result => {{
    console.log(result);
}});

// const executeOnce = (function() {{
//     let executed = false;
//     return function() {{
//         if (executed) {{ return;}}
//         setTimeout(this.createHidden("{name}"), 1000);
//         executed = true;
//     }};
// }})();
// executeOnce();
setTimeout((this || window).createHidden("{name}"), 1000);
"""

LAST_SCRIPT = """setTimeout(this.createHidden(""), 1000);"""

DOM_LOAD_HOOK = f"""
    const newParagraph = document.createElement('script');
    newParagraph.textContent = '{LAST_SCRIPT}';
    const lastElement = (document?.body || document?.head || document).lastElementChild;
    lastElement.insertAdjacentElement('afterend', newParagraph);
"""


class BrowserResponse:

    def __init__(self, content: str, encoding: str = 'utf-8', http_version: str = 'HTTP/1.1',
                 status_code=None, headers=None, cookies=None, reason=None):
        self._content = content
        self._encoding = encoding
        self.reason = reason
        self.status_code = status_code
        self.http_version = http_version
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

    CACHE_DIR = '/data/tider/BrowserDownloader'

    def __init__(self, cache_statics=True, cache_dir=None):
        self._cache_dir = pathlib.Path(cache_dir or self.CACHE_DIR)
        if cache_statics:
            if not os.path.exists(self._cache_dir):
                os.makedirs(self._cache_dir)
        self._cache_statics = cache_statics

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            cache_statics=crawler.settings.getbool('DOWNLOADER_BROWSER_CACHE_STATICS', True),
            cache_dir=crawler.settings.get('DOWNLOADER_BROWSER_CACHE_DIR'),
        )

    @staticmethod
    def get_static_name(url):
        if "?" in url:
            url, _ = url.split('?')
        parts = url.split('/')
        if parts == 1:
            name = parts[0]
        else:
            name = ".".join((parts[-2], parts[-1]))
        # name = re.sub(r'[\\/:*?<>|"]+', '_', name)
        return name

    def download_request(self, request: Request, **_):
        pass

    def close_expired_connections(self):
        pass

    def close(self):
        pass


class PlaywrightDownloader(BrowserDownloader):

    def __init__(self, **kwargs):
        if not sync_playwright:
            raise ImproperlyConfigured(
                'You need to install the playwright library to use PlaywrightDownloader.'
            )
        super().__init__(**kwargs)
        self._wlock = RLock()

    def handle_route(self, route, *_, hooked, banned_routes=None):
        # arg_count = len(inspect.signature(handler).parameters)
        name = self.get_static_name(route.request.url)
        cached_js = self._cache_dir / parse_url_host(route.request.url) / name
        if route.request.resource_type in banned_routes:
            route.abort()
        elif name.endswith('.js') or route.request.resource_type == 'script':
            frame = route.request.frame
            hooking = False
            if not name.endswith('.min.js') and 'element-ui' not in name and 'ajax.js' not in name:
                # include iframes.
                if frame.url != 'about:blank' and frame.page.url != 'about:blank':
                    hooking = True
                    hooked[frame].append(name)
            original_response = None
            # Failed to load module script: Expected a JavaScript module script but the server responded with
            # a MIME type of "". Strict MIME type checking is enforced for module scripts per HTML spec.
            headers = {"content-type": "application/javascript; charset=utf-8"}  # vue?
            # original_len = original_response.headers["content-length"]
            if os.path.exists(cached_js):
                with open(cached_js, 'rb') as fo:
                    body = fo.read()
            else:
                original_response = route.fetch()
                headers.update(original_response.headers)
                body = original_response.body()
            if hooking:
                body += b'\n' + VUE_DOM_LOAD_HOOK.format(name=set_md5(name)).encode('utf-8')
            # maybe replace $createElement in vue.min.js
            route.fulfill(
                response=original_response,
                body=body,
                headers={**headers}
            )
        elif os.path.exists(cached_js):
            with open(cached_js, 'rb') as fo:
                body = fo.read()
            route.fulfill(body=body)
        else:
            route.continue_()

    def on_response(self, response):
        if not self._cache_statics or response.request.method != 'GET':
            return
        if response.request.resource_type in ('xhr', 'document', 'fetch'):
            return
        if response.frame.page.is_closed():
            # don't try to fetch response.body
            return

        name = self.get_static_name(response.request.url)
        host_dir = self._cache_dir / parse_url_host(response.request.url)
        cached_path = host_dir / name
        with self._wlock:
            if not os.path.exists(host_dir):
                os.makedirs(host_dir)
            if not os.path.exists(cached_path):
                body = response.body()
                # recover modified body.
                body = body.replace(b'\n' + VUE_DOM_LOAD_HOOK.format(name=set_md5(name)).encode('utf-8'), b'')
                with open(cached_path, 'wb') as fo:
                    fo.write(body)

    @staticmethod
    def _wait_for_hook(frame, name=None, timeout=0):
        identity = f"foo-tider-{set_md5(name)}" if name else "foo-tider"
        frame.locator(f"#{identity}").wait_for(state='attached', timeout=timeout)  # appropriately increase
        frame.locator(f"#{identity}").evaluate("(ele) => {if (ele) ele.remove()}")
        frame.wait_for_timeout(300)

    def wait_for_hooked_frames(self, hooked, timeout=0, wait_for_load=False):
        for frame in hooked:
            hooks = hooked[frame]
            if frame.url == frame.page.url and wait_for_load:
                hooks.append("")
            while hooks:
                hook = hooks.pop()
                self._wait_for_hook(frame=frame, name=hook, timeout=timeout)

    def download_request(self, request: Request, **_):
        # https://github.com/microsoft/playwright/issues/27997
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
        scroll_to_bottom = request.meta.get('browser_scroll_to_bottom', False)
        scroll_timeout = request.meta.get('browser_scroll_timeout', request.timeout)

        playwright_context = browser = page = None
        hooked = defaultdict(list)
        try:
            playwright_context = sync_playwright()
            playwright = playwright_context.start()

            context_config = {'proxy': {'server': request.selected_proxy}, 'ignore_https_errors': True}
            if device:
                context_config.update(playwright.devices[device])
            browser_type = getattr(playwright, browser_type)
            browser = browser_type.launch(
                channel=browser_channel, headless=True,
                # devtools=True,
                args=['--no-sandbox', '--single-process'],
            )
            context = browser.new_context(**context_config, bypass_csp=True)

            page = context.background_pages[0] if context.background_pages else context.new_page()
            page.set_default_timeout(timeout=request.timeout * 1000)  # ms -> s
            page.add_init_script(script=INIT_SCRIPT)
            if init_script or init_script_path:
                page.add_init_script(script=init_script, path=init_script_path)
            page.set_extra_http_headers(dict(request.headers))

            banned_routes = ['media', 'other', 'websocket']
            if not screenshot_path:
                banned_routes.append('image')
            page.on('response', self.on_response)
            page.route("**/*", functools.partial(self.handle_route, hooked=hooked, banned_routes=banned_routes))

            start = preferred_clock()
            # automatically dismiss dialogs.
            browser_response = page.goto(request.url, wait_until='networkidle')
            page.add_script_tag(content=DOM_LOAD_HOOK)
            self.wait_for_hooked_frames(hooked, timeout=request.timeout * 1000, wait_for_load=True)

            # https://www.reddit.com/
            # https://www.steelwood.amsterdam/
            current_position = 0
            history_heights = []
            page_height = page.evaluate("document.body.scrollHeight")
            scroll_start = preferred_clock()
            while True:
                history_heights.append(page_height)
                history_heights = history_heights[-5:]  # Keep only the last 5 heights, to not run out of memory
                while current_position < page_height:
                    current_position += 300
                    # page.mouse.wheel(0, current_position)
                    page.evaluate(f"window.scrollTo(0, {current_position})")
                    page.wait_for_timeout(300)
                if not scroll_to_bottom:
                    break
                if scroll_to_bottom and history_heights and history_heights[-1] == page_height:
                    break
                scroll_elapsed = preferred_clock() - scroll_start
                if scroll_timeout:
                    if scroll_elapsed > scroll_timeout or len(history_heights) == 5 and len(set(history_heights)) == 1:
                        break
            page.wait_for_load_state('networkidle')
            self.wait_for_hooked_frames(hooked, timeout=request.timeout * 1000)

            html = page.content()
            html = html.replace(DOM_LOAD_HOOK, '').replace(LAST_SCRIPT, '')
            status_code = browser_response.status
            reason = browser_response.status_text
            response_headers = browser_response.headers
            if screenshot_path:
                page.add_style_tag(content='body { height: "" !important; width: "" !important;}')
                page.set_viewport_size({"width": 1920, "height": 1080})
                page.evaluate(STYLE_PATCH)
                page.screenshot(path=screenshot_path, full_page=True)
            elapsed = preferred_clock() - start

            context_cookies = context.cookies()
            cookies = {cookie['name']: cookie['value'] for cookie in context_cookies}
            resp = BrowserResponse(html, headers=response_headers, status_code=status_code, cookies=cookies, reason=reason)
            response = Response.from_origin_resp(resp=resp, request=request)
            response.elapsed = elapsed
            return response
        except Exception as e:
            if 'ERR_TUNNEL_CONNECTION_FAILED' in str(e):
                request.invalidate_proxy()
            if 'ERR_INVALID_AUTH_CREDENTIALS' in str(e):
                request.invalidate_proxy()
            response = Response(request)
            if not response.failed:  # maybe already failed in response.read().
                response.fail(error=e)
            return response
        finally:
            if page is not None:
                page.unroute_all(behavior='ignoreErrors')
            hooked.clear()
            browser and browser.close()
            playwright_context and playwright_context.__exit__()


class DrissionPageDownloader(BrowserDownloader):

    def __init__(self, **kwargs):
        if not Chromium:
            raise ImproperlyConfigured(
                'You need to install the DrissionPage library to use DrissionPageDownloader.'
            )
        super().__init__(**kwargs)

    def download_request(self, request: Request, **_):
        screenshot_path = request.meta.get('browser_screenshot_path')
        response_headers = {}
        browser = tab = None
        try:
            co = ChromiumOptions(ini_path=request.meta.get('browser_ini_path'))
            co.no_imgs(False).mute(True).incognito().headless().auto_port()
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

            start = preferred_clock()
            tab.get(url=request.url, show_errmsg=True, timeout=timeout)
            tab.run_js(script=INIT_SCRIPT)
            tab.wait(second=timeout - (preferred_clock() - start))
            if screenshot_path:
                tab.set.window.size(1920, 1080)
                tab.run_js(STYLE_PATCH)
                tab.get_screenshot(path=screenshot_path, full_page=True)
            html = tab.html
            cookies = tab.cookies().as_dict()
            resp = BrowserResponse(html, headers=response_headers, status_code=200, cookies=cookies)
            elapsed = preferred_clock() - start

            response = Response.from_origin_resp(resp=resp, request=request)
            response.elapsed = elapsed
            return response
        except Exception as e:
            response = Response(request)
            if not response.failed:  # maybe already failed in response.read().
                response.fail(error=e)
            return response
        finally:
            tab and tab.close()
            browser and browser.quit()
