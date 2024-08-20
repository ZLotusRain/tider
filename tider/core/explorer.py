"""execute `Request`."""

import time
import logging
import urllib3
import requests
import platform
import subprocess
from collections import deque
from requests.utils import get_encoding_from_headers

from tider.concurrency.base import worker
from tider.concurrency import get_implementation
from tider.network import ProxyManager, Response
from tider.utils.decorators import inthread
from tider.utils.misc import symbol_by_name, create_instance

urllib3.disable_warnings()
logger = logging.getLogger(__name__)

PLATFORM_SUPPORTS_WGET = platform.system().upper() == 'LINUX'
URLLIB3_SUPPORTS_HTTPS = tuple(urllib3.__version__.split('.')) > ('1', '26')
del platform, urllib3


class Explorer:

    def __init__(self, pool=None, concurrency=1, priority_adjust=1, proxy_args=None):
        self.pool = pool
        self.concurrency = concurrency

        self.pm = ProxyManager(proxy_args=proxy_args)
        self.priority_adjust = priority_adjust
        self.queue = deque()
        self.transferring = set()
        self.running = False
        self._started = False

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        concurrency = settings['CONCURRENCY']
        pool_cls = get_implementation(settings['POOL'])
        pool = pool_cls(limit=concurrency, thread_name_prefix="ExplorerWorker")
        explorer = cls(
            pool=pool,
            concurrency=concurrency,
            priority_adjust=settings.getint('RETRY_PRIORITY_ADJUST'),
            proxy_args=settings.getdict('PROXY_PARAMS')
        )
        proxy_schemas = settings.getdict('PROXY_SCHEMAS')
        for schema in proxy_schemas:
            proxy_cls = symbol_by_name(proxy_schemas[schema])
            proxy_ins = create_instance(proxy_cls, settings=settings, crawler=crawler)
            explorer.pm.register(schema, proxy_ins)
        explorer.pm.stats = crawler.stats
        return explorer

    def active(self):
        return len(self.transferring) + len(self.queue) > 0

    def close(self, reason):
        self.running = False
        self.pool.stop()
        self.queue.clear()
        self.transferring.clear()
        self.pm.close()
        logger.info("Explorer closed (%(reason)s)", {'reason': reason})

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close(reason="finished")

    def fetch(self, request):
        self.queue.append(request)

    def needs_backout(self):
        return len(self.queue) >= self.concurrency * 3

    def reset_proxy(self, request, force=False):
        reset_proxy = force or request.meta.pop('reset_proxy', False)
        if reset_proxy and not request.meta.get('new_proxy'):
            old_proxies = request.proxies.copy() if request.proxies else {}
            self.pm.reset_proxy(schema=request.proxy_schema, old_proxies=old_proxies)

    def build_proxies(self, request):
        self.reset_proxy(request)  # retry from response
        new_proxy = request.meta.get('new_proxy', False)
        proxies = self.pm.get_proxy(request.proxy_schema, request.proxy_params, new_proxy)
        if proxies:
            request.request_kwargs.setdefault('verify', False)
        request.proxies = proxies

    @inthread(name='explorer')
    def async_explore_in_thread(self, output_handler=None):
        return self.async_explore(output_handler=output_handler)

    def async_explore(self, output_handler=None):
        if self.running:
            raise RuntimeError("Explorer already running")
        self.running = True
        if not self._started:
            self.pool.start()
            self._started = True
        for _ in range(self.concurrency):
            self.pool.apply_async(
                worker,
                (self.queue, self.explore, output_handler,
                 lambda: not self.running)
            )

    def _transport(self, request, output_handler=None):
        response = self.explore(request)
        self.transferring.remove(request)
        if output_handler:
            output_handler(response)

    def try_explore(self, request):
        while True:
            response = self.explore(request)
            if isinstance(response, Response):
                return response
            else:
                request = response

    def explore(self, request):
        self.transferring.add(request)
        request.meta['elapsed'] = time.perf_counter()
        if request.delay:
            time.sleep(request.delay)
        self.build_proxies(request)
        if request.meta.get('use_wget') and PLATFORM_SUPPORTS_WGET:
            response = self._wget_download(request)
        else:
            response = self._explore(request)
        if isinstance(response, Response) and not response.ok:
            retry_times = request.meta.get('retry_times', 0)
            logger.error(
                "Gave up retrying %(request)s (failed %(retry_times)d times): "
                "%(reason)s",
                {'request': request, 'retry_times': retry_times, 'reason': response.reason}
            )
        self.transferring.remove(request)
        response.meta.pop('elapsed', None)
        return response

    def _wget_download(self, request):
        cmd = request.to_wget()
        timeout = request.request_kwargs.get("timeout", 50)
        try:
            subprocess.run(cmd, timeout=timeout, check=True)
            response = self.build_response(request)
            response.status_code = 200
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            response = self.get_retry_request(request, None, str(e))
        return response

    def _explore(self, request):
        url = request.url
        # use `request.request_kwargs` directly may lead memory leak on CentOS.
        kwargs = request.request_kwargs.copy()
        session = request.meta.get("session")
        resp = None
        try:
            if session:
                resp = session.request(url=url, **kwargs)
            else:
                resp = requests.request(url=url, **kwargs)
            if request.raise_for_status:
                resp.raise_for_status()
            response = self.build_response(request, resp)
        except requests.HTTPError as e:
            ignored_status_code = request.ignored_status_codes
            status_code = resp.status_code
            if str(status_code).startswith("4"):
                self.reset_proxy(request, force=True)
            if status_code not in ignored_status_code:
                response = self.get_retry_request(request, resp, reason=str(e))
                if str(status_code).startswith("5"):
                    time.sleep(1)
            else:
                response = self.build_response(request, resp)
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            self.reset_proxy(request, force=True)
            response = self.get_retry_request(request, resp, reason=str(e))
        except Exception as e:
            response = self.build_response(request, resp)
            reason = response.reason or str(e)
            response.fail(reason=reason)
        del kwargs
        return response

    def build_response(self, request, resp=None):
        response = Response(request)
        if resp is not None:
            response.status_code = resp.status_code
            response.url = resp.url
            response.headers = resp.headers
            response.cookies = resp.cookies
            response.raw = resp
            response.reason = resp.reason
        response.elapsed = time.perf_counter() - request.meta['elapsed']
        response.encoding = request.encoding or self._encoding_from_content_type(response.headers)
        return response

    @staticmethod
    def _encoding_from_content_type(headers):
        content_type = headers.get("Content-Type", "")
        charset = list(filter(lambda x: "charset" in x, content_type.split(";")))

        if not charset:
            encoding = get_encoding_from_headers(headers)
            if "text" in content_type:
                encoding = None  # maybe "ISO-8859-1" or 'UTF-8'
        else:
            encoding = charset[0].split("=")[-1]
        return encoding

    def get_retry_request(self, request, resp, reason):
        retry_times = request.meta.get('retry_times', 0) + 1
        max_retry_times = request.max_retry_times
        if max_retry_times == -1 or retry_times <= max_retry_times:
            logger.debug(
                "Retrying %(request)s (failed %(retry_times)d times): %(reason)s",
                {'request': request, 'retry_times': retry_times, 'reason': reason}
            )
            request.meta['retry_times'] = retry_times
            request.dup_check = False
            request.priority += self.priority_adjust
            if resp is not None:
                resp.close()
            return request
        else:
            response = self.build_response(request, resp)
            response.fail(reason=reason)
            return response
