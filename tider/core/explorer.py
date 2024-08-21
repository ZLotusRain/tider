"""execute `Request`."""

import time
import logging
import requests
import tempfile
import subprocess
from threading import Event
from collections import deque
from subprocess import DEVNULL

from tider.session import Session
from tider.network import ProxyPoolManager, Response
from tider.platforms import IS_WINDOWS
from tider.utils.decorators import inthread
from tider.utils.misc import symbol_by_name, create_instance

logger = logging.getLogger(__name__)

PLATFORM_SUPPORTS_WGET = not IS_WINDOWS


def _transport(queue, processor, output_handler=None, shutdown_event=None):
    shutdown_event = shutdown_event or Event()
    while not shutdown_event.is_set():
        try:
            request = queue.popleft()
            response = processor(request)
            # don't use thread lock in handler if possible.
            output_handler and output_handler(response)
        except IndexError:
            # switch greenlet if using gevent
            # when the active one can't get the next request
            # to avoid keeping loop.
            time.sleep(0.1)


class Explorer:

    def __init__(self, tider, pool=None, concurrency=1, priority_adjust=1,
                 wget_output_dir="", proxy_args=None):
        # self.session = Session()
        self.pool = pool
        self.concurrency = concurrency

        self.priority_adjust = priority_adjust
        self.pm = ProxyPoolManager(proxypool_kw=proxy_args, stats=tider.stats)  # proxy_key

        self._wget_output_dir = wget_output_dir
        self.queue = deque()
        self.transferring = set()
        self.running = False
        self._shutdown = Event()
        self._pool_started = False

    @classmethod
    def from_tider(cls, tider):
        settings = tider.settings
        explorer = cls(
            tider=tider,
            pool=tider.create_pool(thread_name_prefix="ExplorerWorker"),
            concurrency=tider.concurrency,
            priority_adjust=settings.getint('EXPLORER_RETRY_PRIORITY_ADJUST'),
            wget_output_dir=settings["WGET_OUTPUT_DIR"],
            proxy_args=settings.getdict('PROXY_PARAMS'),
        )
        proxy_schemas = settings.getdict('PROXY_SCHEMAS')
        for schema in proxy_schemas:
            proxy_cls = symbol_by_name(proxy_schemas[schema])
            proxy_ins = create_instance(proxy_cls, settings, tider)
            explorer.register_proxy(schema, proxy_ins)
        return explorer

    def active(self):
        return len(self.transferring) + len(self.queue) > 0

    def reset_status(self):
        """
        Should not be used in other cases except for broker.
        """
        self.queue.clear()
        # self.queue = deque()  # reference may be changed
        self.transferring.clear()
        # self.transferring = set()
        self.pm.clear()

    def close(self, reason):
        self.running = False
        self._shutdown.set()
        self.queue.clear()
        self.pool.stop()
        self._pool_started = False
        self.transferring.clear()
        self.pm.close()
        # self.session.close()
        logger.info("Explorer closed (%(reason)s)", {'reason': reason})

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close(reason="finished")

    def register_proxy(self, schema, proxy_ins):
        self.pm.register(schema, proxy_ins)

    def fetch(self, request):
        self.queue.append(request)

    def needs_backout(self):
        return len(self.queue) >= self.concurrency * 3

    def on_proxy_discard(self, proxy):
        for each in proxy.values():
            self.session.evict_manager_for(each)

    def build_proxies(self, request):
        # if getattr(request.proxy, '_on_discard') is None:
        #     # compat for user customized proxies
        #     setattr(request.proxy, '_on_discard', self.on_proxy_discard)
        new_proxy = request.meta.get('new_proxy', False)
        schema = request.proxy_schema
        proxypool_kw = request.proxy_params
        proxy = self.pm.get_proxy(
            schema=schema,
            proxy_args=proxypool_kw,
            disposable=new_proxy,
            # on_discard=self.on_proxy_discard
        )
        request.update_proxy(proxy)

    @inthread(name='explorer')
    def async_explore_in_thread(self, output_handler=None):
        if self.running:
            raise RuntimeError("Explorer already running")
        self.running = True
        if not self._pool_started:
            self.pool.start()
        self._pool_started = True
        self._blocking_transport(output_handler)

    def _blocking_transport(self, output_handler=None):
        while self.running:
            while len(self.transferring) < self.concurrency:
                try:
                    request = self.queue.popleft()
                except IndexError:
                    break
                self.transferring.add(request)
                self.pool.apply_async(
                    target=self.explore, args=(request,),
                    callback=lambda resp: output_handler and output_handler(resp)
                )
            time.sleep(0.01)

    def async_explore(self, output_handler=None):
        if self.running:
            raise RuntimeError("Explorer already running")
        self.running = True
        if not self._pool_started:
            self.pool.start()
        self._pool_started = True
        # By just passing a reference to the object allows the garbage collector
        # to free self if nobody else has a reference to it.
        queue = self.queue
        processor = self.explore
        shutdown_event = self._shutdown
        for _ in range(self.concurrency):
            self.pool.apply_async(
                _transport,
                (queue, processor, output_handler, shutdown_event)
            )

    def _transport(self, output_handler=None):
        while self.running:
            try:
                request = self.queue.popleft()
                response = self.explore(request)
                # don't use thread lock in handler if possible.
                output_handler and output_handler(response)
            except IndexError:
                # switch greenlet if using gevent
                # when the active one can't get the next request
                # to avoid keeping loop.
                time.sleep(0.1)

    def explore(self, request):
        self.transferring.add(request)
        request.meta['elapsed'] = time.time()
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
        self.transferring.discard(request)
        response.meta.pop('elapsed', None)
        return response

    def _wget_download(self, request):
        ntf = tempfile.NamedTemporaryFile(dir=self._wget_output_dir)
        cmd = request.to_wget(output=ntf.name)
        request.fetch_proxies()  # to increase used_times
        timeout = request.request_kwargs.get("timeout") or 60
        timeout = max(timeout, 100)
        try:
            subprocess.run(cmd, timeout=timeout, check=True, stdout=DEVNULL, stderr=DEVNULL)
            response = self.build_response(request)
            response.raw = ntf
            response.url = request.url
            response.status_code = 200
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            returncode = getattr(e, 'returncode', None)
            if returncode in (1, 4, 5, 7, 8) or isinstance(e, subprocess.TimeoutExpired):
                request.forbid_proxy()
            ntf.close()
            response = self.get_retry_request(request, None, str(e))
        return response

    def _explore(self, request):
        url = request.url
        # use `request.request_kwargs` directly may lead memory leak on CentOS.
        kwargs = request.request_kwargs.copy()
        proxies = request.fetch_proxies()
        kwargs.update(proxies=proxies)

        session = request.meta.get("session")
        resp = None
        try:
            if session:
                resp = session.request(url=url, **kwargs)
            else:
                with Session() as session:
                    resp = session.request(url=url, **kwargs)
            if request.raise_for_status:
                resp.raise_for_status()
            response = self.build_response(request, resp)
        except requests.HTTPError as e:
            ignored_status_code = request.ignored_status_codes
            status_code = resp.status_code
            if str(status_code).startswith("4"):
                request.forbid_proxy()
            if status_code not in ignored_status_code:
                response = self.get_retry_request(request, resp, reason=str(e))
                if str(status_code) == "500":
                    time.sleep(2)
                elif str(status_code).startswith("5"):
                    time.sleep(1)
            else:
                response = self.build_response(request, resp)
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError, requests.TooManyRedirects) as e:
            if isinstance(e, (requests.ConnectionError, requests.Timeout)):
                request.forbid_proxy()
            resp = getattr(e, 'response', resp)
            response = self.get_retry_request(request, resp, reason=str(e))
        except Exception as e:
            resp = getattr(e, 'response', resp)
            response = self.build_response(request, resp)
            reason = response.reason or str(e)
            response.fail(reason=reason)
        del kwargs
        return response

    @staticmethod
    def build_response(request, resp=None):
        if resp is not None:
            resp.request = request
        else:
            resp = Response(request)
        response = resp
        return response

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
