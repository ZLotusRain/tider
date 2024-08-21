import os
import time
import socket
import logging
from threading import Event
from kombu.utils import cached_property

from tider.core.parser import Parser
from tider.core.control import Pidbox
from tider.core.brokers import get_broker
from tider.store import create_fsm, close_fsm
from tider.exceptions import SpiderShutdown
from tider.network import Request, Response
from tider.utils.decorators import inthread
from tider.utils.misc import symbol_by_name, create_instance

logger = logging.getLogger(__name__)

MAX_INTERVAL_TIME = 10 * 60


def _find_stuck_reqs(reqs):
    return (
        req for req in list(reqs)
        if (req.meta.get('elapsed')
            and time.time() - req.meta['elapsed'] >= MAX_INTERVAL_TIME)
    )


class HeartEngine:
    """Heart Engine to run the spider."""

    explorer_cls = 'tider.core.explorer:Explorer'
    scheduler_cls = 'tider.core.scheduler:Scheduler'

    def __init__(self, tider):
        self.tider = tider
        self.spider = None
        self.start_requests = None

        scheduler_cls = self._get_scheduler_class()
        self.scheduler = create_instance(scheduler_cls, tider.settings, self.tider)
        self.parser = Parser(self.tider)

        self.alarm_msg_type = tider.settings['ALARM_MESSAGE_TYPE']
        self.async_parse = tider.settings.getbool('ASYNC_PARSE')

        self.pidbox = Pidbox(self.tider)
        self.connection = tider.connection_for_control()
        create_fsm(tider)

        self.pid = os.getpid()
        self.running = False
        self.paused = False
        self._shutdown = False
        self._comps = set()
        self._stopped = Event()

        self.start_time = round(time.time(), 4)

    def _get_scheduler_class(self) -> type:
        from tider.core.scheduler import BaseScheduler
        scheduler = self.tider.settings.get('SCHEDULER') or self.scheduler_cls
        scheduler_cls = symbol_by_name(scheduler)
        if not issubclass(scheduler_cls, BaseScheduler):
            raise TypeError(
                f"The provided scheduler class ({scheduler})"
                " does not fully implement the scheduler interface"
            )
        return scheduler_cls

    @cached_property
    def explorer(self):
        explorer_cls = self.tider.settings.get('EXPLORER') or self.explorer_cls
        return create_instance(symbol_by_name(explorer_cls),
                               self.tider.settings, self.tider)

    @cached_property
    def broker(self):
        broker_type = self.tider.settings['BROKER_TYPE']
        return get_broker(broker_type)(tider=self.tider)

    def pause(self):
        self.paused = True

    def unpause(self):
        self.paused = False

    def active(self):
        return (self.start_requests is not None
                or self._check_if_active())

    def _check_if_active(self):
        flag = False
        for _ in range(5):
            flag = (self.explorer.active() or
                    self.parser.active() or
                    self.scheduler.has_pending_requests())
            if flag:
                break
            # consider some message queues like redis
            time.sleep(0.15)
        return flag

    def open_spider(self, spider):
        self.spider = spider
        if self.tider.spider_type == 'worker':
            self.start_requests = iter(self._worker_start_requests())
        else:
            self.start_requests = iter(self.spider.start_requests())
        if hasattr(self.scheduler, "open"):
            self.scheduler.open()

    def _worker_start_requests(self):
        timeout = self.tider.settings.get('WORKER_TIMEOUT')
        st = time.time()
        queue_name = self.tider.settings.get('BROKER_QUEUE')
        validate = self.tider.settings.get('BROKER_VALIDATE_MESSAGE')
        while True:
            message = self.broker.consume(queue_name, validate=validate)
            if not message:
                if not self._check_if_active():
                    logger.info(f'Waiting for messages from broker, queue: {queue_name}')
                    if timeout is not None and time.time() - st >= timeout:
                        break
                    self.explorer.reset_status()
                    # self.parser.reset_status()
                    # self.scheduler.reset_status()
                    time.sleep(5)
            else:
                start_requests = self.spider.process(message=message) or []
                yield from start_requests
                if hasattr(start_requests, 'close'):
                    start_requests.close()
            time.sleep(0.5)

    def start(self):
        if self.running:
            raise RuntimeError("Engine already running")
        self.running = True

        self.tider.stats.set_value('pid', self.pid)

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        self.tider.stats.set_value("server_ip", ip)
        s.close()

        start_time = self.start_time
        format_start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_time)))
        self.tider.stats.set_value("time/start_time", start_time)
        self.tider.stats.set_value("time/start_time/format", format_start_time)

        handler = self._handle_explorer_output if self.async_parse else self.parser._parse
        self.explorer.async_explore(handler)
        self.async_parse and self.parser.async_parse()

        self.pidbox.start()
        self._start()

    def _start(self):
        self._comps.add(self._schedule_start_requests())
        self._comps.add(self._beat())
        for comp in self._comps:
            comp.start()
        while self.active():
            try:
                # maybe stuck here when using threads
                # and gevent monkey patch at the same time
                self.connection.drain_events(timeout=2.0)
            except socket.timeout:
                pass
            except SpiderShutdown:
                self._shutdown = True
                break
        self._stopped.set()
        reason = 'shutdown' if self._shutdown else 'finished'
        self.close_spider(self.spider, reason=reason)

    def _needs_backout(self):
        return self.explorer.needs_backout()

    def _overload(self):
        limit = self.tider.concurrency * 5
        return len(self.explorer.queue) + len(self.parser.queue) >= limit

    def schedule_request(self, request):
        if request.broadcast and self.tider.spider_type == 'publisher':
            self.broker.produce(request)
            self.tider.stats.inc_value("request/published")
        elif not self.scheduler.enqueue_request(request):
            logger.error(f"Request dropped: {request}")
            self.tider.stats.inc_value("request/dropped")

    def _handle_explorer_output(self, result):
        if not isinstance(result, (Request, Response)):
            raise TypeError(f"Incorrect type: expected Request, Response or Failure, "
                            f"got {type(result)}: {result!r}")
        self.tider.stats.inc_value("request/count")
        if isinstance(result, Request):
            if result.proxy:
                self.tider.stats.inc_value('request/count/proxy')
            self.tider.stats.inc_value("request/count/retry")
        else:
            if result.request.proxy:
                self.tider.stats.inc_value('request/count/proxy')
        while self._overload():
            time.sleep(1)
        self.parser.enqueue_parser(result)

    def maybe_sleep(self, seconds):
        self.tider.is_green_pool and time.sleep(seconds)

    @inthread(name='start_requests')
    def _schedule_start_requests(self):
        assert self.spider is not None  # typing

        while self.start_requests is not None:
            if self._shutdown:
                self.start_requests = self.start_requests.close()
                break
            while (self.paused
                   or self._needs_backout()
                   or len(self.scheduler) >= 10000
                   or self._overload()):
                time.sleep(1)
            # noinspection PyBroadException
            try:
                request = next(self.start_requests)
            except StopIteration:
                self.start_requests = self.start_requests.close()
            except Exception:
                self.start_requests = self.start_requests.close()
                logger.error('Error while obtaining start requests', exc_info=True)
            else:
                self.schedule_request(request)
            # switch greenlet
            self.maybe_sleep(0.01)

    @inthread(name='beat')
    def _beat(self):
        time_start = 0
        while not self._stopped.is_set():
            while (self.paused
                   or self._needs_backout()):
                time.sleep(0.1)
            request = self.scheduler.next_request()
            if not request:
                if self.start_requests is None:
                    # (worker)start requests have been consumed.
                    if not time_start:
                        time_start = time.monotonic()
                    elif time.monotonic() - time_start >= MAX_INTERVAL_TIME:
                        self._maybe_alarm()
                        time_start = 0
                time.sleep(0.1)
            else:
                self.explorer.fetch(request)
                time_start = 0
            # switch greenlet
            self.maybe_sleep(0.01)

    def _maybe_alarm(self):
        try:
            extra_infos = {
                'stuck_exploring': [str(req) for req in
                                    _find_stuck_reqs(self.explorer.transferring)],
                'stuck_parsing': [
                    {
                        'request': str(request),
                        'callback': getattr(request.callback, '__name__', None),
                        'errback': getattr(request.errback, '__name__', None)
                    }
                    for request in _find_stuck_reqs(self.parser.parsing)
                ]
            }
            if extra_infos['stuck_parsing']:
                message = f"Spider has been stuck for {MAX_INTERVAL_TIME}s"
                self.tider.alarm.alarm(message, self.alarm_msg_type, extra_infos)
            elif extra_infos['stuck_exploring']:
                message = f'Spider receives no response in {MAX_INTERVAL_TIME}s'
                self.tider.alarm.alarm(message, self.alarm_msg_type, extra_infos)
        except Exception as e:
            logger.exception(f'Send alarm message failed, reason: {e}')

    def close(self):
        if self.running:
            return self.stop()  # will also close spider and explorer
        elif self.spider is not None:
            return self.close_spider(self.spider, reason="shutdown")   # will also close explorer

    def stop(self):
        if not self.running:
            raise RuntimeError("Engine not running")
        self.running = False
        if self.spider is not None:
            self.close_spider(self.spider, reason="shutdown")

    def _wait_for_comps(self):
        for comp in self._comps:
            comp.join()
        self._comps.clear()

    def _close_control(self):
        self.pidbox and self.pidbox.shutdown()
        self.connection.release()

    def close_spider(self, spider, reason="finished"):
        # reason: cancelled
        logger.info("Closing spider (%(reason)s)", {'reason': reason}, extra={'spider': spider})

        self._close_control()
        self._wait_for_comps()

        end_time = round(time.time(), 4)
        format_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(end_time)))
        self.tider.stats.set_value("time/end_time", end_time)
        self.tider.stats.set_value("time/end_time/format", format_end_time)
        consumed_time = int(end_time - self.tider.stats.get_value('time/start_time', end_time))
        self.tider.stats.set_value("consumed_time", consumed_time)

        if self.tider.spider_type != 'task':
            self.broker.close()
        self.explorer.close(reason=reason)
        self.parser.close(reason=reason)
        self.scheduler.close(reason=reason)
        self.spider.close(reason=reason)
        self.tider.alarm.close()
        self.tider.stats.close(spider)
        setattr(self, 'spider', None)
        close_fsm()
        logger.info("Spider closed (%(reason)s)", {'reason': reason})
