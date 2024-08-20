import time
import weakref
import logging

from tider.core.parser import Parser
from tider.core.brokers import get_broker
from tider.network import Request, Response
from tider.utils.decorators import inthread, cached_property
from tider.utils.misc import symbol_by_name, create_instance

logger = logging.getLogger(__name__)

MAX_INTERVAL_TIME = 10 * 60


class HeartEngine:

    def __init__(self, crawler):
        if isinstance(crawler, weakref.ProxyType):
            self.crawler = crawler
        else:
            self.crawler = weakref.proxy(crawler)
        self.settings = crawler.settings

        self.spider = None
        self.start_requests = None
        explorer_cls = symbol_by_name(self.settings['EXPLORER'])
        self.explorer = create_instance(explorer_cls, self.settings, self.crawler)
        scheduler_cls = self._get_scheduler_class(self.settings)
        self.scheduler = create_instance(scheduler_cls, self.settings, self.crawler)
        self.broker_name = self.settings['BROKER']
        self.parser = Parser(self.crawler)

        self.concurrency = self.settings['CONCURRENCY']
        self.running = False
        self.paused = False
        self._crawler_role = crawler.role
        self._is_gevent = crawler.settings['POOL'] == 'gevent'

    @staticmethod
    def _get_scheduler_class(settings) -> type:
        from tider.core.scheduler import BaseScheduler
        scheduler_cls = symbol_by_name(settings["SCHEDULER"])
        if not issubclass(scheduler_cls, BaseScheduler):
            raise TypeError(
                f"The provided scheduler class ({settings['SCHEDULER']})"
                " does not fully implement the scheduler interface"
            )
        return scheduler_cls

    @cached_property
    def broker(self):
        return get_broker(self.broker_name)(crawler=self.crawler)

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
        if self.crawler.role == 'worker':
            self.start_requests = iter(self._worker_start_requests())
        else:
            self.start_requests = iter(self.spider.start_requests())
        if hasattr(self.scheduler, "open"):
            self.scheduler.open()

    def _worker_start_requests(self):
        timeout = self.settings.get('WORKER_TIMEOUT')
        st = time.perf_counter()
        queue_name = self.settings.get('BROKER_QUEUE') or self.broker.queue_name
        while True:
            message = self.broker.consume(queue_name)
            if not message:
                if not self._check_if_active():
                    logger.info(f'Waiting for messages from broker, queue: {queue_name}')
                    if timeout is not None and time.perf_counter() - st > timeout:
                        break
                    time.sleep(5)
            else:
                for request in iter(self.spider.process(message=message)):
                    yield request
            time.sleep(0.5)

    def start(self):
        if self.running:
            raise RuntimeError("Engine already running")
        self.running = True
        start_time = round(time.time(), 4)
        format_start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_time)))
        self.crawler.stats.set_value("time/start_time", start_time)
        self.crawler.stats.set_value("time/start_time/format", format_start_time)
        if not self.settings.getbool('IN_THREAD', True):
            self.explorer.async_explore(self._handle_explorer_output)
            self.parser.async_parse()
            self._next_request()
        else:
            self._start()

    def _next_request(self):
        assert self.spider is not None  # typing

        while self.active():
            while self.paused:
                time.sleep(2)

            while not self._needs_backout() and self._next_request_from_scheduler() is not None:
                self.maybe_sleep(0.1)

            if self.start_requests is not None and not self._needs_backout():
                # noinspection PyBroadException
                try:
                    request = next(self.start_requests)
                except StopIteration:
                    self.start_requests = None
                except Exception:
                    self.start_requests = None
                    logger.error('Error while obtaining start requests', exc_info=True, extra={'spider': self.spider})
                else:
                    self.schedule_request(request)
            self.maybe_sleep(0.1)
        self._notify_closing()

    def maybe_sleep(self, seconds):
        self._is_gevent and time.sleep(seconds)

    def _needs_backout(self):
        return self.explorer.needs_backout()

    def _next_request_from_scheduler(self):
        request = self.scheduler.next_request()
        if request is None:
            return None
        self.explorer.fetch(request)
        return request

    def schedule_request(self, request):
        if request.broadcast and self._crawler_role == 'producer':
            self.broker.produce(request)
            self.crawler.stats.inc_value("request/produced")
        elif not self.scheduler.enqueue_request(request):
            logger.error(f"Request dropped: {request}")
            self.crawler.stats.inc_value("request/dropped")

    def _notify_closing(self):
        self.explorer.running = False
        self.parser.running = False

    def _handle_explorer_output(self, result):
        if not isinstance(result, (Request, Response)):
            raise TypeError(f"Incorrect type: expected Request, Response or Failure, "
                            f"got {type(result)}: {result!r}")
        self.crawler.stats.inc_value("request/count")
        if isinstance(result, Request):
            if result.proxies:
                self.crawler.stats.inc_value('request/count/proxy')
            self.crawler.stats.inc_value("request/count/retry")
        else:
            if result.request.proxies:
                self.crawler.stats.inc_value('request/count/proxy')
        self.parser.enqueue_parser(result)

    def _start(self):
        threads = (
            self._start_requests(),
            self._beat(),
            self.explorer.async_explore_in_thread(self._handle_explorer_output),
            self.parser.async_parse_in_thread()
        )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    @inthread(name='start_requests')
    def _start_requests(self):
        assert self.spider is not None  # typing

        while self.start_requests is not None:
            while self.paused:
                time.sleep(2)
            while (self._needs_backout()
                   or len(self.scheduler) > self.concurrency * 5):
                time.sleep(1)
            # noinspection PyBroadException
            try:
                request = next(self.start_requests)
            except StopIteration:
                self.start_requests = None
            except Exception:
                self.start_requests = None
                logger.error('Error while obtaining start requests', exc_info=True, extra={'spider': self.spider})
            else:
                self.schedule_request(request)

    @inthread(name='beat')
    def _beat(self):
        st = 0
        while self.active():
            while self.paused:
                time.sleep(2)
            while self._needs_backout():
                time.sleep(0.1)
            request = self.scheduler.next_request()
            if not request:
                if self.start_requests is None:
                    # (worker)start requests have been consumed.
                    if not st:
                        st = time.perf_counter()
                    elif time.perf_counter() - st > MAX_INTERVAL_TIME:
                        self._send_alarm_message()
                        st = 0
                # switch thread
                time.sleep(0.1)
            else:
                self.explorer.fetch(request)
        self._notify_closing()

    def _send_alarm_message(self):
        msg_type = self.settings['ALARM_MESSAGE_TYPE']
        stuck_parsing = self._find_stuck_reqs(self.parser.parsing)
        stuck_exploring = self._find_stuck_reqs(self.explorer.transferring)
        extra_infos = {
            'stuck_exploring': self._gen_alarm_info(stuck_exploring),
            'stuck_parsing': self._gen_alarm_info(stuck_parsing)
        }
        if stuck_parsing:
            message = f"Spider has been stuck for {MAX_INTERVAL_TIME}s"
            self.crawler.alarm.alarm(message, msg_type, extra_infos)
        elif stuck_exploring:
            message = f'Spider receives no response in {MAX_INTERVAL_TIME}s'
            self.crawler.alarm.alarm(message, msg_type, extra_infos)

    @staticmethod
    def _find_stuck_reqs(reqs):
        return [req for req in list(reqs)
                if time.perf_counter() - req.meta.get('elapsed', time.perf_counter()) > MAX_INTERVAL_TIME]

    @staticmethod
    def _gen_alarm_info(reqs):
        return [
            {
                'request': str(request),
                'callback': getattr(request.callback, '__name__', None),
                'errback': getattr(request.errback, '__name__', None)
            }
            for request in reqs
        ]

    def close(self):
        if self.running:
            return self.stop()  # will also close spider and explorer
        elif self.spider is not None:
            return self.close_spider(self.spider, reason="shutdown")   # will also close explorer

    def stop(self):
        if not self.running:
            raise RuntimeError("Engine not running")
        self.running = False
        self.close_spider(self.spider, reason="finished")

    def close_spider(self, spider, reason="finished"):
        logger.info("Closing spider (%(reason)s)", {'reason': reason}, extra={'spider': spider})
        end_time = round(time.time(), 4)
        format_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(end_time)))
        self.crawler.stats.set_value("time/end_time", end_time)
        self.crawler.stats.set_value("time/end_time/format", format_end_time)
        consumed_time = int(time.time() - self.crawler.stats.get_value('time/start_time'))
        self.crawler.stats.set_value("consumed_time", consumed_time)
        # reason: cancelled
        if self._crawler_role != 'crawler':
            self.broker.close()
        self.explorer.close(reason=reason)
        self.parser.close(reason=reason)
        self.scheduler.close(reason=reason)
        self.crawler.stats.close(spider)
        self.spider.close(reason=reason)
        setattr(self, 'spider', None)
        logger.info("Spider closed (%(reason)s)", {'reason': reason})
