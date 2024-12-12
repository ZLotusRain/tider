import gc
import time
import socket
from threading import Event

from tider import signals, Request, Item
from tider.crawler import state
from tider.exceptions import SpiderShutdown, SpiderTerminate
from tider.network import Response
from tider.utils.log import get_logger
from tider.utils.misc import symbol_by_name, build_from_crawler

logger = get_logger(__name__)

MAX_INTERVAL_TIME = 10 * 60


def active_thread_count():
    from threading import enumerate
    return sum(1 for t in enumerate()
               if not t.name.startswith('Dummy-'))


class MockConnection:

    def __init__(self, sleep=None):
        self._sleep = sleep or time.sleep

    def drain_events(self, timeout=None):
        self._sleep(timeout)


class HeartEngine:
    """Heart Engine to run the spider."""

    explorer_cls = 'tider.crawler.explorer:Explorer'
    parser_cls = 'tider.crawler.parser:Parser'
    scheduler_cls = 'tider.crawler.scheduler:Scheduler'

    def __init__(self, crawler, scheduler_cls=None, on_spider_closed=None):
        self.crawler = crawler
        self.settings = crawler.settings

        self.spider = None
        self.broker = None
        scheduler_cls = self._get_scheduler_class(scheduler_cls=scheduler_cls)
        self.scheduler = build_from_crawler(scheduler_cls, crawler)

        self.parser = symbol_by_name(self.parser_cls)(crawler=crawler)
        self.explorer = build_from_crawler(symbol_by_name(self.explorer_cls), crawler)

        self.running = False
        self.paused = False

        self._spider_closed = Event()
        if on_spider_closed is not None:
            signals.spider_closed.connect(on_spider_closed, sender=self)
        self.start_time = None

    def _get_scheduler_class(self, scheduler_cls=None) -> type:
        from tider.crawler.scheduler import BaseScheduler

        scheduler_cls = scheduler_cls or self.scheduler_cls
        scheduler_cls = symbol_by_name(scheduler_cls)
        if not issubclass(scheduler_cls, BaseScheduler):
            raise TypeError(
                f"The provided scheduler class ({scheduler_cls.__name__})"
                " does not fully implement the scheduler interface"
            )
        return scheduler_cls

    def pause(self):
        self.paused = True

    def unpause(self):
        self.paused = False

    def create_message_handler(self, message_handler=None):
        assert self.spider is not None  # typing

        spider = self.spider
        message_handler = message_handler or spider.start_requests
        stats = self.crawler.stats

        def on_message_received(message, payload, no_ack=True, **_):
            if message is not None:
                stats.inc_value('message/count')
            if not no_ack:
                from tider import Promise

                def _ack_message():
                    message.ack()
                    stats.inc_value('message/count/acked')

                promise = Promise(reqs=message_handler(message=payload), callback=_ack_message)
                start_requests = iter(promise.then())
            else:
                start_requests = iter(message_handler(message=payload))
            while start_requests is not None:
                if self.paused:
                    time.sleep(1)
                    continue

                if self._spider_closed.is_set():
                    if hasattr(start_requests, 'close'):
                        start_requests.close()
                    break

                while (
                    not self._needs_backout()
                    and not self._overload()
                    and self._next_request_from_scheduler() is not None
                ):
                    pass

                if start_requests is not None and not self._needs_backout() and not self._overload():
                    # noinspection PyBroadException
                    try:
                        request = next(start_requests)
                    except StopIteration:
                        start_requests = None
                    except Exception:
                        if hasattr(start_requests, 'close'):
                            start_requests.close()
                        logger.error('Error while obtaining start requests', exc_info=True)
                    else:
                        request and self.schedule_request(request)
                # maybe switch greenlet
                self.crawler.maybe_sleep(0.01)

        return on_message_received

    def on_start_requests_scheduled(self, loop=False, **_):
        first_loop = True
        while first_loop or (loop and self.active()):
            while (
                    not self._needs_backout()
                    and not self._overload()
                    and self._next_request_from_scheduler() is not None
            ):
                pass
            first_loop = False
            # maybe switch greenlet
            self.crawler.maybe_sleep(0.01)

    def _next_request_from_scheduler(self):
        request = self.scheduler.next_request()
        if request is not None:
            self.explorer.fetch(request)
        return request

    def active(self):
        # spider might be closed by other components.
        return not self._spider_closed.is_set() or self._check_if_active()

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

    def start(self, spider, broker):
        if self.running:
            raise RuntimeError("Engine already running")
        self.running = True

        self.spider = spider
        self.broker = broker

        start_time = self.start_time = round(time.time(), 4)
        format_start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(start_time)))
        self.crawler.stats.set_value("time/start_time", start_time)
        self.crawler.stats.set_value("time/start_time/format", format_start_time)

        output_handler = self.create_response_handler()
        self.explorer.async_explore(output_handler)
        self.broker.consume(transport=self.crawler.broker_transport,
                            queues=self.settings.getlist('BROKER_QUEUES'),
                            on_message=self.create_message_handler(), on_messages_consumed=self.on_start_requests_scheduled)

        while self.active():
            try:
                state.maybe_shutdown()
                # maybe stuck here when using threads
                # and gevent monkey patch at the same time
                self.crawler.connection.drain_events(timeout=0.5)
            except socket.timeout:
                pass
            except (SpiderShutdown, SpiderTerminate):  # control shutdown
                return self.close_spider(spider, reason='shutdown')
            if self.explorer.session:
                self.explorer.session.close_expired_connections()
            gc.collect()
        self.close_spider(spider, reason='finished')

    def _needs_backout(self):
        return self.explorer.needs_backout()

    def _overload(self):
        limit = self.crawler.concurrency * 3
        return len(self.explorer.queue) + len(self.parser.queue) >= limit

    def schedule_request(self, request):
        if isinstance(request, Item):
            self.crawler.stats.inc_value(f"item/{request.__class__.__name__}/count")
            return self.parser._quick_process_item(request)
        if not self.scheduler.enqueue_request(request):
            logger.error(f"Request dropped: {request}")
            self.crawler.stats.inc_value("request/dropped")

    def create_response_handler(self):
        async_parse = self.crawler.settings.getbool('PARSER_ASYNC_START')
        overload = self._overload
        schedule_request = self.schedule_request

        self.parser.start()
        if async_parse:
            parse = self.parser.enqueue_parser
        else:
            parse = self.parser.parse

        def on_response(result):
            if not isinstance(result, (Request, Response)):
                raise TypeError(f"Incorrect type: expected Request, Response or Failure, "
                                f"got {type(result)}: {result!r}")
            while overload():
                time.sleep(1)
            if isinstance(result, Request):
                # if processed in explorer,
                # the frame may be stuck for limited queue size.
                schedule_request(result)
            else:
                parse(result)

        return on_response

    def close(self):
        """Gracefully close the engine."""
        if self.running:
            return self.stop()  # will also close spider and other active components
        if self.spider is not None:
            return self.close_spider(self.spider, reason="shutdown")  # will also close other active components
        self.explorer.close(reason='shutdown')
        self.parser.close(reason='shutdown')
        self.scheduler.close(reason='shutdown')

    def stop(self):
        if not self.running:
            raise RuntimeError("Engine not running")
        self.running = False

        if self.spider is not None:
            self.close_spider(self.spider, reason="shutdown")

        self.explorer.close(reason='shutdown')
        self.parser.close(reason='shutdown')
        self.scheduler.close(reason='shutdown')
        signals.engine_stopped.send(sender=self.crawler.hostname)

    def close_spider(self, spider, reason="cancelled"):
        logger.info("Closing spider (%(reason)s)", {'reason': reason}, extra={'spider': spider})

        if not self._spider_closed.is_set():
            self._spider_closed.set()

        end_time = round(time.time(), 4)
        format_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(end_time)))
        self.crawler.stats.set_value("time/end_time", end_time)
        self.crawler.stats.set_value("time/end_time/format", format_end_time)
        consumed_time = int(end_time - self.crawler.stats.get_value('time/start_time', end_time))
        self.crawler.stats.set_value("time/consumed_time", consumed_time)

        self.broker.stop()
        self.explorer.close(reason=reason)
        self.parser.close(reason=reason)
        self.scheduler.close(reason=reason)

        self.spider.close(reason=reason)

        self.crawler.stats.close_spider(spider, reason=reason)
        setattr(self, 'spider', None)

        self.crawler.crawling = False  # finish crawling.
        signals.spider_closed.send(sender=self)

        logger.info("Spider closed (%(reason)s)", {'reason': reason})
