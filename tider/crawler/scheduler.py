from abc import abstractmethod

from kombu.utils import cached_property

from tider.utils.log import get_logger
from tider.utils.misc import symbol_by_name, build_from_crawler


logger = get_logger(__name__)


class BaseSchedulerMeta(type):
    """
    Metaclass to check scheduler classes against the necessary interface
    """
    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))

    def __subclasscheck__(cls, subclass):
        return (
            hasattr(subclass, "has_pending_requests") and callable(subclass.has_pending_requests)
            and hasattr(subclass, "enqueue_request") and callable(subclass.enqueue_request)
            and hasattr(subclass, "next_request") and callable(subclass.next_request)
        )


class BaseScheduler(metaclass=BaseSchedulerMeta):

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    @abstractmethod
    def has_pending_requests(self) -> bool:
        """
        ``True`` if the scheduler has enqueued objects, ``False`` otherwise
        """
        raise NotImplementedError

    @abstractmethod
    def enqueue_request(self, request) -> bool:
        """
        Return ``True`` if the object is stored correctly, ``False`` otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def next_request(self):
        """
        Return the next object to be processed, or ``None`` to indicate that there
        are no objects to be considered ready at the moment.
        """
        raise NotImplementedError

    def close(self, reason):
        """
        Called when the spider is closed by the engine. It receives the reason why the crawl
        finished as argument, and it's useful to execute cleaning code.

        :param reason: a string which describes the reason why the spider was closed
        :type reason: :class:`str`
        """
        pass


class Scheduler(BaseScheduler):

    def __init__(self, crawler, pqueue_cls=None, mqueue_cls=None, df_cls=None, body_priority_adjust=0):
        self.crawler = crawler

        self.dupefilter = None
        if df_cls:
            df_cls = symbol_by_name(df_cls)
            self.dupefilter = build_from_crawler(df_cls, self.crawler.settings, self.crawler)

        self.pq_cls = pqueue_cls
        self.mq_cls = mqueue_cls
        self.body_priority_adjust = body_priority_adjust

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            crawler=crawler,
            pqueue_cls=settings['SCHEDULER_PRIORITY_QUEUE'],
            mqueue_cls=settings['SCHEDULER_MEMORY_QUEUE'],
            df_cls=settings.get("DUPEFILTER_CLASS"),
            body_priority_adjust=settings.get("body_priority_adjust"),
        )

    def __len__(self):
        return len(self.priority_queue)

    def _start_queue_cls(self):
        cls = self.crawler.settings[f"SCHEDULER_START_QUEUE"]
        if not cls:
            return None
        return symbol_by_name(cls)

    @cached_property
    def priority_queue(self):
        return self._pq()

    def _pq(self):
        """ Create a new priority queue instance, with in-memory storage """
        assert self.crawler
        assert self.pq_cls

        pqclass = symbol_by_name(self.pq_cls)
        return build_from_crawler(
            pqclass,
            self.crawler,
            downstream_queue_cls=symbol_by_name(self.mq_cls),
            start_queue_cls=self._start_queue_cls(),
        )

    def enqueue_request(self, request):
        """
        Return ``True`` if the request was stored successfully, ``False`` otherwise.
        """
        if request.dup_check:
            if self.dupefilter.request_seen(request):
                self.dupefilter.log(request)
                return False
        try:
            if self.body_priority_adjust and request.body:
                multiplier = len(request.body) // 1024
                request.priority -= self.body_priority_adjust * multiplier
                request.priority = max(0, request.priority)
            self.priority_queue.push(request)
        except Exception as e:
            logger.error(f"Unable to store {request}: {e}", exc_info=True)
            self.crawler.stats.inc_value('scheduler/failed')
            return False
        else:
            self.crawler.stats.inc_value('scheduler/enqueued')
            return True

    def next_request(self):
        request = self.priority_queue.pop()
        if request is not None:
            self.crawler.stats.inc_value('scheduler/dequeued')
        return request

    def has_pending_requests(self):
        return len(self) > 0

    def clear(self):
        self.priority_queue.close()

    def close(self, reason):
        self.clear()
        if self.dupefilter is not None:
            self.dupefilter.close(reason)
        logger.info(f"Scheduler closed ({reason})")
