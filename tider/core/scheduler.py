import logging
from abc import abstractmethod

from tider.dupefilters import BaseDupeFilter
from tider.structures.pqueues import BaseQueue
from tider.utils.decorators import cached_property
from tider.utils.misc import symbol_by_name, create_instance


logger = logging.getLogger(__name__)


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
    def from_crawler(cls, tider):
        return cls()

    @abstractmethod
    def has_pending_requests(self) -> bool:
        """
        ``True`` if the scheduler has enqueued objects, ``False`` otherwise
        """
        raise NotImplementedError()

    @abstractmethod
    def enqueue_request(self, request) -> bool:
        """
        Return ``True`` if the object is stored correctly, ``False`` otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def next_request(self):
        """
        Return the next object to be processed, or ``None`` to indicate that there
        are no objects to be considered ready at the moment.
        """
        raise NotImplementedError()

    def close(self, reason):
        """
        Called when the spider is closed by the engine. It receives the reason why the crawl
        finished as argument, and it's useful to execute cleaning code.

        :param reason: a string which describes the reason why the spider was closed
        :type reason: :class:`str`
        """
        pass


class Scheduler(BaseScheduler):

    def __init__(self, tider, pqclass=None, dfclass=None):
        self.tider = tider
        self.pqclass = pqclass
        self.dfclass = dfclass

    @classmethod
    def from_crawler(cls, tider):
        settings = tider.settings
        return cls(
            tider=tider,
            pqclass=settings['SCHEDULER_PRIORITY_QUEUE'],
            dfclass=settings.get("DUPEFILTER_CLASS")
        )

    def __len__(self):
        return len(self.pq)

    def open(self):
        if self.dupefilter:
            self.dupefilter.open()

    @cached_property
    def pq(self):
        return self._pq()

    def _pq(self):
        """ Create a new priority queue instance, with in-memory storage """
        pqclass = symbol_by_name(self.pqclass)
        if not issubclass(pqclass, BaseQueue):
            raise ValueError("Priority queue must inherit from `BaseQueue`")
        return create_instance(pqclass, self.tider.settings, self.tider)

    @cached_property
    def dupefilter(self):
        return self._dupefilter()

    def _dupefilter(self):
        if not self.dfclass:
            return None
        dfclass = symbol_by_name(self.dfclass)
        if not issubclass(dfclass, BaseDupeFilter):
            raise ValueError("Duplicate filter must implement the interface `request_seen`")
        return create_instance(dfclass, self.tider.settings, self.tider)

    def enqueue_request(self, request):
        if request.dup_check and self.dupefilter and self.dupefilter.request_seen(request):
            self.dupefilter.log(request)
            self.tider.stats.inc_value('filter/request/filtered')
            return False
        try:
            self.pq.push(request)
        except Exception as e:
            logger.error(f"Unable to push {request}: {e}", exc_info=True)
            self.tider.stats.inc_value('scheduler/request/failed')
            return False
        else:
            self.tider.stats.inc_value('scheduler/request/enqueued')
            return True

    def next_request(self):
        request = self.pq.pop()
        if request is not None:
            self.tider.stats.inc_value('scheduler/request/dequeued')
        return request

    def has_pending_requests(self):
        return len(self.pq) > 0

    def close(self, reason):
        self.pq.close()
        if self.dupefilter:
            self.dupefilter.close(reason)
        logger.info(f"Scheduler closed ({reason})")
