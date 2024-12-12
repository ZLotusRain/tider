import queue
import threading
from abc import abstractmethod
from typing import Any, Optional

from tider.utils.misc import build_from_crawler


class _BaseQueueMeta(type):
    """
    Metaclass to check queue classes against the necessary interface
    """

    def __instancecheck__(cls, instance):
        return cls.__subclasscheck__(type(instance))  # pylint: disable=no-value-for-parameter

    def __subclasscheck__(cls, subclass):
        return (
            hasattr(subclass, "push")
            and callable(subclass.push)
            and hasattr(subclass, "pop")
            and callable(subclass.pop)
            and hasattr(subclass, "close")
            and callable(subclass.close)
            and hasattr(subclass, "__len__")
            and callable(subclass.__len__)
        )


class BaseQueue(metaclass=_BaseQueueMeta):
    @abstractmethod
    def push(self, obj: Any) -> None:
        raise NotImplementedError()

    @abstractmethod
    def pop(self) -> Optional[Any]:
        raise NotImplementedError()

    @abstractmethod
    def __len__(self):
        raise NotImplementedError()

    def close(self) -> None:
        pass


class PriorityQueue:
    """A priority queue implemented using multiple internal queues (typically,
    FIFO queues). It uses one internal queue for each priority value."""

    @classmethod
    def from_crawler(cls, crawler, downstream_queue_cls, startprios=()):
        return cls(crawler, downstream_queue_cls, startprios)

    def __init__(self, crawler, downstream_queue_cls, startprios=()):
        self.crawler = crawler
        self.downstream_queue_cls = downstream_queue_cls
        self.queues = {}

        self._mutex = threading.Lock()
        self.priority_change = threading.Condition(self._mutex)
        self.curprio = None
        self.init_prios(startprios)

    def init_prios(self, startprios):
        if not startprios:
            return

        for priority in startprios:
            self.queues[priority] = self.qfactory()

        self.curprio = min(startprios)

    def qfactory(self):
        downstream_queue = build_from_crawler(
            self.downstream_queue_cls,
            self.crawler,
        )
        if not isinstance(downstream_queue, BaseQueue):
            raise ValueError("Priority downstream queue must be the subclass of `BaseQueue`")
        return downstream_queue

    def push(self, request):
        priority = request.priority
        with self.priority_change:
            if priority not in self.queues:
                self.queues[priority] = self.qfactory()
            q = self.queues[priority]
            q.push(request)  # this may fail (eg. serialization error)
            if self.curprio is None or priority < self.curprio:
                self.curprio = priority

    def pop(self):
        with self.priority_change:
            if self.curprio is None:
                return
            q = self.queues[self.curprio]
            m = q.pop()
            if not q:
                del self.queues[self.curprio]
                q.close()
                prios = [p for p, q in self.queues.items() if q]
                self.curprio = min(prios) if prios else None
            return m

    def peek(self):
        """Returns the next object to be returned by :meth:`pop`,
        but without removing it from the queue.

        Raises :exc:`NotImplementedError` if the underlying queue class does
        not implement a ``peek`` method, which is optional for queues.
        """
        with self.priority_change:
            if self.curprio is None:
                return None
            curprio_queue = self.queues[self.curprio]
            return curprio_queue.peek()

    def close(self):
        active = []
        for p, q in self.queues.items():
            active.append(p)
            q.close()
        return active

    def __len__(self):
        return sum(len(x) for x in list(iter(self.queues.values()))) if self.queues else 0


class LifoMemoryQueue(queue.LifoQueue):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(maxsize=crawler.settings.get('SCHEDULER_DOWNSTREAM_QUEUE_MAXSIZE') or 0)

    def push(self, request):
        self.put(request)

    def pop(self):
        try:
            return self.get_nowait()
        except queue.Empty:
            return None

    def __len__(self):
        return self.qsize()

    def close(self):
        self.queue.clear()


class FifoMemoryQueue(queue.Queue):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(maxsize=crawler.settings.get('SCHEDULER_DOWNSTREAM_QUEUE_MAXSIZE') or 0)

    def push(self, request):
        self.put(request)

    def pop(self):
        try:
            return self.get_nowait()
        except queue.Empty:
            return None

    def __len__(self):
        return self.qsize()

    def close(self):
        self.queue.clear()
