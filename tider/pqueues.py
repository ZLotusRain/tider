import queue
import threading
from typing import Optional, Dict, List, Iterable, Protocol, cast

from tider import Request
from tider.utils.collections import DummyLock
from tider.utils.misc import build_from_crawler


class QueueProtocol(Protocol):
    """Protocol for downstream queues of ``PriorityQueue``."""

    def push(self, request: Request) -> None: ...

    def pop(self) -> Optional[Request]: ...

    def __len__(self) -> int: ...

    def empty(self) -> bool: ...

    def close(self) -> None: ...


class PriorityQueue:
    """A priority queue implemented using multiple internal queues (typically,
    FIFO queues). It uses one internal queue for each priority value.

    Only integer priorities should be used. Lower numbers are higher priorities.
    """

    @classmethod
    def from_crawler(cls, crawler, downstream_queue_cls: QueueProtocol,
                     startprios: Iterable[int] = (),
                     start_queue_cls: QueueProtocol = None):
        return cls(crawler, downstream_queue_cls, startprios,
                   start_queue_cls=start_queue_cls)

    def __init__(self, crawler, downstream_queue_cls: QueueProtocol,
                 startprios: Iterable[int] = (),
                 start_queue_cls: QueueProtocol = None):
        self.crawler = crawler
        self.downstream_queue_cls = downstream_queue_cls
        self._start_queue_cls = start_queue_cls
        self.queues: dict[int, QueueProtocol] = {}
        self._start_queues: dict[int, QueueProtocol] = {}
        self._mutex = threading.Lock()
        self.priority_change = threading.Condition(self._mutex)
        self.curprio = None
        self.init_prios(startprios)

    def init_prios(self, startprios):
        if not startprios:
            return

        for priority in startprios:
            q = self.qfactory()
            if q:
                self.queues[priority] = q
            if self._start_queue_cls:
                q = self._sqfactory()
                if q:
                    self._start_queues[priority] = q

        self.curprio = min(startprios)

    def qfactory(self) -> QueueProtocol:
        return build_from_crawler(
            self.downstream_queue_cls,
            self.crawler,
        )

    def _sqfactory(self) -> QueueProtocol:
        assert self._start_queue_cls is not None
        return build_from_crawler(
            self._start_queue_cls,
            self.crawler,
        )

    def priority(self, request) -> int:
        return -request.priority

    def push(self, request):
        priority = self.priority(request)
        is_start_request = request.meta.get("is_start_request", False)
        with self.priority_change:
            if is_start_request and self._start_queue_cls:
                if priority not in self._start_queues:
                    self._start_queues[priority] = self._sqfactory()
                q = self._start_queues[priority]
            else:
                if priority not in self.queues:
                    self.queues[priority] = self.qfactory()
                q = self.queues[priority]
            q.push(request)  # this may fail (eg. serialization error)
            if self.curprio is None or priority < self.curprio:
                self.curprio = priority

    def pop(self):
        if self.curprio is None:
            return None
        with self.priority_change:
            try:
                q = self.queues[self.curprio]
            except KeyError:
                pass
            else:
                m = q.pop()
                if q.empty():  # don't use `not q` to compatible for gevent > 24.11.1
                    del self.queues[self.curprio]
                    q.close()
                    if not self._start_queues:
                        self._update_curprio()
                return m
            if self._start_queues:
                try:
                    q = self._start_queues[self.curprio]
                except KeyError:
                    self._update_curprio()
                else:
                    m = q.pop()
                    if q.empty():  # don't use `not q` to compatible for gevent > 24.11.1
                        del self._start_queues[self.curprio]
                        q.close()
                        self._update_curprio()
                    return m
            else:
                self._update_curprio()

    def _update_curprio(self) -> None:
        prios = {
            p
            for queues in (self.queues, self._start_queues)
            for p, q in queues.items() if q
        }
        self.curprio = min(prios) if prios else None

    def peek(self):
        """Returns the next object to be returned by :meth:`pop`,
        but without removing it from the queue.

        Raises :exc:`NotImplementedError` if the underlying queue class does
        not implement a ``peek`` method, which is optional for queues.
        """
        if self.curprio is None:
            return None
        with self.priority_change:
            try:
                curprio_queue = self._start_queues[self.curprio]
            except KeyError:
                curprio_queue = self.queues[self.curprio]
            # Protocols can't declare optional members
            return cast("Request", curprio_queue.peek())  # type: ignore[attr-defined]

    def close(self):
        active = set()
        for queues in (self.queues, self._start_queues):
            for p, q in queues.items():
                active.add(p)
                q.close()
        return list(active)

    def __len__(self):
        return (
            sum(
                len(x)
                for queues in (self.queues, self._start_queues)
                for x in queues.values()
            )
            if self.queues or self._start_queues
            else 0
        )


class ExplorerInterface:
    def __init__(self, crawler):
        assert crawler.engine
        self.explorer = crawler.engine.explorer

    def stats(self, possible_slots: Iterable[str]):
        return [(self._dispatched_concurrency(slot), slot) for slot in possible_slots if self._dispatched_concurrency(slot)]

    def get_slot_key(self, request) -> str:
        matched = self.explorer.climits.match(request)
        if matched:
            matched = min(matched)
            return ":".join((matched[1], matched[2]))
        return 'unlimited'

    def _dispatched_concurrency(self, slot):
        if slot == 'unlimited':
            return self.explorer.concurrency
        limit_type, pattern = slot.split(':', maxsplit=1)
        # maybe not available
        return min(self.explorer.climits.qsize(limit_type, pattern), self.explorer.concurrency)


class ExplorerAwarePriorityQueue:
    """PriorityQueue which takes Explorer activity into account:
    domains/urls with the available slots and the least amount of dispatched concurrency are dequeued
    first.
    """

    @classmethod
    def from_crawler(cls, crawler, downstream_queue_cls, startprios=None, start_queue_cls=None):
        return cls(crawler, downstream_queue_cls, startprios, start_queue_cls=start_queue_cls)

    def __init__(self, crawler, downstream_queue_cls, slot_startprios=None, start_queue_cls=None):
        if slot_startprios and not isinstance(slot_startprios, dict):
            raise ValueError(
                "ExplorerAwarePriorityQueue accepts "
                "``slot_startprios`` as a dict; "
                f"{slot_startprios.__class__!r} instance "
                "is passed. Most likely, it means the state is"
                "created by an incompatible priority queue. "
                "Only a crawl started with the same priority "
                "queue class can be resumed."
            )

        self._explorer_interface = ExplorerInterface(crawler)
        self.downstream_queue_cls = downstream_queue_cls
        self._start_queue_cls = start_queue_cls
        self.crawler = crawler
        self._slot_mutex = threading.Lock()
        self._dummy_mutex = DummyLock()
        self.pqueues: Dict[str, PriorityQueue] = {}  # slot -> priority queue
        for slot, startprios in (slot_startprios or {}).items():
            self.pqueues[slot] = self.pqfactory(startprios)
        self.pqueues.setdefault('unlimited', self.pqfactory())

    def pqfactory(
            self, startprios: Iterable[int] = ()
    ) -> PriorityQueue:
        return PriorityQueue(
            self.crawler,
            self.downstream_queue_cls,
            startprios,
            start_queue_cls=self._start_queue_cls,
        )

    def pop(self):
        stats = self._explorer_interface.stats(self.pqueues)

        if not stats:
            return None

        slot = min(stats)[1]
        slot_queue = self.pqueues[slot]
        request = slot_queue.pop()
        if slot != 'unlimited':
            with self._slot_mutex:
                if len(slot_queue) == 0 and slot in self.pqueues:
                    del self.pqueues[slot]
        return request

    def push(self, request):
        slot = self._explorer_interface.get_slot_key(request)
        mutex = self._slot_mutex if slot != 'unlimited' else self._dummy_mutex
        with mutex:
            if slot not in self.pqueues:
                self.pqueues[slot] = self.pqfactory()
            slot_queue = self.pqueues[slot]
            slot_queue.push(request)

    def peek(self):
        """Returns the next object to be returned by :meth:`pop`,
        but without removing it from the queue.

        Raises :exc:`NotImplementedError` if the underlying queue class does
        not implement a ``peek`` method, which is optional for queues.
        """
        stats = self._explorer_interface.stats(self.pqueues)
        if not stats:
            return None
        slot = min(stats)[1]
        with self._slot_mutex:
            slot_queue = self.pqueues[slot]
            return slot_queue.peek()

    def close(self) -> Dict[str, List[int]]:
        # dictionary changed size during iteration
        active = {slot: q.close() for slot, q in dict(self.pqueues).items()}
        self.pqueues.clear()
        return active

    def __len__(self) -> int:
        return sum(len(x) for x in list(self.pqueues.values())) if self.pqueues else 0

    def __contains__(self, slot: str) -> bool:
        return slot in self.pqueues


class LifoMemoryQueue(queue.LifoQueue):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(maxsize=crawler.settings.get('SCHEDULER_DOWNSTREAM_QUEUE_MAXSIZE') or -1)

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
        return cls(maxsize=crawler.settings.get('SCHEDULER_DOWNSTREAM_QUEUE_MAXSIZE') or -1)

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
