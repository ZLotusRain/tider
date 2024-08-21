from abc import abstractmethod
from typing import Any, Optional
from queue import PriorityQueue, Empty

from tider.backends.redis import RedisBackend
from tider.utils.serialize import pickle_loads, pickle_dumps


QUEUE_MAX_SIZE = 12000


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


class PriorityRedisQueue(BaseQueue):
    def __init__(self, key, url=None, **connection_kwargs):
        self.key = key
        self.redis_client = RedisBackend(url=url, **connection_kwargs)

    @classmethod
    def from_settings(cls, settings, tider=None):
        url = settings.get("SCHEDULER_URL")
        key = settings.get("SCHEDULER_KEY")
        if not key and tider:
            key = f'tider.engine.scheduler.{tider.spidername}'
        connection_kwargs = settings.get("SCHEDULER_CONNECTION_KWARGS", {})
        return cls(key=key, url=url, **connection_kwargs)

    @classmethod
    def from_tider(cls, tider):
        settings = tider.settings
        return cls.from_settings(settings, tider)

    def push(self, request):
        priority = request.priority
        pickled_request = pickle_dumps(request)
        mapping = {pickled_request: priority}
        self.redis_client.zadd(key=self.key, mapping=mapping)

    def pop(self):
        results = self.redis_client.zget(key=self.key, pop=True, count=1)
        if not results:
            return None
        result = results[0]
        request = pickle_loads(result)
        return request

    def pop_all(self):
        results = self.redis_client.zget(key=self.key, pop=True, count=len(self))
        results = [pickle_loads(result) for result in results]
        return results

    def peek(self):
        results = self.redis_client.zget(key=self.key, pop=False, count=1)
        if not results:
            return None
        result = results[0]
        request = pickle_loads(result)
        return request

    def __len__(self):
        return self.redis_client.get_zset_length(self.key)

    def close(self):
        self.redis_client.close()


class PriorityMemoryQueue(BaseQueue):
    def __init__(self):
        self.queue = PriorityQueue()

    def push(self, request):
        self.queue.put(request)

    def pop(self):
        try:
            return self.queue.get_nowait()
        except Empty:
            return None

    def pop_all(self):
        results = []
        while len(self) > 0:
            result = self.pop()
            if not result:
                continue
            results.append(result)
        return results

    def __len__(self):
        return self.queue.qsize()

    def close(self):
        self.pop_all()
        self.queue.queue.clear()
        self.queue.queue = []
