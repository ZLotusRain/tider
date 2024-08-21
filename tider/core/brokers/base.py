import time
from collections import namedtuple
from kombu.utils.objects import cached_property

request_message = namedtuple('message',
                             ('headers', 'body'))


class Broker:

    def __init__(self, tider):
        self.tider = tider
        self.spidername = tider.spidername
        self.limit = tider.settings.get('BROKER_LIMIT', 10000)

    @cached_property
    def client(self):
        raise NotImplementedError

    @cached_property
    def queue_name(self):
        return f'tider.broker.{self.spidername}'

    def create_message(self, expires=None, retries=0, kwargs=None):
        kwargs = {} if kwargs is None else kwargs
        headers = {
            'lang': 'py',
            'task': self.spidername,
            'publish_time': round(time.time(), 4),
            'publish_time_format': time.strftime('%Y-%m-%d %H:%M:%S'),
            'expires': expires,
            'retries': retries
        }
        message = {
            'headers': headers,
            'body': kwargs.copy()
        }
        return message

    def produce(self, request):
        raise NotImplementedError

    def consume(self, key=None, count=1, validate=True):
        raise NotImplementedError

    def close(self):
        pass
