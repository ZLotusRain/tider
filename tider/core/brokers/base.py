import time
from collections import namedtuple

from tider.utils.spider import get_spider_name
from tider.utils.decorators import cached_property

request_message = namedtuple('message',
                             ('headers', 'body'))


class Broker:

    def __init__(self, crawler):
        self.crawler = crawler
        self.limit = crawler.settings.get('BROKER_LIMIT', 10000)

    @cached_property
    def client(self):
        raise NotImplementedError

    @cached_property
    def spider_name(self):
        return get_spider_name(self.crawler.spider)

    @cached_property
    def queue_name(self):
        return f'tider.broker.{self.spider_name}'

    def create_message(self, expires=None, retries=0, kwargs=None):
        kwargs = {} if kwargs is None else kwargs
        headers = {
            'lang': 'py',
            'task': self.spider_name,
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

    def consume(self, queue=None):
        raise NotImplementedError

    def close(self):
        pass
