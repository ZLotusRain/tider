import logging
import time
import json

from .base import Broker
from tider.backends.redis import RedisBackend
from tider.utils.decorators import cached_property

logger = logging.getLogger(__name__)


class RedisBroker(Broker):

    @cached_property
    def client(self):
        broker_url = self.crawler.settings.get('BROKER_URL')
        conn_kwargs = self.crawler.settings.getdict('BROKER_CONN_KWARGS').copy()
        concurrency = self.crawler.settings['CONCURRENCY']
        return RedisBackend(url=broker_url, max_connections=concurrency, **conn_kwargs)

    def produce(self, request):
        queue_name = request.meta.get('queue_name') or self.queue_name
        while self.client.get_list_length(queue_name) > self.limit:
            logger.warning(f'Exceed broker limit, queue: {queue_name}')
            time.sleep(2)

        message = self.create_message(
            retries=request.max_retry_times,
            kwargs=request.cb_kwargs
        )
        message = json.dumps(message, ensure_ascii=False)
        self.client.rpush(queue_name, message)
        logger.info(f'Publish to broker successfully, queue: {queue_name}, '
                    f'message: {message}')

    def consume(self, queue=None):
        queue_name = queue or self.queue_name
        try:
            result = self.client.lpop(queue_name, count=1)
            result = json.loads(result[0].decode())
            if 'body' in result:
                result = result['body']
            return result
        except IndexError:
            return None

    def close(self):
        self.client.close()
