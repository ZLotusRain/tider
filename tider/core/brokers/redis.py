import time
import json
import logging
from kombu.utils import cached_property

from .base import Broker
from tider.backends.redis import RedisBackend

logger = logging.getLogger(__name__)


class RedisBroker(Broker):

    @cached_property
    def client(self):
        broker_url = self.tider.settings.get('BROKER_URL')
        conn_kwargs = self.tider.settings.getdict('BROKER_CONN_KWARGS').copy()
        return RedisBackend(url=broker_url, **conn_kwargs)

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

    def consume(self, key=None, count=1, validate=True):
        queue_name = key or self.queue_name
        result = self.client.lpop(queue_name, count=count)
        if not isinstance(result, list):
            result = [result]
        result = result[0]
        if result:
            result = result.decode()
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                result = result
            if validate:
                result = result['body']
        return result

    def close(self):
        self.client.close()
