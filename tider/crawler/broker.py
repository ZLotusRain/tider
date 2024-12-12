from tider.utils.misc import symbol_by_name


class BrokersManager:

    transports = {
        'default': 'tider.brokers:DummyBroker',
        'files': 'tider.brokers.files:FilesBroker',
        'redis': 'tider.brokers.amqp:AMQPBroker',
        'amqp': 'tider.brokers.amqp:AMQPBroker',
        'amqps': 'tider.brokers.amqp:AMQPBroker',
        'pyamqp': 'tider.brokers.amqp:AMQPBroker',
        'librabbitmq': 'tider.brokers.amqp:AMQPBroker',
        'confluentkafka': 'tider.brokers.amqp:AMQPBroker',
        'memory': 'tider.brokers.amqp:AMQPBroker',
        'rediss': 'tider.brokers.amqp:AMQPBroker',
        'SQS': 'tider.brokers.amqp:AMQPBroker',
        'sqs': 'tider.brokers.amqp:AMQPBroker',
        'mongodb': 'tider.brokers.amqp:AMQPBroker',
        'zookeeper': 'tider.brokers.amqp:AMQPBroker',
        'sqlalchemy': 'tider.brokers.amqp:AMQPBroker',
        'sqla': 'tider.brokers.amqp:AMQPBroker',
        'SLMQ': 'tider.brokers.amqp:AMQPBroker',
        'slmq': 'tider.brokers.amqp:AMQPBroker',
        'filesystem': 'tider.brokers.amqp:AMQPBroker',
        'qpid': 'tider.brokers.amqp:AMQPBroker',
        'sentinel': 'tider.brokers.amqp:AMQPBroker',
        'consul': 'tider.brokers.amqp:AMQPBroker',
        'etcd': 'tider.brokers.amqp:AMQPBroker',
        'azurestoragequeues': 'tider.brokers.amqp:AMQPBroker',
        'azureservicebus': 'tider.brokers.amqp:AMQPBroker',
        'pyro': 'tider.brokers.amqp:AMQPBroker'
    }

    def __init__(self, crawler, custom_transports=None):
        super().__init__()

        self.crawler = crawler
        self.custom_transports = dict(custom_transports or {})

        self._brokers = set()
        self._consuming = False

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler=crawler,
            custom_transports=crawler.settings.get('BROKER_CUSTOM_TRANSPORTS'),
        )

    def update_transports(self, transports):
        self.transports.update(transports)

    def _instantiate_broker(self, transport, queues=None, on_message=None, on_message_consumed=None):
        try:
            transport_cls = self.custom_transports[transport]
        except KeyError:
            try:
                transport_cls = self.transports[transport]
            except KeyError:
                raise ValueError(f'Not transported transport: `{transport}`')
        transport_cls = symbol_by_name(transport_cls)
        return transport_cls(crawler=self.crawler,
                             queues=queues,
                             on_message=on_message,
                             on_message_consumed=on_message_consumed,)

    def consume(self, transport='default', queues=None, on_message=None, on_messages_consumed=None):
        broker = self._instantiate_broker(transport, queues, on_message, on_messages_consumed)
        self._brokers.add(broker)
        broker.start()

    def stop(self):
        for broker in self._brokers:
            broker.stop()
