import re
from datetime import datetime, timedelta
from kombu.utils import cached_property
from kombu.utils.url import urlparse, maybe_sanitize_url
from kombu.exceptions import EncodeError

from tider.backends.base import BaseBackend, State
from tider.exceptions import ImproperlyConfigured

try:
    import pymongo
except ImportError:
    pymongo = None

if pymongo:
    try:
        from bson.binary import Binary
    except ImportError:
        from pymongo.binary import Binary
    from pymongo import ReadPreference
    from pymongo.errors import InvalidDocument

    SUPPORTS_MONGODB32 = tuple(int(v) if v.isdigit() else v
                               for v in pymongo.__version__.split('.')) < (4, 0, 0)
else:
    Binary = None
    SUPPORTS_MONGODB32 = False

    class ReadPreference:
        pass

    class InvalidDocument(Exception):
        pass

__all__ = ('MongoBackend',)

BINARY_CODECS = frozenset(['pickle', 'msgpack'])


class MongoBackend(BaseBackend):
    """MongoDB stats backend.

    Raises:
        tider.exceptions.ImproperlyConfigured:
            if module :pypi:`pymongo` is not available.
    """

    MODES = {
        'primary': ReadPreference.PRIMARY,
        'primaryPreferred': ReadPreference.PRIMARY_PREFERRED,
        'secondary': ReadPreference.SECONDARY,
        'secondaryPreferred': ReadPreference.SECONDARY_PREFERRED,
        'nearest': ReadPreference.NEAREST,
    }

    mongo_host = None
    host = 'localhost'
    port = 27017
    username = None
    password = None
    database_name = 'tider'
    crawlermeta_collection = 'tider_crawlermeta'
    max_pool_size = 10
    options = None

    supports_autoexpire = False

    _connection = None

    def __init__(self, crawler=None, replica_set=None, read_preference=None, **kwargs):
        self.options = {}
        super().__init__(crawler, **kwargs)

        if not pymongo:
            raise ImproperlyConfigured(
                'You need to install the pymongo library to use the '
                'MongoDB backend.')

        # Set option defaults
        for key, value in self._prepare_client_options().items():
            self.options.setdefault(key, value)
        self.replica_set = replica_set
        self.read_preference = read_preference

        # update conf with mongo uri data, only if uri was given
        if self.url:
            self.url = self._ensure_mongodb_uri_compliance(self.url)

            uri_data = pymongo.uri_parser.parse_uri(self.url)
            # build the hosts list to create a mongo connection
            hostslist = [
                f'{x[0]}:{x[1]}' for x in uri_data['nodelist']
            ]
            self.username = uri_data['username']
            self.password = uri_data['password']
            self.mongo_host = hostslist
            if uri_data['database']:
                # if no database is provided in the uri, use default
                self.database_name = uri_data['database']

            self.options.update(uri_data['options'])

        # update conf with specific settings
        config = self.crawler.settings.get(f'{self.config_namespace}_MONGODB_SETTINGS')
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'MongoDB backend settings should be grouped in a dict')
            config = dict(config)  # don't modify original

            if 'host' in config or 'port' in config:
                # these should take over uri conf
                self.mongo_host = None

            self.host = config.pop('host', self.host)
            self.port = config.pop('port', self.port)
            self.mongo_host = config.pop('mongo_host', self.mongo_host)
            self.username = config.pop('username', self.username)
            self.password = config.pop('password', self.password)
            self.database_name = config.pop('database', self.database_name)
            self.replica_set = config.pop('replica_set', self.replica_set)
            self.read_preference = config.pop('read_preference', self.read_preference)
            self.crawlermeta_collection = config.pop(
                'crawlermeta_collection', self.crawlermeta_collection,
            )

            self.options.update(config.pop('options', {}))
            self.options.update(config)

    @staticmethod
    def _ensure_mongodb_uri_compliance(url):
        parsed_url = urlparse(url)
        if not parsed_url.scheme.startswith('mongodb'):
            url = f'mongodb+{url}'

        if url == 'mongodb://':
            url += 'localhost'

        return url

    def _prepare_client_options(self):
        if pymongo.version_tuple >= (3,):
            return {'maxPoolSize': self.max_pool_size}
        else:  # pragma: no cover
            return {'max_pool_size': self.max_pool_size,
                    'auto_start_request': False}

    def _get_connection(self):
        """Connect to the MongoDB server."""
        if self._connection is None:
            from pymongo import MongoClient

            host = self.mongo_host
            if not host:
                # The first pymongo.Connection() argument (host) can be
                # a list of ['host:port'] elements or a mongodb connection
                # URI.  If this is the case, don't use self.port
                # but let pymongo get the port(s) from the URI instead.
                # This enables the use of replica sets and sharding.
                # See pymongo.Connection() for more info.
                host = self.host
                if isinstance(host, str) \
                   and not host.startswith('mongodb://'):
                    host = f'mongodb://{host}:{self.port}'
            # don't change self.options
            conf = dict(self.options)
            conf['host'] = host
            if self.read_preference:
                conf['read_preference'] = self.MODES.get(self.read_preference)
            conf['replicaSet'] = self.replica_set

            self._connection = MongoClient(**conf)

        return self._connection

    def encode(self, data):
        if self.serializer == 'bson':
            # mongodb handles serialization
            return data
        payload = super().encode(data)

        # serializer which are in an unsupported format (pickle/binary)
        if self.serializer in BINARY_CODECS:
            payload = Binary(payload)
        return payload

    def decode(self, data):
        if self.serializer == 'bson':
            return data
        return super().decode(data)

    def _store_result(self, result, state, traceback=None, **kwargs):
        """Store stats of a crawler."""
        meta = self._gen_crawler_meta(result=self.encode(result), state=state, traceback=traceback)
        # Add the _id for mongodb
        meta['_id'] = self.crawler.hostname

        try:
            self.collection.replace_one({'_id': self.crawler.hostname}, meta, upsert=True)
        except InvalidDocument as exc:
            raise EncodeError(exc)

    def _get_crawler_meta(self):
        """Get crawler meta-data."""
        obj = self.collection.find_one({'_id': self.crawler.hostname})
        if obj:
            return self.meta_from_decoded({
                'crawler_id': obj['_id'],
                'status': obj['status'],
                'stats': self.decode(obj['stats']),
                'date_done': obj['date_done'],
                'traceback': obj['traceback'],
            })
        return {'status': State.PENDING, 'result': None}

    def _get_group_meta(self):
        """Get all the meta-datas for a group."""
        objs = self.collection.find({'_id': {"$regex": re.compile(f'{self.crawler.group}.*')}}) or []
        result = {}
        for obj in objs:
            meta = self.meta_from_decoded({
                '_id': obj['_id'],
                'schema': obj['schema'],
                'spidername': obj['spidername'],
                'pid': obj['pid'],
                'server': obj['server'],
                'group': obj['group'],
                'status': obj['status'],
                'stats': self.decode(obj['stats']),
                'failures': obj['failures'],
                'errors': obj['errors'],
                'traceback': obj['traceback'],
                'date_done': obj['date_done'],
            })
            _id = meta.pop('_id')
            result[_id] = meta
        return result

    def _delete_group(self):
        """Delete a group."""
        self.collection.delete_many({'_id': {"$regex": re.compile(f'{self.crawler.group}.*')}})

    def _forget(self):
        """Remove result from MongoDB.

        Raises:
            pymongo.exceptions.OperationsError:
                if the data could not be removed.
        """
        # By using safe=True, this will wait until it receives a response from
        # the server.  Likewise, it will raise an OperationsError if the
        # response was unable to be completed.
        self.collection.delete_one({'_id': self.crawler.hostname})

    def cleanup(self):
        """Delete expired meta-data."""
        if not self.expires:
            return

        self.collection.delete_many(
            {'date_done': {'$lt': self.crawler.app.now() - self.expires_delta}},
        )

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return super().__reduce__(
            args, dict(kwargs, expires=self.expires, url=self.url))

    def _get_database(self):
        conn = self._get_connection()
        database = conn[self.database_name]
        if self.username or self.password:
            database.authenticate(self.username, self.password)
        return database

    @cached_property
    def database(self):
        """Get database from MongoDB connection.

        performs authentication if necessary.
        """
        return self._get_database()

    @cached_property
    def collection(self):
        """Get the meta-data crawler collection."""
        collection = self.database[self.crawlermeta_collection]

        # Ensure an index on date_done is there, if not process the index
        # in the background.  Once completed cleanup will be much faster
        collection.create_index('date_done', background=True, expireAfterSeconds=self.expires)
        return collection

    @cached_property
    def expires_delta(self):
        return timedelta(seconds=self.expires)

    def as_uri(self, include_password=False):
        """Return the backend as an URI.

        Arguments:
            include_password (bool): Password censored if disabled.
        """
        if not self.url:
            return 'mongodb://'
        if include_password:
            return self.url

        if ',' not in self.url:
            return maybe_sanitize_url(self.url)

        uri1, remainder = self.url.split(',', 1)
        return ','.join([maybe_sanitize_url(uri1), remainder])

    def close(self):
        if self._connection is None:
            return
        self._connection.close()
