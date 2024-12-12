from datetime import datetime, timedelta
from kombu.utils import cached_property
from kombu.utils.url import urlparse, maybe_sanitize_url

from tider.backends.base import Backend
from tider.exceptions import ImproperlyConfigured

try:
    import pymongo
except ImportError:
    pymongo = None

if pymongo:
    SUPPORTS_MONGODB32 = tuple(int(v) if v.isdigit() else v
                               for v in pymongo.__version__.split('.')) < (4, 0, 0)
else:
    SUPPORTS_MONGODB32 = False


class MongoBackend(Backend):
    """MongoDB result backend.

    Raises:
        tider.exceptions.ImproperlyConfigured:
            if module :pypi:`pymongo` is not available.
    """

    mongo_host = None
    host = 'localhost'
    port = 27017
    username = None
    password = None
    database_name = 'tider'
    statsmeta_collection = 'tider_stats'
    taskmeta_collection = 'tider_taskmeta'
    max_pool_size = 10
    options = None

    supports_autoexpire = False

    _connection = None

    def __init__(self, crawler=None, **kwargs):
        self.options = {}

        super().__init__(crawler, **kwargs)

        if not pymongo:
            raise ImproperlyConfigured(
                'You need to install the pymongo library to use the '
                'MongoDB backend.')

        # Set option defaults
        for key, value in self._prepare_client_options().items():
            self.options.setdefault(key, value)

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
        config = self.crawler.settings.get('BACKEND_SETTINGS')
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
            self.username = config.pop('user', self.username)
            self.password = config.pop('password', self.password)
            self.database_name = config.pop('database', self.database_name)
            self.taskmeta_collection = config.pop(
                'taskmeta_collection', self.taskmeta_collection,
            )
            self.statsmeta_collection = config.pop(
                'statsmeta_collection', self.statsmeta_collection,
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
            if self.username:
                conf['username'] = self.username
            if self.password:
                conf['password'] = self.password

            self._connection = MongoClient(**conf)

        return self._connection

    def cleanup(self):
        """Delete expired meta-data."""
        if not self.expires:
            return

        self.stats_collection.delete_many(
            {'date_done': {'$lt': self.crawler.tider.now() - self.expires_delta}},
        )
        # self.group_collection.delete_many(
        #     {'date_done': {'$lt': self.crawler.tider.now() - self.expires_delta}},
        # )

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return super().__reduce__(
            args, dict(kwargs, expires=self.expires, url=self.url))

    def _get_database(self):
        conn = self._get_connection()
        return conn[self.database_name]

    @cached_property
    def database(self):
        """Get database from MongoDB connection.

        performs authentication if necessary.
        """
        return self._get_database()

    def _save_stats(self, spider_id, spider_name, stats, schema=None):
        """Save the stats."""
        meta = {
            '_id': spider_id,
            'spider': spider_name,
            'schema': schema,
            'stats': stats,
            'date_done': datetime.utcnow(),
        }
        self.stats_collection.replace_one({'_id': spider_id}, meta, upsert=True)
        return stats

    @cached_property
    def stats_collection(self):
        """Get the stats collection."""
        collection = self.database[self.stats_collection]

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
