import re
import io
import json
from re import RegexFlag
from itertools import chain
from collections.abc import MutableMapping
from typing import Any, Mapping, Callable, Iterable, Dict, Union


def uniq(it):
    """Return all unique elements in ``it``, preserving order."""
    seen = set()
    return (seen.add(obj) or obj for obj in it if obj not in seen)


class AttributeDictMixin:
    """Mixin for Mapping interface that adds attribute access.

    I.e., `d.key -> d[key]`).
    """

    def __getattr__(self, k):
        # type: (str) -> Any
        """`d.key -> d[key]`."""
        try:
            return self[k]
        except KeyError:
            raise AttributeError(
                f'{type(self).__name__!r} object has no attribute {k!r}')

    def __setattr__(self, key: str, value) -> None:
        """`d[key] = value -> d.key = value`."""
        self[key] = value


class AttributeDict(dict, AttributeDictMixin):
    """Dict subclass with attribute access."""


class GetsDictMixin:
    """Mixin for Mapping interface that adds get-related method. """

    def getbool(self, key, default=False):
        """
        Get a setting value as a boolean.

        :param key: the setting name
        :type key: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        got = self.get(key, default)
        try:
            return bool(int(got))
        except ValueError:
            if got in ("True", "true"):
                return True
            if got in ("False", "false"):
                return False
            raise ValueError("Supported values for boolean settings "
                             "are 0/1, True/False, '0'/'1', "
                             "'True'/'False' and 'true'/'false'")

    def getint(self, key, default=0):
        """
        Get a setting value as an int.

        :param key: the setting name
        :type key: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        return int(self.get(key, default))

    def getfloat(self, key, default=0.0):
        """
        Get a setting value as a float.

        :param key: the setting name
        :type key: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        return float(self.get(key, default))

    def getlist(self, key, default=None):
        """
        Get a setting value as a list. If the setting original type is a list, a
        copy of it will be returned. If it's a string it will be split by ",".

        For example, settings populated through environment variables set to
        ``'one,two'`` will return a list ['one', 'two'] when using this method.

        :param key: the setting name
        :type key: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        value = self.get(key, default or [])
        if isinstance(value, str):
            value = value.split(',')
        return list(value)

    def getdict(self, key, default=None):
        """
        Get a setting value as a dictionary. If the setting original type is a
        dictionary, a copy of it will be returned. If it is a string it will be
        evaluated as a JSON dictionary. In the case that it is a
        :class:`~tider.settings.BaseSettings` instance itself, it will be
        converted to a dictionary, containing all its current settings values
        as they would be returned by :meth:`~tider.settings.BaseSettings.get`,
        and losing all information about priority and mutability.

        :param key: the setting name
        :type key: str

        :param default: the value to return if no setting is found
        :type default: object
        """
        value = self.get(key, default or {})
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value)

    def get_namespace(
            self, namespace: str, lowercase: bool = True, trim_namespace: bool = True
    ) -> Dict[str, Any]:
        """Returns a dictionary containing a subset of configuration options
        that match the specified namespace/prefix. Example usage::

            settings['IMAGE_STORE_TYPE'] = 'fs'
            settings['IMAGE_STORE_PATH'] = '/var/app/images'
            settings['IMAGE_STORE_BASE_URL'] = 'http://img.website.com'
            image_store_config = settings.get_namespace('IMAGE_STORE_')

        The resulting dictionary `image_store_config` would look like::

            {
                'type': 'fs',
                'path': '/var/app/images',
                'base_url': 'http://img.website.com'
            }

        This is often useful when configuration options map directly to
        keyword arguments in functions or class constructors.

        :param namespace: a configuration namespace
        :param lowercase: a flag indicating if the keys of the resulting
                          dictionary should be lowercase
        :param trim_namespace: a flag indicating if the keys of the resulting
                          dictionary should not include the namespace
        """
        rv = {}
        for k, v in self.items():
            if not k.startswith(namespace):
                continue
            if trim_namespace:
                key = k[len(namespace):]
            else:
                key = k
            if lowercase:
                key = key.lower()
            rv[key] = v
        return rv

    def get_pattern(self, pattern: str, flags: Union[int, RegexFlag] = 0):
        rv = {}
        pattern = re.compile(pattern, flags)
        for k, v in self.items():
            if not pattern.search(k):
                continue
            rv[k] = v
        return rv


class ChainMap(MutableMapping):

    changes = None
    defaults = None
    maps = None
    _observers = []

    def __init__(self, *maps, **kwargs):
        # type: (*Mapping, **Any) -> None
        maps = list(maps or [{}])
        self.__dict__.update(
            maps=maps,
            changes=maps[0],
            defaults=maps[1:],
        )

    def add_defaults(self, d):
        self.defaults.insert(0, d)
        self.maps.insert(1, d)

    def pop(self, key, *default):
        # type: (Any, *Any) -> Any
        try:
            return self.maps[0].pop(key, *default)
        except KeyError:
            raise KeyError(
                f'Key not found in the first mapping: {key!r}')

    def __missing__(self, key):
        # type: (Any) -> Any
        raise KeyError(key)

    def __getitem__(self, key):
        # type: (Any) -> Any
        for mapping in self.maps:
            try:
                return mapping[key]
            except KeyError:
                pass
        return self.__missing__(key)

    def __setitem__(self, key, value):
        # type: (Any, Any) -> None
        self.changes[key] = value

    def __delitem__(self, key):
        # type: (Any) -> None
        try:
            del self.changes[key]
        except KeyError:
            raise KeyError(f'Key not found in first mapping: {key!r}')

    def clear(self):
        # type: () -> None
        self.changes.clear()

    def get(self, key, default=None):
        # type: (Any, Any) -> Any
        try:
            return self[key]
        except KeyError:
            return default

    def __len__(self):
        # type: () -> int
        return len(set().union(*self.maps))

    def __iter__(self):
        return self._iterate_keys()

    def __contains__(self, key):
        # type: (Any) -> bool
        return any(key in m for m in self.maps)

    def __bool__(self):
        # type: () -> bool
        return any(self.maps)

    def setdefault(self, key, default=None):
        # type: (Any, Any) -> None
        if key not in self:
            self[key] = default

    def update(self, *args, **kwargs):
        # type: (*Any, **Any) -> Any
        result = self.changes.update(*args, **kwargs)
        for callback in self._observers:
            callback(*args, **kwargs)
        return result

    def __repr__(self):
        # type: () -> str
        return '{0.__class__.__name__}({1})'.format(
            self, ', '.join(map(repr, self.maps)))

    @classmethod
    def fromkeys(cls, iterable, *args):
        # type: (type, Iterable, *Any) -> 'ChainMap'
        """Create a ChainMap with a single dict created from the iterable."""
        return cls(dict.fromkeys(iterable, *args))

    def copy(self):
        # type: () -> 'ChainMap'
        return self.__class__(self.maps[0].copy(), *self.maps[1:])

    def _iter(self, op):
        # type: (Callable) -> Iterable
        # defaults must be first in the stream, so values in
        # changes take precedence.
        # pylint: disable=bad-reversed-sequence
        #   Someone should teach pylint about properties.
        return chain(*(op(d) for d in reversed(self.maps)))

    def _iterate_keys(self):
        # type: () -> Iterable
        return uniq(self._iter(lambda d: d.keys()))

    def _iterate_items(self):
        # type: () -> Iterable
        return ((key, self[key]) for key in self)

    def _iterate_values(self):
        # type: () -> Iterable
        return (self[key] for key in self)
    itervalues = _iterate_values

    def bind_to(self, callback):
        self._observers.append(callback)

    keys = _iterate_keys
    items = _iterate_items
    values = _iterate_values


class BytesSlicer:

    def __init__(self, chunk_size=None):
        self._buffer = io.BytesIO()
        self._chunk_size = chunk_size

    def slice(self, content):
        if self._chunk_size is None:
            # get all
            return [content] if content else []

        self._buffer.write(content)
        if self._buffer.tell() >= self._chunk_size:
            value = self._buffer.getvalue()
            # slice
            chunks = [value[i: i+self._chunk_size]
                      for i in range(0, len(value), self._chunk_size)]
            if len(chunks[-1]) == self._chunk_size:
                # every chunk can be sliced to chunk size
                self._buffer.seek(0)
                self._buffer.truncate()
                return chunks
            else:
                # use flush to get the last chunk
                self._buffer.seek(0)
                self._buffer.write(chunks[-1])
                self._buffer.truncate()
                return chunks[:-1]
        else:
            return []

    def flush(self):
        value = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate()
        return [value] if value else []
