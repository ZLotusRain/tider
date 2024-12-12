import re
import copy
from typing import Mapping
from importlib import import_module
from kombu.utils.url import maybe_sanitize_url

from tider.utils.text import pretty
from tider.utils.collections import ChainMap, GetsDictMixin
from tider.settings import default_settings
from tider.security import maybe_evaluate

HIDDEN_SETTINGS = re.compile(
    'API|TOKEN|KEY|SECRET|PASS|PROFANITIES_LIST|SIGNATURE|DATABASE',
    re.IGNORECASE,
)

SETTINGS_PRIORITIES = {
    'default': 0,
    'command': 10,
    'project': 20,
    'spider': 30,
    'cmdline': 40,
}


def get_settings_priority(priority):
    """
    Small helper function that looks up a given string priority in the
    :attr:`~tider.settings.SETTINGS_PRIORITIES` dictionary and returns its
    numerical value, or directly returns a given numerical priority.
    """
    if isinstance(priority, str):
        return SETTINGS_PRIORITIES[priority]
    else:
        return priority


class SettingsAttribute:

    """Class for storing data related to settings attributes.

    This class is intended for internal usage, you should try Settings class
    for settings configuration, not this one.
    """

    def __init__(self, value, priority):
        self.value = value
        if isinstance(self.value, BaseSettings):
            self.priority = max(self.value.maxpriority(), priority)
        else:
            self.priority = priority

    def set(self, value, priority):
        """Sets value if priority is higher or equal than current priority."""
        if priority < self.priority:
            return
        if isinstance(self.value, BaseSettings):
            value = BaseSettings(value, priority=priority)
        self.value = value
        self.priority = priority

    def __str__(self):
        return f"<SettingsAttribute value={self.value!r} priority={self.priority}>"

    __repr__ = __str__


class BaseSettings(ChainMap, GetsDictMixin):
    """
    Instances of this class behave like dictionaries, but store priorities
    along with their ``(key, value)`` pairs, and can be frozen (i.e. marked
    immutable).

    Key-value entries can be passed on initialization with the ``values``
    argument, and they would take the ``priority`` level (unless ``values`` is
    already an instance of :class:`~tider.settings.BaseSettings`, in which
    case the existing priority levels will be kept).  If the ``priority``
    argument is a string, the priority name will be looked up in
    :attr:`~tider.settings.SETTINGS_PRIORITIES`. Otherwise, a specific integer
    should be provided.

    Once the object is created, new settings can be loaded or updated with the
    :meth:`~tider.settings.BaseSettings.set` method, and can be accessed with
    the square bracket notation of dictionaries, or with the
    :meth:`~tider.settings.BaseSettings.get` method of the instance and its
    value conversion variants. When requesting a stored key, the value with the
    highest priority will be retrieved.
    """

    def __init__(self, values=None, priority='project', defaults=None):
        super().__init__()
        self.frozen = False
        defaults = [{}] if defaults is None else defaults
        for default in defaults:
            self.add_defaults(default)
        self.update(values, priority)

    def _assert_mutability(self):
        if self.frozen:
            raise TypeError("Trying to modify an immutable Settings object")

    @staticmethod
    def _evaluate_value(value):
        if isinstance(value, SettingsAttribute):
            return value.value
        return value

    def add_defaults(self, d):
        self._assert_mutability()
        d = {key: SettingsAttribute(self._evaluate_value(d[key]), 'default') for key in d}
        super().add_defaults(d)

    def pop(self, key, *default):
        self._assert_mutability()
        return super().pop(key, *default)

    def __setitem__(self, key, value):
        self._assert_mutability()
        self.set(key, value)

    def __delitem__(self, key):
        self._assert_mutability()
        super().__delitem__(key)

    def clear(self):
        self._assert_mutability()
        super().clear()

    def setdefault(self, key, default=None):
        self._assert_mutability()
        super().setdefault(key, default)

    def __getitem__(self, key):
        for mapping in self.maps:
            try:
                return maybe_evaluate(mapping[key].value)
            except KeyError:
                pass
        return self.__missing__(key)

    def get_with_unevaluated(self, key, default=None):
        for mapping in self.maps:
            try:
                return mapping[key].value
            except KeyError:
                pass
        return default

    def getpriority(self, name):
        """
        Return the current numerical priority value of a setting, or ``None`` if
        the given ``name`` does not exist.

        :param name: the setting name
        :type name: str
        """
        for mapping in self.maps:
            try:
                return mapping[name].priority
            except KeyError:
                pass
        return None

    def maxpriority(self):
        """
        Return the numerical value of the highest priority present throughout
        all settings, or the numerical value for ``default`` from
        :attr:`~tider.settings.SETTINGS_PRIORITIES` if there are no settings
        stored.
        """
        if len(self) > 0:
            return max(self.getpriority(name) for name in self.changes)
        else:
            return get_settings_priority('default')

    def set(self, name, value, priority='project'):
        """
        Store a key/value attribute with a given priority.

        Settings should be populated *before* configuring the Tider object,
        otherwise they won't have any effect.

        If ``value`` is a :class:`~tider.settings.SettingsAttribute` instance,
        the attribute priority will be used and the ``priority`` parameter ignored.
        """
        self._assert_mutability()
        priority = get_settings_priority(priority)

        if priority == 'default':
            updating = self.defaults[0]
        else:
            updating = self.changes
        if name not in updating:
            if isinstance(value, SettingsAttribute):
                updating[name] = value
            else:
                updating[name] = SettingsAttribute(value, priority)
        else:
            updating[name].set(value, priority)

    def setdict(self, values, priority='project'):
        self.update(values, priority)

    def setmodule(self, module, priority='project'):
        """
        Store settings from a module with a given priority.

        :param module: the module or the path of the module
        :type module: types.ModuleType or str

        :param priority: the priority of the settings. Should be a key of
            :attr:`~tider.settings.SETTINGS_PRIORITIES` or an integer
        :type priority: str or int
        """
        self._assert_mutability()
        if isinstance(module, str):
            module = import_module(module)
        values = {key: getattr(module, key) for key in dir(module) if key.isupper()}
        self.setdict(values, priority)

    def update(self, values, priority='project'):
        """
        Store key/value pairs with a given priority.

        If ``values`` is a :class:`~tider.settings.BaseSettings` instance,
        the per-key priorities will be used and the ``priority`` parameter ignored. This allows
        inserting/updating settings with different priorities with a single
        command.

        :param values: key-value pairs
        :type values: dict or string or :class:`~tider.settings.BaseSettings`

        :param priority: the priority of the settings. Should be a key of
            :attr:`~tider.settings.SETTINGS_PRIORITIES` or an integer
        :type priority: str or int
        """
        self._assert_mutability()
        values = values or {}
        if isinstance(values, BaseSettings):
            default = {}
            for d in values.defaults[::-1]:
                # reverse to keep order
                default.update(d)
            self.add_defaults(default)
            for name, value in values.changes:
                self.set(name, value, value.priority)
        elif isinstance(values, Mapping):
            if priority == 'default':
                self.add_defaults(values)
            for name, value in values.items():
                self.set(name, value, priority)
        else:
            raise TypeError(f"Incorrect type: expected Mapping, "
                            f"got {type(values)}: {values!r}")
        for callback in self._observers:
            callback(**values)

    def delete(self, name, priority='project'):
        self._assert_mutability()
        priority = get_settings_priority(priority)
        if priority >= self.getpriority(name):
            for mapping in self.maps:
                try:
                    del mapping[name]
                except KeyError:
                    pass

    def freeze(self):
        """
        Disable further changes to the current settings.

        After calling this method, the present state of the settings will become
        immutable. Trying to change values through the :meth:`~set` method and
        its variants won't be possible and will be alerted.
        """
        self.frozen = True

    def frozencopy(self):
        """
        Return an immutable copy of the current settings.

        Alias for a :meth:`~freeze` call in the object returned by :meth:`copy`.
        """
        copy_settings = self.copy()
        copy_settings.freeze()
        return copy_settings

    def _to_dict(self):
        return {k: (v._to_dict() if isinstance(v, BaseSettings) else v)
                for k, v in self.items()}

    def copy_to_dict(self):
        """
        Make a copy of current settings and convert to a dict.

        This method returns a new dict populated with the same values
        and their priorities as the current settings.

        Modifications to the returned dict won't be reflected on the original
        settings.

        This method can be useful for example for printing settings
        in Tider shell.
        """
        settings = self.copy()
        return settings._to_dict()

    def copy(self):
        # type: () -> 'BaseSettings'
        # priority has no effect here
        return copy.deepcopy(self)

    def without_defaults(self):
        """Return the current configuration, but without defaults."""
        # the last stash is the default settings, so just skip that
        return BaseSettings({}, 'default', self.maps[:-1])

    def table(self, with_defaults=False, censored=True):
        filt = filter_hidden_settings if censored else lambda v: v
        dict_members = dir(dict)
        settings = self if with_defaults else self.without_defaults()
        return filt({
            k: settings.get_with_unevaluated(k) for k in settings.keys()
            if not k.startswith('_') and k not in dict_members
        })

    def humanize(self, with_defaults=False, censored=True):
        """Return a human readable text showing configuration changes."""
        return '\n'.join(
            f'{key}: {pretty(value, width=50)}'
            for key, value in self.table(with_defaults, censored).items())


class Settings(BaseSettings):
    """
    This object stores Tider settings for the configuration of internal
    components, and can be used for any further customization.

    It is a direct subclass and supports all methods of
    :class:`~tider.settings.BaseSettings`. Additionally, after instantiation
    of this class, the new object will have the global default settings
    """

    def __init__(self, values=None, priority='project', defaults=None):
        # Do not pass kwarg values here. We don't want to promote user-defined
        # dicts, and we want to update, not replace, default dicts with the
        # values given by the user
        default = {key: getattr(default_settings, key) for key in dir(default_settings) if key.isupper()}
        defaults = [] if defaults is None else defaults
        defaults.insert(0, default)
        super().__init__(values, priority, defaults)
        self.update(values, priority)


def filter_hidden_settings(conf):
    """Filter sensitive settings."""
    def maybe_censor(key, value, mask='*' * 8):
        if isinstance(value, Mapping):
            return filter_hidden_settings(value)
        if isinstance(key, str):
            if HIDDEN_SETTINGS.search(key):
                return mask
            elif 'control_url' in key.lower():
                from kombu import Connection
                return Connection(value).as_uri(mask=mask)
            elif 'backend' in key.lower() or 'url' in key.lower():
                return maybe_sanitize_url(value, mask=mask)

        return value

    return {k: maybe_censor(k, v) for k, v in conf.items()}


def iter_default_settings():
    """Return the default settings as an iterator of (name, value) tuples"""
    dict_members = dir(dict)
    for name in dir(default_settings):
        if not name.startswith('_') and name not in dict_members:
            yield name, getattr(default_settings, name)


def overridden_settings(settings):
    """Return a dict of the settings that have been overridden"""
    for name, defvalue in iter_default_settings():
        value = settings[name]
        if not isinstance(defvalue, dict) and value != defvalue:
            yield name, value
