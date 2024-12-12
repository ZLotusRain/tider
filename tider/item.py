"""Tider Item"""

import json
from pprint import pformat
from typing import Dict
from abc import ABCMeta
from copy import deepcopy
from collections.abc import MutableMapping


class Field(dict):
    """
    Container of field metadata.
    Each key defined in Field objects could be used
    by a different component, and only those components know about it.
    """


class ItemMeta(ABCMeta):

    def __new__(mcs, class_name, bases, attrs):
        classcell = attrs.pop('__classcell__', None)
        new_bases = tuple(getattr(base, "_class") for base in bases if hasattr(base, '_class'))
        _class = super().__new__(mcs, 'x_' + class_name, new_bases, attrs)

        new_attrs = {}
        fields = getattr(_class, 'fields', {})
        slots = getattr(_class, '__slots__', ())
        for slot in slots:
            fields[slot] = Field(default=None)
        for n in dir(_class):
            v = getattr(_class, n)
            if isinstance(v, Field):
                fields[n] = v
            elif n in attrs:
                new_attrs[n] = attrs[n]

        new_attrs.pop("__slots__", ())
        new_attrs['fields'] = fields
        new_attrs['_class'] = _class
        if classcell is not None:
            new_attrs['__classcell__'] = classcell
        return super().__new__(mcs, class_name, bases, new_attrs)


class Item(MutableMapping, metaclass=ItemMeta):

    fields: Dict[str, Field]

    def __init__(self, *args, **kwargs):
        # 数据描述器 > 实例属性 > 非数据描述器; 实例属性 > 类属性
        self._values = {}
        self._pipelines = {}
        self._set_default()
        if args or kwargs:  # avoid creating dict for most common case
            for k, v in dict(*args, **kwargs).items():
                self[k] = v
        self._discarded = False

    def _set_default(self):
        # self.fields = self.__class__.fields
        for field in self.fields:
            meta = self.fields[field]
            if 'default' in meta:
                self[field] = deepcopy(meta['default'])
            elif "datatype" in meta:
                datatype = meta["datatype"]
                if type(datatype).__name__ in ('classobj', 'type'):
                    self[field] = datatype()

    def discard(self):
        self._values.clear()
        self._discarded = True

    @property
    def discarded(self):
        return self._discarded

    def __getitem__(self, key):
        return self._values[key]

    def __setitem__(self, key, value):
        if key in self.fields:
            self._values[key] = value
        else:
            raise KeyError(f"{self.__class__.__name__} does not support field: {key}")

    def __delitem__(self, key):
        del self._values[key]

    def __getattr__(self, name):
        if name in self.fields:
            raise AttributeError(f"Use item[{name!r}] to get field value")
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if not name.startswith('_'):
            raise AttributeError(f"Use item[{name!r}] = {value!r} to set field value")
        super().__setattr__(name, value)

    @classmethod
    def from_dict(cls, data):
        for key in data:
            cls.fields[key] = Field(value=data[key])
        return cls(**data)

    @classmethod
    def from_json(cls, json_data):
        try:
            data = json.loads(json_data)
        except Exception:
            raise AttributeError(f"Incorrect json data: {json_data}")
        else:
            return cls.from_dict(data)

    def add_pipeline(self, pipeline, priority):
        self._pipelines.update({pipeline: priority})

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        return iter(self._values)

    def keys(self):
        return self._values.keys()

    def __repr__(self):
        return pformat(dict(self))

    def copy(self):
        return self.__class__(self)

    def deepcopy(self):
        """Return a :func:`~copy.deepcopy` of this item.
        """
        return deepcopy(self)

    def jsonify(self):
        return json.dumps(dict(self), ensure_ascii=False)
