import copy

from tider.local import Proxy


class Transformer(Proxy):

    """Proxy that transforms the given value in use and evaluates object once.
    """

    __slots__ = ('__pending__', '__weakref__')

    def __init__(self, value,
                 args=None, kwargs=None, name=None, __doc__=None):
        super().__init__(local=self.transform, args=args, kwargs=kwargs, name=name, __doc__=__doc__)
        object.__setattr__(self, '__source_value__', value)

    def transform(self, *args, **kwargs):
        raise NotImplementedError

    def _get_source_value(self):
        return object.__getattribute__(self, '__source_value__')

    def __repr__(self):
        value = self._get_source_value()
        return repr(value)

    def __str__(self):
        value = self._get_source_value()
        return str(value)

    def _get_current_object(self):
        try:
            return object.__getattribute__(self, '__thing')
        except AttributeError:
            return self.__evaluate__()

    def __then__(self, fun, *args, **kwargs):
        if self.__evaluated__():
            return fun(*args, **kwargs)
        from collections import deque
        try:
            pending = object.__getattribute__(self, '__pending__')
        except AttributeError:
            pending = None
        if pending is None:
            pending = deque()
            object.__setattr__(self, '__pending__', pending)
        pending.append((fun, args, kwargs))

    def __evaluated__(self):
        try:
            object.__getattribute__(self, '__thing')
        except AttributeError:
            return False
        return True

    def __maybe_evaluate__(self):
        return self._get_current_object()

    def __evaluate__(self,
                     _clean=('_Proxy__local',
                             '_Proxy__args',
                             '_Proxy__kwargs')):
        try:
            thing = Proxy._get_current_object(self)
        except Exception:
            raise
        else:
            object.__setattr__(self, '__thing', thing)
            for attr in _clean:
                try:
                    object.__delattr__(self, attr)
                except AttributeError:  # pragma: no cover
                    # May mask errors so ignore
                    pass
            try:
                pending = object.__getattribute__(self, '__pending__')
            except AttributeError:
                pass
            else:
                try:
                    while pending:
                        fun, args, kwargs = pending.popleft()
                        fun(*args, **kwargs)
                finally:
                    try:
                        object.__delattr__(self, '__pending__')
                    except AttributeError:  # pragma: no cover
                        pass
            return thing

    def __deepcopy__(self, memo=None, **kwargs):
        return copy.deepcopy(self._get_current_object(), memo=memo)


def maybe_evaluate(obj):
    """Attempt to evaluate promise, even if obj is not a promise."""
    try:
        return obj.__maybe_evaluate__()
    except AttributeError:
        return obj
