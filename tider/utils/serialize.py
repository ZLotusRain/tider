import pickle
import marshal


def pickle_dumps(obj):
    try:
        return pickle.dumps(obj, protocol=4)
    # Both pickle.PicklingError and AttributeError can be raised by pickle.dump(s)
    # TypeError is raised from parsel.Selector
    except (pickle.PicklingError, AttributeError, TypeError) as e:
        raise ValueError(str(e)) from e


def pickle_loads(obj):
    return pickle.loads(obj)


def marshal_dumps(obj):
    return marshal.dumps(obj)


def marshal_loads(obj):
    return marshal.loads(obj)
