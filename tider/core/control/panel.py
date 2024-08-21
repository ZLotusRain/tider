"""Spider remote control command implementations stolen from Celery."""

import json
import time
import logging
from tempfile import NamedTemporaryFile
from collections import UserDict, namedtuple

from tider.exceptions import SpiderShutdown

logger = logging.getLogger(__name__)

controller_info_t = namedtuple('controller_info_t', [
    'alias', 'type', 'visible', 'default_timeout',
    'help', 'signature', 'args', 'variadic',
])


def ok(value):
    return {'ok': value}


def nok(value):
    return {'error': value}


class Panel(UserDict):
    """Global registry of remote control commands."""

    data = {}      # global dict.
    meta = {}      # -"-

    @classmethod
    def register(cls, *args, **kwargs):
        if args:
            return cls._register(**kwargs)(*args)
        return cls._register(**kwargs)

    @classmethod
    def _register(cls, name=None, alias=None, type='control',
                  visible=True, default_timeout=1.0, help=None,
                  signature=None, args=None, variadic=None):

        def _inner(fun):
            control_name = name or fun.__name__
            _help = help or (fun.__doc__ or '').strip().split('\n')[0]
            cls.data[control_name] = fun
            cls.meta[control_name] = controller_info_t(
                alias, type, visible, default_timeout,
                _help, signature, args, variadic)
            if alias:
                cls.data[alias] = fun
            return fun
        return _inner


def control_command(**kwargs):
    return Panel.register(type='control', **kwargs)


def inspect_command(**kwargs):
    return Panel.register(type='inspect', **kwargs)


@inspect_command(default_timeout=0.2)
def ping(state, **kwargs):
    """Ping worker(s)."""
    return ok('pong')


@inspect_command()
def settings(state, **kwargs):
    return state.tider.settings.table(with_defaults=True, censored=True)


@inspect_command(default_timeout=1)
def stats(state, **kwargs):
    s = state.tider.stats.get_stats().copy()
    return s


def exploring(state):
    results = []
    requests = state.tider.engine.explorer.transferring
    for request in requests:
        try:
            rd = request.to_dict(spider=state.tider.spider)
        except AttributeError as e:
            if 'callback' in str(e):
                continue
            raise e
        for key in rd:
            try:
                json.dumps(rd[key])
            except (json.JSONDecodeError, TypeError):
                rd[key] = str(rd[key])
        results.append(rd)
    return results


def parsing(state):
    results = []
    requests = state.tider.engine.parser.parsing
    for request in requests:
        rd = request.to_dict(spider=state.tider.spider)
        for key in rd:
            try:
                json.dumps(rd[key])
            except (json.JSONDecodeError, TypeError):
                rd[key] = str(rd[key])
        results.append(rd)
    return results


@inspect_command(default_timeout=1)
def engine(state, **kwargs):
    result = {
        'time()-tider.engine.start_time': time.time() - state.tider.engine.start_time,
        'tider.engine.active()': state.tider.engine.active(),
        'tider.engine._overload()': state.tider.engine._overload(),
        'tider.engine.explorer.needs_backout()': state.tider.engine.explorer.needs_backout(),
        'len(state.tider.engine.scheduler)': len(state.tider.engine.scheduler),
        'len(tider.engine.explorer.queue)': len(state.tider.engine.explorer.queue),
        'len(tider.engine.parser.queue)': len(state.tider.engine.parser.queue),
        'len(tider.engine.explorer.transferring)': len(state.tider.engine.explorer.transferring),
        'len(tider.engine.parser.parsing)': len(state.tider.engine.parser.parsing),
        'tider.engine.explorer.transferring': exploring(state),
        'tider.engine.parser.parsing': parsing(state),
    }
    return result


@inspect_command(default_timeout=60.0)
def objmct(state, **kwargs):
    try:
        import objgraph as _objgraph
    except ImportError:
        raise ImportError('Requires the objgraph library')
    with NamedTemporaryFile(mode='w+') as fp:
        _objgraph.show_most_common_types(file=fp)
        fp.seek(0)
        return fp.read()


@inspect_command(
    default_timeout=60.0,
    args=[('type', str), ('num', int), ('max_depth', int)],
    signature='[object_type=Request] [num=200 [max_depth=10]]',
)
def objgraph(state, num=200, max_depth=10, type='Request'):  # pragma: no cover
    """Create graph of uncollected objects (memory-leak debugging).

    Arguments:
        state
        num (int): Max number of objects to graph.
        max_depth (int): Traverse at most n levels deep.
        type (str): Name of object to graph.  Default is ``"Request"``.
    """
    try:
        import objgraph as _objgraph
    except ImportError:
        raise ImportError('Requires the objgraph library')
    logger.info('Dumping graph for type %r', type)
    with NamedTemporaryFile(prefix='tobjg', suffix='.png', delete=False) as fh:
        objects = _objgraph.by_type(type)[:num]
        _objgraph.show_backrefs(
            objects,
            max_depth=max_depth, highlight=lambda v: v in objects,
            filename=fh.name,
        )
        return {'filename': fh.name}


@control_command()
def shutdown(state, msg='Got shutdown from remote', **kwargs):
    logger.warning(msg)
    raise SpiderShutdown(msg)
