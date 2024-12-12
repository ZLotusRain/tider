"""Spider remote control command implementations stolen from Celery."""

import json
import time
from tempfile import NamedTemporaryFile
from collections import UserDict, namedtuple

from tider.utils.log import get_logger
from tider.exceptions import SpiderShutdown

logger = get_logger(__name__)

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


@inspect_command(visible=False)
def hello(state, from_node, **kwargs):
    """Request mingle sync-data."""
    # pylint: disable=redefined-outer-name
    # XXX Note that this redefines `revoked`:
    #     Outside of this scope that is a function.
    if from_node != state.hostname:
        logger.info('sync with %s', from_node)
        # Do not send expired items to the other crawler.
        return {
            'clock': state.app.clock.forward(),
        }


@inspect_command(default_timeout=0.2)
def ping(state, **kwargs):
    """Ping crawler(s)."""
    return ok('pong')


@inspect_command()
def settings(state, **kwargs):
    return state.crawler.settings.table(with_defaults=True, censored=True)


@inspect_command(default_timeout=1)
def stats(state, **kwargs):
    s = state.crawler.dump_stats()
    return s


@inspect_command(default_timeout=5)
def transferring(state):
    results = []
    requests = list(state.crawler.engine.explorer.transferring)
    for request in requests:
        rd = request.to_dict(spider=state.crawler.spider)
        for key in rd:
            try:
                json.dumps(rd[key])
            except (json.JSONDecodeError, TypeError):
                rd[key] = str(rd[key])
        results.append(rd)
    return results


@inspect_command(default_timeout=5)
def parsing(state):
    results = []
    requests = list(state.crawler.engine.parser.parsing)
    for request in requests:
        rd = request.to_dict(spider=state.crawler.spider)
        for key in rd:
            try:
                json.dumps(rd[key])
            except (json.JSONDecodeError, TypeError):
                rd[key] = str(rd[key])
        results.append(rd)
    return results


@inspect_command(default_timeout=5)
def slots(state):
    priority_queue = state.crawler.engine.scheduler.priority_queue
    pqueues = dict(priority_queue.pqueues)
    return {slot: len(pqueues[slot]) for slot in pqueues}


@inspect_command(default_timeout=1)
def engine(state, **kwargs):
    result = {
        'time()-crawler.engine.start_time': time.time() - state.crawler.engine.start_time,
        'crawler.engine.active()': state.crawler.engine.active(),
        'crawler.engine._overload()': state.crawler.engine._overload(),
        'crawler.engine._spider_closed.is_set()': state.crawler.engine._spider_closed.is_set(),
        'crawler.engine.explorer.needs_backout()': state.crawler.engine.explorer.needs_backout(),
        'len(state.crawler.engine.scheduler)': len(state.crawler.engine.scheduler),
        'len(crawler.engine.explorer.queue)': len(state.crawler.engine.explorer.queue),
        'len(crawler.engine.parser.queue)': len(state.crawler.engine.parser.queue),
        'len(crawler.engine.explorer.transferring)': len(state.crawler.engine.explorer.transferring),
        'len(crawler.engine.parser.parsing)': len(state.crawler.engine.parser.parsing),
    }
    return result


@inspect_command(default_timeout=3)
def sse(state, **kwargs):
    s1 = state.crawler.settings.table(with_defaults=True, censored=True)
    s2 = state.crawler.dump_stats()
    e = {
        'time()-crawler.engine.start_time': time.time() - state.crawler.engine.start_time,
        'crawler.engine.active()': state.crawler.engine.active(),
        'crawler.engine._overload()': state.crawler.engine._overload(),
        'crawler.engine._spider_closed.is_set()': state.crawler.engine._spider_closed.is_set(),
        'crawler.engine.explorer.needs_backout()': state.crawler.engine.explorer.needs_backout(),
        'len(state.crawler.engine.scheduler)': len(state.crawler.engine.scheduler),
        'len(crawler.engine.explorer.queue)': len(state.crawler.engine.explorer.queue),
        'len(crawler.engine.parser.queue)': len(state.crawler.engine.parser.queue),
        'len(crawler.engine.explorer.transferring)': len(state.crawler.engine.explorer.transferring),
        'len(crawler.engine.parser.parsing)': len(state.crawler.engine.parser.parsing),
    }
    if state.crawler.engine.explorer.session._downloaders.get('default'):
        proxy_manager = state.crawler.engine.explorer.session._downloaders["default"].proxy_manager
        e.update({'len(crawler.engine.explorer.session._downloaders["default"].proxy_manager)': len(proxy_manager)})
    return {
        'settings': s1,
        'stats': s2,
        'engine': e
    }


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
    raise SpiderShutdown(0)
