"""
Tider - next-generation web crawling and scraping framework
"""

import os
import sys
import pkgutil


__all__ = (
    '__version__', 'version_info'
)


__version__ = (pkgutil.get_data(__package__, "VERSION") or b"").decode("ascii").strip()
version_info = tuple(int(v) if v.isdigit() else v for v in __version__.split('.'))


# Check minimum required Python version
if sys.version_info < (3, 3):
    print(f"Tider {__version__} requires Python 3.3+")
    sys.exit(1)


def _find_option_with_arg(argv, short_opts=None, long_opts=None):
    """Search argv for options specifying short and longopt alternatives.

    Returns:
        str: value for option found
    Raises:
        KeyError: if option not found.
    """
    for i, arg in enumerate(argv):
        if arg.startswith('-'):
            if long_opts and arg.startswith('--'):
                name, sep, val = arg.partition('=')
                if name in long_opts:
                    return val if sep else argv[i + 1]
            if short_opts and arg in short_opts:
                return argv[i + 1]
    raise KeyError('|'.join(short_opts or [] + long_opts or []))


def _patch_eventlet():
    import eventlet.debug

    eventlet.monkey_patch()
    blockdetect = float(os.environ.get('EVENTLET_NOBLOCK', 0))
    if blockdetect:
        eventlet.debug.hub_blocking_detection(blockdetect, blockdetect)


def _patch_gevent():
    import gevent.monkey
    import gevent.signal

    gevent.monkey.patch_all()


def maybe_patch_concurrency(argv=None, short_opts=None,
                            long_opts=None, patches=None):
    """Apply eventlet/gevent monkeypatches.

    With short and long opt alternatives that specify the command line
    option to set the pool, this makes sure that anything that needs
    to be patched is completed as early as possible.
    (e.g., eventlet/gevent monkey patches).
    """
    argv = argv if argv else sys.argv
    short_opts = short_opts if short_opts else ['-P']
    long_opts = long_opts if long_opts else ['--pool']
    patches = patches if patches else {'eventlet': _patch_eventlet,
                                       'gevent': _patch_gevent}
    try:
        pool = _find_option_with_arg(argv, short_opts, long_opts)
    except KeyError:
        pass
    else:
        try:
            patcher = patches[pool]
        except KeyError:
            pass
        else:
            patcher()

        # set up eventlet/gevent environments ASAP
        from tider import concurrency
        if pool in concurrency.get_available_pool_names():
            concurrency.get_implementation(pool)


del pkgutil
