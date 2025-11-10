"""
Tider - next-generation web crawling and scraping framework
"""

import os
import sys
import pkgutil
from functools import wraps

from . import local

__all__ = (
    'Tider', 'Item', 'Field', 'Promise',
    'Request', 'Response', '__version__', 'version_info',
)


__version__ = (pkgutil.get_data(__package__, "VERSION") or b"").decode("ascii").strip()
version_info = tuple(int(v) if v.isdigit() else v for v in __version__.split('.'))


# Check minimum required Python version
if sys.version_info < (3, 8):
    print(f"Tider {__version__} requires Python 3.8 or higher")
    sys.exit(1)

# https://github.com/gevent/gevent/issues/1909
# os.environ.setdefault('PURE_PYTHON', '1')

# This is never executed, but tricks static analyzers (PyDev, PyCharm,
# pylint, etc.) into knowing the types of these symbols, and what
# they contain.
STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from tider.base import Tider
    from tider.promise import Promise
    from tider.item import Item, Field
    from tider.network import Request, Response


# Eventlet/gevent patching must happen before importing
# anything else, so these tools must be at top-level.


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

    if int(gevent.__version__.split('.')[0]) < 25:
        from gevent.queue import LifoQueue
        from urllib3.connectionpool import ConnectionPool
        # https://github.com/gevent/gevent/issues/1957
        # https://github.com/urllib3/urllib3/issues/3289
        ConnectionPool.QueueCls = LifoQueue


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


def _patch_pymupdf():
    try:
        import fitz as pymupdf
    except ImportError:
        try:
            import pymupdf
        except ImportError:
            return

    from pymupdf import Document, Page, table

    def patched_close(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            func(self, *args, **kwargs)  # will remove page refs
            if table.TEXTPAGE:
                table.TEXTPAGE.this = table.TEXTPAGE.parent = None
                table.TEXTPAGE = None
            if pymupdf.g_img_info:
                del pymupdf.g_img_info[:]
            del table.EDGES[:], table.CHARS[:]
            pymupdf._log_items_clear()
            # https://pymupdf.readthedocs.io/en/latest/tools.html
            pymupdf.TOOLS.reset_mupdf_warnings()
            pymupdf.TOOLS.store_shrink(100)
        return wrapper

    def get_drawings(self, extended: bool = False):
        """Retrieve vector graphics. The extended version includes clips.

        Note:
        For greater comfort, this method converts point-likes, rect-likes, quad-likes
        of the C version to respective Point / Rect / Quad objects.
        It also adds default items that are missing in original path types.
        """
        allkeys = (
            'closePath',
            'fill',
            'color',
            'width',
            'lineCap',
            'lineJoin',
            'dashes',
            'stroke_opacity',
            'fill_opacity',
            'even_odd',
        )
        val = self.get_cdrawings(extended=extended)

        def _update_items(items):
            for pitem in items:  # don't use enumerate here.
                cmd, rest = pitem[0], pitem[1:]
                if cmd == "re":
                    pitem = ("re", pymupdf.Rect(rest[0]).normalize(), rest[1])
                elif cmd == "qu":
                    pitem = ("qu", pymupdf.Quad(rest[0]))
                else:
                    pitem = tuple([cmd] + [pymupdf.Point(i) for i in rest])
                yield pitem

        for i in range(len(val)):
            npath = val[i]
            if not npath["type"].startswith("clip"):
                npath["rect"] = pymupdf.Rect(npath["rect"])
            else:
                npath["scissor"] = pymupdf.Rect(npath["scissor"])
            if npath["type"] != "group":
                npath["items"] = list(_update_items(npath["items"]))
            if npath['type'] in ('f', 's'):
                for k in allkeys:
                    npath[k] = npath.get(k)

            val[i] = npath
        return val

    Page.get_drawings = get_drawings
    Document.close = patched_close(Document.close)


def maybe_patch_third_party():
    _patch_pymupdf()


# this just creates a new module, that imports stuff on first attribute
# access.  This makes the library faster to use.
old_module, new_module = local.recreate_module(  # pragma: no cover
    __name__,
    by_module={
        'tider.base': ['Tider'],
        'tider.item': ['Item', 'Field'],
        'tider.promise': ['Promise'],
        'tider.network': ['Request', 'Response'],
        'tider.security': ['Transformer'],
    },
    __package__='tider', __file__=__file__,
    __path__=__path__, __doc__=__doc__, __version__=__version__,
    local=local, version_info=version_info,
    maybe_patch_concurrency=maybe_patch_concurrency,
    maybe_patch_third_party=maybe_patch_third_party,
    _find_option_with_arg=_find_option_with_arg,
)
