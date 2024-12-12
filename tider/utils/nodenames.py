"""Spider name utilities."""
import socket
from typing import Optional
from functools import partial

from kombu.utils.functional import memoize

from .text import simple_format

#: Separator for spider node name and hostname.
NODENAME_SEP = '@'

NODENAME_DEFAULT = 'tider'


gethostname = memoize(1, Cache=dict)(socket.gethostname)

__all__ = (
    'gethostname', 'nodename', 'nodesplit', 'default_nodename', 'node_format'
)


def nodename(name, hostname):
    """Create node name from name/hostname pair."""
    return NODENAME_SEP.join((name, hostname))


def nodesplit(name):
    """Split node name into tuple of name/hostname."""
    parts = name.rsplit(NODENAME_SEP, 1)
    if len(parts) == 1:
        return None, parts[0]
    return parts


def default_nodename(hostname: str) -> str:
    """Return the default nodename for this process."""
    name, host = nodesplit(hostname or '')
    return nodename(name or NODENAME_DEFAULT, host or gethostname())


def node_format(s: str, name: str, **extra: dict) -> str:
    """Format crawler node name (name@host.com)."""
    shortname, host = nodesplit(name)
    return host_format(s, host, shortname or NODENAME_DEFAULT, p=name, **extra)


def _fmt_process_index(prefix: str = '', default: str = '0') -> str:
    from .log import current_process_index

    index = current_process_index()
    return f'{prefix}{index}' if index else default


_fmt_process_index_with_prefix = partial(_fmt_process_index, '-', '')


def host_format(s: str, host: Optional[str] = None, name: Optional[str] = None, **extra: dict) -> str:
    """Format host %x abbreviations.
    The -name(--hostname) argument can expand the following variables:
        - %h: Hostname, including domain name.
        - %n: Hostname only.
        - %d: Domain name only.

    If the current hostname is `george.example.com`, these will expand to:
        Variable    Template    Result
        %h          worker1@%h  worker1@george.example.com
        %n          worker1@%n  worker1@george
        %d          worker1@%d  worker1@example.com
    Node name replacements
        - %p: Full node name.
        - %h: Hostname, including domain name.
        - %n: Hostname only.
        - %d: Domain name only.
        - %i: Prefork pool process index or 0 if MainProcess.
        - %I: Prefork pool process index with separator.
    For example, if the current hostname is `george@foo.example.com` then these will expand to:
        * --logfile=%p.log -> george@foo.example.com.log
        * --logfile=%h.log -> foo.example.com.log
        * --logfile=%n.log -> george.log
        * --logfile=%d.log -> example.com.log
    """
    host = host or gethostname()
    hname, _, domain = host.partition('.')
    name = name or hname
    keys = dict(
        {
            'h': host,
            'n': name,
            'd': domain,
            'i': _fmt_process_index,
            'I': _fmt_process_index_with_prefix,
        },
        **extra,
    )
    return simple_format(s, keys)
