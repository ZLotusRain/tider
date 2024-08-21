"""Spider name utilities."""
import socket

from kombu.utils.functional import memoize

#: Separator for spider node name and hostname.
NODENAME_SEP = '@'


gethostname = memoize(1, Cache=dict)(socket.gethostname)

__all__ = (
    'gethostname', 'nodename'
)


def nodename(name, hostname):
    """Create node name from name/hostname pair."""
    return NODENAME_SEP.join((name, hostname))


def nodesplit(name):
    """Split node name into tuple of name/hostname."""
    parts = name.split(NODENAME_SEP, 1)
    if len(parts) == 1:
        return None, parts[0]
    return parts
