"""Platforms.

Utilities dealing with platform specifics stolen from celery.
"""

import os
import sys
import math
import errno
import atexit
import shutil
import numbers
import signal as _signal
import platform as _platform
from contextlib import contextmanager

from kombu.utils.compat import maybe_fileno
from billiard.compat import close_open_fds, get_fdmax
from billiard.util import set_pdeathsig as _set_pdeathsig

from tider.utils.misc import try_import
from tider.utils.nodenames import nodesplit
from tider.exceptions import reraise, SecurityError

resource = try_import("resource")
pwd = try_import('pwd')
grp = try_import('grp')
mputil = try_import('multiprocessing.util')

__all__ = (
    'EX_OK', 'EX_FAILURE', 'EX_UNAVAILABLE', 'EX_USAGE', 'SYSTEM',
    'IS_macOS', 'IS_WINDOWS', 'SIGMAP', 'pyimplementation', 'LockFailed',
    'get_fdmax', 'Pidfile', 'PidDirectory', 'create_pidlock', 'close_open_fds',
    'DaemonContext', 'detached', 'parse_uid', 'parse_gid', 'setgroups',
    'initgroups', 'setgid', 'setuid', 'maybe_drop_privileges', 'signals',
    'signal_name', 'get_errno_name', 'ignore_errno', 'fd_by_path', 'isatty',
)

# exitcodes
EX_OK = getattr(os, 'EX_OK', 0)
EX_FAILURE = 1
EX_UNAVAILABLE = getattr(os, 'EX_UNAVAILABLE', 69)
EX_USAGE = getattr(os, 'EX_USAGE', 64)
EX_CANTCREAT = getattr(os, 'EX_CANTCREAT', 73)

SYSTEM = _platform.system()
IS_macOS = SYSTEM == 'Darwin'
IS_WINDOWS = SYSTEM == 'Windows'

DAEMON_WORKDIR = '/'

PIDFILE_FLAGS = os.O_CREAT | os.O_EXCL | os.O_WRONLY
PIDFILE_MODE = ((os.R_OK | os.W_OK) << 6) | ((os.R_OK) << 3) | (os.R_OK)

PIDLOCKED = """ERROR: Pidfile ({0}) already exists.
Seems we're already running? (pid: {1})"""

SIGNAMES = {
    sig for sig in dir(_signal)
    if sig.startswith('SIG') and '_' not in sig
}
SIGMAP = {getattr(_signal, name): name for name in SIGNAMES}


def isatty(fh):
    """Return true if the process has a controlling terminal."""
    try:
        return fh.isatty()
    except AttributeError:
        pass


def pyimplementation():
    """Return string identifying the current Python implementation."""
    if hasattr(_platform, 'python_implementation'):
        return _platform.python_implementation()
    elif sys.platform.startswith('java'):
        return 'Jython ' + sys.platform
    elif hasattr(sys, 'pypy_version_info'):
        v = '.'.join(str(p) for p in sys.pypy_version_info[:3])
        if sys.pypy_version_info[3:]:
            v += '-' + ''.join(str(p) for p in sys.pypy_version_info[3:])
        return 'PyPy ' + v
    else:
        return 'CPython'


class LockFailed(Exception):
    """Raised if a PID lock can't be acquired."""


class Pidfile:
    """Pidfile.

    This is the type returned by :func:`create_pidlock`.

    See Also:
        Best practice is to not use this directly but rather use
        the :func:`create_pidlock` function instead:
        more convenient and also removes stale pidfiles (when
        the process holding the lock is no longer running).
    """

    #: Path to the pid lock file.
    path = None

    def __init__(self, path, group=None):
        self.path = os.path.abspath(path)
        self._dirname = os.path.dirname(self.path)
        if self._dirname and not os.path.exists(self._dirname):
            os.makedirs(self._dirname)

        self.group = group

    def acquire(self):
        """Acquire lock."""
        try:
            self.write_pid()
        except OSError as exc:
            reraise(LockFailed, LockFailed(str(exc)), sys.exc_info()[2])
        return self

    __enter__ = acquire

    def is_locked(self):
        """Return true if the pid lock exists."""
        return os.path.exists(self.path)

    def release(self, *args):
        """Release lock."""
        self.remove()

    __exit__ = release

    def read_pid(self):
        """Read and return the current pid."""
        with ignore_errno('ENOENT'):
            with open(self.path) as fh:
                line = fh.readline()
                if line.strip() == line:  # must contain '\n'
                    raise ValueError(
                        f'Partial or invalid pidfile {self.path}')

                try:
                    return int(line.strip())
                except ValueError:
                    raise ValueError(
                        f'pidfile {self.path} contents invalid.')

    def remove(self):
        """Remove the lock."""
        with ignore_errno(errno.ENOENT, errno.EACCES):
            os.unlink(self.path)
        if self.group and self._dirname.endswith(self.group):
            pidd = PidDirectory(self._dirname)
            pidd.remove_if_stale()

    def remove_if_stale(self):
        """Remove the lock if the process isn't running.

        I.e. process does not respond to signal.
        """
        try:
            pid = self.read_pid()
        except ValueError:
            print('Broken pidfile found - Removing it.', file=sys.stderr)
            self.remove()
            return True
        if not pid:
            self.remove()
            return True
        if pid == os.getpid():
            # this can be common in k8s pod with PID of 1 - don't kill
            self.remove()
            return True

        try:
            os.kill(pid, 0)
        except OSError as exc:
            if exc.errno == errno.ESRCH or exc.errno == errno.EPERM:
                print('Stale pidfile exists - Removing it.', file=sys.stderr)
                self.remove()
                return True
        except SystemError:
            print('Stale pidfile exists - Removing it.', file=sys.stderr)
            self.remove()
            return True
        return False

    def write_pid(self):
        pid = os.getpid()
        content = f'{pid}\n'

        pidfile_fd = os.open(self.path, PIDFILE_FLAGS, PIDFILE_MODE)
        pidfile = os.fdopen(pidfile_fd, 'w')
        try:
            pidfile.write(content)
            # flush and sync so that the re-read below works.
            pidfile.flush()
            try:
                os.fsync(pidfile_fd)
            except AttributeError:  # pragma: no cover
                pass
        finally:
            pidfile.close()

        rfh = open(self.path)
        try:
            if rfh.read() != content:
                raise LockFailed(
                    "Inconsistency: Pidfile content doesn't match at re-read")
        finally:
            rfh.close()


class PidDirectory:

    dirname = None

    def __init__(self, dirname):
        if dirname == '/' or '*' in dirname:
            raise ValueError("Can't use root or vague directory to store pidfile.")

        self.dirname = dirname
        self.program_name = dirname.split('/')[-1]

    def exists(self):
        return os.path.exists(self.dirname)

    def pidlocks(self):
        if not self.exists():
            return
        group = nodesplit(self.program_name)[0] or self.program_name
        for name in os.listdir(self.dirname):
            if not name.endswith('.pid'):
                continue
            pidlock = Pidfile(os.path.join(self.dirname, name))
            if name.startswith(group):
                yield pidlock

    def remove_if_stale(self):
        for pidlock in self.pidlocks():
            pidlock.remove_if_stale()
        if self.exists() and not os.listdir(self.dirname):
            print('Stale pid directory exists - Removing it.', file=sys.stderr)
            try:
                shutil.rmtree(self.dirname)
            except FileNotFoundError:
                pass

    def read_pids(self):
        pids = []
        for pidlock in self.pidlocks():
            if not pidlock.remove_if_stale():
                pids.append(pidlock.read_pid())
        if not pids:
            self.remove_if_stale()
        return pids


def create_pidlock(pidfile, group=None, allow_duplicates=False):
    """Create and verify pidfile.

    If the pidfile already exists the program exits with an error message,
    however if the process it refers to isn't running anymore, the pidfile
    is deleted and the program continues.

    This function will automatically install an :mod:`atexit` handler
    to release the lock at exit, you can skip this by calling
    :func:`_create_pidlock` instead.

    Returns:
       Pidfile: used to manage the lock.
    """
    pidlock = _create_pidlock(pidfile, group=group, allow_duplicates=allow_duplicates)
    atexit.register(pidlock.release)
    return pidlock


def _create_pidlock(pidfile, group=None, allow_duplicates=False):
    pidlock = Pidfile(pidfile, group=group)
    if pidlock.is_locked() and not pidlock.remove_if_stale():
        if allow_duplicates:
            pidfile = pidfile.replace('.pid', f'.{os.getpid()}.pid')
            return _create_pidlock(pidfile, group=group)
        print(PIDLOCKED.format(pidfile, pidlock.read_pid()), file=sys.stderr)
        raise SystemExit(EX_CANTCREAT)
    pidlock.acquire()
    return pidlock


def fd_by_path(paths):
    """Return a list of file descriptors.

    This method returns list of file descriptors corresponding to
    file paths passed in paths variable.

    Arguments:
        paths: List[str]: List of file paths.

    Returns:
        List[int]: List of file descriptors.

    Example:
        >>> keep = fd_by_path(['/dev/urandom', '/my/precious/'])
    """
    stats = set()
    for path in paths:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            continue
        try:
            stats.add(os.fstat(fd)[1:3])
        finally:
            os.close(fd)

    def fd_in_stats(fd):
        try:
            return os.fstat(fd)[1:3] in stats
        except OSError:
            return False

    return [_fd for _fd in range(get_fdmax(2048)) if fd_in_stats(_fd)]


class DaemonContext:
    """Context manager daemonizing the process."""

    _is_open = False

    def __init__(self, pidfile=None, workdir=None, umask=None,
                 fake=False, after_chdir=None, after_forkers=True,
                 **kwargs):
        if isinstance(umask, str):
            # octal or decimal, depending on initial zero.
            umask = int(umask, 8 if umask.startswith('0') else 10)
        self.workdir = workdir or DAEMON_WORKDIR
        self.umask = umask
        self.fake = fake
        self.after_chdir = after_chdir
        self.after_forkers = after_forkers
        self.stdfds = (sys.stdin, sys.stdout, sys.stderr)

    def redirect_to_null(self, fd):
        if fd is not None:
            dest = os.open(os.devnull, os.O_RDWR)
            os.dup2(dest, fd)

    def open(self):
        if not self._is_open:
            if not self.fake:
                self._detach()

            os.chdir(self.workdir)
            if self.umask is not None:
                os.umask(self.umask)

            if self.after_chdir:
                self.after_chdir()

            if not self.fake:
                # We need to keep /dev/urandom from closing because
                # shelve needs it, and Beat needs shelve to start.
                keep = list(self.stdfds) + fd_by_path(['/dev/urandom'])
                try:
                    close_open_fds(keep)
                except OSError:
                    # compat for gevent(>=25.4.1)
                    pass
                for fd in self.stdfds:
                    self.redirect_to_null(maybe_fileno(fd))
                if self.after_forkers and mputil is not None:
                    mputil._run_after_forkers()

            self._is_open = True

    __enter__ = open

    def close(self, *args):
        if self._is_open:
            self._is_open = False

    __exit__ = close

    def _detach(self):
        if os.fork() == 0:  # first child
            os.setsid()  # create new session
            if os.fork() > 0:  # pragma: no cover
                # second child
                os._exit(0)
        else:
            os._exit(0)
        return self


def detached(logfile=None, pidfile=None, uid=None, gid=None, umask=0,
             workdir=None, group=None, allow_duplicates=False, fake=False):
    """Detach the current process in the background (daemonize).

    Arguments:
        logfile (str): Optional log file.
            The ability to write to this file
            will be verified before the process is detached.
        pidfile (str): Optional pid file.
            The pidfile won't be created,
            as this is the responsibility of the child.  But the process will
            exit if the pid lock exists and the pid written is still running.
        uid (int, str): Optional user id or user name to change
            effective privileges to.
        gid (int, str): Optional group id or group name to change
            effective privileges to.
        umask (str, int): Optional umask that'll be effective in
            the child process.
        workdir (str): Optional new working directory.
        group (str):
        allow_duplicates (bool):
        fake (bool): Don't actually detach, intended for debugging purposes.
    """
    if not resource:
        raise RuntimeError('This platform does not support detach.')
    workdir = os.getcwd() if workdir is None else workdir

    signals.reset('SIGCLD')  # Make sure SIGCLD is using the default handler.
    maybe_drop_privileges(uid=uid, gid=gid)

    def after_chdir_do():
        # Since without stderr any errors will be silently suppressed,
        # we need to know that we have access to the logfile.
        logfile and open(logfile, 'a').close()
        # Doesn't actually create the pidfile, but makes sure it's not stale.
        if pidfile:
            # avoid pidlock conflicts.
            _create_pidlock(pidfile, group=group, allow_duplicates=allow_duplicates).release()

    return DaemonContext(
        umask=umask, workdir=workdir, fake=fake, after_chdir=after_chdir_do,
    )


def parse_uid(uid):
    """Parse user id.

    Arguments:
        uid (str, int): Actual uid, or the username of a user.
    Returns:
        int: The actual uid.
    """
    try:
        return int(uid)
    except ValueError:
        try:
            return pwd.getpwnam(uid).pw_uid
        except (AttributeError, KeyError):
            raise KeyError(f'User does not exist: {uid}')


def parse_gid(gid):
    """Parse group id.

    Arguments:
        gid (str, int): Actual gid, or the name of a group.
    Returns:
        int: The actual gid of the group.
    """
    try:
        return int(gid)
    except ValueError:
        try:
            return grp.getgrnam(gid).gr_gid
        except (AttributeError, KeyError):
            raise KeyError(f'Group does not exist: {gid}')


def _setgroups_hack(groups):
    # :fun:`setgroups` may have a platform-dependent limit,
    # and it's not always possible to know in advance what this limit
    # is, so we use this ugly hack stolen from glibc.
    groups = groups[:]

    while 1:
        try:
            return os.setgroups(groups)
        except ValueError:  # error from Python's check.
            if len(groups) <= 1:
                raise
            groups[:] = groups[:-1]
        except OSError as exc:  # error from the OS.
            if exc.errno != errno.EINVAL or len(groups) <= 1:
                raise
            groups[:] = groups[:-1]


def setgroups(groups):
    """Set active groups from a list of group ids."""
    max_groups = None
    try:
        max_groups = os.sysconf('SC_NGROUPS_MAX')
    except Exception:  # pylint: disable=broad-except
        pass
    try:
        return _setgroups_hack(groups[:max_groups])
    except OSError as exc:
        if exc.errno != errno.EPERM:
            raise
        if any(group not in groups for group in os.getgroups()):
            # we shouldn't be allowed to change to this group.
            raise


def initgroups(uid, gid):
    """Init process group permissions.

    Compat version of :func:`os.initgroups` that was first
    added to Python 2.7.
    """
    if not pwd:  # pragma: no cover
        return
    username = pwd.getpwuid(uid)[0]
    if hasattr(os, 'initgroups'):  # Python 2.7+
        return os.initgroups(username, gid)
    groups = [gr.gr_gid for gr in grp.getgrall()
              if username in gr.gr_mem]
    setgroups(groups)


def setgid(gid):
    """Version of :func:`os.setgid` supporting group names."""
    os.setgid(parse_gid(gid))


def setuid(uid):
    """Version of :func:`os.setuid` supporting usernames."""
    os.setuid(parse_uid(uid))


def maybe_drop_privileges(uid=None, gid=None):
    """Change process privileges to new user/group.

    If UID and GID is specified, the real user/group is changed.

    If only UID is specified, the real user is changed, and the group is
    changed to the users primary group.

    If only GID is specified, only the group is changed.
    """
    if sys.platform == 'win32':
        return
    if os.geteuid():
        # no point trying to setuid unless we're root.
        if not os.getuid():
            raise SecurityError('contact support')
    uid = uid and parse_uid(uid)
    gid = gid and parse_gid(gid)

    if uid:
        _setuid(uid, gid)
    else:
        gid and setgid(gid)

    if uid and not os.getuid() and not os.geteuid():
        raise SecurityError('Still root uid after drop privileges!')
    if gid and not os.getgid() and not os.getegid():
        raise SecurityError('Still root gid after drop privileges!')


def _setuid(uid, gid):
    # If GID isn't defined, get the primary GID of the user.
    if not gid and pwd:
        gid = pwd.getpwuid(uid).pw_gid
    # Must set the GID before initgroups(), as setgid()
    # is known to zap the group list on some platforms.

    # setgid must happen before setuid (otherwise the setgid operation
    # may fail because of insufficient privileges and possibly stay
    # in a privileged group).
    setgid(gid)
    initgroups(uid, gid)

    # at last:
    setuid(uid)
    # ... and make sure privileges cannot be restored:
    try:
        setuid(0)
    except OSError as exc:
        if exc.errno != errno.EPERM:
            raise
        # we should get here: cannot restore privileges,
        # everything was fine.
    else:
        raise SecurityError(
            'non-root user able to restore privileges after setuid.')


if hasattr(_signal, 'setitimer'):
    def _arm_alarm(seconds):
        _signal.setitimer(_signal.ITIMER_REAL, seconds)
else:
    def _arm_alarm(seconds):
        _signal.alarm(math.ceil(seconds))


class Signals:
    """Convenience interface to :mod:`signals`.

    If the requested signal isn't supported on the current platform,
    the operation will be ignored.
    """

    ignored = _signal.SIG_IGN
    default = _signal.SIG_DFL

    def arm_alarm(self, seconds):
        return _arm_alarm(seconds)

    def reset_alarm(self):
        return _signal.alarm(0)

    def supported(self, name):
        """Return true value if signal by ``name`` exists on this platform."""
        try:
            self.signum(name)
        except AttributeError:
            return False
        else:
            return True

    def signum(self, name):
        """Get signal number by name."""
        if isinstance(name, numbers.Integral):
            return name
        if not isinstance(name, str) \
                or not name.isupper():
            raise TypeError('signal name must be uppercase string.')
        if not name.startswith('SIG'):
            name = 'SIG' + name
        return getattr(_signal, name)

    def reset(self, *signal_names):
        """Reset signals to the default signal handler.

        Does nothing if the platform has no support for signals,
        or the specified signal in particular.
        """
        self.update((sig, self.default) for sig in signal_names)

    def ignore(self, *names):
        """Ignore signal using :const:`SIG_IGN`.

        Does nothing if the platform has no support for signals,
        or the specified signal in particular.
        """
        self.update((sig, self.ignored) for sig in names)

    def __getitem__(self, name):
        return _signal.getsignal(self.signum(name))

    def __setitem__(self, name, handler):
        """Install signal handler.

        Does nothing if the current platform has no support for signals,
        or the specified signal in particular.
        """
        try:
            _signal.signal(self.signum(name), handler)
        except (AttributeError, ValueError):
            pass

    def update(self, _d_=None, **sigmap):
        """Set signal handlers from a mapping."""
        for name, handler in dict(_d_ or {}, **sigmap).items():
            self[name] = handler


signals = Signals()
get_signal = signals.signum  # compat
install_signal_handler = signals.__setitem__  # compat
reset_signal = signals.reset  # compat
ignore_signal = signals.ignore  # compat


def signal_name(signum):
    """Return name of signal from signal number."""
    return SIGMAP[signum][3:]


def set_pdeathsig(name):
    """Sends signal ``name`` to process when parent process terminates."""
    if signals.supported('SIGKILL'):
        try:
            _set_pdeathsig(signals.signum('SIGKILL'))
        except OSError:
            # We ignore when OS does not support set_pdeathsig
            pass


def get_errno_name(n):
    """Get errno for string (e.g., ``ENOENT``)."""
    if isinstance(n, str):
        return getattr(errno, n)
    return n


@contextmanager
def ignore_errno(*errnos, **kwargs):
    """Context manager to ignore specific POSIX error codes.

    Takes a list of error codes to ignore: this can be either
    the name of the code, or the code integer itself::

        >>> with ignore_errno('ENOENT'):
        ...     with open('foo', 'r') as fh:
        ...         return fh.read()

        >>> with ignore_errno(errno.ENOENT, errno.EPERM):
        ...    pass

    Arguments:
        types (Tuple[Exception]): A tuple of exceptions to ignore
            (when the errno matches).  Defaults to :exc:`Exception`.
    """
    types = kwargs.get('types') or (Exception,)
    errnos = [get_errno_name(errno) for errno in errnos]
    try:
        yield
    except types as exc:
        if not hasattr(exc, 'errno'):
            raise
        if exc.errno not in errnos:
            raise
