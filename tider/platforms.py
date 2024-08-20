import os
from celery.platforms import (
    DaemonContext as CeleryDaemonContext,
    signals, maybe_drop_privileges, _create_pidlock,
    close_open_fds, maybe_fileno
)

from tider.utils.misc import try_import


resource = try_import("resource")
mputil = try_import('multiprocessing.util')


class DaemonContext(CeleryDaemonContext):

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
                keep = list(self.stdfds)
                close_open_fds(keep)
                for fd in self.stdfds:
                    self.redirect_to_null(maybe_fileno(fd))
                if self.after_forkers and mputil is not None:
                    mputil._run_after_forkers()

            self._is_open = True

    def _detach(self):
        # os._exit() will exit the program immediately,
        # sys.exit() will raise exception otherwise.
        pid = os.fork()
        if pid == 0:  # first child
            os.setsid()  # create new session
            pid = os.fork()
            if pid > 0:  # pragma: no cover
                # second child
                os._exit(0)
        else:
            print(f"> Start daemon process: {pid} ")
            os._exit(0)
        return self


def detached(logfile=None, pidfile=None, uid=None, gid=None, umask=0,
             workdir=None, fake=False, **opts):
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
            _create_pidlock(pidfile).release()

    return DaemonContext(
        umask=umask, workdir=workdir, fake=fake, after_chdir=after_chdir_do,
    )
