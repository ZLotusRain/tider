from click import ClickException


def reraise(tp, value, tb=None):
    """Reraise exception."""
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


class SpiderShutdown(SystemExit):
    """Signals that the crawler should perform a warm shutdown."""


class TiderDeprecationWarning(Warning):
    """Warning category for deprecated features, since the default
    DeprecationWarning is silenced on Python 2.7+
    """


class TiderWarning(UserWarning):
    """Base class for all Tider warnings."""


class DuplicateSpiderWarning(TiderWarning):
    """Multiple spiders are using the same name."""


class TiderException(Exception):
    """Base class for all Tider errors."""


class WgetError(TiderException):
    def __init__(self, cmd, returncode=None, timeout=None, output=None, stderr=None):
        self.cmd = cmd
        self.returncode = returncode
        self.timeout = timeout
        self.output = output
        self.stderr = stderr

    def __str__(self):
        if self.returncode is not None:
            return f'Wget failed with returncode {self.returncode}): {self.cmd}'
        elif self.timeout is not None:
            return f'Wget commands {self.cmd} timed out after {self.timeout} seconds'
        else:
            return f'Wget failed with unknown error: {self.cmd}'

    @property
    def stdout(self):
        """Alias for output attribute, to match stderr"""
        return self.output

    @stdout.setter
    def stdout(self, value):
        # There's no obvious reason to set this, but allow it anyway so
        # .stdout is a transparent alias for .output
        self.output = value


class TiderCommandException(ClickException):
    """A general command exception which stores an exit code."""

    def __init__(self, message, exit_code):
        super().__init__(message=message)
        self.exit_code = exit_code
