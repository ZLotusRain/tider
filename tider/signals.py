"""Tider Signals.

This module defines the signals (Observer pattern).

Functions can be connected to these signals, and connected
functions are called whenever a signal is called.
"""

from .utils.signal import Signal

__all__ = ('setup_logging', 'after_setup_logger', 'message_rejected', )


setup_logging = Signal(
    name='setup_logging',
    providing_args={
        'loglevel', 'logfile', 'format', 'colorize',
    },
)

after_setup_logger = Signal(
    name='after_setup_logger',
    providing_args={
        'logger', 'loglevel', 'logfile', 'format', 'colorize',
    },
)

crawler_shutting_down = Signal(
    name='crawler_shutting_down',
    providing_args={'sig', 'how', 'exitcode'},
)

message_rejected = Signal(
    name='message_rejected',
    providing_args={'message', 'exc'},
)

engine_started = Signal(
    name='engine_started',
)

engine_stopped = Signal(
    name='engine_stopped',
)

engine_shutdown = Signal(
    name='engine_shutdown',
)

spider_closed = Signal(
    name='spider_closed',
)
