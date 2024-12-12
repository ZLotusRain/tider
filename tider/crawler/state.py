from tider.exceptions import SpiderShutdown, SpiderTerminate

should_stop = None
should_terminate = None


def maybe_shutdown():
    """Shutdown if flags have been set."""
    if should_terminate is not None and should_terminate is not False:
        raise SpiderTerminate(should_terminate)
    elif should_stop is not None and should_stop is not False:
        raise SpiderShutdown(should_stop)
