import time
import inspect
from itertools import count
from typing import Iterator


def dictfilter(d=None, **kw):
    """Remove all keys from dict ``d`` whose value is :const:`None`."""
    d = kw if d is None else (dict(d, **kw) if kw else d)
    return {k: v for k, v in d.items() if v is not None}


def fxrange(start=1.0, stop=None, step=1.0, repeatlast=False):
    cur = start * 1.0
    while 1:
        if not stop or cur <= stop:
            yield cur
            cur += step
        else:
            if not repeatlast:
                break
            yield cur - step


def fun_accepts_kwargs(fun):
    """Return true if function accepts arbitrary keyword arguments."""
    return any(
        p for p in inspect.signature(fun).parameters.values()
        if p.kind == p.VAR_KEYWORD
    )


def retry_over_time(fun, catch, args=None, kwargs=None, errback=None,
                    max_retries=None, interval_start=2, interval_step=2,
                    interval_max=30, callback=None, timeout=None):
    """Retry the function over and over until max retries is exceeded.

    For each retry we sleep a for a while before we try again, this interval
    is increased for every retry until the max seconds is reached.

    Arguments:
        fun (Callable): The function to try
        catch (Tuple[BaseException]): Exceptions to catch, can be either
            tuple or a single exception class.

    Keyword Arguments:
        args (Tuple): Positional arguments passed on to the function.
        kwargs (Dict): Keyword arguments passed on to the function.
        errback (Callable): Callback for when an exception in ``catch``
            is raised.  The callback must take three arguments:
            ``exc``, ``interval_range`` and ``retries``, where ``exc``
            is the exception instance, ``interval_range`` is an iterator
            which return the time in seconds to sleep next, and ``retries``
            is the number of previous retries.
        max_retries (int): Maximum number of retries before we give up.
            If neither of this and timeout is set, we will retry forever.
            If one of this and timeout is reached, stop.
        interval_start (float): How long (in seconds) we start sleeping
            between retries.
        interval_step (float): By how much the interval is increased for
            each retry.
        interval_max (float): Maximum number of seconds to sleep
            between retries.
        timeout (int): Maximum seconds waiting before we give up.
    """
    kwargs = {} if not kwargs else kwargs
    args = [] if not args else args
    interval_range = fxrange(interval_start,
                             interval_max + interval_start,
                             interval_step, repeatlast=True)
    end = time.time() + timeout if timeout else None
    for retries in count():
        try:
            return fun(*args, **kwargs)
        except catch as exc:
            if max_retries is not None and retries >= max_retries:
                raise
            if end and time.time() > end:
                raise
            if callback:
                callback()
            tts = float(errback(exc, interval_range, retries) if errback
                        else next(interval_range))
            if tts:
                for _ in range(int(tts)):
                    if callback:
                        callback()
                    time.sleep(1.0)
                # sleep remainder after int truncation above.
                time.sleep(abs(int(tts) - tts))


def noop(*args, **kwargs):
    """No operation.

    Takes any arguments/keyword arguments and does nothing.
    """


def iter_generator(iterable, sleep=time.sleep):
    it = iterable = iterable if iterable is not None else []
    # a generator is always an iterator.
    if not isinstance(iterable, Iterator):
        try:
            it = iter(iterable)
        except TypeError:
            it = iter([iterable])
    try:
        while True:
            try:
                # generator's return value will be ignored here.
                yield next(it)
            except StopIteration:
                break
            sleep(0.01)
    finally:
        if isinstance(iterable, Iterator) and hasattr(iterable, 'close'):
            iterable.close()
        del iterable, it
