import sys
import time as _time
from datetime import date, datetime, timedelta
from datetime import timezone as datetime_timezone
from datetime import tzinfo
from typing import Optional, Union, Dict

from dateutil import tz as dateutil_tz
from kombu.utils import cached_property

from tider.utils.text import pluralize

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


__all__ = (
    'preferred_clock', 'LocalTimezone', 'timezone',
    'is_naive', 'make_aware', 'to_utc', 'localize', 'maybe_make_aware', 'humanize_seconds',
)

# Preferred clock, based on which one is more accurate on a given system.
if sys.platform == "win32":
    preferred_clock = _time.perf_counter
else:
    preferred_clock = _time.time

TIME_UNITS = (
    ('day', 60 * 60 * 24.0, lambda n: format(n, '.2f')),
    ('hour', 60 * 60.0, lambda n: format(n, '.2f')),
    ('minute', 60.0, lambda n: format(n, '.2f')),
    ('second', 1.0, lambda n: format(n, '.2f')),
)

ZERO = timedelta(0)

_local_timezone = None


class LocalTimezone(tzinfo):
    """Local time implementation. Provided in _Zone to the app when `enable_utc` is disabled.
    Otherwise, _Zone provides a UTC ZoneInfo instance as the timezone implementation for the application.

    Note:
        Used only when the :setting:`enable_utc` setting is disabled.
    """

    _offset_cache: Dict[int, tzinfo] = {}

    def __init__(self) -> None:
        # This code is moved in __init__ to execute it as late as possible
        # See get_default_timezone().
        self.STDOFFSET = timedelta(seconds=-_time.timezone)
        if _time.daylight:
            self.DSTOFFSET = timedelta(seconds=-_time.altzone)
        else:
            self.DSTOFFSET = self.STDOFFSET
        self.DSTDIFF = self.DSTOFFSET - self.STDOFFSET
        super().__init__()

    def __repr__(self) -> str:
        return f'<LocalTimezone: UTC{int(self.DSTOFFSET.total_seconds() / 3600):+03d}>'

    def utcoffset(self, dt: datetime) -> timedelta:
        return self.DSTOFFSET if self._isdst(dt) else self.STDOFFSET

    def dst(self, dt: datetime) -> timedelta:
        return self.DSTDIFF if self._isdst(dt) else ZERO

    def tzname(self, dt: datetime) -> str:
        return _time.tzname[self._isdst(dt)]

    def fromutc(self, dt: datetime) -> datetime:
        # The base tzinfo class no longer implements a DST
        # offset aware .fromutc() in Python 3 (Issue #2306).
        offset = int(self.utcoffset(dt).seconds / 60.0)
        try:
            tz = self._offset_cache[offset]
        except KeyError:
            tz = self._offset_cache[offset] = datetime_timezone(
                timedelta(minutes=offset))
        return tz.fromutc(dt.replace(tzinfo=tz))

    def _isdst(self, dt: datetime) -> bool:
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0


class _Zone:
    """Timezone class that provides the timezone for the application.
    If `enable_utc` is disabled, LocalTimezone is provided as the timezone provider through local().
    Otherwise, this class provides a UTC ZoneInfo instance as the timezone provider for the application.

    Additionally, this class provides a few utility methods for converting datetimes.
    """

    def tz_or_local(self, tzinfo: Optional[tzinfo] = None) -> tzinfo:
        """Return either our local timezone or the provided timezone."""

        # pylint: disable=redefined-outer-name
        if tzinfo is None:
            return self.local
        return self.get_timezone(tzinfo)

    def to_local(self, dt: datetime, local=None, orig=None):
        """Converts a datetime to the local timezone."""

        if is_naive(dt):
            dt = make_aware(dt, orig or self.utc)
        return localize(dt, self.tz_or_local(local))

    def to_system(self, dt: datetime) -> datetime:
        """Converts a datetime to the system timezone."""

        # tz=None is a special case since Python 3.3, and will
        # convert to the current local timezone (Issue #2306).
        return dt.astimezone(tz=None)

    def to_local_fallback(self, dt: datetime) -> datetime:
        """Converts a datetime to the local timezone, or the system timezone."""
        if is_naive(dt):
            return make_aware(dt, self.local)
        return localize(dt, self.local)

    def get_timezone(self, zone: Union[str, tzinfo]) -> tzinfo:
        """Returns ZoneInfo timezone if the provided zone is a string, otherwise return the zone."""
        if isinstance(zone, str):
            return ZoneInfo(zone)
        return zone

    @cached_property
    def local(self) -> LocalTimezone:
        """Return LocalTimezone instance for the application."""
        return LocalTimezone()

    @cached_property
    def utc(self) -> tzinfo:
        """Return UTC timezone created with ZoneInfo."""
        return self.get_timezone('UTC')


timezone = _Zone()


def is_naive(dt: datetime) -> bool:
    """Return True if :class:`~datetime.datetime` is naive, meaning it doesn't have timezone info set."""
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None


def _can_detect_ambiguous(tz: tzinfo) -> bool:
    """Helper function to determine if a timezone can detect ambiguous times using dateutil."""

    return isinstance(tz, ZoneInfo) or hasattr(tz, "is_ambiguous")


def _is_ambigious(dt: datetime, tz: tzinfo) -> bool:
    """Helper function to determine if a timezone is ambiguous using python's dateutil module.

    Returns False if the timezone cannot detect ambiguity, or if there is no ambiguity, otherwise True.

    In order to detect ambiguous datetimes, the timezone must be built using ZoneInfo, or have an is_ambiguous
    method. Previously, pytz timezones would throw an AmbiguousTimeError if the localized dt was ambiguous,
    but now we need to specifically check for ambiguity with dateutil, as pytz is deprecated.
    """

    return _can_detect_ambiguous(tz) and dateutil_tz.datetime_ambiguous(dt)


def make_aware(dt: datetime, tz: tzinfo) -> datetime:
    """Set timezone for a :class:`~datetime.datetime` object."""

    dt = dt.replace(tzinfo=tz)
    if _is_ambigious(dt, tz):
        dt = min(dt.replace(fold=0), dt.replace(fold=1))
    return dt


def localize(dt: datetime, tz: tzinfo) -> datetime:
    """Convert aware :class:`~datetime.datetime` to another timezone.

    Using a ZoneInfo timezone will give the most flexibility in terms of ambiguous DST handling.
    """
    if is_naive(dt):  # Ensure timezone aware datetime
        dt = make_aware(dt, tz)
    if dt.tzinfo == ZoneInfo("UTC"):
        dt = dt.astimezone(tz)  # Always safe to call astimezone on utc zones
    return dt


def to_utc(dt: datetime) -> datetime:
    """Convert naive :class:`~datetime.datetime` to UTC."""
    return make_aware(dt, timezone.utc)


def maybe_make_aware(dt: datetime, tz: Optional[tzinfo] = None,
                     naive_as_utc: bool = True) -> datetime:
    """Convert dt to aware datetime, do nothing if dt is already aware."""
    if is_naive(dt):
        if naive_as_utc:
            dt = to_utc(dt)
        return localize(
            dt, timezone.utc if tz is None else timezone.tz_or_local(tz),
        )
    return dt


def humanize_seconds(
        secs: int, prefix: str = '', sep: str = '', now: str = 'now',
        microseconds: bool = False) -> str:
    """Show seconds in human form.

    For example, 60 becomes "1 minute", and 7200 becomes "2 hours".

    Arguments:
        prefix (str): can be used to add a preposition to the output
            (e.g., 'in' will give 'in 1 second', but add nothing to 'now').
        now (str): Literal 'now'.
        microseconds (bool): Include microseconds.
    """
    secs = float(format(float(secs), '.2f'))
    for unit, divider, formatter in TIME_UNITS:
        if secs >= divider:
            w = secs / float(divider)
            return '{}{}{} {}'.format(prefix, sep, formatter(w),
                                      pluralize(w, unit))
    if microseconds and secs > 0.0:
        return '{prefix}{sep}{0:.2f} seconds'.format(
            secs, sep=sep, prefix=prefix)
    return now
