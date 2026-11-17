"""Time helpers, including GB settlement periods across clock changes.

A GB settlement day runs from local midnight to local midnight (Europe/London) and is
split into 30-minute settlement periods. That gives 48 periods on a normal day, 46 on
the spring clock change and 50 on the autumn one.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")
BRUSSELS = ZoneInfo("Europe/Brussels")
UTC = timezone.utc
PERIOD = timedelta(minutes=30)


def local_midnight_utc(d: date, tz: ZoneInfo = LONDON) -> datetime:
    """UTC instant of local midnight at the start of ``d`` in ``tz``."""
    return datetime.combine(d, time(0, 0), tzinfo=tz).astimezone(UTC)


def periods_in_settlement_day(d: date) -> int:
    """Number of half-hour settlement periods in a GB settlement day (46, 48 or 50)."""
    start = local_midnight_utc(d)
    end = local_midnight_utc(d + timedelta(days=1))
    return int((end - start) / PERIOD)


def settlement_period_start_utc(d: date, period: int) -> datetime:
    """UTC start time of settlement ``period`` (1-based) on settlement date ``d``."""
    n = periods_in_settlement_day(d)
    if not 1 <= period <= n:
        raise ValueError(f"{d} has {n} settlement periods; got period {period}")
    return local_midnight_utc(d) + (period - 1) * PERIOD


def daterange(start: date, end: date) -> Iterator[date]:
    """Inclusive range of dates."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    for i in range((end - start).days + 1):
        yield start + timedelta(days=i)


def today_london() -> date:
    return datetime.now(LONDON).date()


def parse_iso_utc(value: str) -> datetime:
    """Parse ISO-8601 timestamps such as ``2026-11-11T23:00Z`` into aware UTC datetimes."""
    v = value.strip().replace("Z", "+00:00")
    # ENTSO-E sometimes omits seconds: 2026-11-11T23:00+00:00 is valid for fromisoformat.
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
