from datetime import date, datetime, timezone

import pytest

from energy_lakehouse.timeutils import (
    parse_iso_utc,
    periods_in_settlement_day,
    settlement_period_start_utc,
)

UTC = timezone.utc


@pytest.mark.parametrize(
    "d, expected",
    [
        (date(2026, 11, 12), 48),   # normal day
        (date(2026, 3, 29), 46),    # clocks go forward (BST starts)
        (date(2026, 10, 25), 50),   # clocks go back (BST ends)
        (date(2025, 3, 30), 46),
        (date(2025, 10, 26), 50),
    ],
)
def test_periods_in_settlement_day(d, expected):
    assert periods_in_settlement_day(d) == expected


def test_period_starts_on_normal_winter_day():
    assert settlement_period_start_utc(date(2026, 11, 12), 1) == datetime(2026, 11, 12, 0, 0, tzinfo=UTC)
    assert settlement_period_start_utc(date(2026, 11, 12), 48) == datetime(2026, 11, 12, 23, 30, tzinfo=UTC)


def test_period_starts_on_autumn_clock_change():
    d = date(2026, 10, 25)
    # Local midnight is still BST (UTC+1), so period 1 starts 23:00 UTC the day before.
    assert settlement_period_start_utc(d, 1) == datetime(2026, 10, 24, 23, 0, tzinfo=UTC)
    # Local 01:00-02:00 happens twice; periods 3-6 run through it continuously in UTC.
    assert settlement_period_start_utc(d, 5) == datetime(2026, 10, 25, 1, 0, tzinfo=UTC)
    assert settlement_period_start_utc(d, 50) == datetime(2026, 10, 25, 23, 30, tzinfo=UTC)


def test_period_starts_on_spring_clock_change():
    d = date(2026, 3, 29)
    assert settlement_period_start_utc(d, 1) == datetime(2026, 3, 29, 0, 0, tzinfo=UTC)
    assert settlement_period_start_utc(d, 46) == datetime(2026, 3, 29, 22, 30, tzinfo=UTC)


def test_invalid_period_rejected():
    with pytest.raises(ValueError):
        settlement_period_start_utc(date(2026, 3, 29), 47)


def test_parse_iso_utc_handles_z_without_seconds():
    assert parse_iso_utc("2026-11-11T23:00Z") == datetime(2026, 11, 11, 23, 0, tzinfo=UTC)
