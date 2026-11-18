import importlib.util
from datetime import date
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "backfill", Path(__file__).parents[1] / "scripts" / "backfill.py")
backfill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(backfill)


def test_range_up_to_current_month_stops_at_yesterday():
    w = backfill.month_windows({"start_month": "2024-01", "end_month": "current"},
                               today=date(2026, 11, 18))
    assert len(w) == 35                                     # Jan 2024 .. Nov 2026
    assert w[0] == (date(2024, 1, 1), date(2024, 1, 31))
    assert w[1] == (date(2024, 2, 1), date(2024, 2, 29))    # leap year
    assert w[-1] == (date(2026, 11, 1), date(2026, 11, 17))


def test_first_of_month_drops_the_empty_current_month():
    w = backfill.month_windows({"start_month": "2026-10", "end_month": "current"},
                               today=date(2026, 11, 1))
    assert w == [(date(2026, 10, 1), date(2026, 10, 31))]


def test_explicit_months_override_the_range_and_are_sorted():
    w = backfill.month_windows({"start_month": "2020-01", "months": ["2024-10", "2024-03"]},
                               today=date(2026, 11, 18))
    assert w == [(date(2024, 3, 1), date(2024, 3, 31)), (date(2024, 10, 1), date(2024, 10, 31))]


def test_every_window_fits_the_ingest_cost_guard():
    from energy_lakehouse.config import DEFAULT_MAX_DAYS_PER_RUN

    w = backfill.month_windows({"start_month": "2024-01"}, today=date(2026, 11, 18))
    assert all((e - s).days + 1 <= DEFAULT_MAX_DAYS_PER_RUN for s, e in w)
