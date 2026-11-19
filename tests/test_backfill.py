import importlib.util
import json
from datetime import date
from pathlib import Path

from energy_lakehouse.backfill import month_windows, plan_main, source_flags
from energy_lakehouse.config import DEFAULT_MAX_DAYS_PER_RUN

TODAY = date(2026, 11, 18)


def test_range_up_to_current_month_stops_at_yesterday():
    w = month_windows("2024-01", "current", today=TODAY)
    assert len(w) == 35                                     # Jan 2024 .. Nov 2026
    assert w[0] == (date(2024, 1, 1), date(2024, 1, 31))
    assert w[1] == (date(2024, 2, 1), date(2024, 2, 29))    # leap year
    assert w[-1] == (date(2026, 11, 1), date(2026, 11, 17))


def test_first_of_month_drops_the_empty_current_month():
    assert month_windows("2026-10", "current", today=date(2026, 11, 1)) == [
        (date(2026, 10, 1), date(2026, 10, 31))]


def test_explicit_months_override_the_range_and_are_sorted():
    assert month_windows("2020-01", months=["2024-10", "2024-03"], today=TODAY) == [
        (date(2024, 3, 1), date(2024, 3, 31)), (date(2024, 10, 1), date(2024, 10, 31))]


def test_every_window_fits_the_ingest_cost_guard():
    w = month_windows("2024-01", today=TODAY)
    assert all((e - s).days + 1 <= DEFAULT_MAX_DAYS_PER_RUN for s, e in w)


def test_plan_task_prints_for_each_inputs(capsys, monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    # Same shape the job passes: blank months, space-separated list form also accepted.
    assert plan_main(["--start-month=", "--end-month=current", "--months=2024-03 2024-10"]) == 0
    plan = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert plan == [{"start": "2024-03-01", "end": "2024-03-31"},
                    {"start": "2024-10-01", "end": "2024-10-31"}]


def test_plan_task_fails_when_there_is_nothing_to_run():
    assert plan_main(["--start-month=2999-01", "--end-month=2999-02"]) == 2


def test_script_builds_job_params():
    spec = importlib.util.spec_from_file_location(
        "backfill_script", Path(__file__).parents[1] / "scripts" / "backfill.py")
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    assert script.job_params({"start_month": "2024-01", "months": ["2024-03", "2024-10"],
                              "include_weather": True}) == (
        "start_month=2024-01,end_month=current,months=2024-03 2024-10,include_weather=true,"
        "weather_only=false")


def test_source_flags():
    # Blank job parameters (the CLI default) mean markets only.
    assert source_flags("", "") == {"run_markets": "true", "run_weather": "false"}
    assert source_flags("true", "false") == {"run_markets": "true", "run_weather": "true"}
    # weather_only implies ERA5 even when include_weather is off.
    assert source_flags("false", "TRUE") == {"run_markets": "false", "run_weather": "true"}
