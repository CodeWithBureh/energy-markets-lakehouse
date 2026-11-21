from datetime import date

import pytest

from energy_lakehouse import checks
from energy_lakehouse.checks import Check, build_checks, main, run_checks, summarise

TODAY = date(2026, 11, 21)


def test_checks_target_the_given_schemas_and_dates():
    built = build_checks("energy_dev", "ci_silver", "ci_gold", 3, TODAY)
    sql = "\n".join(c.sql for c in built)
    assert "`energy_dev`.`ci_silver`.`gb_system_prices`" in sql
    assert "`energy_dev`.`ci_gold`.`fact_gb_half_hourly`" in sql
    assert "DATE'2026-11-18'" in sql                     # freshness: today - 3 days
    assert "`energy`." not in sql                         # never reads another catalog


def test_era5_checks_are_optional_but_market_checks_are_required():
    built = {c.name: c for c in build_checks("c", "s", "g", 3, TODAY)}
    assert not built["silver.era5_gb_grid_hourly has rows"].required
    assert not built["gold.gb_weather_hourly has rows"].required
    assert built["silver.gb_system_prices has rows"].required
    assert all(c.required for n, c in built.items() if "fresh" in n)


def test_uniqueness_checks_only_look_at_current_scd2_rows():
    unique = [c for c in build_checks("c", "s", "g", 3, TODAY) if "unique" in c.name]
    assert len(unique) == len(checks.SILVER_KEYS)
    assert all("__END_AT IS NULL" in c.sql for c in unique)


def test_query_errors_become_failed_checks():
    def query(sql):
        if "missing" in sql:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND\nlong stack trace")
        return 0

    results = run_checks([Check("ok", "SELECT 0"), Check("gone", "SELECT * FROM missing")],
                         query)
    assert [r.passed for r in results] == [True, False]
    assert results[1].error == "RuntimeError: TABLE_OR_VIEW_NOT_FOUND"


def test_optional_failures_warn_and_required_failures_fail():
    results = run_checks([Check("req", "a"), Check("opt", "b", required=False)], lambda _: 2)
    failures, warnings = summarise(results)
    assert [r.check.name for r in failures] == ["req"]
    assert [r.check.name for r in warnings] == ["opt"]


def test_main_exit_code_reflects_required_checks_only():
    argv = ["--catalog", "c", "--silver-schema", "s", "--gold-schema", "g"]
    era5_only = lambda sql: 1 if ("era5" in sql or "gb_weather" in sql) else 0
    assert main(argv, query=era5_only, today=TODAY) == 0
    assert main(argv, query=lambda sql: 1, today=TODAY) == 1


def test_wheel_entry_point_raises_so_the_task_fails(monkeypatch):
    monkeypatch.setattr(checks, "main", lambda argv=None: 1)
    with pytest.raises(RuntimeError):
        checks.run([])
