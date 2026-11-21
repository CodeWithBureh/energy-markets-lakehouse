"""Data checks run after a deploy (and on demand): fail the task if the lakehouse looks wrong.

The energy_checks job runs this against the target's own schemas, so the same checks
cover the CI copy in energy_dev and prod:

    check_lakehouse --catalog energy --silver-schema silver --gold-schema gold --max-age-days 3

Each check is one SQL query returning a single number of problems (0 = pass). Required
checks fail the task; optional ones (ERA5, which only the weekly job loads) just warn.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from energy_lakehouse.timeutils import today_london

log = logging.getLogger("energy_lakehouse")

# Silver tables merged as SCD type 2 and the keys that must be unique among current rows.
SILVER_KEYS = {
    "gb_system_prices": ["settlement_date", "settlement_period"],
    "gb_generation_by_fuel": ["settlement_date", "settlement_period", "fuel_type"],
    "eu_day_ahead_prices": ["zone", "delivery_start_utc", "resolution_minutes",
                            "classification_sequence"],
    "fred_observations": ["series_id", "obs_date"],
    "stock_prices_daily": ["ticker", "trade_date"],
    "era5_gb_grid_hourly": ["valid_time_utc", "latitude", "longitude"],
    "gb_demand_outturn": ["settlement_date", "settlement_period"],
    "gb_market_index_prices": ["settlement_date", "settlement_period", "data_provider"],
    "gb_demand_forecast": ["publish_time_utc", "settlement_date", "settlement_period"],
    "gb_wind_forecast": ["publish_time_utc", "start_time_utc"],
    "gb_wind_solar_forecast": ["publish_time_utc", "process_type", "psr_type",
                               "settlement_date", "settlement_period"],
}
SILVER_TABLES = [*SILVER_KEYS, "fx_rates_daily", "gas_prices"]
GOLD_TABLES = ["dim_date", "dim_company", "dim_fx_daily", "gb_weather_hourly",
               "fact_gb_half_hourly", "fact_eu_day_ahead_hourly", "fact_market_daily",
               "fact_stock_returns_daily", "mart_stocks_vs_market",
               "fact_gb_day_ahead_forecasts"]
# Loaded only by the weekly ERA5 job or a weather backfill, so may be empty in a fresh copy.
OPTIONAL_TABLES = {"era5_gb_grid_hourly", "gb_weather_hourly"}
# Added in Nov 2026. Prod's deploy checks run before the first daily run has loaded them,
# so they only warn for now; move them to required once prod has been backfilled.
OPTIONAL_TABLES |= {"gb_demand_outturn", "gb_market_index_prices", "gb_demand_forecast",
                    "gb_wind_forecast", "gb_wind_solar_forecast",
                    "fact_gb_day_ahead_forecasts"}

# Wide enough for any real GB imbalance price; anything outside is a parsing bug.
PRICE_BOUNDS = (-1000.0, 10000.0)


@dataclass(frozen=True)
class Check:
    name: str
    sql: str            # returns one row with one number: how many problems
    required: bool = True


@dataclass(frozen=True)
class Result:
    check: Check
    problems: int | None
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and self.problems == 0


def build_checks(catalog: str, silver: str, gold: str, max_age_days: int,
                 today: date) -> list[Check]:
    s = lambda t: f"`{catalog}`.`{silver}`.`{t}`"
    g = lambda t: f"`{catalog}`.`{gold}`.`{t}`"
    oldest_ok = (today - timedelta(days=max_age_days)).isoformat()
    lo, hi = PRICE_BOUNDS
    checks: list[Check] = []

    for t in SILVER_TABLES:
        checks.append(Check(f"silver.{t} has rows",
                            f"SELECT IF(COUNT(*) > 0, 0, 1) FROM {s(t)}",
                            required=t not in OPTIONAL_TABLES))
    for t in GOLD_TABLES:
        checks.append(Check(f"gold.{t} has rows",
                            f"SELECT IF(COUNT(*) > 0, 0, 1) FROM {g(t)}",
                            required=t not in OPTIONAL_TABLES))

    for t, keys in SILVER_KEYS.items():
        cols = ", ".join(keys)
        checks.append(Check(
            f"silver.{t} current rows are unique on ({cols})",
            f"SELECT COUNT(*) FROM (SELECT {cols} FROM {s(t)} WHERE __END_AT IS NULL "
            f"GROUP BY {cols} HAVING COUNT(*) > 1)",
            required=t not in OPTIONAL_TABLES,
        ))

    checks += [
        Check(f"GB prices are fresh (latest day on or after {oldest_ok})",
              f"SELECT IF(MAX(settlement_date) >= DATE'{oldest_ok}', 0, 1) "
              f"FROM {g('fact_gb_half_hourly')}"),
        Check(f"EU day-ahead prices are fresh (latest day on or after {oldest_ok})",
              f"SELECT IF(MAX(DATE(hour_utc)) >= DATE'{oldest_ok}', 0, 1) "
              f"FROM {g('fact_eu_day_ahead_hourly')}"),
        Check("No incomplete GB settlement days in the last 30 days",
              f"SELECT COUNT(*) FROM {g('dq_settlement_period_completeness')} "
              f"WHERE date >= DATE'{(today - timedelta(days=30)).isoformat()}' "
              f"AND date < DATE'{today.isoformat()}'"),
        Check(f"GB prices in the last 30 days are present and within {lo:g} to {hi:g} GBP/MWh",
              f"SELECT COUNT(*) FROM {g('fact_gb_half_hourly')} "
              f"WHERE settlement_date >= DATE'{(today - timedelta(days=30)).isoformat()}' "
              f"AND (system_price_gbp_mwh IS NULL "
              f"OR system_price_gbp_mwh NOT BETWEEN {lo} AND {hi})"),
    ]
    return checks


def run_checks(checks: list[Check], query: Callable[[str], int]) -> list[Result]:
    results = []
    for c in checks:
        try:
            results.append(Result(c, int(query(c.sql))))
        except Exception as exc:  # noqa: BLE001 - a missing table is a failed check, not a crash
            results.append(Result(c, None, f"{type(exc).__name__}: {exc}".splitlines()[0]))
    return results


def summarise(results: list[Result]) -> tuple[list[Result], list[Result]]:
    """(failures, warnings): failed required checks, and failed optional ones."""
    failed = [r for r in results if not r.passed]
    return [r for r in failed if r.check.required], [r for r in failed if not r.check.required]


def _spark_query(sql: str) -> int:
    from databricks.sdk.runtime import spark

    value = spark.sql(sql).collect()[0][0]
    return 0 if value is None else int(value)


def main(argv: list[str] | None = None, query: Callable[[str], int] | None = None,
         today: date | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="check_lakehouse", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", required=True)
    p.add_argument("--silver-schema", required=True)
    p.add_argument("--gold-schema", required=True)
    p.add_argument("--max-age-days", type=int, default=3,
                   help="GB and EU prices must reach at least this many days back from today")
    args = p.parse_args(argv)

    checks = build_checks(args.catalog, args.silver_schema, args.gold_schema,
                          args.max_age_days, today or today_london())
    results = run_checks(checks, query or _spark_query)
    for r in results:
        status = "PASS" if r.passed else ("FAIL" if r.check.required else "WARN")
        detail = r.error or ("" if r.passed else f"{r.problems} problem(s)")
        log.info("%s  %s%s", status, r.check.name, f"  [{detail}]" if detail else "")

    failures, warnings = summarise(results)
    log.info("%d checks: %d passed, %d failed, %d warnings", len(results),
             len(results) - len(failures) - len(warnings), len(failures), len(warnings))
    return 1 if failures else 0


def run(argv: list[str] | None = None) -> None:
    """Entry point for python_wheel_task, which ignores return values (see cli.run)."""
    if main(argv):
        raise RuntimeError("lakehouse checks failed; see the FAIL lines above")


if __name__ == "__main__":
    sys.exit(main())
