"""Month windows for backfills, and the planning step of the energy_backfill job.

The job's first task runs ``plan_backfill``, which turns start/end months (or an explicit
list) into ``[{"start": "2024-01-01", "end": "2024-01-31"}, ...]`` and publishes it as the
task value ``months``. Each source's for_each task then ingests one month per iteration,
and the pipeline refreshes once at the end.

    plan_backfill --start-month 2024-01 --end-month current
    plan_backfill --months "2024-03 2024-10"
    plan_backfill --start-month 2024-01 --weather-only true

It also publishes ``run_markets`` and ``run_weather`` ("true"/"false"), which the job's
condition tasks check to skip the market sources or ERA5.
"""

from __future__ import annotations

import argparse
import calendar
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta

from energy_lakehouse.timeutils import today_london

log = logging.getLogger("energy_lakehouse")


def parse_month(value: str) -> date:
    return datetime.strptime(value.strip(), "%Y-%m").date()


def month_windows(
    start_month: str | None = None,
    end_month: str = "current",
    months: list[str] | None = None,
    today: date | None = None,
) -> list[tuple[date, date]]:
    """(first_day, last_day) for each month, oldest first. ``months`` overrides the range.
    The current month stops at yesterday and future months are dropped, so every window is
    complete data that has been published."""
    today = today or today_london()
    if months:
        firsts = sorted({parse_month(m) for m in months})
    else:
        if not start_month:
            raise ValueError("give start_month or a list of months")
        first = parse_month(start_month)
        last = today.replace(day=1) if end_month == "current" else parse_month(end_month)
        if last < first:
            raise ValueError(f"end month {end_month} is before start month {start_month}")
        firsts, m = [], first
        while m <= last:
            firsts.append(m)
            m = (m.replace(day=28) + timedelta(days=4)).replace(day=1)

    yesterday = today - timedelta(days=1)
    windows = []
    for m in firsts:
        end = min(m.replace(day=calendar.monthrange(m.year, m.month)[1]), yesterday)
        if end >= m:
            windows.append((m, end))
    return windows


def is_true(value: str | None) -> bool:
    return (value or "").strip().lower() in ("true", "1", "yes")


def source_flags(include_weather: str = "", weather_only: str = "") -> dict[str, str]:
    """Which halves of the backfill run. weather_only implies ERA5 and skips the market
    sources (Elexon, ENTSO-E, FRED, stocks)."""
    only = is_true(weather_only)
    weather = only or is_true(include_weather)
    return {"run_markets": "false" if only else "true",
            "run_weather": "true" if weather else "false"}


def plan_main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(prog="plan_backfill", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start-month", default="")
    p.add_argument("--end-month", default="current")
    p.add_argument("--months", default="",
                   help='explicit months, space- or semicolon-separated: "2024-03 2024-10"')
    p.add_argument("--include-weather", default="false", help="also backfill ERA5")
    p.add_argument("--weather-only", default="false",
                   help="backfill ERA5 only, skipping the market sources")
    args = p.parse_args(argv)
    flags = source_flags(args.include_weather, args.weather_only)

    listed = [m for m in args.months.replace(";", " ").split() if m]
    windows = month_windows(args.start_month or None, args.end_month or "current", listed)
    if not windows:
        log.error("No complete months to backfill for %s", vars(args))
        return 2

    plan = [{"start": s.isoformat(), "end": e.isoformat()} for s, e in windows]
    log.info("Backfill plan: %d month(s), %s .. %s; markets=%s weather=%s", len(plan),
             plan[0]["start"], plan[-1]["end"], flags["run_markets"], flags["run_weather"])
    print(json.dumps(plan))

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # On Databricks: hand the list to the for_each tasks. Without it they'd have
        # nothing to loop over, so a failure here must fail the task.
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key="months", value=plan)
        for key, value in flags.items():
            dbutils.jobs.taskValues.set(key=key, value=value)
    return 0


def plan_run(argv: list[str] | None = None) -> None:
    """Entry point for python_wheel_task, which ignores return values (see cli.run)."""
    code = plan_main(argv)
    if code:
        raise RuntimeError(f"plan_backfill exited with code {code}; see the errors logged above")


if __name__ == "__main__":
    sys.exit(plan_main())
