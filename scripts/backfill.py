"""Backfill history month by month by running a bundle job once per month.

Reads the plan from backfill.toml, then for each month runs (and waits for)

    databricks bundle run -t <target> <job> --params start=YYYY-MM-01,end=YYYY-MM-DD

Runs are sequential because the jobs allow one run at a time. Progress is saved after
every month, so you can stop with Ctrl+C and rerun to carry on.

    python scripts/backfill.py --dry-run
    python scripts/backfill.py
    python scripts/backfill.py --config other.toml --restart

Uses your current Databricks CLI profile (e.g. $env:DATABRICKS_CONFIG_PROFILE = "bureh").
Needs Python 3.11+ (tomllib).
"""

from __future__ import annotations

import argparse
import calendar
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import tomllib

ROOT = Path(__file__).resolve().parents[1]
JOBS = ("energy_daily", "energy_weekly_weather")


def parse_month(value: str) -> date:
    return datetime.strptime(value, "%Y-%m").date()


def month_windows(cfg: dict, today: date) -> list[tuple[date, date]]:
    """(first_day, last_day) per month in the plan. The current month stops at yesterday,
    and future months are dropped."""
    if cfg.get("months"):
        firsts = sorted({parse_month(m) for m in cfg["months"]})
    else:
        first = parse_month(cfg["start_month"])
        end_cfg = cfg.get("end_month", "current")
        last = today.replace(day=1) if end_cfg == "current" else parse_month(end_cfg)
        if last < first:
            raise ValueError(f"end_month {end_cfg} is before start_month {cfg['start_month']}")
        firsts, m = [], first
        while m <= last:
            firsts.append(m)
            m = (m.replace(day=28) + timedelta(days=4)).replace(day=1)

    yesterday = today - timedelta(days=1)
    windows = []
    for m in firsts:
        end = m.replace(day=calendar.monthrange(m.year, m.month)[1])
        end = min(end, yesterday)
        if end >= m:
            windows.append((m, end))
    return windows


def run_month(target: str, job: str, start: date, end: date) -> str:
    """Run one month and return 'success', 'partial' (some tasks failed) or 'failed'."""
    cmd = ["databricks", "bundle", "run", "-t", target, job,
           "--params", f"start={start.isoformat()},end={end.isoformat()}"]
    print(f"\n=== {start:%Y-%m}: {' '.join(cmd)}", flush=True)
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", errors="replace")
    partial = False
    for line in proc.stdout:
        print(line, end="", flush=True)
        partial |= "SUCCESS_WITH_FAILURES" in line
    if proc.wait() != 0:
        return "failed"
    return "partial" if partial else "success"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=str(ROOT / "backfill.toml"))
    p.add_argument("--dry-run", action="store_true", help="show the plan, run nothing")
    p.add_argument("--restart", action="store_true", help="ignore saved progress")
    p.add_argument("--keep-going", action="store_true",
                   help="carry on after a failed month instead of stopping")
    args = p.parse_args(argv)

    cfg = tomllib.loads(Path(args.config).read_text(encoding="utf-8"))
    target, job = cfg.get("target", "dev"), cfg.get("job", "energy_daily")
    if job not in JOBS:
        raise SystemExit(f"job must be one of {JOBS}, got {job!r}")

    today = datetime.now(ZoneInfo("Europe/London")).date()
    windows = month_windows(cfg, today)
    state_file = ROOT / ".databricks" / "backfill" / f"{target}-{job}.json"
    state = {} if args.restart or not state_file.exists() else json.loads(state_file.read_text())
    todo = [(s, e) for s, e in windows if state.get(f"{s:%Y-%m}", {}).get("status") != "success"]

    print(f"Backfill {job} on {target}: {len(windows)} month(s) planned, "
          f"{len(windows) - len(todo)} already done, {len(todo)} to run.")
    for s, e in windows:
        prev = state.get(f"{s:%Y-%m}", {}).get("status", "-")
        print(f"  {s:%Y-%m}  {s} .. {e}  previous: {prev}")
    if args.dry_run or not todo:
        return 0

    failed = 0
    for s, e in todo:
        status = run_month(target, job, s, e)
        state[f"{s:%Y-%m}"] = {"status": status, "start": s.isoformat(), "end": e.isoformat(),
                               "at": datetime.now().isoformat(timespec="seconds")}
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps(state, indent=2, sort_keys=True))
        print(f"=== {s:%Y-%m}: {status}", flush=True)
        if status == "failed":
            failed += 1
            if not args.keep_going:
                print("Stopping. Fix the problem, then rerun to resume from this month.")
                return 1

    partial = [m for m, v in state.items() if v["status"] == "partial"]
    if partial:
        print(f"Partly failed (rerun to retry): {', '.join(sorted(partial))}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
