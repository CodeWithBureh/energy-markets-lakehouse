"""Start the energy_backfill job with the months listed in backfill.toml.

The job does the work inside Databricks: one for_each loop per source, a month per
iteration, then a single pipeline refresh. This script only reads the config, shows the
plan, and starts (and waits for) one job run:

    databricks bundle run -t <target> energy_backfill --params start_month=...,end_month=...

    python scripts/backfill.py --dry-run
    python scripts/backfill.py
    python scripts/backfill.py --config other.toml

If some months fail, open the run in the Jobs UI and click "Repair run": it reruns only
the failed iterations. Uses your current Databricks CLI profile
(e.g. $env:DATABRICKS_CONFIG_PROFILE = "bureh"). Needs Python 3.11+ (tomllib).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import tomllib

from energy_lakehouse.backfill import month_windows

ROOT = Path(__file__).resolve().parents[1]


def job_params(cfg: dict) -> str:
    """The --params value for energy_backfill. Months are space-separated because the CLI
    splits --params on commas."""
    params = {
        "start_month": cfg.get("start_month", ""),
        "end_month": cfg.get("end_month", "current"),
        "months": " ".join(cfg.get("months", [])),
        "include_weather": "true" if cfg.get("include_weather") else "false",
        "weather_only": "true" if cfg.get("weather_only") else "false",
    }
    return ",".join(f"{k}={v}" for k, v in params.items())


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=str(ROOT / "backfill.toml"))
    p.add_argument("--dry-run", action="store_true", help="show the plan, run nothing")
    args = p.parse_args(argv)

    cfg = tomllib.loads(Path(args.config).read_text(encoding="utf-8"))
    target = cfg.get("target", "dev")
    windows = month_windows(cfg.get("start_month"), cfg.get("end_month", "current"),
                            cfg.get("months"))
    if cfg.get("weather_only"):
        sources = "ERA5 only"
    else:
        sources = "Elexon, ENTSO-E, FRED, stocks" + (", ERA5" if cfg.get("include_weather") else "")
    print(f"energy_backfill on {target}: {len(windows)} month(s) of {sources}")
    for s, e in windows:
        print(f"  {s:%Y-%m}  {s} .. {e}")
    if not windows:
        print("Nothing to run: no complete months in the plan.")
        return 1

    cmd = ["databricks", "bundle", "run", "-t", target, "energy_backfill",
           "--params", job_params(cfg)]
    print("\n" + " ".join(cmd if " " not in cmd[-1] else [*cmd[:-1], f'"{cmd[-1]}"']))
    if args.dry_run:
        return 0
    return subprocess.run(cmd, check=False, cwd=ROOT).returncode


if __name__ == "__main__":
    sys.exit(main())
