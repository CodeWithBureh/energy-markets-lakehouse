"""Command-line entry point used by the Databricks job (python_wheel_task, entry_point=ingest).

Examples
--------
    ingest --source elexon --landing-root /Volumes/energy/raw/landing
    ingest --source entsoe --start 2026-10-01 --end 2026-10-31 --landing-root ./landing
    ingest --source era5   --secret-scope energy --landing-root /Volumes/energy/raw/landing

With no --start/--end, each source uses a sensible rolling window that overlaps earlier
runs, so revisions and late data get picked up. Bronze is append-only and silver dedupes.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from energy_lakehouse import config
from energy_lakehouse.credentials import get_secret
from energy_lakehouse.http_client import make_session
from energy_lakehouse.landing import land_bytes, run_timestamp
from energy_lakehouse.parsers import entsoe_xml
from energy_lakehouse.timeutils import BRUSSELS, daterange, parse_iso_utc, today_london

log = logging.getLogger("energy_lakehouse")

SOURCES = ("elexon", "entsoe", "fred", "stocks", "era5")


@dataclass
class RunReport:
    source: str
    files: list[str] = field(default_factory=list)
    rows_hint: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "source": self.source,
            "files_written": len(self.files),
            "rows_hint": self.rows_hint,
            "errors": self.errors,
        }


def default_window(source: str, today: date) -> tuple[date, date]:
    if source == "elexon":
        return today - timedelta(days=3), today - timedelta(days=1)
    if source == "entsoe":
        return today - timedelta(days=1), today + timedelta(days=1)  # tomorrow may be absent
    if source == "fred":
        return today - timedelta(days=14), today
    if source == "stocks":
        return today - timedelta(days=7), today
    if source == "era5":
        end = today - timedelta(days=config.ERA5_LATENCY_DAYS)
        return end - timedelta(days=6), end
    raise ValueError(source)


# --- per-source runners ----------------------------------------------------------------

def run_elexon(root: str, start: date, end: date, ts: str) -> RunReport:
    from energy_lakehouse.sources import elexon

    rep = RunReport("elexon")
    s = make_session()
    for d in daterange(start, end):
        for dataset, fn in (
            ("system_prices", elexon.fetch_system_prices),
            ("generation_by_fuel", elexon.fetch_generation_by_fuel),
        ):
            t0 = time.monotonic()
            try:
                body = fn(s, d)
                n = len(json.loads(body)["data"])
                rep.rows_hint += n
                rep.files.append(str(land_bytes(root, "elexon", dataset, d, body, "json", ts)))
                log.info("Elexon %s %s: %d rows in %.1fs", dataset, d, n, time.monotonic() - t0)
                if n == 0:
                    log.warning("Elexon %s %s returned no rows", dataset, d)
            except Exception as exc:  # noqa: BLE001
                log.warning("Elexon %s %s failed after %.1fs: %s", dataset, d,
                            time.monotonic() - t0, exc)
                rep.errors.append(f"{dataset} {d}: {exc}")
    return rep


def run_entsoe(root: str, start: date, end: date, ts: str, scope: str | None) -> RunReport:
    from energy_lakehouse.sources import entsoe

    rep = RunReport("entsoe")
    token = get_secret("entsoe", scope)
    s = make_session()
    for zone, eic in config.ENTSOE_ZONES.items():
        t0 = time.monotonic()
        try:
            # One request per zone for the whole window (see fetch_day_ahead_prices).
            xml = entsoe.fetch_day_ahead_prices(s, token, eic, start, end)
            # Keep the untouched XML as the immutable archive...
            land_bytes(root, "entsoe", "day_ahead_xml", start, xml, "xml", f"{ts}_{zone}")
            rows = entsoe_xml.parse_day_ahead(xml, zone)
            log.info("ENTSO-E %s %s..%s: %d rows in %.1fs (%d KB)", zone, start, end,
                     len(rows), time.monotonic() - t0, len(xml) // 1024)
            if not rows:
                log.warning("ENTSO-E %s %s..%s: no data (%s)", zone, start, end,
                            entsoe_xml.acknowledgement_reason(xml))
                continue
            # ...and land the parsed rows as JSON lines, one file per CET/CEST delivery day,
            # so Auto Loader and silver see the same layout as before.
            by_day: dict[date, list[dict]] = defaultdict(list)
            for r in rows:
                day = parse_iso_utc(r["delivery_start_utc"]).astimezone(BRUSSELS).date()
                by_day[day].append(r)
            for day in daterange(start, end):
                if day not in by_day:
                    log.warning("ENTSO-E %s %s: missing from the response", zone, day)
                    continue
                payload = ("\n".join(json.dumps(r) for r in by_day[day]) + "\n").encode()
                path = land_bytes(root, "entsoe", "day_ahead", day, payload, "jsonl",
                                  f"{ts}_{zone}")
                rep.files.append(str(path))
                rep.rows_hint += len(by_day[day])
        except Exception as exc:  # noqa: BLE001
            log.warning("ENTSO-E %s %s..%s failed after %.1fs: %s", zone, start, end,
                        time.monotonic() - t0, exc)
            rep.errors.append(f"{zone} {start}..{end}: {exc}")
    return rep


def run_fred(root: str, start: date, end: date, ts: str, scope: str | None) -> RunReport:
    from energy_lakehouse.sources import fred

    rep = RunReport("fred")
    key = get_secret("fred", scope)
    s = make_session()
    for series_id in config.FRED_SERIES:
        t0 = time.monotonic()
        try:
            body = fred.fetch_observations(s, key, series_id, start, end)
            n = len(json.loads(body).get("observations", []))
            rep.rows_hint += n
            log.info("FRED %s %s..%s: %d observations in %.1fs", series_id, start, end, n,
                     time.monotonic() - t0)
            rep.files.append(
                str(land_bytes(root, "fred", "observations", end, body, "json",
                               f"{ts}_{series_id}"))
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("FRED %s failed after %.1fs: %s", series_id, time.monotonic() - t0, exc)
            rep.errors.append(f"{series_id}: {exc}")
    return rep


def run_stocks(root: str, start: date, end: date, ts: str) -> RunReport:
    from energy_lakehouse.sources import stocks

    rep = RunReport("stocks")
    for ticker in config.STOCKS:
        t0 = time.monotonic()
        try:
            body = stocks.fetch_daily_prices(ticker, start, end)
            n = body.count(b"\n")
            log.info("Stocks %s %s..%s: %d trading days in %.1fs", ticker, start, end, n,
                     time.monotonic() - t0)
            if not body:
                # Normal for a weekend/holiday window; worth a look if it persists.
                log.warning("Stocks %s %s..%s: no rows", ticker, start, end)
                continue
            rep.rows_hint += n
            safe = ticker.replace(".", "_")
            rep.files.append(
                str(land_bytes(root, "stocks", "daily_prices", end, body, "jsonl",
                               f"{ts}_{safe}"))
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Stocks %s failed after %.1fs: %s", ticker, time.monotonic() - t0, exc)
            rep.errors.append(f"{ticker}: {exc}")
    return rep


def run_era5(root: str, start: date, end: date, ts: str, scope: str | None) -> RunReport:
    from energy_lakehouse.parsers.era5_netcdf import flatten_era5, to_parquet_bytes
    from energy_lakehouse.sources import era5

    rep = RunReport("era5")
    key = get_secret("cds", scope)
    days = list(daterange(start, end))
    with tempfile.TemporaryDirectory() as tmp:
        for (year, month), group in era5.group_by_month(days).items():
            label = f"{year:04d}-{month:02d}"
            t0 = time.monotonic()
            log.info("ERA5 %s: requesting %d day(s); the CDS queue can take a while",
                     label, len(group))
            try:
                nc = Path(tmp) / f"era5_gb_{label}.nc"
                era5.fetch_month(key, year, month, [d.day for d in group], nc)
                anchor = group[0]
                land_bytes(root, "era5", "gb_hourly_netcdf", anchor, nc.read_bytes(), "nc", ts)
                df = flatten_era5(nc)
                rep.rows_hint += len(df)
                log.info("ERA5 %s: %d grid rows in %.1fs", label, len(df), time.monotonic() - t0)
                rep.files.append(
                    str(land_bytes(root, "era5", "gb_hourly", anchor,
                                   to_parquet_bytes(df), "parquet", ts))
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("ERA5 %s failed after %.1fs: %s", label, time.monotonic() - t0, exc)
                rep.errors.append(f"{label}: {exc}")
    return rep


# --- CLI ------------------------------------------------------------------------------

def _parse_date(v: str) -> date | None:
    # Jobs always pass --start={{job.parameters.start}}; blank means "use the default window".
    return date.fromisoformat(v) if v.strip() else None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ingest", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True, choices=SOURCES)
    p.add_argument("--landing-root", required=True,
                   help="Volume path, e.g. /Volumes/energy/raw/landing, or a local folder")
    p.add_argument("--start", type=_parse_date, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--end", type=_parse_date, help="YYYY-MM-DD (inclusive)")
    p.add_argument("--secret-scope", default=None,
                   help="Databricks secret scope (Key Vault-backed in prod)")
    p.add_argument("--max-days", type=int, default=config.DEFAULT_MAX_DAYS_PER_RUN,
                   help="Cost guard: refuse bigger windows unless raised explicitly")
    p.add_argument("--allow-partial", action="store_true",
                   help="Exit 0 even if some requests failed")
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    start, end = default_window(args.source, today_london())
    start = args.start or start
    end = args.end or end
    span = (end - start).days + 1
    if span > args.max_days:
        log.error("Window %s..%s is %d days (> --max-days %d). Backfill in smaller chunks.",
                  start, end, span, args.max_days)
        return 2

    ts = run_timestamp()
    root = args.landing_root
    log.info("Ingesting %s for %s..%s (%d days) into %s", args.source, start, end, span, root)
    t0 = time.monotonic()

    if args.source == "elexon":
        rep = run_elexon(root, start, end, ts)
    elif args.source == "entsoe":
        rep = run_entsoe(root, start, end, ts, args.secret_scope)
    elif args.source == "fred":
        rep = run_fred(root, start, end, ts, args.secret_scope)
    elif args.source == "stocks":
        rep = run_stocks(root, start, end, ts)
    else:
        rep = run_era5(root, start, end, ts, args.secret_scope)

    elapsed = time.monotonic() - t0
    log.info("Finished %s in %.1fs: %d files, ~%d rows, %d errors", args.source, elapsed,
             len(rep.files), rep.rows_hint, len(rep.errors))
    for err in rep.errors:
        log.error("%s: %s", args.source, err)
    print(json.dumps({**rep.summary(), "elapsed_seconds": round(elapsed, 1)}, indent=2))
    if rep.errors and not args.allow_partial:
        # Fail the task (so retries/alerts fire) only if *nothing* landed. A single
        # missing zone or ticker shouldn't block the whole pipeline.
        return 1 if not rep.files else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
