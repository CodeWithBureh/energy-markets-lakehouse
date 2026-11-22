"""Command-line entry point used by the Databricks job (python_wheel_task, entry_point=ingest).

Examples
--------
    ingest --source elexon --landing-root /Volumes/energy/raw/landing
    ingest --source entsoe --start 2026-10-01 --end 2026-10-31 --landing-root ./landing
    ingest --source era5   --secret-scope energy --landing-root /Volumes/energy/raw/landing
    ingest --source gas    --start 2024-01-01 --end 2024-01-31 --landing-root ./landing

With no --start/--end, each source uses a sensible rolling window that overlaps earlier
runs, so revisions and late data get picked up. Bronze is append-only and silver dedupes.

Work already done is skipped: a partition landed after its source stopped revising it is not
downloaded again, and a download identical to the last landed file is not written. Use
--refetch to download and land everything regardless.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from energy_lakehouse import config
from energy_lakehouse.credentials import get_secret
from energy_lakehouse.http_client import make_session
from energy_lakehouse.landing import is_settled, land_bytes, land_if_changed, run_timestamp
from energy_lakehouse.parsers import entsoe_xml
from energy_lakehouse.timeutils import BRUSSELS, daterange, parse_iso_utc, today_london

log = logging.getLogger("energy_lakehouse")

SOURCES = ("elexon", "entsoe", "fred", "stocks", "era5", "gas", "weather_forecast",
           "weather_forecast_archive")


@dataclass
class RunReport:
    source: str
    files: list[str] = field(default_factory=list)
    rows_hint: int = 0
    errors: list[str] = field(default_factory=list)
    skipped_settled: int = 0      # not downloaded: already landed after the source settled
    unchanged: int = 0            # downloaded, identical to what was landed, not written

    def summary(self) -> dict:
        return {
            "source": self.source,
            "files_written": len(self.files),
            "skipped_settled": self.skipped_settled,
            "unchanged": self.unchanged,
            "rows_hint": self.rows_hint,
            "errors": self.errors,
        }

    def did_something(self) -> bool:
        return bool(self.files or self.skipped_settled or self.unchanged)


def default_window(source: str, today: date) -> tuple[date, date]:
    if source == "elexon":
        # Up to today: forecasts published overnight (WINDFOR for tomorrow) are needed by
        # this morning's outlook. Today's partial partitions are fetched again tomorrow.
        return today - timedelta(days=3), today
    if source == "entsoe":
        return today - timedelta(days=1), today + timedelta(days=1)  # tomorrow may be absent
    if source == "fred":
        return today - timedelta(days=14), today
    if source == "stocks":
        return today - timedelta(days=7), today
    if source == "weather_forecast":
        return today, today                         # always "now"; dates are ignored
    if source == "weather_forecast_archive":
        return today - timedelta(days=14), today - timedelta(days=1)
    if source == "gas":
        # SAP for gas day D is final around 11:40 on D+1, and TTF settles after the close.
        return today - timedelta(days=7), today
    if source == "era5":
        end = today - timedelta(days=config.ERA5_LATENCY_DAYS)
        return end - timedelta(days=6), end
    raise ValueError(source)


@dataclass
class Lander:
    """Writes into the landing zone for one run, skipping work already done unless
    ``refetch`` is set (see landing.is_settled and landing.land_if_changed)."""
    root: str
    ts: str
    refetch: bool = False

    def settled(self, rep: RunReport, source: str, dataset: str, d: date, tag: str = "") -> bool:
        if self.refetch or not is_settled(self.root, source, dataset, d, tag):
            return False
        rep.skipped_settled += 1
        return True

    def land(self, rep: RunReport, source: str, dataset: str, d: date, body: bytes, ext: str,
             tag: str = "", fingerprint=None) -> None:
        if self.refetch:
            path = land_bytes(self.root, source, dataset, d, body, ext,
                              f"{self.ts}_{tag}" if tag else self.ts)
        else:
            path = land_if_changed(self.root, source, dataset, d, body, ext, self.ts, tag,
                                   fingerprint)
        if path is None:
            rep.unchanged += 1
            log.info("%s %s %s: unchanged since the last landing; not written",
                     source, dataset, d)
        else:
            rep.files.append(str(path))


# --- per-source runners ----------------------------------------------------------------

def run_elexon(lander: Lander, start: date, end: date) -> RunReport:
    from energy_lakehouse.sources import elexon

    rep = RunReport("elexon")
    s = make_session()
    for d in daterange(start, end):
        # Outturn and prices are dated by settlement day, forecasts by publish day.
        for dataset, fn in elexon.DATASETS.items():
            if lander.settled(rep, "elexon", dataset, d):
                continue
            t0 = time.monotonic()
            try:
                body = fn(s, d)
                n = len(json.loads(body)["data"])
                rep.rows_hint += n
                lander.land(rep, "elexon", dataset, d, body, "json")
                log.info("Elexon %s %s: %d rows in %.1fs", dataset, d, n, time.monotonic() - t0)
                if n == 0:
                    log.warning("Elexon %s %s returned no rows", dataset, d)
            except Exception as exc:  # noqa: BLE001
                log.warning("Elexon %s %s failed after %.1fs: %s", dataset, d,
                            time.monotonic() - t0, exc)
                rep.errors.append(f"{dataset} {d}: {exc}")
    return rep


def run_entsoe(lander: Lander, start: date, end: date, scope: str | None) -> RunReport:
    from energy_lakehouse.sources import entsoe

    rep = RunReport("entsoe")
    token = get_secret("entsoe", scope)
    s = make_session()
    days = list(daterange(start, end))
    for zone, eic in config.ENTSOE_ZONES.items():
        if not lander.refetch and all(
                is_settled(lander.root, "entsoe", "day_ahead", d, zone) for d in days):
            rep.skipped_settled += 1
            continue
        t0 = time.monotonic()
        try:
            # One request per zone for the whole window (see fetch_day_ahead_prices).
            xml = entsoe.fetch_day_ahead_prices(s, token, eic, start, end)
            rows = entsoe_xml.parse_day_ahead(xml, zone)
            log.info("ENTSO-E %s %s..%s: %d rows in %.1fs (%d KB)", zone, start, end,
                     len(rows), time.monotonic() - t0, len(xml) // 1024)
            if not rows:
                log.warning("ENTSO-E %s %s..%s: no data (%s)", zone, start, end,
                            entsoe_xml.acknowledgement_reason(xml))
                continue
            # Land the parsed rows as JSON lines, one file per CET/CEST delivery day, so Auto
            # Loader and silver see the same layout as before...
            by_day: dict[date, list[dict]] = defaultdict(list)
            for r in rows:
                day = parse_iso_utc(r["delivery_start_utc"]).astimezone(BRUSSELS).date()
                by_day[day].append(r)
            before = len(rep.files)
            for day in days:
                if day not in by_day:
                    log.warning("ENTSO-E %s %s: missing from the response", zone, day)
                    continue
                payload = ("\n".join(json.dumps(r) for r in by_day[day]) + "\n").encode()
                lander.land(rep, "entsoe", "day_ahead", day, payload, "jsonl", zone)
                rep.rows_hint += len(by_day[day])
            # ...and archive the untouched XML only when some day changed. (The XML carries
            # its creation time, so it never compares equal by itself.)
            if len(rep.files) > before:
                land_bytes(lander.root, "entsoe", "day_ahead_xml", start, xml, "xml",
                           f"{lander.ts}_{zone}")
        except Exception as exc:  # noqa: BLE001
            log.warning("ENTSO-E %s %s..%s failed after %.1fs: %s", zone, start, end,
                        time.monotonic() - t0, exc)
            rep.errors.append(f"{zone} {start}..{end}: {exc}")
    return rep


def _fred_observations(body: bytes):
    # FRED stamps every response with today's realtime_start/end; compare the data only.
    return json.loads(body).get("observations")


def run_fred(lander: Lander, start: date, end: date, scope: str | None) -> RunReport:
    from energy_lakehouse.sources import fred

    rep = RunReport("fred")
    key = get_secret("fred", scope)
    s = make_session()
    for series_id in config.FRED_SERIES:
        # Range sources land one file per window, keyed by its end date; the tag records the
        # start, so only a file for exactly this window counts as already done.
        tag = f"{series_id}_from{start.isoformat()}"
        if lander.settled(rep, "fred", "observations", end, tag):
            continue
        t0 = time.monotonic()
        try:
            body = fred.fetch_observations(s, key, series_id, start, end)
            n = len(json.loads(body).get("observations", []))
            rep.rows_hint += n
            log.info("FRED %s %s..%s: %d observations in %.1fs", series_id, start, end, n,
                     time.monotonic() - t0)
            lander.land(rep, "fred", "observations", end, body, "json", tag, _fred_observations)
        except Exception as exc:  # noqa: BLE001
            log.warning("FRED %s failed after %.1fs: %s", series_id, time.monotonic() - t0, exc)
            rep.errors.append(f"{series_id}: {exc}")
    return rep


def run_stocks(lander: Lander, start: date, end: date) -> RunReport:
    from energy_lakehouse.sources import stocks

    rep = RunReport("stocks")
    for ticker in config.STOCKS:
        tag = f"{ticker.replace('.', '_')}_from{start.isoformat()}"
        if lander.settled(rep, "stocks", "daily_prices", end, tag):
            continue
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
            lander.land(rep, "stocks", "daily_prices", end, body, "jsonl", tag)
        except Exception as exc:  # noqa: BLE001
            log.warning("Stocks %s failed after %.1fs: %s", ticker, time.monotonic() - t0, exc)
            rep.errors.append(f"{ticker}: {exc}")
    return rep


def run_gas(lander: Lander, start: date, end: date) -> RunReport:
    from energy_lakehouse.sources import gas

    rep = RunReport("gas")
    s = make_session()
    fetchers = {"sap": lambda: gas.fetch_sap(s, start, end),
                "ttf": lambda: gas.fetch_ttf(start, end)}
    tag = f"from{start.isoformat()}"
    for dataset, fetch in fetchers.items():
        if lander.settled(rep, "gas", dataset, end, tag):
            continue
        t0 = time.monotonic()
        try:
            body = fetch()
            n = body.count(b"\n")
            log.info("Gas %s %s..%s: %d days in %.1fs", dataset, start, end, n,
                     time.monotonic() - t0)
            if not body:
                log.warning("Gas %s %s..%s: no rows", dataset, start, end)
                continue
            rep.rows_hint += n
            lander.land(rep, "gas", dataset, end, body, "jsonl", tag)
        except Exception as exc:  # noqa: BLE001
            log.warning("Gas %s failed after %.1fs: %s", dataset, time.monotonic() - t0, exc)
            rep.errors.append(f"{dataset}: {exc}")
    return rep


def run_weather_forecast(lander: Lander, today: date) -> RunReport:
    from energy_lakehouse.sources import weather_forecast

    # Every fetch is a new forecast, so there is nothing to skip.
    rep = RunReport("weather_forecast")
    t0 = time.monotonic()
    try:
        body = weather_forecast.fetch(make_session())
        n = body.count(b"\n")
        rep.rows_hint = n
        rep.files.append(str(land_bytes(lander.root, "weather_forecast", "gb_hourly", today,
                                        body, "jsonl", lander.ts)))
        log.info("Weather forecast: %d point-hours in %.1fs", n, time.monotonic() - t0)
    except Exception as exc:  # noqa: BLE001
        log.warning("Weather forecast failed after %.1fs: %s", time.monotonic() - t0, exc)
        rep.errors.append(str(exc))
    return rep


def run_weather_forecast_archive(lander: Lander, start: date, end: date,
                                 today: date) -> RunReport:
    """Past forecast issues rebuilt from Open-Meteo's Previous Runs API, one file per month
    of issue days. Only issues at least ARCHIVE_DELAY_DAYS old, once every forecast they use
    has been made, so every value is final."""
    import pandas as pd

    from energy_lakehouse.parsers.era5_netcdf import to_parquet_bytes
    from energy_lakehouse.sources import weather_forecast as wf
    from energy_lakehouse.sources.era5 import group_by_month

    rep = RunReport("weather_forecast_archive")
    first = max(start, wf.ARCHIVE_SINCE)
    last = min(end, today - timedelta(days=wf.ARCHIVE_DELAY_DAYS))
    if first > last:
        log.info("Forecast archive: no complete issues in %s..%s", start, end)
        return rep
    s = make_session()
    for (year, month), group in group_by_month(list(daterange(first, last))).items():
        label = f"{year:04d}-{month:02d}"
        anchor, tag = group[0], f"to{group[-1].isoformat()}"
        if lander.settled(rep, "weather_forecast", "gb_hourly_archive", anchor, tag):
            continue
        t0 = time.monotonic()
        try:
            rows = wf.fetch_archive(s, group[0], group[-1])
            rep.rows_hint += len(rows)
            log.info("Forecast archive %s: %d issues, %d point-hours in %.1fs", label,
                     len(group), len(rows), time.monotonic() - t0)
            lander.land(rep, "weather_forecast", "gb_hourly_archive", anchor,
                        to_parquet_bytes(pd.DataFrame(rows)), "parquet", tag)
        except Exception as exc:  # noqa: BLE001
            log.warning("Forecast archive %s failed after %.1fs: %s", label,
                        time.monotonic() - t0, exc)
            rep.errors.append(f"{label}: {exc}")
    return rep


def run_era5(lander: Lander, start: date, end: date, scope: str | None) -> RunReport:
    from energy_lakehouse.parsers.era5_netcdf import flatten_era5, to_parquet_bytes
    from energy_lakehouse.sources import era5

    rep = RunReport("era5")
    key = None
    days = list(daterange(start, end))
    with tempfile.TemporaryDirectory() as tmp:
        for (year, month), group in era5.group_by_month(days).items():
            label = f"{year:04d}-{month:02d}"
            anchor, tag = group[0], f"to{group[-1].isoformat()}"
            # The CDS queue is the slow part, so this is where skipping pays most.
            if lander.settled(rep, "era5", "gb_hourly", anchor, tag):
                log.info("ERA5 %s: final data already landed; skipping the CDS request", label)
                continue
            key = key or get_secret("cds", scope)
            t0 = time.monotonic()
            log.info("ERA5 %s: requesting %d day(s); the CDS queue can take a while",
                     label, len(group))
            try:
                nc = Path(tmp) / f"era5_gb_{label}.nc"
                era5.fetch_month(key, year, month, [d.day for d in group], nc)
                df = flatten_era5(nc)
                rep.rows_hint += len(df)
                log.info("ERA5 %s: %d grid rows in %.1fs", label, len(df), time.monotonic() - t0)
                before = len(rep.files)
                lander.land(rep, "era5", "gb_hourly", anchor, to_parquet_bytes(df), "parquet",
                            tag)
                if len(rep.files) > before:
                    # Mixed instant/accumulated variables come back zipped (see era5_netcdf).
                    ext = "zip" if zipfile.is_zipfile(nc) else "nc"
                    land_bytes(lander.root, "era5", "gb_hourly_netcdf", anchor, nc.read_bytes(),
                               ext, f"{lander.ts}_{tag}")
            except Exception as exc:  # noqa: BLE001
                log.warning("ERA5 %s failed after %.1fs: %s", label, time.monotonic() - t0, exc)
                rep.errors.append(f"{label}: {exc}")
    return rep


# --- CLI ------------------------------------------------------------------------------

def _parse_date(v: str) -> date | None:
    # Jobs always pass --start={{job.parameters.start}}; blank means "use the default window".
    return date.fromisoformat(v) if v.strip() else None


def _parse_bool(v: str) -> bool:
    # Jobs pass --refetch={{job.parameters.refetch}}, which may be blank.
    return v.strip().lower() in ("true", "1", "yes")


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
    p.add_argument("--refetch", type=_parse_bool, nargs="?", const=True, default=False,
                   help="Download and land everything, even data already landed and unchanged")
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    start, end = default_window(args.source, today_london())
    start = args.start or start
    end = args.end or end
    if args.source == "era5":
        # CDS hasn't published the last few days yet, and asking for them fails the whole
        # month's request. Stop at the same lag the rolling window uses.
        latest = today_london() - timedelta(days=config.ERA5_LATENCY_DAYS)
        if end > latest:
            log.info("ERA5 is published ~%d days behind; clipping end %s to %s",
                     config.ERA5_LATENCY_DAYS, end, latest)
            end = latest
        if start > end:
            log.info("ERA5: nothing published yet for %s onwards; skipping", start)
            print(json.dumps({**RunReport("era5").summary(), "elapsed_seconds": 0.0}, indent=2))
            return 0
    span = (end - start).days + 1
    if span > args.max_days:
        log.error("Window %s..%s is %d days (> --max-days %d). Backfill in smaller chunks.",
                  start, end, span, args.max_days)
        return 2

    ts = run_timestamp()
    root = args.landing_root
    lander = Lander(root, ts, refetch=args.refetch)
    log.info("Ingesting %s for %s..%s (%d days) into %s", args.source, start, end, span, root)
    t0 = time.monotonic()

    if args.source == "elexon":
        rep = run_elexon(lander, start, end)
    elif args.source == "entsoe":
        rep = run_entsoe(lander, start, end, args.secret_scope)
    elif args.source == "fred":
        rep = run_fred(lander, start, end, args.secret_scope)
    elif args.source == "stocks":
        rep = run_stocks(lander, start, end)
    elif args.source == "gas":
        rep = run_gas(lander, start, end)
    elif args.source == "weather_forecast":
        # A forecast is only ever "as of now": there is no history to backfill.
        if args.start or args.end:
            log.warning("weather_forecast ignores --start/--end; fetching the latest forecast")
        rep = run_weather_forecast(lander, today_london())
    elif args.source == "weather_forecast_archive":
        rep = run_weather_forecast_archive(lander, start, end, today_london())
    else:
        rep = run_era5(lander, start, end, args.secret_scope)

    elapsed = time.monotonic() - t0
    log.info("Finished %s in %.1fs: %d files written, %d skipped (settled), %d unchanged, "
             "~%d rows, %d errors", args.source, elapsed, len(rep.files), rep.skipped_settled,
             rep.unchanged, rep.rows_hint, len(rep.errors))
    for err in rep.errors:
        log.error("%s: %s", args.source, err)
    print(json.dumps({**rep.summary(), "elapsed_seconds": round(elapsed, 1)}, indent=2))
    if rep.errors and not args.allow_partial:
        # Fail the task (so retries/alerts fire) only if *nothing* landed. A single
        # missing zone or ticker shouldn't block the whole pipeline.
        # Skipped or unchanged partitions count as done.
        return 1 if not rep.did_something() else 0
    return 0


class IngestFailed(RuntimeError):
    """Raised by the wheel entry point so the Databricks task is marked as failed."""


def run(argv: list[str] | None = None) -> None:
    """Entry point for python_wheel_task. Databricks calls it and ignores the return
    value, so a non-zero exit code from main() would otherwise leave the task green."""
    code = main(argv)
    if code:
        raise IngestFailed(f"ingest exited with code {code}; see the errors logged above")


if __name__ == "__main__":
    sys.exit(main())
