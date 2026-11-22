"""Write raw payloads into the landing zone (a Unity Catalog Volume, or a local folder).

Layout:  {root}/{source}/{dataset}/{YYYY}/{MM}/{DD}/{dataset}_{YYYY-MM-DD}_{run_ts}.{ext}

Every run writes a *new* file (the run timestamp is in the name) instead of overwriting.
Bronze is therefore append-only, and silver keeps the latest version of each record.
That is how we capture revisions (e.g. Elexon settlement re-runs) without losing history.

Directories deliberately avoid ``key=value`` names so Spark doesn't infer partition
columns that clash with fields inside the files.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


def run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def landing_path(
    root: str | os.PathLike,
    source: str,
    dataset: str,
    partition_date: date,
    ext: str,
    run_ts: str | None = None,
) -> Path:
    ts = run_ts or run_timestamp()
    d = partition_date
    return (
        Path(root)
        / source
        / dataset
        / f"{d:%Y}"
        / f"{d:%m}"
        / f"{d:%d}"
        / f"{dataset}_{d.isoformat()}_{ts}.{ext.lstrip('.')}"
    )


def land_bytes(
    root: str | os.PathLike,
    source: str,
    dataset: str,
    partition_date: date,
    payload: bytes,
    ext: str,
    run_ts: str | None = None,
) -> Path:
    """Write ``payload`` to the landing zone and return the final path.

    Writes to a ``_``-prefixed temp file first, then renames. Spark and Auto Loader
    ignore files starting with ``_`` so a half-written file is never ingested.
    """
    final = landing_path(root, source, dataset, partition_date, ext, run_ts)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(f"_tmp_{final.name}")
    tmp.write_bytes(payload)
    try:
        os.replace(tmp, final)
    except OSError:
        # Some FUSE mounts don't support rename; fall back to a direct write.
        final.write_bytes(payload)
        tmp.unlink(missing_ok=True)
    return final


# --- Skipping work we've already done ----------------------------------------------------
#
# None of the APIs offer ETags or Last-Modified, and Elexon re-sends identical bytes for data
# that hasn't changed. So two checks against what is already in the landing zone:
#   * settled: a file for this partition was landed long enough after it that the source can
#     no longer change it -> don't even download;
#   * unchanged: the new payload is byte-identical to the newest file for the partition ->
#     download, but don't land a duplicate.
# A file's tag is whatever follows the run timestamp in its name (zone, series, window start),
# so different slices of one partition folder are compared only with their own kind.

# Days after a partition's date by which the source stops revising it.
SETTLE_DAYS: dict[tuple[str, str], int] = {
    ("elexon", "system_prices"): 30,         # settlement runs: II ~D+5, SF ~D+22 (working days)
    ("elexon", "generation_by_fuel"): 3,
    ("elexon", "demand_outturn"): 2,
    ("elexon", "market_index"): 2,
    ("elexon", "demand_forecast"): 1,        # forecasts are dated by publish day: complete
    ("elexon", "wind_forecast"): 1,          # once that day has ended
    ("elexon", "wind_solar_forecast"): 1,
    ("elexon", "demand_forecast_daily"): 1,
    ("entsoe", "day_ahead"): 2,
    ("fred", "observations"): 45,            # monthly EU gas lands about a month late
    ("stocks", "daily_prices"): 7,
    ("gas", "sap"): 3,
    ("gas", "ttf"): 3,
    # ERA5T is replaced by final ERA5 2-3 months later; counted from the month's first day.
    ("era5", "gb_hourly"): 120,
    # Only issues whose whole week is past are rebuilt, so they're final once landed.
    ("weather_forecast", "gb_hourly_archive"): 0,
}


@dataclass(frozen=True)
class Landed:
    path: Path
    landed_at: datetime
    tag: str


def _parse_name(name: str, prefix: str) -> tuple[datetime, str] | None:
    """``{prefix}{run_ts}[_{tag}].{ext}`` -> (landed_at, tag)."""
    if not name.startswith(prefix) or name.startswith("_"):
        return None
    rest = name[len(prefix):].rsplit(".", 1)[0]
    ts, _, tag = rest.partition("_")
    try:
        landed = datetime.strptime(ts, "%Y%m%dT%H%M%S%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return landed, tag


def landed_files(root: str | os.PathLike, source: str, dataset: str, partition_date: date,
                 tag: str = "") -> list[Landed]:
    """Files already landed for one partition and tag, oldest first."""
    folder = landing_path(root, source, dataset, partition_date, "x", "x").parent
    if not folder.is_dir():
        return []
    prefix = f"{dataset}_{partition_date.isoformat()}_"
    found = []
    for p in folder.iterdir():
        parsed = _parse_name(p.name, prefix)
        if parsed and parsed[1] == tag:
            found.append(Landed(p, parsed[0], tag))
    return sorted(found, key=lambda f: f.landed_at)


def is_settled(root: str | os.PathLike, source: str, dataset: str, partition_date: date,
               tag: str = "") -> bool:
    """True if a file for this partition was landed after the source stopped revising it."""
    days = SETTLE_DAYS.get((source, dataset))
    if days is None:
        return False
    settled_from = partition_date + timedelta(days=days)
    return any(f.landed_at.date() >= settled_from
               for f in landed_files(root, source, dataset, partition_date, tag))


def land_if_changed(root: str | os.PathLike, source: str, dataset: str, partition_date: date,
                    payload: bytes, ext: str, run_ts: str, tag: str = "",
                    fingerprint: Callable[[bytes], object] | None = None) -> Path | None:
    """Land ``payload`` unless it matches the newest file for the partition: byte for byte,
    or by ``fingerprint`` for payloads with volatile parts. Returns the new path, or None
    when nothing changed."""
    previous = landed_files(root, source, dataset, partition_date, tag)
    if previous:
        old = previous[-1].path.read_bytes()
        same = (fingerprint(old) == fingerprint(payload)) if fingerprint else old == payload
        if same:
            return None
    return land_bytes(root, source, dataset, partition_date, payload, ext,
                      f"{run_ts}_{tag}" if tag else run_ts)
