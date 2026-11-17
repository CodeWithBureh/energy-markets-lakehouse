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
from datetime import date, datetime, timezone
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
