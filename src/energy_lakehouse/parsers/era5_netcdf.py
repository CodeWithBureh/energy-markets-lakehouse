"""Flatten ERA5 NetCDF grids (time x latitude x longitude) into a tidy table.

Output: one row per (valid_time_utc, latitude, longitude) with one column per variable.
Spark can't read NetCDF natively, so we write Parquet and let Auto Loader take over.

Notes:
* The new CDS backend names the time dimension ``valid_time`` (older files used ``time``).
* New-style files may carry ``number``/``expver`` coordinates that we drop.
* ``ssrd`` is accumulated over the hour *ending* at valid_time, in J/m^2. Silver divides by
  3600 to get mean W/m^2 for that hour.
* A request mixing instantaneous variables (t2m, winds) with accumulated ones (ssrd) comes
  back as a zip of two NetCDF files (``..._stepType-instant.nc``, ``..._stepType-accum.nc``)
  even with ``download_format: unarchived``. We accept either form and merge on the grid.
"""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

import pandas as pd

KNOWN_VARS = ["t2m", "u10", "v10", "u100", "v100", "ssrd"]


def _load(nc_file: Path):
    """One NetCDF file, normalised and loaded into memory (so the file can be removed)."""
    import xarray as xr

    with xr.open_dataset(nc_file) as ds:
        if "valid_time" not in ds.dims and "time" in ds.dims:
            ds = ds.rename({"time": "valid_time"})
        ds = ds.drop_vars([c for c in ("number", "expver") if c in ds.variables])
        return ds[[v for v in KNOWN_VARS if v in ds.data_vars]].load()


def _load_all(path: Path) -> list:
    if not zipfile.is_zipfile(path):
        return [_load(path)]
    with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(path) as zf:
        names = [n for n in zf.namelist() if n.endswith(".nc")]
        if not names:
            raise ValueError(f"No NetCDF files inside {path}: {zf.namelist()}")
        return [_load(Path(zf.extract(n, tmp))) for n in names]


def flatten_era5(nc_path: str | Path) -> pd.DataFrame:
    """Accepts a NetCDF file or the zip of NetCDF files the CDS returns."""
    import xarray as xr

    ds = xr.merge(_load_all(Path(nc_path)), join="outer", compat="override")
    present = [v for v in KNOWN_VARS if v in ds.data_vars]
    if not present:
        raise ValueError(f"No known ERA5 variables in {nc_path}: {list(ds.data_vars)}")
    df = ds[present].to_dataframe().reset_index()

    df = df.rename(columns={"valid_time": "valid_time_utc"})
    df["valid_time_utc"] = pd.to_datetime(df["valid_time_utc"], utc=True)
    cols = ["valid_time_utc", "latitude", "longitude", *present]
    # ERA5 stores variables as float32; widen so bronze's DOUBLE schema always matches.
    df[cols[1:]] = df[cols[1:]].astype("float64")
    return df[cols].sort_values(cols[:3]).reset_index(drop=True)


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Parquet with microsecond timestamps. Spark rejects INT64 nanosecond timestamps."""
    import io

    buf = io.BytesIO()
    df.to_parquet(
        buf,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    return buf.getvalue()
