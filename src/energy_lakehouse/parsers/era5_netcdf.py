"""Flatten ERA5 NetCDF grids (time x latitude x longitude) into a tidy table.

Output: one row per (valid_time_utc, latitude, longitude) with one column per variable.
Spark can't read NetCDF natively, so we write Parquet and let Auto Loader take over.

Notes:
* The new CDS backend names the time dimension ``valid_time`` (older files used ``time``).
* New-style files may carry ``number``/``expver`` coordinates that we drop.
* ``ssrd`` is accumulated over the hour *ending* at valid_time, in J/m^2. Silver divides by
  3600 to get mean W/m^2 for that hour.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

KNOWN_VARS = ["t2m", "u10", "v10", "u100", "v100", "ssrd"]


def flatten_era5(nc_path: str | Path) -> pd.DataFrame:
    import xarray as xr

    with xr.open_dataset(nc_path) as ds:
        if "valid_time" not in ds.dims and "time" in ds.dims:
            ds = ds.rename({"time": "valid_time"})
        drop = [c for c in ("number", "expver") if c in ds.variables]
        ds = ds.drop_vars(drop)
        present = [v for v in KNOWN_VARS if v in ds.data_vars]
        if not present:
            raise ValueError(f"No known ERA5 variables in {nc_path}: {list(ds.data_vars)}")
        df = ds[present].to_dataframe().reset_index()

    df = df.rename(columns={"valid_time": "valid_time_utc"})
    df["valid_time_utc"] = pd.to_datetime(df["valid_time_utc"], utc=True)
    cols = ["valid_time_utc", "latitude", "longitude", *present]
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
