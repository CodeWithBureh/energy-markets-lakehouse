from datetime import date

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from energy_lakehouse.landing import land_bytes
from energy_lakehouse.sources.era5 import build_request, group_by_month


def _synthetic_era5(path):
    xr = pytest.importorskip("xarray")
    times = pd.date_range("2026-11-01T00:00", periods=3, freq="h")
    lats = np.array([55.0, 54.75])
    lons = np.array([-3.0, -2.75])
    shape = (len(times), len(lats), len(lons))
    ds = xr.Dataset(
        {
            "t2m": (("valid_time", "latitude", "longitude"), np.full(shape, 280.15)),
            "u100": (("valid_time", "latitude", "longitude"), np.full(shape, 3.0)),
            "v100": (("valid_time", "latitude", "longitude"), np.full(shape, 4.0)),
            "ssrd": (("valid_time", "latitude", "longitude"), np.full(shape, 7200.0)),
        },
        coords={"valid_time": times, "latitude": lats, "longitude": lons, "number": 0,
                "expver": ("valid_time", ["0005"] * 3)},
    )
    ds.to_netcdf(path)


def test_flatten_era5(tmp_path):
    from energy_lakehouse.parsers.era5_netcdf import flatten_era5, to_parquet_bytes

    nc = tmp_path / "sample.nc"
    _synthetic_era5(nc)
    df = flatten_era5(nc)
    assert list(df.columns) == ["valid_time_utc", "latitude", "longitude", "t2m", "u100", "v100", "ssrd"]
    assert len(df) == 3 * 2 * 2
    assert str(df["valid_time_utc"].dt.tz) == "UTC"

    out = tmp_path / "x.parquet"
    out.write_bytes(to_parquet_bytes(df))
    ts_type = pq.read_schema(out).field("valid_time_utc").type
    assert ts_type.unit == "us"   # Spark-friendly precision


def test_group_by_month_splits_requests():
    days = [date(2026, 10, 30), date(2026, 10, 31), date(2026, 11, 1)]
    groups = group_by_month(days)
    assert set(groups) == {(2026, 10), (2026, 11)}
    req = build_request(2026, 10, [30, 31], [61, -8, 49.5, 2])
    assert req["day"] == ["30", "31"] and req["data_format"] == "netcdf"
    assert len(req["time"]) == 24


def test_land_bytes_layout_and_no_temp_left(tmp_path):
    p = land_bytes(tmp_path, "elexon", "system_prices", date(2026, 11, 12), b"{}", "json", "RUN1")
    assert p == tmp_path / "elexon/system_prices/2026/11/12/system_prices_2026-11-12_RUN1.json"
    assert p.read_bytes() == b"{}"
    assert not list(p.parent.glob("_tmp_*"))
