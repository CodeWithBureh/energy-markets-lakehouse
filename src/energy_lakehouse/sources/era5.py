"""ERA5 hourly reanalysis from the Copernicus Climate Data Store (CDS).

Free account: https://cds.climate.copernicus.eu. Copy your API key from your profile and
accept the ERA5 licence on the dataset page once, or every request will be refused.

Requests are queued on the CDS side and can take minutes or longer. To keep Databricks
costs down, this runs weekly (one request per calendar month touched), not daily.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path

from energy_lakehouse.config import ERA5_DATASET, ERA5_GB_AREA, ERA5_VARIABLES

CDS_URL = "https://cds.climate.copernicus.eu/api"


def group_by_month(days: list[date]) -> dict[tuple[int, int], list[date]]:
    """CDS requests are a cartesian product of year x month x day, so a request that
    spans months over-fetches. Split the date range into one request per month."""
    groups: dict[tuple[int, int], list[date]] = defaultdict(list)
    for d in sorted(days):
        groups[(d.year, d.month)].append(d)
    return dict(groups)


def build_request(year: int, month: int, days: list[int], area: list[float]) -> dict:
    return {
        "product_type": ["reanalysis"],
        "variable": ERA5_VARIABLES,
        "year": [f"{year:04d}"],
        "month": [f"{month:02d}"],
        "day": [f"{d:02d}" for d in sorted(days)],
        "time": [f"{h:02d}:00" for h in range(24)],
        "area": area,
        "data_format": "netcdf",
        "download_format": "unarchived",
    }


def fetch_month(
    api_key: str, year: int, month: int, days: list[int], target: Path,
    area: list[float] | None = None,
) -> Path:
    import cdsapi  # lazy import

    client = cdsapi.Client(url=CDS_URL, key=api_key, quiet=True)
    request = build_request(year, month, days, area or ERA5_GB_AREA)
    client.retrieve(ERA5_DATASET, request).download(str(target))
    return target
