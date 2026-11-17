"""FRED (Federal Reserve Bank of St. Louis): gas prices and FX rates.

Free key: https://fred.stlouisfed.org/docs/api/api_key.html
"""

from __future__ import annotations

import json
from datetime import date

import requests

from energy_lakehouse.http_client import DEFAULT_TIMEOUT

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch_observations(
    session: requests.Session, api_key: str, series_id: str, start: date, end: date
) -> bytes:
    """Observations for one series. Missing values come back as the string '.'."""
    resp = session.get(
        BASE_URL,
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": start.isoformat(),
            "observation_end": end.isoformat(),
        },
        timeout=DEFAULT_TIMEOUT,
    )
    resp.raise_for_status()
    body = json.loads(resp.content)
    # The response doesn't name the series, so stamp it in. Bronze can then use it
    # directly instead of parsing it out of the file path.
    body["series_id"] = series_id
    return json.dumps(body).encode()
