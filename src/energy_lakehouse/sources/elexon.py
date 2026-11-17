"""Elexon Insights Solution API (GB balancing market). No API key required.

Docs: https://developer.data.elexon.co.uk
Data licence: "Contains BMRS data © Elexon Limited copyright and database right".
"""

from __future__ import annotations

import json
from datetime import date

import requests

from energy_lakehouse.http_client import DEFAULT_TIMEOUT

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"


def _get(session: requests.Session, path: str, params: dict) -> bytes:
    resp = session.get(f"{BASE_URL}{path}", params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    body = resp.content
    parsed = json.loads(body)
    if "data" not in parsed:
        raise ValueError(f"Unexpected Elexon response shape for {path}: keys={list(parsed)}")
    return body


def fetch_system_prices(session: requests.Session, settlement_date: date) -> bytes:
    """System (imbalance) prices and net imbalance volume for every settlement period."""
    return _get(
        session,
        f"/balancing/settlement/system-prices/{settlement_date.isoformat()}",
        {"format": "json"},
    )


def fetch_generation_by_fuel(session: requests.Session, settlement_date: date) -> bytes:
    """Half-hourly generation outturn by fuel type (FUELHH), incl. interconnector flows."""
    return _get(
        session,
        "/datasets/FUELHH",
        {
            "settlementDateFrom": settlement_date.isoformat(),
            "settlementDateTo": settlement_date.isoformat(),
            "format": "json",
        },
    )
