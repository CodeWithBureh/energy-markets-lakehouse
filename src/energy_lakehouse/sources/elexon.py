"""Elexon Insights Solution API (GB balancing market). No API key required.

Docs: https://developer.data.elexon.co.uk
Data licence: "Contains BMRS data © Elexon Limited copyright and database right".

Two kinds of daily request:
  * outturn and prices are asked for by settlement date (the day the power was delivered);
  * forecasts are asked for by publish date, so every vintage published that day is kept.
    The API allows at most one day of publish times per request. Landing them by publish
    date means a backfill captures what was actually known at each point in time.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, timedelta

import requests

from energy_lakehouse.http_client import DEFAULT_TIMEOUT
from energy_lakehouse.timeutils import PERIOD, local_midnight_utc

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"


def _get(session: requests.Session, path: str, params: dict) -> bytes:
    resp = session.get(f"{BASE_URL}{path}", params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    body = resp.content
    parsed = json.loads(body)
    if "data" not in parsed:
        raise ValueError(f"Unexpected Elexon response shape for {path}: keys={list(parsed)}")
    return body


def _settlement_day(d: date) -> dict:
    return {"settlementDateFrom": d.isoformat(), "settlementDateTo": d.isoformat(),
            "format": "json"}


def _publish_day(d: date) -> dict:
    """Everything published on UTC day ``d`` (the API caps the window at one day)."""
    return {"publishDateTimeFrom": f"{d.isoformat()}T00:00:00Z",
            "publishDateTimeTo": f"{d.isoformat()}T23:59:59Z", "format": "json"}


def fetch_system_prices(session: requests.Session, settlement_date: date) -> bytes:
    """System (imbalance) prices and net imbalance volume for every settlement period."""
    return _get(
        session,
        f"/balancing/settlement/system-prices/{settlement_date.isoformat()}",
        {"format": "json"},
    )


def fetch_generation_by_fuel(session: requests.Session, settlement_date: date) -> bytes:
    """Half-hourly generation outturn by fuel type (FUELHH), incl. interconnector flows."""
    return _get(session, "/datasets/FUELHH", _settlement_day(settlement_date))


def fetch_demand_outturn(session: requests.Session, settlement_date: date) -> bytes:
    """Initial national demand outturn (INDO) and transmission system demand (ITSDO) per
    settlement period. Both are net of embedded wind and solar, which show up as lower
    demand rather than as generation."""
    return _get(session, "/demand/outturn", _settlement_day(settlement_date))


def fetch_market_index(session: requests.Session, settlement_date: date) -> bytes:
    """Market index data (MID): volume-weighted price of short-term GB power trades per
    settlement period, from APX (EPEX). N2EX also reports, but with zero volume.

    ``from``/``to`` filter on period start time, inclusive, so the window runs from local
    midnight to the start of the day's last period (46, 48 or 50 periods)."""
    first = local_midnight_utc(settlement_date)
    last = local_midnight_utc(settlement_date + timedelta(days=1)) - PERIOD
    return _get(session, "/datasets/MID", {
        "from": first.strftime("%Y-%m-%dT%H:%MZ"),
        "to": last.strftime("%Y-%m-%dT%H:%MZ"),
        "format": "json",
    })


def fetch_demand_forecast(session: requests.Session, publish_date: date) -> bytes:
    """National demand forecast (NDF), every half-hourly publish on ``publish_date``.
    Each publish covers roughly the next 1-2 days."""
    return _get(session, "/datasets/NDF", _publish_day(publish_date))


def fetch_wind_forecast(session: requests.Session, publish_date: date) -> bytes:
    """Hourly wind generation forecast for transmission-connected wind (WINDFOR), every
    publish on ``publish_date``."""
    return _get(session, "/datasets/WINDFOR", _publish_day(publish_date))


def fetch_wind_solar_forecast(session: requests.Session, publish_date: date) -> bytes:
    """Half-hourly solar, onshore and offshore wind forecasts (DGWS, ENTSO-E B1440), every
    publish on ``publish_date``: day-ahead (about 16:45 UK the day before) and intraday."""
    return _get(session, "/datasets/DGWS", _publish_day(publish_date))


def fetch_demand_forecast_daily(session: requests.Session, publish_date: date) -> bytes:
    """National demand forecast 2-14 days ahead (NDFD): one publish a day (~14:45 UTC), one
    figure per day, the forecast daily PEAK of national demand in MW."""
    return _get(session, "/datasets/NDFD", _publish_day(publish_date))


# Landing dataset name -> fetcher. run_elexon calls each one per day of its window.
DATASETS: dict[str, Callable[[requests.Session, date], bytes]] = {
    "system_prices": fetch_system_prices,
    "generation_by_fuel": fetch_generation_by_fuel,
    "demand_outturn": fetch_demand_outturn,
    "market_index": fetch_market_index,
    "demand_forecast": fetch_demand_forecast,
    "wind_forecast": fetch_wind_forecast,
    "wind_solar_forecast": fetch_wind_solar_forecast,
    "demand_forecast_daily": fetch_demand_forecast_daily,
}
