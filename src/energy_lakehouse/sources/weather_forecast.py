"""Hourly weather forecasts over GB from Open-Meteo (no key; free for non-commercial use).

ERA5 only covers the past, so the week-ahead outlook needs a forecast. Each run asks for
the next ``FORECAST_DAYS`` days at 1-degree points inside the ERA5 box and lands one JSON
line per point and hour. ``issued_utc`` is when we fetched it: Open-Meteo blends several
models and doesn't report a single run time.

Past issues are rebuilt from Open-Meteo's Previous Runs API (``fetch_archive``): for each
past day, the forecast as it stood when energy_daily would have fetched it (07:40 UK time),
so the week-ahead outlook can be backtested.

Data: Open-Meteo.com, CC BY 4.0.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from energy_lakehouse.config import ERA5_GB_AREA
from energy_lakehouse.http_client import DEFAULT_TIMEOUT

URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_DAYS = 9                      # today + 8: a full week ahead from tomorrow
VARIABLES = ("wind_speed_100m", "temperature_2m", "shortwave_radiation")
CHUNK = 50                             # points per request

ARCHIVE_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
ARCHIVE_VARIABLES = ("wind_speed_100m", "temperature_2m")   # the outlook doesn't use solar
MAX_LEAD_DAYS = 7                      # the API keeps forecasts up to 7 days before valid time
OUTLOOK_DAYS = 7                       # targets D+1..D+7, as gold.fact_gb_week_ahead_outlook
ISSUE_LOCAL_TIME = time(7, 40)         # energy_daily's schedule
ARCHIVE_SINCE = date(2024, 3, 14)      # first day with every lead up to 7 days
LONDON = ZoneInfo("Europe/London")


def grid_points(area: list[float] = ERA5_GB_AREA, step: float = 1.0) -> list[tuple[float, float]]:
    """Cell centres on a ``step``-degree grid inside [North, West, South, East]."""
    north, west, south, east = area
    pts, lat = [], south + step / 2
    while lat < north:
        lon = west + step / 2
        while lon < east:
            pts.append((round(lat, 2), round(lon, 2)))
            lon += step
        lat += step
    return pts


def parse(locations: list[dict], points: list[tuple[float, float]], issued_utc: str) -> list[dict]:
    """Open-Meteo's per-location arrays -> one record per point and hour. ``points`` are
    the coordinates we asked for: the API snaps to its own grid and reports that instead."""
    rows = []
    for (lat, lon), loc in zip(points, locations, strict=True):
        h = loc["hourly"]
        for i, t in enumerate(h["time"]):
            rows.append({
                "issued_utc": issued_utc,
                "latitude": lat,
                "longitude": lon,
                "valid_time_utc": f"{t}:00Z",
                "wind_speed_100m_ms": h["wind_speed_100m"][i],
                "temp_c": h["temperature_2m"][i],
                "solar_irradiance_wm2": h["shortwave_radiation"][i],
            })
    return rows


def fetch(session: requests.Session, now: datetime | None = None) -> bytes:
    issued = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    points = grid_points()
    rows = []
    for i in range(0, len(points), CHUNK):
        chunk = points[i:i + CHUNK]
        resp = session.get(URL, params={
            "latitude": ",".join(str(p[0]) for p in chunk),
            "longitude": ",".join(str(p[1]) for p in chunk),
            "hourly": ",".join(VARIABLES), "wind_speed_unit": "ms", "timezone": "UTC",
            "forecast_days": FORECAST_DAYS,
        }, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        rows += parse(body if isinstance(body, list) else [body], chunk, issued)
    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode()


# --- Archived issues ---------------------------------------------------------------------

def issue_time_utc(issue_day: date) -> datetime:
    """When energy_daily would have fetched the forecast on ``issue_day``."""
    return datetime.combine(issue_day, ISSUE_LOCAL_TIME, LONDON).astimezone(timezone.utc)


def lead_for(valid: datetime, issued: datetime) -> tuple[int, int]:
    """Which ``_previous_dayN`` to use for ``valid``, so the forecast was made by ``issued``:
    the API's dayN is the forecast made 24*N hours before valid time. Returns (N, how many
    hours after the issue that forecast was made), the second non-zero only when the issue
    would need more than ``MAX_LEAD_DAYS``."""
    hours = (valid - issued).total_seconds() / 3600
    n = max(1, math.ceil(hours / 24))
    if n <= MAX_LEAD_DAYS:
        return n, 0
    return MAX_LEAD_DAYS, math.ceil(hours - 24 * MAX_LEAD_DAYS)


def parse_archive(locations: list[dict], points: list[tuple[float, float]],
                  issue_days: Iterable[date]) -> list[dict]:
    """Previous Runs API arrays -> one record per issue, point and hour of D+1..D+7, in the
    live forecast's shape plus the lead used."""
    issue_days = list(issue_days)
    times = [datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
             for t in locations[0]["hourly"]["time"]] if locations else []
    local_days = [t.astimezone(LONDON).date() for t in times]
    rows = []
    for issue_day in issue_days:
        issued = issue_time_utc(issue_day)
        issued_s = issued.strftime("%Y-%m-%dT%H:%M:%SZ")
        first, last = issue_day + timedelta(days=1), issue_day + timedelta(days=OUTLOOK_DAYS)
        hours = [(i, t, *lead_for(t, issued)) for i, (t, d) in enumerate(zip(times, local_days))
                 if first <= d <= last]
        for (lat, lon), loc in zip(points, locations, strict=True):
            h = loc["hourly"]
            for i, t, n, shortfall in hours:
                wind = h[f"wind_speed_100m_previous_day{n}"][i]
                if wind is None:
                    continue
                rows.append({
                    "issued_utc": issued_s,
                    "latitude": lat,
                    "longitude": lon,
                    "valid_time_utc": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "wind_speed_100m_ms": wind,
                    "temp_c": h[f"temperature_2m_previous_day{n}"][i],
                    "lead_days": n,
                    "lead_shortfall_hours": shortfall,
                })
    return rows


def fetch_archive(session: requests.Session, first_issue: date, last_issue: date) -> list[dict]:
    """Rebuilt forecasts for every issue day in [first_issue, last_issue]."""
    points = grid_points()
    hourly = ",".join(f"{v}_previous_day{n}" for v in ARCHIVE_VARIABLES
                      for n in range(1, MAX_LEAD_DAYS + 1))
    issue_days = [first_issue + timedelta(days=i)
                  for i in range((last_issue - first_issue).days + 1)]
    rows = []
    for i in range(0, len(points), CHUNK):
        chunk = points[i:i + CHUNK]
        resp = session.get(ARCHIVE_URL, params={
            "latitude": ",".join(str(p[0]) for p in chunk),
            "longitude": ",".join(str(p[1]) for p in chunk),
            "hourly": hourly, "wind_speed_unit": "ms", "timezone": "UTC",
            # UTC dates: D+1 starts at 23:00 UTC on D in summer.
            "start_date": first_issue.isoformat(),
            "end_date": (last_issue + timedelta(days=OUTLOOK_DAYS + 1)).isoformat(),
        }, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        body = resp.json()
        rows += parse_archive(body if isinstance(body, list) else [body], chunk, issue_days)
    return rows
