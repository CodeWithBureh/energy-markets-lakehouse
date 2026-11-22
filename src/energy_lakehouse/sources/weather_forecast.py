"""Hourly weather forecasts over GB from Open-Meteo (no key; free for non-commercial use).

ERA5 only covers the past, so the week-ahead outlook needs a forecast. Each run asks for
the next ``FORECAST_DAYS`` days at 1-degree points inside the ERA5 box and lands one JSON
line per point and hour. ``issued_utc`` is when we fetched it: Open-Meteo blends several
models and doesn't report a single run time.

Data: Open-Meteo.com, CC BY 4.0.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import requests

from energy_lakehouse.config import ERA5_GB_AREA
from energy_lakehouse.http_client import DEFAULT_TIMEOUT

URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_DAYS = 9                      # today + 8: a full week ahead from tomorrow
VARIABLES = ("wind_speed_100m", "temperature_2m", "shortwave_radiation")
CHUNK = 50                             # points per request


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
