"""ENTSO-E Transparency Platform: day-ahead prices (document type A44).

Free token: register at https://transparency.entsoe.eu, then email transparency@entsoe.eu
asking for "Restful API access". Rate limit is roughly 400 requests per minute.
"""

from __future__ import annotations

from datetime import date, timedelta

import requests

from energy_lakehouse.http_client import DEFAULT_TIMEOUT
from energy_lakehouse.timeutils import BRUSSELS, local_midnight_utc

BASE_URL = "https://web-api.tp.entsoe.eu/api"


def delivery_day_bounds(d: date) -> tuple[str, str]:
    """periodStart/periodEnd (yyyyMMddHHmm, UTC) covering CET/CEST delivery day ``d``."""
    start = local_midnight_utc(d, BRUSSELS)
    end = local_midnight_utc(d + timedelta(days=1), BRUSSELS)
    return start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M")


def fetch_day_ahead_prices(
    session: requests.Session, token: str, zone_eic: str, delivery_date: date
) -> bytes:
    """Raw XML for one zone and one delivery day.

    ENTSO-E answers "no data yet" with an Acknowledgement_MarketDocument, sometimes
    as HTTP 200 and sometimes as 400. Both come back as bytes; the parser handles them.
    """
    period_start, period_end = delivery_day_bounds(delivery_date)
    resp = session.get(
        BASE_URL,
        params={
            "securityToken": token,
            "documentType": "A44",
            "contract_MarketAgreement.type": "A01",  # day-ahead
            "in_Domain": zone_eic,
            "out_Domain": zone_eic,
            "periodStart": period_start,
            "periodEnd": period_end,
        },
        timeout=DEFAULT_TIMEOUT,
    )
    if resp.status_code == 400 and b"Acknowledgement_MarketDocument" in resp.content:
        return resp.content
    resp.raise_for_status()
    return resp.content
