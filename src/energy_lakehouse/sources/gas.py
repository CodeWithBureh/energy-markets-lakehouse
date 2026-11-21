"""Daily gas prices: GB System Average Price (SAP) and Dutch TTF.

SAP comes from the National Gas data portal (no key): the average price of gas traded on
the On-the-day Commodity Market for each gas day (05:00 to 05:00 UK time), in pence per
kWh. Data item PUBOB603 is "SAP, Actual Day". Source: National Gas Transmission.

TTF is the front-month ICE Endex future via yfinance (ticker TTF=F), EUR/MWh. yfinance is
unofficial and for personal use; swap in ICE Endex end-of-day data if this ever matters.

Both are landed as JSON lines we write ourselves, so bronze can use an explicit schema.
"""

from __future__ import annotations

import csv
import io
import json
import math
from datetime import date, datetime, timedelta

import requests

from energy_lakehouse.http_client import DEFAULT_TIMEOUT

NATIONAL_GAS_URL = "https://data.nationalgas.com/api/find-gas-data-download"
SAP_ITEM_ID = "PUBOB603"
SAP_ITEM_NAME = "SAP, Actual Day"
TTF_TICKER = "TTF=F"

_NG_TIME = "%d/%m/%Y %H:%M:%S"


def _ng_time(value: str) -> str | None:
    """National Gas timestamps are UK local time, dd/mm/yyyy hh:mm:ss; kept as given."""
    value = (value or "").strip()
    return datetime.strptime(value, _NG_TIME).isoformat() if value else None


def parse_sap_csv(text: str) -> list[dict]:
    """Rows of the portal's CSV download -> one record per gas day."""
    records = []
    for row in csv.DictReader(io.StringIO(text)):
        if row.get("Data Item") != SAP_ITEM_NAME:
            continue
        try:
            value = float(row["Value"])
        except (TypeError, ValueError):
            continue                                   # "N/A" and blanks
        records.append({
            "gas_day": datetime.strptime(row["Applicable For"].strip(), "%d/%m/%Y").date().isoformat(),
            "sap_p_kwh": value,
            "applicable_at_local": _ng_time(row.get("Applicable At", "")),
            "generated_at_local": _ng_time(row.get("Generated Time", "")),
            "quality_indicator": (row.get("Quality Indicator") or "").strip() or None,
        })
    return records


def fetch_sap(session: requests.Session, start: date, end: date) -> bytes:
    """SAP for gas days ``start``..``end`` (inclusive), latest published value of each."""
    resp = session.get(NATIONAL_GAS_URL, params={
        "applicableFor": "Y", "dateFrom": start.isoformat(), "dateTo": end.isoformat(),
        "dateType": "GASDAY", "latestFlag": "Y", "ids": SAP_ITEM_ID, "type": "CSV",
    }, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    if not resp.text.startswith("Applicable At"):
        raise ValueError(f"Unexpected National Gas response: {resp.text[:200]!r}")
    return _jsonl(parse_sap_csv(resp.text))


def _clean(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def fetch_ttf(start: date, end: date) -> bytes:
    """Daily TTF front-month settlement (``end`` inclusive), one record per trading day."""
    import yfinance as yf  # imported lazily: heavy, and only needed for this source

    hist = yf.Ticker(TTF_TICKER).history(
        start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(),
        interval="1d", auto_adjust=False,
    )
    return _jsonl([
        {"ticker": TTF_TICKER, "trade_date": ts.date().isoformat(),
         "open": _clean(row.get("Open")), "high": _clean(row.get("High")),
         "low": _clean(row.get("Low")), "close": _clean(row.get("Close")),
         "volume": _clean(row.get("Volume")), "currency": "EUR", "unit": "MWh"}
        for ts, row in hist.iterrows()
    ])


def _jsonl(records: list[dict]) -> bytes:
    return ("\n".join(json.dumps(r) for r in records) + ("\n" if records else "")).encode()
