"""Daily share prices for London-listed energy companies via yfinance.

yfinance is an unofficial Yahoo Finance wrapper. It's fine for a portfolio project but
can break without notice. Swap in Alpaca/Twelve Data/Marketstack if it does; only this
module needs to change.
"""

from __future__ import annotations

import json
import math
from datetime import date, timedelta

from energy_lakehouse.config import STOCKS


def _clean(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def fetch_daily_prices(ticker: str, start: date, end: date) -> bytes:
    """JSON-lines payload, one record per trading day (``end`` inclusive)."""
    import yfinance as yf  # imported lazily: heavy, and only needed for this source

    hist = yf.Ticker(ticker).history(
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),  # yfinance 'end' is exclusive
        interval="1d",
        auto_adjust=False,
        actions=True,
    )
    meta = STOCKS.get(ticker, {})
    lines = []
    for ts, row in hist.iterrows():
        lines.append(
            json.dumps(
                {
                    "ticker": ticker,
                    "trade_date": ts.date().isoformat(),
                    "open": _clean(row.get("Open")),
                    "high": _clean(row.get("High")),
                    "low": _clean(row.get("Low")),
                    "close": _clean(row.get("Close")),
                    "adj_close": _clean(row.get("Adj Close")),
                    "volume": _clean(row.get("Volume")),
                    "dividends": _clean(row.get("Dividends")),
                    "stock_splits": _clean(row.get("Stock Splits")),
                    "quote_currency": meta.get("currency", "GBp"),
                }
            )
        )
    return ("\n".join(lines) + ("\n" if lines else "")).encode()
