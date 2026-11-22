import json

import pandas as pd

from energy_lakehouse import cli
from energy_lakehouse.sources import gas

SAP_CSV = (
    "Applicable At,Applicable For,Data Item,Value,Generated Time,Quality Indicator\n"
    '21/11/2026 11:40:00,19/11/2026,"SAP, Actual Day",7.1646,21/11/2026 11:41:01,L\n'
    '21/11/2026 11:40:00,20/11/2026,"SAP, Actual Day",7.4839,21/11/2026 11:41:01,L\n'
    '21/11/2026 11:40:00,18/11/2026,"SAP, Actual Day",N/A,21/11/2026 11:41:01,\n'
    '21/11/2026 11:40:00,20/11/2026,"SAP, 7 Day rolling average",7.52,21/11/2026 11:41:01,\n'
)


def test_sap_csv_keeps_actual_day_rows_with_numbers():
    rows = gas.parse_sap_csv(SAP_CSV)
    assert [(r["gas_day"], r["sap_p_kwh"]) for r in rows] == [
        ("2026-11-19", 7.1646), ("2026-11-20", 7.4839)]
    assert rows[0]["generated_at_local"] == "2026-11-21T11:41:01"
    assert rows[0]["quality_indicator"] == "L"


class FakeResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, text):
        self.text, self.params = text, None

    def get(self, url, params, timeout):
        self.params = params
        return FakeResponse(self.text)


def test_sap_request_covers_the_window_and_lands_json_lines():
    s = FakeSession(SAP_CSV)
    from datetime import date

    body = gas.fetch_sap(s, date(2026, 11, 14), date(2026, 11, 20))
    assert s.params["ids"] == "PUBOB603" and s.params["latestFlag"] == "Y"
    assert (s.params["dateFrom"], s.params["dateTo"]) == ("2026-11-14", "2026-11-20")
    assert [json.loads(line)["gas_day"] for line in body.decode().splitlines()] == [
        "2026-11-19", "2026-11-20"]


def test_sap_rejects_an_error_page():
    from datetime import date

    import pytest

    with pytest.raises(ValueError):
        gas.fetch_sap(FakeSession("<html>maintenance</html>"), date(2026, 11, 1), date(2026, 11, 2))


def test_run_gas_lands_sap_and_ttf_and_survives_one_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(gas, "fetch_sap", lambda s, a, b: b'{"gas_day": "2026-11-20"}\n')

    def broken_ttf(a, b):
        raise RuntimeError("Yahoo down")

    monkeypatch.setattr(gas, "fetch_ttf", broken_ttf)
    from datetime import date

    rep = cli.run_gas(cli.Lander(str(tmp_path), "T1"), date(2026, 11, 14), date(2026, 11, 20))
    assert len(rep.files) == 1 and "gas" in rep.files[0] and "sap" in rep.files[0]
    assert rep.errors == ["ttf: Yahoo down"]


def test_ttf_records_are_one_per_trading_day(monkeypatch):
    import sys
    import types
    from datetime import date

    idx = pd.to_datetime(["2026-11-19", "2026-11-20"]).tz_localize("America/New_York")
    hist = pd.DataFrame({"Open": [84.0, 85.0], "High": [86.0, 89.0], "Low": [80.0, 84.0],
                         "Close": [84.46, 88.45], "Volume": [10.0, float("nan")]}, index=idx)

    class Ticker:
        def __init__(self, t):
            assert t == "TTF=F"

        def history(self, **kw):
            assert kw["end"] == "2026-11-21"
            return hist

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(Ticker=Ticker))
    rows = [json.loads(x) for x in gas.fetch_ttf(date(2026, 11, 19), date(2026, 11, 20)).splitlines()]
    assert [(r["trade_date"], r["close"]) for r in rows] == [("2026-11-19", 84.46), ("2026-11-20", 88.45)]
    assert rows[1]["volume"] is None and rows[0]["currency"] == "EUR"
