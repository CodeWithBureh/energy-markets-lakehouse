import json
from datetime import date
from pathlib import Path

import pytest

from energy_lakehouse.sources import elexon


class FakeResponse:
    def __init__(self, body: bytes):
        self.content = body

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, body: bytes = b'{"data": []}'):
        self.body = body
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params, timeout):
        self.calls.append((url.removeprefix(elexon.BASE_URL), params))
        return FakeResponse(self.body)


def test_forecasts_are_requested_by_publish_day():
    s = FakeSession()
    for fn, path in ((elexon.fetch_demand_forecast, "/datasets/NDF"),
                     (elexon.fetch_wind_forecast, "/datasets/WINDFOR"),
                     (elexon.fetch_wind_solar_forecast, "/datasets/DGWS")):
        fn(s, date(2026, 11, 18))
        assert s.calls[-1] == (path, {"publishDateTimeFrom": "2026-11-18T00:00:00Z",
                                      "publishDateTimeTo": "2026-11-18T23:59:59Z",
                                      "format": "json"})


def test_outturn_is_requested_by_settlement_day():
    s = FakeSession()
    elexon.fetch_demand_outturn(s, date(2026, 11, 19))
    assert s.calls == [("/demand/outturn", {"settlementDateFrom": "2026-11-19",
                                            "settlementDateTo": "2026-11-19",
                                            "format": "json"})]


@pytest.mark.parametrize("day, first, last", [
    # Winter: 48 periods, London midnight is 00:00 UTC.
    (date(2026, 11, 19), "2026-11-19T00:00Z", "2026-11-19T23:30Z"),
    # Summer time: the settlement day starts at 23:00 UTC the evening before.
    (date(2026, 7, 1), "2026-06-30T23:00Z", "2026-07-01T22:30Z"),
    # Clock changes: 46 and 50 periods.
    (date(2026, 3, 29), "2026-03-29T00:00Z", "2026-03-29T22:30Z"),
    (date(2026, 10, 25), "2026-10-24T23:00Z", "2026-10-25T23:30Z"),
])
def test_market_index_window_covers_exactly_one_settlement_day(day, first, last):
    s = FakeSession()
    elexon.fetch_market_index(s, day)
    path, params = s.calls[0]
    assert path == "/datasets/MID"
    assert (params["from"], params["to"]) == (first, last)


def test_unexpected_response_shape_raises():
    with pytest.raises(ValueError, match="Unexpected Elexon response"):
        elexon.fetch_demand_forecast(FakeSession(b'{"errors": {}}'), date(2026, 11, 18))


def test_run_elexon_lands_every_dataset_per_day(tmp_path, monkeypatch):
    from energy_lakehouse import cli

    body = json.dumps({"data": [{"x": 1}, {"x": 2}]}).encode()
    seen = []

    def fake(name):
        def fetch(session, d):
            seen.append((name, d))
            if name == "wind_forecast":
                raise RuntimeError("boom")
            return body
        return fetch

    monkeypatch.setattr(elexon, "DATASETS", {n: fake(n) for n in elexon.DATASETS})
    rep = cli.run_elexon(str(tmp_path), date(2026, 11, 18), date(2026, 11, 19), "ts")

    assert len(seen) == 2 * 7
    assert len(rep.files) == 2 * 6 and rep.rows_hint == 2 * 6 * 2
    # One failing dataset is reported, and the others still land.
    assert rep.errors == ["wind_forecast 2026-11-18: boom", "wind_forecast 2026-11-19: boom"]
    landed = {Path(f).relative_to(tmp_path).parts[1] for f in rep.files}
    assert landed == set(elexon.DATASETS) - {"wind_forecast"}
