from datetime import date, datetime, timedelta, timezone

from energy_lakehouse import cli
from energy_lakehouse.sources import weather_forecast as wf

UTC = timezone.utc


def test_issue_time_is_0740_uk_in_winter_and_summer():
    assert wf.issue_time_utc(date(2026, 1, 10)) == datetime(2026, 1, 10, 7, 40, tzinfo=UTC)
    assert wf.issue_time_utc(date(2026, 7, 10)) == datetime(2026, 7, 10, 6, 40, tzinfo=UTC)


def test_lead_is_the_shortest_made_by_the_issue():
    issued = datetime(2026, 1, 10, 7, 40, tzinfo=UTC)
    # D+1 00:00 is 16h20 after the issue: the forecast made 24h before was made by then.
    assert wf.lead_for(datetime(2026, 1, 11, 0, tzinfo=UTC), issued) == (1, 0)
    # D+1 08:00 is 24h20 after: 24h before is 08:00 on D, after the issue, so 48h.
    assert wf.lead_for(datetime(2026, 1, 11, 8, tzinfo=UTC), issued) == (2, 0)
    assert wf.lead_for(datetime(2026, 1, 17, 7, tzinfo=UTC), issued) == (7, 0)
    # Late on D+7 would need 8 days; the API keeps 7, made up to 16 hours after the issue.
    assert wf.lead_for(datetime(2026, 1, 17, 8, tzinfo=UTC), issued) == (7, 1)
    assert wf.lead_for(datetime(2026, 1, 17, 23, tzinfo=UTC), issued) == (7, 16)


def _archive_location(start: datetime, hours: int):
    times = [start + timedelta(hours=h) for h in range(hours)]
    h = {"time": [t.strftime("%Y-%m-%dT%H:%M") for t in times]}
    for n in range(1, 8):
        h[f"wind_speed_100m_previous_day{n}"] = [float(n)] * hours     # value = lead used
        h[f"temperature_2m_previous_day{n}"] = [10.0 + n] * hours
    h["wind_speed_100m_previous_day1"][30] = None                      # a gap is skipped
    return {"latitude": 55.01, "longitude": -2.99, "hourly": h}


def test_parse_archive_builds_d1_to_d7_per_issue_in_uk_days():
    issue = date(2026, 3, 27)                    # week spans the clock change on 29 Mar
    loc = _archive_location(datetime(2026, 3, 27, tzinfo=UTC), 24 * 10)
    rows = wf.parse_archive([loc], [(55.0, -3.0)], [issue])
    assert {r["issued_utc"] for r in rows} == {"2026-03-27T07:40:00Z"}
    # 7 UK days, one of them 23 hours, minus the one gap.
    assert len(rows) == 24 * 7 - 1 - 1
    first = rows[0]
    assert first["valid_time_utc"] == "2026-03-28T00:00:00Z"
    assert (first["lead_days"], first["wind_speed_100m_ms"], first["temp_c"]) == (1, 1.0, 11.0)
    assert (first["latitude"], first["longitude"]) == (55.0, -3.0)
    last = rows[-1]
    assert last["valid_time_utc"] == "2026-04-03T22:00:00Z"   # 23:00 BST on D+7
    assert last["lead_days"] == 7 and last["lead_shortfall_hours"] > 0
    assert all(r["wind_speed_100m_ms"] == r["lead_days"] for r in rows)


class FakeSession:
    def __init__(self):
        self.params = []

    def get(self, url, params, timeout):
        assert url == wf.ARCHIVE_URL
        self.params.append(params)
        n = len(params["latitude"].split(","))
        start = datetime.fromisoformat(params["start_date"]).replace(tzinfo=UTC)
        end = datetime.fromisoformat(params["end_date"]).replace(tzinfo=UTC)
        hours = int((end - start).total_seconds() // 3600) + 24
        return _Resp([_archive_location(start, hours) for _ in range(n)])


class _Resp:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


def test_fetch_archive_asks_for_every_lead_and_the_whole_last_week():
    s = FakeSession()
    rows = wf.fetch_archive(s, date(2026, 1, 10), date(2026, 1, 11))
    assert [len(p["latitude"].split(",")) for p in s.params] == [50, 50, 10]
    p = s.params[0]
    assert (p["start_date"], p["end_date"]) == ("2026-01-10", "2026-01-19")
    assert "wind_speed_100m_previous_day7" in p["hourly"]
    assert "temperature_2m_previous_day1" in p["hourly"]
    assert len({r["issued_utc"] for r in rows}) == 2


def test_runner_lands_complete_issues_once_per_month(tmp_path, monkeypatch):
    calls = []

    def fake(session, first, last):
        calls.append((first, last))
        return [{"issued_utc": f"{first}T07:40:00Z", "latitude": 55.0, "longitude": -3.0,
                 "valid_time_utc": f"{first}T12:00:00Z", "wind_speed_100m_ms": 9.0,
                 "temp_c": 5.0, "lead_days": 1, "lead_shortfall_hours": 0}]

    monkeypatch.setattr(wf, "fetch_archive", fake)
    today = date(2026, 11, 22)
    lander = cli.Lander(str(tmp_path), "20261122T120000000000Z")
    rep = cli.run_weather_forecast_archive(lander, date(2026, 10, 20), date(2026, 11, 21), today)
    # Issues whose week isn't over (after 14 Nov) are left for the live forecast.
    assert calls == [(date(2026, 10, 20), date(2026, 10, 31)),
                     (date(2026, 11, 1), date(2026, 11, 14))]
    assert len(rep.files) == 2 and all(f.endswith(".parquet") for f in rep.files)

    calls.clear()
    again = cli.run_weather_forecast_archive(
        cli.Lander(str(tmp_path), "20261123T120000000000Z"),
        date(2026, 10, 20), date(2026, 11, 21), today)
    assert calls == [] and again.skipped_settled == 2


def test_runner_skips_before_the_archive_starts(tmp_path, monkeypatch):
    monkeypatch.setattr(wf, "fetch_archive", lambda *a: (_ for _ in ()).throw(AssertionError))
    rep = cli.run_weather_forecast_archive(cli.Lander(str(tmp_path), "T"),
                                           date(2024, 1, 1), date(2024, 1, 31), date(2026, 11, 22))
    assert not rep.files and not rep.errors
