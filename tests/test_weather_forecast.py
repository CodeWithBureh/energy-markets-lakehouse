import json
from datetime import datetime, timezone

from energy_lakehouse import cli
from energy_lakehouse.sources import weather_forecast as wf


def test_grid_covers_the_era5_box_at_one_degree():
    pts = wf.grid_points()
    assert len(pts) == 11 * 10                    # 49.5-61 N x 8 W-2 E
    assert min(p[0] for p in pts) == 50.0 and max(p[0] for p in pts) == 60.0
    assert min(p[1] for p in pts) == -7.5 and max(p[1] for p in pts) == 1.5


def _location(n_hours):
    return {"latitude": 55.01, "longitude": -2.99, "hourly": {
        "time": [f"2026-11-21T{h:02d}:00" for h in range(n_hours)],
        "wind_speed_100m": [10.0 + h for h in range(n_hours)],
        "temperature_2m": [5.0] * n_hours,
        "shortwave_radiation": [0.0] * n_hours}}


def test_parse_keeps_the_requested_coordinates_and_utc_times():
    rows = wf.parse([_location(2)], [(55.0, -3.0)], "2026-11-21T07:40:00Z")
    assert rows[1] == {"issued_utc": "2026-11-21T07:40:00Z", "latitude": 55.0, "longitude": -3.0,
                       "valid_time_utc": "2026-11-21T01:00:00Z", "wind_speed_100m_ms": 11.0,
                       "temp_c": 5.0, "solar_irradiance_wm2": 0.0}


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, params, timeout):
        n = len(params["latitude"].split(","))
        self.calls.append(n)
        return FakeResponse([_location(3) for _ in range(n)])


def test_fetch_chunks_requests_and_stamps_one_issue_time():
    s = FakeSession()
    now = datetime(2026, 11, 21, 7, 40, tzinfo=timezone.utc)
    rows = [json.loads(x) for x in wf.fetch(s, now).splitlines()]
    assert s.calls == [50, 50, 10]
    assert len(rows) == 110 * 3
    assert {r["issued_utc"] for r in rows} == {"2026-11-21T07:40:00Z"}


def test_cli_lands_one_file_and_ignores_dates(tmp_path, monkeypatch):
    monkeypatch.setattr(wf, "fetch", lambda session: b'{"a": 1}\n{"a": 2}\n')
    code = cli.main(["--source", "weather_forecast", "--landing-root", str(tmp_path),
                     "--start=", "--end="])
    assert code == 0
    files = list(tmp_path.rglob("*.jsonl"))
    assert len(files) == 1 and "weather_forecast" in str(files[0])
