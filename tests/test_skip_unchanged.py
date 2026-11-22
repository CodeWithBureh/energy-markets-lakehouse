import json
from datetime import date

from energy_lakehouse import cli
from energy_lakehouse.landing import is_settled, land_bytes, land_if_changed, landed_files
from energy_lakehouse.sources import elexon

DAY = date(2026, 11, 18)
EARLY = "20261118T120000000000Z"      # landed on the day itself
LATER = "20261120T074000000000Z"      # landed two days after


def test_settled_depends_on_when_the_file_was_landed(tmp_path):
    land_bytes(tmp_path, "elexon", "demand_forecast", DAY, b"{}", "json", EARLY)
    assert not is_settled(tmp_path, "elexon", "demand_forecast", DAY)   # publish day not over
    land_bytes(tmp_path, "elexon", "demand_forecast", DAY, b"{}", "json", LATER)
    assert is_settled(tmp_path, "elexon", "demand_forecast", DAY)


def test_system_prices_stay_open_for_settlement_runs(tmp_path):
    land_bytes(tmp_path, "elexon", "system_prices", DAY, b"{}", "json", LATER)
    assert not is_settled(tmp_path, "elexon", "system_prices", DAY)
    land_bytes(tmp_path, "elexon", "system_prices", DAY, b"{}", "json", "20261219T074000000000Z")
    assert is_settled(tmp_path, "elexon", "system_prices", DAY)


def test_tags_keep_slices_apart(tmp_path):
    land_bytes(tmp_path, "entsoe", "day_ahead", DAY, b"fr", "jsonl", f"{LATER}_FR")
    assert is_settled(tmp_path, "entsoe", "day_ahead", DAY, "FR")
    assert not is_settled(tmp_path, "entsoe", "day_ahead", DAY, "NL")
    # Files landed before tags carried a window start don't count for the new tag.
    assert not is_settled(tmp_path, "entsoe", "day_ahead", DAY, "FR_from2026-11-01")


def test_temp_and_unparseable_files_are_ignored(tmp_path):
    p = land_bytes(tmp_path, "gas", "sap", DAY, b"x", "jsonl", LATER)
    (p.parent / f"_tmp_{p.name}").write_bytes(b"x")
    (p.parent / "sap_2026-11-18_not-a-timestamp.jsonl").write_bytes(b"x")
    assert [f.path for f in landed_files(tmp_path, "gas", "sap", DAY)] == [p]


def test_identical_payload_is_not_landed_again(tmp_path):
    first = land_if_changed(tmp_path, "elexon", "system_prices", DAY, b"A", "json", EARLY)
    assert first is not None
    assert land_if_changed(tmp_path, "elexon", "system_prices", DAY, b"A", "json", LATER) is None
    changed = land_if_changed(tmp_path, "elexon", "system_prices", DAY, b"B", "json", LATER)
    assert changed is not None and changed != first
    # Compared with the NEWEST file: going back to A is a change again.
    assert land_if_changed(tmp_path, "elexon", "system_prices", DAY, b"A", "json",
                           "20261121T074000000000Z") is not None


def test_fingerprint_ignores_volatile_fields(tmp_path):
    def obs(body):
        return json.loads(body)["observations"]

    a = json.dumps({"realtime_start": "2026-11-18", "observations": [1, 2]}).encode()
    b = json.dumps({"realtime_start": "2026-11-20", "observations": [1, 2]}).encode()
    land_if_changed(tmp_path, "fred", "observations", DAY, a, "json", EARLY, "X", obs)
    assert land_if_changed(tmp_path, "fred", "observations", DAY, b, "json", LATER, "X", obs) is None


def _fake_datasets(monkeypatch, calls, body=b'{"data": [{"x": 1}]}'):
    def fake(name):
        def fetch(session, d):
            calls.append((name, d))
            return body
        return fetch
    monkeypatch.setattr(elexon, "DATASETS",
                        {n: fake(n) for n in ("system_prices", "demand_forecast")})


def test_second_elexon_run_skips_settled_and_unchanged(tmp_path, monkeypatch):
    calls = []
    _fake_datasets(monkeypatch, calls)
    first = cli.run_elexon(cli.Lander(str(tmp_path), LATER), DAY, DAY)
    assert len(first.files) == 2 and len(calls) == 2

    calls.clear()
    second = cli.run_elexon(cli.Lander(str(tmp_path), "20261121T074000000000Z"), DAY, DAY)
    # The forecast publish day is over and was landed after it: not even downloaded.
    assert calls == [("system_prices", DAY)]
    assert second.skipped_settled == 1 and second.unchanged == 1 and not second.files


def test_refetch_downloads_and_lands_everything(tmp_path, monkeypatch):
    calls = []
    _fake_datasets(monkeypatch, calls)
    cli.run_elexon(cli.Lander(str(tmp_path), LATER), DAY, DAY)
    calls.clear()
    again = cli.run_elexon(cli.Lander(str(tmp_path), "20261121T074000000000Z", refetch=True),
                           DAY, DAY)
    assert len(calls) == 2 and len(again.files) == 2


def test_a_run_that_skips_everything_succeeds(tmp_path, monkeypatch):
    calls = []
    _fake_datasets(monkeypatch, calls)
    assert cli.main(["--source=elexon", f"--landing-root={tmp_path}",
                     "--start=2026-11-18", "--end=2026-11-18"]) == 0
    # Same day again: the forecast is settled now (landed after its publish day),
    # system prices are unchanged. Nothing written, and that's a success.
    assert cli.main(["--source=elexon", f"--landing-root={tmp_path}",
                     "--start=2026-11-18", "--end=2026-11-18"]) == 0
    assert len(list(tmp_path.rglob("*.json"))) == 2


def test_refetch_flag_accepts_blank_job_parameter():
    p = cli.build_parser()
    assert p.parse_args(["--source=gas", "--landing-root=x", "--refetch="]).refetch is False
    assert p.parse_args(["--source=gas", "--landing-root=x", "--refetch=true"]).refetch is True
    assert p.parse_args(["--source=gas", "--landing-root=x", "--refetch"]).refetch is True
    assert p.parse_args(["--source=gas", "--landing-root=x"]).refetch is False
