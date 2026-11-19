from datetime import date

from energy_lakehouse.cli import build_parser, default_window, main


def test_default_windows_overlap_for_revisions():
    today = date(2026, 11, 12)
    assert default_window("elexon", today) == (date(2026, 11, 9), date(2026, 11, 11))
    assert default_window("entsoe", today) == (date(2026, 11, 11), date(2026, 11, 13))
    start, end = default_window("era5", today)
    assert end == date(2026, 11, 6) and (end - start).days == 6


def test_cost_guard_blocks_huge_backfill(tmp_path):
    code = main(["--source", "elexon", "--landing-root", str(tmp_path),
                 "--start", "2025-01-01", "--end", "2025-12-31"])
    assert code == 2
    assert not any(tmp_path.iterdir())


def test_blank_job_parameters_fall_back_to_default_window():
    args = build_parser().parse_args(["--source", "elexon", "--landing-root", "x",
                                      "--start=", "--end="])
    assert args.start is None and args.end is None
    args = build_parser().parse_args(["--source", "elexon", "--landing-root", "x",
                                      "--start=2026-10-01", "--end=2026-10-31"])
    assert args.start == date(2026, 10, 1) and args.end == date(2026, 10, 31)


def test_entsoe_fetches_once_per_zone_and_lands_one_file_per_day(tmp_path, monkeypatch, caplog):
    from pathlib import Path

    from energy_lakehouse import cli, config
    from energy_lakehouse.sources import entsoe

    xml = (Path(__file__).parent / "fixtures" / "entsoe_a44_sample.xml").read_bytes()
    calls = []

    def fake_fetch(session, token, eic, first_day, last_day):
        calls.append((eic, first_day, last_day))
        return xml

    monkeypatch.setattr(entsoe, "fetch_day_ahead_prices", fake_fetch)
    monkeypatch.setattr(cli, "get_secret", lambda source, scope: "token")
    monkeypatch.setattr(config, "ENTSOE_ZONES", {"FR": "10YFR-RTE------C"})

    # The fixture covers delivery day 12 Nov (CET), which starts at 23:00 UTC on the 11th.
    rep = cli.run_entsoe(str(tmp_path), date(2026, 11, 11), date(2026, 11, 12), "ts", None)

    assert calls == [("10YFR-RTE------C", date(2026, 11, 11), date(2026, 11, 12))]
    assert [Path(f).parent.name for f in rep.files] == ["12"]   # .../2026/11/12/
    assert not rep.errors
    assert "ENTSO-E FR 2026-11-11: missing from the response" in caplog.text


def test_era5_end_is_clipped_to_the_publication_lag(tmp_path, monkeypatch):
    from energy_lakehouse import cli

    seen = []
    monkeypatch.setattr(cli, "today_london", lambda: date(2026, 11, 19))
    monkeypatch.setattr(cli, "run_era5", lambda root, start, end, ts, scope:
                        seen.append((start, end)) or cli.RunReport("era5"))

    # Backfill of the current month: stop 6 days back instead of asking for unpublished days.
    assert main(["--source=era5", f"--landing-root={tmp_path}",
                 "--start=2026-11-01", "--end=2026-11-18"]) == 0
    assert seen == [(date(2026, 11, 1), date(2026, 11, 13))]

    # Nothing published yet for the window: succeed without calling CDS.
    assert main(["--source=era5", f"--landing-root={tmp_path}",
                 "--start=2026-11-15", "--end=2026-11-18"]) == 0
    assert len(seen) == 1