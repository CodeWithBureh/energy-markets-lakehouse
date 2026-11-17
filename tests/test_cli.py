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
