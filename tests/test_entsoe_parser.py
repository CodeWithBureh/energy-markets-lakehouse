from datetime import date, timedelta
from pathlib import Path

import pytest

from energy_lakehouse.parsers.entsoe_xml import (
    acknowledgement_reason,
    parse_day_ahead,
    parse_resolution,
)
from energy_lakehouse.sources.entsoe import delivery_day_bounds

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def rows():
    return parse_day_ahead((FIXTURES / "entsoe_a44_sample.xml").read_bytes(), "FR")


def test_a03_curve_is_forward_filled(rows):
    hourly = [r for r in rows if r["resolution_minutes"] == 60]
    assert [r["position"] for r in hourly] == [1, 2, 3, 4]
    assert [r["price"] for r in hourly] == [80.5, 80.5, 72.0, 72.0]
    assert [r["is_forward_filled"] for r in hourly] == [False, True, False, True]
    assert hourly[0]["delivery_start_utc"] == "2026-11-11T23:00:00+00:00"
    assert hourly[3]["delivery_end_utc"] == "2026-11-12T03:00:00+00:00"


def test_a01_quarter_hourly_points(rows):
    q = [r for r in rows if r["resolution_minutes"] == 15]
    assert len(q) == 4
    assert q[2]["price"] == -5.25          # negative prices are real; keep them
    assert q[1]["delivery_start_utc"] == "2026-11-11T23:15:00+00:00"
    assert q[0]["classification_sequence"] == 1


def test_metadata(rows):
    r = rows[0]
    assert r["zone"] == "FR"
    assert r["zone_eic"] == "10YFR-RTE------C"
    assert r["currency"] == "EUR"
    assert r["document_mrid"] == "test-doc-001"


def test_acknowledgement_means_no_rows():
    xml = (FIXTURES / "entsoe_ack_no_data.xml").read_bytes()
    assert parse_day_ahead(xml, "FR") == []
    assert "No matching data" in acknowledgement_reason(xml)


@pytest.mark.parametrize("value, minutes", [("PT15M", 15), ("PT60M", 60), ("PT1H", 60), ("P1D", 1440)])
def test_parse_resolution(value, minutes):
    assert parse_resolution(value) == timedelta(minutes=minutes)


def test_delivery_day_bounds_follow_cet_cest():
    assert delivery_day_bounds(date(2026, 11, 12)) == ("202611112300", "202611122300")
    assert delivery_day_bounds(date(2026, 7, 1)) == ("202606302200", "202607012200")
    # Autumn change: 25-hour delivery day
    assert delivery_day_bounds(date(2026, 10, 25)) == ("202610242200", "202610252300")
