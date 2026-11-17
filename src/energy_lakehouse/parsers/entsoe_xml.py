"""Parse ENTSO-E A44 (day-ahead price) XML into flat rows.

Why this isn't trivial:
* XML namespaces change between document versions, so we match on local tag names.
* Nesting runs TimeSeries -> Period -> Point, and a Point has a *position*, not a timestamp.
  We rebuild timestamps from Period.timeInterval.start + (position - 1) * resolution.
* Resolution varies: PT60M historically, PT15M since the EU day-ahead market moved to
  15-minute products, and some zones publish both.
* curveType A03 ("variable sized block") leaves out points whose value equals the previous
  one. We forward-fill them so every interval has a row. A01 lists every point explicitly.
* "No data" comes back as an Acknowledgement_MarketDocument, which gives zero rows.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import timedelta

from energy_lakehouse.timeutils import parse_iso_utc

_RES = re.compile(r"^P(?:T(?P<num>\d+)(?P<unit>[MH])|(?P<days>\d+)D)$")


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child(el: ET.Element, name: str) -> ET.Element | None:
    for c in el:
        if _local(c.tag) == name:
            return c
    return None


def _children(el: ET.Element, name: str) -> list[ET.Element]:
    return [c for c in el if _local(c.tag) == name]


def _text(el: ET.Element, path: str) -> str | None:
    """Follow a '/'-separated path of local names and return the text."""
    cur: ET.Element | None = el
    for part in path.split("/"):
        if cur is None:
            return None
        cur = _child(cur, part)
    return cur.text.strip() if cur is not None and cur.text else None


def parse_resolution(value: str) -> timedelta:
    m = _RES.match(value.strip())
    if not m:
        raise ValueError(f"Unsupported resolution: {value!r}")
    if m.group("days"):
        return timedelta(days=int(m.group("days")))
    n = int(m.group("num"))
    return timedelta(minutes=n) if m.group("unit") == "M" else timedelta(hours=n)


def is_acknowledgement(xml_bytes: bytes) -> bool:
    root = ET.fromstring(xml_bytes)
    return _local(root.tag) == "Acknowledgement_MarketDocument"


def acknowledgement_reason(xml_bytes: bytes) -> str | None:
    root = ET.fromstring(xml_bytes)
    if _local(root.tag) != "Acknowledgement_MarketDocument":
        return None
    reason = _child(root, "Reason")
    return _text(reason, "text") if reason is not None else None


def parse_day_ahead(xml_bytes: bytes, zone: str) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    if _local(root.tag) == "Acknowledgement_MarketDocument":
        return []
    if _local(root.tag) != "Publication_MarketDocument":
        raise ValueError(f"Unexpected root element: {_local(root.tag)}")

    doc_mrid = _text(root, "mRID")
    rows: list[dict] = []

    for ts in _children(root, "TimeSeries"):
        series_mrid = _text(ts, "mRID")
        eic = _text(ts, "in_Domain.mRID")
        currency = _text(ts, "currency_Unit.name")
        unit = _text(ts, "price_Measure_Unit.name")
        curve_type = _text(ts, "curveType") or "A01"
        seq = _text(ts, "classificationSequence_AttributeInstanceComponent.position")

        for period in _children(ts, "Period"):
            start = parse_iso_utc(_text(period, "timeInterval/start"))
            end = parse_iso_utc(_text(period, "timeInterval/end"))
            res = parse_resolution(_text(period, "resolution"))
            n_slots = int((end - start) / res)

            points: dict[int, float] = {}
            for p in _children(period, "Point"):
                pos = int(_text(p, "position"))
                price = _text(p, "price.amount")
                if price is not None:
                    points[pos] = float(price)

            if curve_type == "A03":
                positions = range(1, n_slots + 1)
            else:
                positions = sorted(points)

            last: float | None = None
            for pos in positions:
                if pos in points:
                    last = points[pos]
                    filled = False
                elif last is not None:
                    filled = True
                else:
                    continue  # nothing to carry forward yet
                slot_start = start + (pos - 1) * res
                rows.append(
                    {
                        "zone": zone,
                        "zone_eic": eic,
                        "delivery_start_utc": slot_start.isoformat(),
                        "delivery_end_utc": (slot_start + res).isoformat(),
                        "resolution_minutes": int(res.total_seconds() // 60),
                        "position": pos,
                        "price": last,
                        "is_forward_filled": filled,
                        "currency": currency,
                        "price_unit": unit,
                        "curve_type": curve_type,
                        "classification_sequence": int(seq) if seq else None,
                        "series_mrid": series_mrid,
                        "document_mrid": doc_mrid,
                    }
                )
    return rows
