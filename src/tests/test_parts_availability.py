"""Unit tests for the parts_availability handler (pure: record -> Write)."""
from __future__ import annotations

from datetime import datetime, timezone

from handlers import get_handler, parts_availability
from persistence import Write

_NS = 1_760_000_000_123_000_000
_FIGURES = ("on_hand", "lead_time_days", "nearest_site_with_stock",
            "nearest_on_hand", "source")


def _record(**over):
    rec = {
        "site": "site-a",
        "part_ref": "PN-100",
        "item": "widget",
        "on_hand": 5,
        "lead_time_days": 2.5,
        "nearest_site_with_stock": "site-b",
        "nearest_on_hand": 9,
        "source": "stand-in",
        "observed_at_ns": _NS,
        "provenance": {"originator_nation": "AAA", "releasable_to": []},
        "extraction": {"cursor": f"sweep-{_NS}", "extracted_at_ns": _NS},
    }
    rec.update(over)
    return rec


def test_registered():
    assert get_handler("parts_availability") is parts_availability.handle


def test_full_record_upsert_write():
    w = parts_availability.handle("site-a:PN-100", _record())
    assert isinstance(w, Write)
    assert w.table == "parts_availability"
    assert w.mode == "upsert"
    assert w.key_columns == ["site", "part_ref"]
    row = dict(w.row)
    updated = row.pop("updated_at")
    assert updated.tzinfo is not None
    assert row == {
        "site": "site-a", "part_ref": "PN-100", "on_hand": 5,
        "lead_time_days": 2.5, "nearest_site_with_stock": "site-b",
        "nearest_on_hand": 9, "source": "stand-in",
        "observed_at": datetime.fromtimestamp(_NS // 10**9, tz=timezone.utc).replace(
            microsecond=(_NS % 10**9) // 1000),
        "originator_nation": "AAA", "releasable_to": [],
    }


def test_absent_figures_written_as_none():
    rec = _record()
    for k in _FIGURES:
        del rec[k]
    w = parts_availability.handle("k", rec)
    for k in _FIGURES:
        assert k in w.row and w.row[k] is None


def test_lead_time_and_nearest_on_hand_absent_are_none():
    rec = _record()
    del rec["lead_time_days"]
    del rec["nearest_on_hand"]
    w = parts_availability.handle("k", rec)
    assert w.row["lead_time_days"] is None
    assert w.row["nearest_on_hand"] is None
    assert w.row["on_hand"] == 5


def test_unlabelled_has_no_label_keys():
    w = parts_availability.handle("k", _record(provenance={}))
    assert "originator_nation" not in w.row and "releasable_to" not in w.row


def test_empty_releasable_to_stays_empty_list():
    w = parts_availability.handle("k", _record())
    assert w.row["releasable_to"] == []


def test_missing_required_fields_refused():
    assert parts_availability.handle("k", _record(site="")) is None
    assert parts_availability.handle("k", _record(part_ref=None)) is None
    rec = _record()
    del rec["observed_at_ns"]
    assert parts_availability.handle("k", rec) is None
    assert parts_availability.handle("k", _record(observed_at_ns="x")) is None
    assert parts_availability.handle("k", _record(observed_at_ns=True)) is None
