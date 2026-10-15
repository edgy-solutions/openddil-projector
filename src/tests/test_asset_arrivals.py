"""Unit tests for the ADR-0050 handlers: asset_declaration, asset_correlation,
asset_fielding (pure: record -> Write), and their config mappings."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from config import load_config
from handlers import asset_correlation, asset_declaration, asset_fielding, get_handler
from main import build_prune_targets
from persistence import Write

_ROOT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "projector_config.yaml"


def _decl(**over):
    rec = {
        "asset_id": "dis:1:1:42", "serial": "SN-0001", "unit": "unit-a",
        "originator_nation": "AAA", "releasable_to": ["BBB"],
        "baseline_id": "bl-1", "declared_by": "operator-1",
        "declared_at": "2026-10-15T12:00:00Z", "source": "declare",
        "edge_id": "edge-01", "region_id": "region-1", "owning_tier": "edge",
    }
    rec.update(over)
    return rec


def _field(**over):
    rec = {
        "serial": "SN-0001", "platform_variant": "variant-x", "unit": "unit-a",
        "owning_nation": "AAA", "releasable_to": ["BBB"],
        "fielded_to": "edge-01", "region_id": None,
        "due_by": "2026-11-01T00:00:00Z", "recorded_at": "2026-10-15T12:00:00Z",
    }
    rec.update(over)
    return rec


def test_registered():
    assert get_handler("asset_declaration") is asset_declaration.handle
    assert get_handler("asset_correlation") is asset_correlation.handle
    assert get_handler("asset_fielding") is asset_fielding.handle


# -- asset_declaration -------------------------------------------------------

def test_declaration_good_record():
    w = asset_declaration.handle("dis:1:1:42", _decl())
    assert isinstance(w, Write)
    assert (w.table, w.mode, w.key_columns) == (
        "asset_declaration", "upsert", ["asset_id"])
    r = w.row
    assert r["serial"] == "SN-0001" and r["baseline_id"] == "bl-1"
    assert r["releasable_to"] == ["BBB"]
    assert r["declared_at"] == datetime(2026, 10, 15, 12, tzinfo=timezone.utc)
    assert r["owning_tier"] == "edge" and r["source"] == "declare"


@pytest.mark.parametrize("val", [None, "", "none", "None"])
def test_declaration_serial_and_baseline_unset_become_null(val):
    r = asset_declaration.handle("k", _decl(serial=val, baseline_id=val)).row
    assert r["serial"] is None and r["baseline_id"] is None


def test_declaration_absent_serial_is_null_and_empty_release_defaults():
    rec = _decl()
    del rec["serial"]
    del rec["releasable_to"]
    r = asset_declaration.handle("k", rec).row
    assert r["serial"] is None and r["releasable_to"] == []


@pytest.mark.parametrize("field", [
    "asset_id", "unit", "originator_nation", "declared_by", "declared_at",
    "source", "edge_id", "owning_tier"])
def test_declaration_missing_required_is_skipped(field):
    rec = _decl()
    del rec[field]
    assert asset_declaration.handle("k", rec) is None


def test_declaration_bad_source_skipped():
    assert asset_declaration.handle("k", _decl(source="other")) is None


# -- asset_correlation -------------------------------------------------------

def test_correlation_good_record():
    w = asset_correlation.handle("dis:1:1:42", _decl())
    assert isinstance(w, Write)
    assert (w.table, w.mode, w.key_columns) == (
        "asset_correlation", "upsert", ["serial"])
    assert w.row["serial"] == "SN-0001"
    assert w.row["asset_id"] == "dis:1:1:42"
    assert w.row["edge_id"] == "edge-01"
    assert "unit" not in w.row


@pytest.mark.parametrize("val", [None, "", "none", "NONE"])
def test_correlation_no_serial_returns_none(val):
    assert asset_correlation.handle("k", _decl(serial=val)) is None


def test_correlation_absent_serial_returns_none():
    rec = _decl()
    del rec["serial"]
    assert asset_correlation.handle("k", rec) is None


def test_correlation_missing_required_is_skipped():
    rec = _decl()
    del rec["edge_id"]
    assert asset_correlation.handle("k", rec) is None


# -- asset_fielding ----------------------------------------------------------

def test_fielding_good_record_maps_owning_nation():
    w = asset_fielding.handle("SN-0001", _field())
    assert isinstance(w, Write)
    assert (w.table, w.mode, w.key_columns) == (
        "asset_fielding", "upsert", ["serial"])
    r = w.row
    assert r["originator_nation"] == "AAA"
    assert "owning_nation" not in r
    assert r["fielded_to"] == "edge-01" and r["region_id"] is None
    assert r["due_by"] == datetime(2026, 11, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("val", [None, "", "none"])
def test_fielding_no_serial_is_skipped(val):
    assert asset_fielding.handle("k", _field(serial=val)) is None


@pytest.mark.parametrize("field", [
    "platform_variant", "unit", "owning_nation", "fielded_to", "due_by",
    "recorded_at"])
def test_fielding_missing_required_is_skipped(field):
    rec = _field()
    del rec[field]
    assert asset_fielding.handle("k", rec) is None


# -- config ------------------------------------------------------------------

def test_config_mappings_and_never_pruned():
    cfg = load_config(_ROOT_CONFIG)
    by_group = {m.consumer_group: m for m in cfg.mappings}
    expect = {
        "projector-asset-declaration": ("asset-declarations", "asset_declaration"),
        "projector-asset-correlation": ("asset-declarations", "asset_correlation"),
        "projector-asset-fielding": ("asset-fielding", "asset_fielding"),
    }
    for group, (topic, name) in expect.items():
        m = by_group[group]
        assert (m.topic, m.handler, m.table) == (topic, name, name)
        assert m.decode_as == "json" and m.mode == "upsert"
        assert m.asset_ttl_hours is None
        assert callable(get_handler(m.handler))
    tables = {t[0] for t in build_prune_targets(cfg.mappings)}
    assert not tables & {"asset_declaration", "asset_correlation",
                         "asset_fielding"}
