"""Unit tests for the region_fleet_summary handler — ADR-0044 §3 terminal
operational-status partitions (destroyed/deactivated/removed).

Same style as test_handlers.py: pure (kafka_key, decoded_dict) -> Write,
no live infra.
"""
from __future__ import annotations

from handlers import get_handler


def _decoded(**overrides):
    base = {
        "region_id": "region-east",
        "nominal": 10,
        "degraded": 2,
        "critical": 1,
        "non_operational": 0,
        "asset_count": 14,
        "observed_at": "2026-05-14T03:00:00Z",
    }
    base.update(overrides)
    return base


def test_persists_destroyed_deactivated_removed():
    decoded = _decoded(destroyed=3, deactivated=1, removed=2, asset_count=19)
    write = get_handler("region_fleet_summary")("region-east", decoded)
    assert write is not None
    assert write.table == "region_fleet_summary"
    assert write.row["destroyed"] == 3
    assert write.row["deactivated"] == 1
    assert write.row["removed"] == 2
    assert write.row["asset_count"] == 19


def test_defaults_to_zero_when_absent():
    decoded = _decoded()  # no destroyed/deactivated/removed keys at all
    write = get_handler("region_fleet_summary")("region-east", decoded)
    assert write is not None
    assert write.row["destroyed"] == 0
    assert write.row["deactivated"] == 0
    assert write.row["removed"] == 0
