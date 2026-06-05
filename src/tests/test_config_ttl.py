"""Tests for the asset_ttl_hours config knob.

Pins the env-override precedence + the per-mode gating so a future
refactor doesn't silently drop the long-tail postgres TTL on the
per-asset upsert tables.
"""
from __future__ import annotations

import textwrap

from config import load_config


def _write_yaml(tmp_path, body):
    p = tmp_path / "projector_config.yaml"
    p.write_text(textwrap.dedent(body))
    return p


def test_asset_ttl_hours_from_yaml(tmp_path, monkeypatch):
    """Per-mapping yaml value is honored when no env override is set."""
    monkeypatch.delenv("PROJECTOR_ASSET_TTL_HOURS", raising=False)
    cfg = load_config(_write_yaml(tmp_path, """
        mappings:
          - topic: telemetry-latest-state
            handler: telemetry_latest
            table: telemetry_latest_state
            consumer_group: g1
            decode_as: json
            mode: upsert
            asset_ttl_hours: 24
          - topic: region-fleet-summary
            handler: region_fleet_summary
            table: region_fleet_summary
            consumer_group: g2
            decode_as: json
            mode: upsert
    """))
    by_table = {m.table: m for m in cfg.mappings}
    assert by_table["telemetry_latest_state"].asset_ttl_hours == 24
    # Rollup table intentionally omits asset_ttl_hours (aggregates
    # shouldn't be aged out by asset turnover).
    assert by_table["region_fleet_summary"].asset_ttl_hours is None


def test_env_override_replaces_yaml_value(tmp_path, monkeypatch):
    """PROJECTOR_ASSET_TTL_HOURS=N overrides every upsert mapping's
    asset_ttl_hours to N, regardless of what yaml said."""
    monkeypatch.setenv("PROJECTOR_ASSET_TTL_HOURS", "6")
    cfg = load_config(_write_yaml(tmp_path, """
        mappings:
          - topic: a
            handler: h
            table: a_table
            consumer_group: g
            decode_as: json
            mode: upsert
            asset_ttl_hours: 24
          - topic: b
            handler: h
            table: b_table
            consumer_group: g
            decode_as: json
            mode: upsert
            # asset_ttl_hours omitted -> env should still apply
          - topic: c
            handler: h
            table: c_table
            consumer_group: g
            decode_as: json
            mode: append
            retention_hours: 12
    """))
    by_table = {m.table: m for m in cfg.mappings}
    assert by_table["a_table"].asset_ttl_hours == 6
    assert by_table["b_table"].asset_ttl_hours == 6
    # Append-mode mappings are NOT touched by the asset-TTL override.
    assert by_table["c_table"].asset_ttl_hours is None
    assert by_table["c_table"].retention_hours == 12


def test_env_override_zero_disables_for_all_upsert(tmp_path, monkeypatch):
    """PROJECTOR_ASSET_TTL_HOURS=0 is the escape hatch: every upsert
    mapping's asset_ttl_hours becomes None (no pruning), even when yaml
    sets a positive value."""
    monkeypatch.setenv("PROJECTOR_ASSET_TTL_HOURS", "0")
    cfg = load_config(_write_yaml(tmp_path, """
        mappings:
          - topic: a
            handler: h
            table: a_table
            consumer_group: g
            decode_as: json
            mode: upsert
            asset_ttl_hours: 24
    """))
    assert cfg.mappings[0].asset_ttl_hours is None


def test_env_unset_preserves_yaml(tmp_path, monkeypatch):
    """Empty / unset env var means yaml is source of truth."""
    monkeypatch.setenv("PROJECTOR_ASSET_TTL_HOURS", "")
    cfg = load_config(_write_yaml(tmp_path, """
        mappings:
          - topic: a
            handler: h
            table: a_table
            consumer_group: g
            decode_as: json
            mode: upsert
            asset_ttl_hours: 48
    """))
    assert cfg.mappings[0].asset_ttl_hours == 48
