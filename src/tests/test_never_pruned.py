"""Every never-pruned table is protected in code, not by the yaml (S7 change C).

The three guards must hold for every member of config.NEVER_PRUNED_TABLES:
a yaml TTL raises at load, the PROJECTOR_ASSET_TTL_HOURS override skips it,
and build_prune_targets never names it. Against the REAL projector_config.yaml.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from config import NEVER_PRUNED_TABLES, Mapping, load_config
from main import build_prune_targets

CONFIG_YAML = Path(__file__).parent.parent / "config" / "projector_config.yaml"

EXPECTED = {
    "failure_episodes", "failure_episodes_withheld", "effector_launch",
    "effector_resupply", "munition_track", "asset_declaration",
    "asset_correlation", "asset_fielding",
}


def test_the_set_is_exactly_the_durable_records():
    assert NEVER_PRUNED_TABLES == EXPECTED
    # refreshed tables, not records
    for t in ("parts_availability", "region_fleet_summary",
              "region_top_factors", "region_wear_trends"):
        assert t not in NEVER_PRUNED_TABLES


def test_real_yaml_has_no_ttl_on_a_never_pruned_table(monkeypatch):
    monkeypatch.setenv("PROJECTOR_ASSET_TTL_HOURS", "1")
    cfg = load_config(CONFIG_YAML)
    for m in cfg.mappings:
        if m.table in NEVER_PRUNED_TABLES:
            assert m.asset_ttl_hours is None, m.table
    # the env override still reaches an ordinary upsert mapping
    assert any(m.asset_ttl_hours == 1 for m in cfg.mappings)


def test_build_prune_targets_names_none_of_them(monkeypatch):
    monkeypatch.setenv("PROJECTOR_ASSET_TTL_HOURS", "1")
    cfg = load_config(CONFIG_YAML)
    named = {t[0] for t in build_prune_targets(cfg.mappings)}
    assert not (named & NEVER_PRUNED_TABLES)
    assert named  # the check is not vacuous: ordinary tables are pruned


@pytest.mark.parametrize("table", sorted(NEVER_PRUNED_TABLES))
@pytest.mark.parametrize("mode", ["upsert", "custom", "append"])
def test_build_prune_targets_excludes_a_handbuilt_ttl(table, mode):
    m = Mapping(topic="t", handler="h", table=table, consumer_group="g",
                decode_as="json", mode=mode, asset_ttl_hours=24,
                retention_hours=24)
    assert build_prune_targets([m]) == []


def test_every_yaml_table_in_the_set_refuses_a_planted_ttl(tmp_path):
    raw = yaml.safe_load(CONFIG_YAML.read_bytes())
    in_yaml = {m["table"] for m in raw["mappings"]} & NEVER_PRUNED_TABLES
    assert in_yaml, "no never-pruned table in the yaml: nothing was checked"
    for table in sorted(in_yaml):
        planted = yaml.safe_load(CONFIG_YAML.read_bytes())
        for m in planted["mappings"]:
            if m["table"] == table:
                m["asset_ttl_hours"] = 24
        p = tmp_path / f"{table}.yaml"
        p.write_text(yaml.safe_dump(planted))
        with pytest.raises(ValueError, match="never pruned"):
            load_config(p)
