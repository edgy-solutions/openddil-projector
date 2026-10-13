"""Root projector config: the parts-availability mapping, and the effector
tables that must never be pruned."""
from __future__ import annotations

from pathlib import Path

from config import load_config
from handlers import get_handler
from main import build_prune_targets

_ROOT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "projector_config.yaml"


def test_root_config_has_parts_availability_mapping():
    cfg = load_config(_ROOT_CONFIG)
    m = cfg.mapping_by_topic()["parts-availability"]
    assert m.handler == "parts_availability"
    assert m.table == "parts_availability"
    assert m.mode == "upsert"
    assert m.decode_as == "json"
    assert m.asset_ttl_hours is None
    assert callable(get_handler(m.handler))


def test_prune_targets_exclude_effector_and_parts_tables():
    cfg = load_config(_ROOT_CONFIG)
    tables = {t[0] for t in build_prune_targets(cfg.mappings)}
    assert "effector_resupply" not in tables
    assert "effector_launch" not in tables
    assert "parts_availability" not in tables
