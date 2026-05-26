"""Tests for the edge_assignment strategy module.

Test coordinates are deliberately abstract (numbers like (10, 20), not real
places) — real-world FOB lists belong in deployment-specific overlays, not
in the OSS test suite.
"""
from __future__ import annotations

import pytest

from edge_assignment import (
    AssetContext,
    EdgeAssignment,
    Fob,
    asset_id_prefix_strategy,
    build_strategy_from_config,
    chained_strategy,
    configure_from_config,
    extract_wgs84,
    great_circle_km,
    nearest_fob_strategy,
    register_strategy,
    resolve_for,
    static_strategy,
)


# Abstract test FOBs — no geographic significance.
TEST_FOBS = [
    Fob(edge_id="edge-A", region_id="region-N", lat=10.0, lon=20.0, label="A"),
    Fob(edge_id="edge-B", region_id="region-N", lat=10.0, lon=40.0, label="B"),
    Fob(edge_id="edge-C", region_id="region-S", lat=-10.0, lon=20.0, label="C"),
]


# ---- great_circle_km ------------------------------------------------------

def test_great_circle_known_distance():
    # London (51.5074, -0.1278) -> New York (40.7128, -74.0060) ~ 5570 km.
    # Classic textbook geo-distance pair, kept as a sanity check.
    d = great_circle_km(51.5074, -0.1278, 40.7128, -74.0060)
    assert 5550 < d < 5590, f"got {d} km"


def test_great_circle_zero_at_same_point():
    assert great_circle_km(10.0, 10.0, 10.0, 10.0) == pytest.approx(0.0, abs=1e-6)


# ---- nearest_fob_strategy -------------------------------------------------

def test_nearest_fob_picks_closest_in_north_region():
    # Close to edge-A at (10, 20).
    s = nearest_fob_strategy(TEST_FOBS)
    a = s(AssetContext("UNIT-A", lat=10.5, lon=20.5))
    assert a is not None
    assert a.edge_id == "edge-A"
    assert a.region_id == "region-N"
    assert a.derivation_basis["method"] == "nearest_fob"
    assert a.derivation_basis["fob_edge_id"] == "edge-A"


def test_nearest_fob_picks_south_region():
    s = nearest_fob_strategy(TEST_FOBS)
    a = s(AssetContext("UNIT-B", lat=-10.5, lon=20.5))
    assert a is not None and a.edge_id == "edge-C"
    assert a.region_id == "region-S"


def test_nearest_fob_returns_none_without_position():
    s = nearest_fob_strategy(TEST_FOBS)
    assert s(AssetContext("UNIT-X", lat=None, lon=None)) is None
    assert s(AssetContext("UNIT-X", lat=10.0, lon=None)) is None


def test_nearest_fob_returns_none_with_empty_fobs():
    s = nearest_fob_strategy([])
    assert s(AssetContext("UNIT-X", lat=10.0, lon=20.0)) is None


# ---- asset_id_prefix_strategy --------------------------------------------

def test_prefix_longest_match_wins():
    s = asset_id_prefix_strategy({
        "AAA_": ("edge-1", "region-1"),
        "AAA_BBB_": ("edge-2", "region-2"),
    })
    a = s(AssetContext("AAA_BBB_widget"))
    assert a is not None and a.edge_id == "edge-2"
    assert a.derivation_basis["prefix"] == "AAA_BBB_"


def test_prefix_no_match_returns_none():
    s = asset_id_prefix_strategy({"FOO_": ("edge-99", "region-99")})
    assert s(AssetContext("BAR_thing")) is None


# ---- static_strategy ------------------------------------------------------

def test_static_hit_and_miss():
    s = static_strategy({"ASSET_A": ("edge-1", "region-1")})
    a = s(AssetContext("ASSET_A"))
    assert a is not None and a.edge_id == "edge-1"
    assert s(AssetContext("ASSET_Z")) is None


# ---- chained_strategy -----------------------------------------------------

def test_chain_first_non_none_wins():
    nearest = nearest_fob_strategy(TEST_FOBS)
    prefix = asset_id_prefix_strategy({"DEMO_": ("edge-A", "region-N")})
    chain = chained_strategy(nearest, prefix)
    # positionless asset that matches the prefix
    a = chain(AssetContext("DEMO_thing"))
    assert a is not None and a.derivation_basis["method"] == "asset_id_prefix"
    # asset with a position — nearest_fob wins, prefix never runs
    a = chain(AssetContext("DEMO_thing", lat=10.5, lon=20.5))
    assert a is not None and a.derivation_basis["method"] == "nearest_fob"


def test_chain_all_none_returns_none():
    chain = chained_strategy(
        nearest_fob_strategy(TEST_FOBS),  # no position
        asset_id_prefix_strategy({"X_": ("e", "r")}),  # no match
    )
    assert chain(AssetContext("ZZZ")) is None


# ---- build_strategy_from_config ------------------------------------------

def test_build_nearest_fob_from_config():
    s = build_strategy_from_config({
        "strategy": "nearest_fob",
        "fobs": [
            {"edge_id": "e1", "region_id": "r1", "lat": 10.0, "lon": 20.0,
             "label": "L1"},
        ],
    })
    a = s(AssetContext("X", lat=10.1, lon=20.1))
    assert a is not None and a.edge_id == "e1"


def test_build_chain_from_config():
    s = build_strategy_from_config({
        "strategy": "chain",
        "chain": [
            {"strategy": "nearest_fob", "fobs": []},  # always None
            {"strategy": "asset_id_prefix",
             "asset_id_prefix_map": {
                 "DEMO_": {"edge_id": "edge-X", "region_id": "region-X"},
             }},
        ],
    })
    a = s(AssetContext("DEMO_X"))
    assert a is not None and a.edge_id == "edge-X"


def test_build_unknown_strategy_raises():
    with pytest.raises(ValueError, match="unknown edge_assignment strategy"):
        build_strategy_from_config({"strategy": "not-a-real-strategy"})


def test_build_missing_strategy_raises():
    with pytest.raises(ValueError, match="missing `strategy:`"):
        build_strategy_from_config({})


# ---- register_strategy (external pluggability) ---------------------------

def test_register_strategy_allows_external_builder():
    @register_strategy("__test_custom__")
    def _build(_cfg: dict):
        return lambda _ctx: EdgeAssignment(
            edge_id="custom-edge", region_id="custom-region",
            derivation_basis={"method": "__test_custom__"},
        )

    s = build_strategy_from_config({"strategy": "__test_custom__"})
    a = s(AssetContext("anything"))
    assert a is not None and a.edge_id == "custom-edge"


# ---- configure_from_config + resolve_for ---------------------------------

def test_resolve_for_uses_strategy_then_fallback():
    configure_from_config({
        "strategy": "chain",
        "chain": [
            {"strategy": "nearest_fob",
             "fobs": [
                 {"edge_id": "edge-A", "region_id": "region-N",
                  "lat": 10.0, "lon": 20.0, "label": "A"},
             ]},
            {"strategy": "asset_id_prefix",
             "asset_id_prefix_map": {
                 "DEMO_": {"edge_id": "edge-A", "region_id": "region-N"},
             }},
        ],
        "fallback": {"edge_id": "edge-unspecified",
                      "region_id": "region-unspecified"},
    })

    # Position present -> nearest_fob.
    a = resolve_for("UNIT-A", lat=10.5, lon=20.5, handler_label="test")
    assert a.edge_id == "edge-A"
    assert a.derivation_basis["method"] == "nearest_fob"

    # No position, prefix matches.
    a = resolve_for("DEMO_widget", lat=None, lon=None, handler_label="test")
    assert a.edge_id == "edge-A"
    assert a.derivation_basis["method"] == "asset_id_prefix"

    # No position, no prefix match -> fallback.
    a = resolve_for("RANDOM_X", lat=None, lon=None, handler_label="test")
    assert a.edge_id == "edge-unspecified"
    assert a.region_id == "region-unspecified"
    assert a.derivation_basis["method"] == "fallback"


def test_configure_from_empty_config_installs_noop():
    configure_from_config({})
    a = resolve_for("ANY", lat=None, lon=None, handler_label="test")
    assert a.edge_id == "edge-unspecified"
    assert a.derivation_basis["method"] == "fallback"


# ---- extract_wgs84 -------------------------------------------------------

def test_extract_wgs84_camel_case():
    lat, lon = extract_wgs84({
        "position": {"wgs84": {"latitude": 10.0, "longitude": 20.5}},
    })
    assert lat == 10.0 and lon == 20.5


def test_extract_wgs84_short_keys():
    lat, lon = extract_wgs84({
        "position": {"wgs84": {"lat": 10.0, "lon": 20.5}},
    })
    assert lat == 10.0 and lon == 20.5


def test_extract_wgs84_missing_returns_none():
    assert extract_wgs84(None) == (None, None)
    assert extract_wgs84({}) == (None, None)
    assert extract_wgs84({"position": {}}) == (None, None)
    assert extract_wgs84({"position": {"wgs84": {}}}) == (None, None)


def test_extract_wgs84_bad_type_returns_none():
    assert extract_wgs84({"position": {"wgs84": {"latitude": "not-a-number",
                                                   "longitude": 20.5}}}) == (None, None)


# ---- EDGE_ASSIGNMENT_CONFIG env-var override (config.load_config) --------

def test_env_override_replaces_main_edge_assignment(tmp_path, monkeypatch):
    """An overlay-supplied file pointed at by EDGE_ASSIGNMENT_CONFIG fully
    replaces the main config's edge_assignment block — that is the
    mechanism deployment overlays use to inject their own FOBs."""
    from config import load_config

    main = tmp_path / "projector_config.yaml"
    main.write_text(
        "mappings: []\n"
        "edge_assignment:\n"
        "  strategy: chain\n"
        "  chain: []\n"
        "  fallback: {edge_id: edge-unspecified, region_id: region-unspecified}\n"
    )
    override = tmp_path / "edge-assignment.yaml"
    override.write_text(
        "strategy: asset_id_prefix\n"
        "asset_id_prefix_map:\n"
        "  TEST_:\n"
        "    edge_id: edge-X\n"
        "    region_id: region-X\n"
    )
    monkeypatch.setenv("EDGE_ASSIGNMENT_CONFIG", str(override))

    cfg = load_config(main)
    assert cfg.edge_assignment["strategy"] == "asset_id_prefix"
    assert "TEST_" in cfg.edge_assignment["asset_id_prefix_map"]


def test_env_override_accepts_wrapper_shape(tmp_path, monkeypatch):
    """Override file may either use the bare top-level structure or wrap
    it under `edge_assignment:` — both are accepted."""
    from config import load_config

    main = tmp_path / "projector_config.yaml"
    main.write_text("mappings: []\n")
    override = tmp_path / "edge-assignment.yaml"
    override.write_text(
        "edge_assignment:\n"
        "  strategy: static\n"
        "  static_map:\n"
        "    ASSET_A: {edge_id: edge-1, region_id: region-1}\n"
    )
    monkeypatch.setenv("EDGE_ASSIGNMENT_CONFIG", str(override))

    cfg = load_config(main)
    assert cfg.edge_assignment["strategy"] == "static"
    assert cfg.edge_assignment["static_map"]["ASSET_A"]["edge_id"] == "edge-1"


def test_env_override_missing_file_is_silently_ignored(tmp_path, monkeypatch):
    """A nonexistent override path falls back to the main config's block —
    a misconfigured deployment shouldn't crash the projector on start."""
    from config import load_config

    main = tmp_path / "projector_config.yaml"
    main.write_text(
        "mappings: []\n"
        "edge_assignment:\n"
        "  strategy: chain\n"
        "  chain: []\n"
    )
    monkeypatch.setenv("EDGE_ASSIGNMENT_CONFIG",
                        str(tmp_path / "does-not-exist.yaml"))

    cfg = load_config(main)
    assert cfg.edge_assignment["strategy"] == "chain"
