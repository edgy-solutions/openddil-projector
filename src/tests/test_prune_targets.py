"""ADR-0044 lifecycle slice "A" — prune-target ordering and shape.

`main.build_prune_targets` is the pure split-out of `prune_loop`'s target
list so the dependents-before-`telemetry_latest_state` ordering (§1 rule 3 /
§4) can be pinned without asyncio or a live Postgres pool — fabricated
`Mapping` objects in, an ordered tuple list out, same pattern as
`PostgresPool.build_sql`.
"""
from __future__ import annotations

from config import Mapping
from main import NON_ASSET_ID_KEYED_UPSERT_TABLES, build_prune_targets


def _mapping(table: str, mode: str, *, asset_ttl_hours=None,
             retention_hours=None) -> Mapping:
    return Mapping(
        topic=f"{table}-topic",
        handler="h",
        table=table,
        consumer_group="g",
        decode_as="json",
        mode=mode,
        retention_hours=retention_hours,
        asset_ttl_hours=asset_ttl_hours,
    )


def test_append_tables_are_never_asset_id_keyed():
    mappings = [_mapping("tactical_events", "append", retention_hours=720)]
    targets = build_prune_targets(mappings)
    assert targets == [("tactical_events", "time", 720, False)]


def test_upsert_without_ttl_is_excluded():
    """Rollup tables (region_*) omit asset_ttl_hours and must not appear
    in the prune loop's targets at all — they're aggregates, not aged out."""
    mappings = [_mapping("region_fleet_summary", "upsert", asset_ttl_hours=None)]
    assert build_prune_targets(mappings) == []


def test_non_asset_id_keyed_upsert_table_is_flagged_false():
    mappings = [_mapping("inventory_items", "upsert", asset_ttl_hours=24)]
    targets = build_prune_targets(mappings)
    assert targets == [("inventory_items", "updated_at", 24, False)]
    assert "inventory_items" in NON_ASSET_ID_KEYED_UPSERT_TABLES


def test_asset_id_keyed_upsert_table_is_flagged_true():
    mappings = [_mapping("asset_cm_state", "upsert", asset_ttl_hours=24)]
    targets = build_prune_targets(mappings)
    assert targets == [("asset_cm_state", "updated_at", 24, True)]


def test_telemetry_latest_state_pruned_last_among_dependents():
    """The order in the yaml today puts telemetry_latest_state BEFORE
    several dependents (asset_telemetry_windows, asset_capability_state,
    asset_element_telemetry) — this asserts build_prune_targets reorders it
    to run last regardless of input order, so a dependent's EXISTS-join
    always sees the lifecycle row still present within the same pass."""
    mappings = [
        _mapping("asset_cm_state", "upsert", asset_ttl_hours=24),
        _mapping("asset_logistics_status", "upsert", asset_ttl_hours=24),
        _mapping("telemetry_latest_state", "upsert", asset_ttl_hours=24),
        _mapping("tactical_events", "append", retention_hours=720),
        _mapping("asset_telemetry_windows", "upsert", asset_ttl_hours=24),
        _mapping("asset_capability_state", "upsert", asset_ttl_hours=24),
        _mapping("asset_element_telemetry", "upsert", asset_ttl_hours=24),
        _mapping("inventory_items", "upsert", asset_ttl_hours=24),
    ]
    targets = build_prune_targets(mappings)
    tables_in_order = [t[0] for t in targets]
    # telemetry_latest_state is last overall...
    assert tables_in_order[-1] == "telemetry_latest_state"
    # ...and every other asset_id-keyed dependent still precedes it, in
    # their original relative order (stable sort).
    dependents = ["asset_cm_state", "asset_logistics_status",
                  "asset_telemetry_windows", "asset_capability_state",
                  "asset_element_telemetry"]
    assert [t for t in tables_in_order if t in dependents] == dependents
    for dep in dependents:
        assert tables_in_order.index(dep) < tables_in_order.index("telemetry_latest_state")


def test_reporting_status_never_appears_as_a_target_column():
    """reporting_status must never be a prune input (ADR §2) — pinned at the
    shape level: the time column driving eligibility is always "updated_at"
    (upsert) or "time" (append), never reporting_status_at."""
    mappings = [
        _mapping("telemetry_latest_state", "upsert", asset_ttl_hours=24),
        _mapping("tactical_events", "append", retention_hours=720),
    ]
    time_columns = {t[1] for t in build_prune_targets(mappings)}
    assert time_columns == {"updated_at", "time"}
