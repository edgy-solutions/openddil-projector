"""Handler: asset-element-inventory -> inventory_items.

Drives the maintainer view's "Local FOB Inventory" card from the
same per-element signal that drives the 3D drill-down's tile colors.

Per-tick the sim (openddil-logistics-sim) emits one envelope per
(asset_id, layer_name) on the asset-element-inventory topic. This
handler upserts one inventory_items row per envelope, keyed by a
deterministic id of shape `<asset_id>:<layer_name>` so the same
(asset, layer) always lands on the same row -- repeated emits update
in place, no row growth, no duplicates. Topic isn't compacted (Kafka
broker-side), but the upsert-by-id pattern makes that immaterial.

Envelope shape (per openddil-logistics-sim publisher.HqProducer.
publish_inventory, 2026-06-30):

  {
    "asset_id":         "<asset_id>",
    "layer_name":       "T/R MODULE",
    "platform_variant": "<variant>",
    "available_count":  91,
    "allocated_count":  5,
    "total_count":      96,
    "observed_at_ns":   <int>
  }

Origin provenance: same as asset_element_telemetry -- the sim runs at
HQ and emits no edge_id of its own; the projector backfills via
resolve_origin_or_derive so the row carries the asset's home edge for
attribution. inventory_items doesn't currently have edge_id columns
(it predates the origin-node provenance refactor), so prov isn't
spread into the row today; the lookup still happens to keep the
fallback-rate warning rate-limited via the shared throttle.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import now_utc, resolve_origin_or_derive

TABLE = "inventory_items"
HANDLER_LABEL = "asset_element_inventory"


def _row_id(asset_id: str, layer_name: str) -> str:
    """Deterministic per-(asset, layer) row id. Format chosen so a
    human running `psql -c "SELECT id FROM inventory_items ..."`
    immediately sees which asset + layer the row belongs to."""
    return f"{asset_id}:{layer_name}"


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset_id = (decoded.get("asset_id") or "").strip()
    layer_name = (decoded.get("layer_name") or "").strip()
    if not asset_id or not layer_name:
        return None

    # Touch the provenance resolver so the rate-limited fallback warning
    # fires consistently across sim-sourced handlers, even though
    # inventory_items doesn't have edge/region columns to spread into.
    resolve_origin_or_derive(None, asset_id, HANDLER_LABEL)

    available = _coerce_int(decoded.get("available_count"))
    allocated = _coerce_int(decoded.get("allocated_count"))

    row = {
        "id": _row_id(asset_id, layer_name),
        "name": layer_name,
        "asset_id": asset_id,
        "layer_name": layer_name,
        "available_count": available,
        "allocated_count": allocated,
        "updated_at": now_utc(),
    }
    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["id"],
        row=row,
    )
