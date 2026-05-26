"""Handler: asset-logistics-status -> asset_logistics_status.

Decodes openddil.logistics.v1.AssetLogisticsStatusUpdate. Compacted topic,
keyed by asset_id -> UPSERT.

The wire message is an *envelope*: it wraps the AssetLogisticsStatus plus
three envelope flags (previous_severity, is_transition, is_initial). The
projector unwraps it — status fields land in their columns, envelope flags
land alongside so the UI can tell a transition from a cadenced recompute.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import duration_to_seconds, now_utc, parse_timestamp, resolve_origin_or_derive

TABLE = "asset_logistics_status"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    status = decoded.get("status") or {}
    asset_id = status.get("asset_id") or key
    if not asset_id:
        return None

    # DIS path: fusion stamps Provenance.edge_id/region_id from the source
    # event; pass through. Customer path: source event (asset-capability-
    # snapshot) has no edge_id, fusion can't invent one — derive at the
    # projector via the configured strategy (asset_id_prefix / static for
    # positionless customer assets).
    row = {
        "asset_id": asset_id,
        **resolve_origin_or_derive(decoded.get("provenance") or {},
                                      asset_id, "logistics_status"),
        "platform_variant": status.get("platform_variant"),
        "overall_severity": status.get(
            "overall_severity", "LOGISTICS_SEVERITY_UNSPECIFIED"
        ),
        "previous_severity": decoded.get("previous_severity"),
        "constraining_factors": status.get("constraining_factors", []),
        "projected_mission_capable_remaining_seconds": duration_to_seconds(
            status.get("projected_mission_capable_remaining")
        ),
        "projected_time_to_next_constraint_seconds": duration_to_seconds(
            status.get("projected_time_to_next_constraint")
        ),
        "cm_baseline_id": status.get("cm_baseline_id"),
        "status_revision": int(status.get("status_revision", 0) or 0),
        "computed_at": parse_timestamp(status.get("computed_at")),
        "latest_telemetry_sample_time": parse_timestamp(
            status.get("latest_telemetry_sample_time")
        ),
        "is_transition": bool(decoded.get("is_transition", False)),
        "is_initial": bool(decoded.get("is_initial", False)),
        "updated_at": now_utc(),
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={"constraining_factors"},
    )
