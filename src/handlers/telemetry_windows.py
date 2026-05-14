"""Handler: asset-telemetry-windows -> asset_telemetry_windows.

Decodes openddil.logistics.v1.WindowedTelemetry. Compacted topic, keyed by
asset_id -> UPSERT.

Trend collections are stored as JSONB verbatim. Note the proto field name
is `wear_trends`; the column is `component_wear_trends` (the more explicit
name) — this handler is the rename boundary. `window` (WindowSpec) is
unpacked into the flat window_duration_seconds / sample_count columns.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import duration_to_seconds, now_utc, origin_provenance, parse_timestamp

TABLE = "asset_telemetry_windows"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset_id = decoded.get("asset_id") or key
    if not asset_id:
        return None

    window = decoded.get("window") or {}
    # WindowSpec carries the duration as a proto Duration and the sample
    # count as an int; field names are best-effort tolerant.
    duration_seconds = duration_to_seconds(
        window.get("duration") or window.get("window_duration")
    )
    sample_count = window.get("sample_count")

    # fluid_trends is a proto map<string, ScalarTrend> -> JSON object.
    # consumable_trends / wear_trends are repeated -> JSON arrays. All three
    # are stored verbatim.
    row = {
        "asset_id": asset_id,
        **origin_provenance(),
        "platform_variant": decoded.get("platform_variant"),
        "fluid_trends": decoded.get("fluid_trends", []),
        "consumable_trends": decoded.get("consumable_trends", []),
        "component_wear_trends": decoded.get("wear_trends", []),
        "window_duration_seconds": duration_seconds,
        "sample_count": int(sample_count) if sample_count is not None else None,
        "computed_at": parse_timestamp(decoded.get("computed_at")),
        "updated_at": now_utc(),
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={
            "fluid_trends",
            "consumable_trends",
            "component_wear_trends",
        },
    )
