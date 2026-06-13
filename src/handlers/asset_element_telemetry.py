"""Handler: asset-element-telemetry -> asset_element_telemetry.

Decodes the openddil-logistics-sim per-asset envelope:
  {
    "asset_id": "<asset_id>",
    "platform_variant": "<variant>",
    "profile_name": "<asset_profiles[].name>",
    "observed_at_ns": <int>,
    "operational": {
      "power_state": "...", "health_state": "...",
      "actively_transmitting": ..., "actively_receiving": ...,
      "degraded": ...
    },
    "elements": [
      {"element_id": ..., "layer_depth": ..., "layer_name": ...,
       "health": ..., "temp_c": ..., "load_pct": ...,
       "tx_active": ..., "rx_active": ...},
      ...
    ]
  }

Compacted topic keyed by asset_id -> UPSERT. elements + operational
stored verbatim as JSONB so the SensorArrayView can build its per-
element lookup AND show the asset-level status banner without
re-deriving anything.

Origin-node provenance: logistics-sim runs on HQ and emits no edge_id
of its own. The projector backfills via resolve_origin_or_derive(
provenance=None, asset_id, ...) -- same path capability_state uses
for strike-only launchers that have no position-bearing telemetry.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from persistence import Write

from .base import now_utc, resolve_origin_or_derive

TABLE = "asset_element_telemetry"
HANDLER_LABEL = "asset_element_telemetry"


def _observed_at_from_ns(observed_at_ns: Any) -> datetime:
    """Convert the sim's observed_at_ns (int wall-clock nanoseconds) to
    a timezone-aware datetime for postgres."""
    try:
        ns = int(observed_at_ns)
        return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
    except (TypeError, ValueError):
        return now_utc()


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset_id = decoded.get("asset_id") or key
    if not asset_id:
        return None

    elements = decoded.get("elements") or []
    if not isinstance(elements, list):
        return None

    prov = resolve_origin_or_derive(None, asset_id, HANDLER_LABEL)

    row = {
        "asset_id": asset_id,
        "platform_variant": str(decoded.get("platform_variant") or ""),
        "profile_name": str(decoded.get("profile_name") or ""),
        "elements": elements,
        "operational": decoded.get("operational") or {},
        "observed_at": _observed_at_from_ns(decoded.get("observed_at_ns")),
        "updated_at": now_utc(),
        **prov,
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={"elements", "operational"},
    )
