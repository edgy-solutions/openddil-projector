"""Handler: telemetry-latest-state -> telemetry_latest_state.

Decodes openddil.telemetry.v1.EntityTelemetryEvent. Compacted topic, keyed
by asset_id -> UPSERT.

Identity fields (platform_variant, callsign, force) are flattened out of the
nested `asset` (AssetIdentity) message into their own columns so the UI's
fleet picker can read them without a JSONB path. The bulky nested blocks —
kinematics, sustainment, provenance — are stored as JSONB verbatim; the UI
reaches into them via JSONB paths and pulls Quantity {value, unit} pairs.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import now_utc, parse_timestamp

TABLE = "telemetry_latest_state"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset = decoded.get("asset") or {}
    asset_id = asset.get("asset_id") or key
    if not asset_id:
        return None

    provenance = decoded.get("provenance") or {}

    row = {
        "asset_id": asset_id,
        "platform_variant": asset.get("platform_variant"),
        "callsign": asset.get("callsign"),
        # ForceAffiliation enum -> its string name (e.g. "FORCE_FRIENDLY").
        "force_id": asset.get("force"),
        "kinematics": decoded.get("kinematics"),
        "sustainment": decoded.get("sustainment"),
        "provenance": provenance,
        "last_sample_at": parse_timestamp(provenance.get("sample_time")),
        "schema_revision": int(decoded.get("schema_revision", 0) or 0),
        "updated_at": now_utc(),
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={"kinematics", "sustainment", "provenance"},
    )
