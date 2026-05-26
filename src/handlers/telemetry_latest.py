"""Handler: telemetry-latest-state -> telemetry_latest_state.

Decodes openddil.telemetry.v1.EntityTelemetryEvent. Compacted topic, keyed
by asset_id -> UPSERT.

Identity fields (platform_variant, callsign, force) are flattened out of the
nested `asset` (AssetIdentity) message into their own columns so the UI's
fleet picker can read them without a JSONB path. The bulky nested blocks —
kinematics, sustainment, provenance — are stored as JSONB verbatim; the UI
reaches into them via JSONB paths and pulls Quantity {value, unit} pairs.

ADR-0023 / Phase 6a: this handler reads edge_id / region_id from the
inbound message's provenance (stamped at sensor-ingest, preserved through
the DIS-mapper) instead of the projector's env defaults. Falls back to
env defaults when the message-field is absent — that path emits a
rate-limited WARN so post-deploy field-uptake regressions surface in
logs. **The other four per-asset handlers (cm_state, logistics_status,
telemetry_windows, tactical_events) still use origin_provenance() env
defaults** — their cm-service / fusion / faust-edge emitters get their
own coordinated upgrades in 6b. Consequence detectable in monitoring:
`SELECT DISTINCT edge_id FROM asset_cm_state` will show only the
projector's env default in 6a regardless of which edge the underlying
events came from. That is expected partial-state during 6a, not a bug.
"""
from __future__ import annotations

from typing import Any

from edge_assignment import extract_wgs84
from persistence import Write

from .base import now_utc, parse_timestamp, resolve_origin_or_derive

TABLE = "telemetry_latest_state"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset = decoded.get("asset") or {}
    asset_id = asset.get("asset_id") or key
    if not asset_id:
        return None

    provenance = decoded.get("provenance") or {}
    kinematics = decoded.get("kinematics")

    # DIS path: provenance carries edge_id/region_id stamped at sensor-ingest;
    # pass through.
    # Customer path (Unit telemetry): provenance.edge_id is empty — derive via
    # the configured edge_assignment strategy (typically nearest-FOB on the
    # asset's WGS84 position). See src/edge_assignment.py.
    lat, lon = extract_wgs84(kinematics)
    origin = resolve_origin_or_derive(
        provenance, asset_id, "telemetry_latest",
        asset_lat=lat, asset_lon=lon,
    )

    row = {
        "asset_id": asset_id,
        **origin,
        "platform_variant": asset.get("platform_variant"),
        "callsign": asset.get("callsign"),
        # ForceAffiliation enum -> its string name (e.g. "FORCE_FRIENDLY").
        "force_id": asset.get("force"),
        "kinematics": kinematics,
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
