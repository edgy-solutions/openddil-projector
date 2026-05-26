"""Handler: asset-capability-snapshot -> asset_capability_state.

Decodes the customer-overlay AssetCapabilitySnapshot JSON shape produced by
the customer-overlay bundle's strike-capability Bloblang (recipe v3 Sub-phase A).
Compacted topic keyed by asset_id -> UPSERT. Recipe v3 Sub-phase E.

The customer's StrikeCapabilityMessage feed lands one snapshot per asset
carrying the current Ammo count for every loaded store. This handler
persists the latest snapshot per asset; `capabilities` is stored verbatim
as a JSONB blob (the per-store array). The projector's handler contract is
one Write per message, so per-store rows would need a multi-Write
signature change -- the JSONB array keeps the projector model intact while
still carrying per-store granularity for the UI (Sub-phase G) and the
engagement-worthiness ConstrainingFactor (Sub-phase F).
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import now_utc, parse_timestamp, resolve_origin_or_derive

TABLE = "asset_capability_state"
HANDLER_LABEL = "capability_state"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset_id = decoded.get("asset_id") or key
    if not asset_id:
        return None

    # The strike-capability Silver shape has empty provenance.edge_id (the
    # customer Bloblang leaves it for the projector). And these messages
    # carry no position — strike-only launchers have no telemetry. The
    # configured edge_assignment strategy decides via asset_id_prefix /
    # static / fallback. See src/edge_assignment.py.
    prov = resolve_origin_or_derive(
        decoded.get("provenance"), asset_id, HANDLER_LABEL,
    )

    row = {
        "asset_id": asset_id,
        "capabilities": decoded.get("capabilities", []),
        "schema_version": decoded.get("schema_version"),
        "mode": decoded.get("mode"),
        "observed_at": parse_timestamp(decoded.get("observed_at")),
        "updated_at": now_utc(),
        **prov,
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={"capabilities"},
    )
