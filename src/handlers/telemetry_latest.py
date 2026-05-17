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

import logging
import time
from typing import Any

from persistence import Write

from .base import ORIGIN_EDGE_ID, ORIGIN_REGION_ID, now_utc, parse_timestamp

log = logging.getLogger(__name__)
TABLE = "telemetry_latest_state"


# Rate-limited fallback WARN. Per-process throttle keyed by
# (edge_id_we_fell_back_to, asset_id) so a single misbehaving asset
# doesn't flood the log; one WARN per minute per (key) with a count of
# suppressed messages. INFO log on flush so the count surfaces.
_FALLBACK_WARN_WINDOW_S = 60.0
_fallback_last_warn: dict[tuple[str, str], tuple[float, int]] = {}


def _warn_fallback(edge_id: str, asset_id: str) -> None:
    """Emit a WARN at most once per _FALLBACK_WARN_WINDOW_S per
    (edge_id, asset_id), with a count of suppressed-in-window."""
    key = (edge_id, asset_id)
    now = time.monotonic()
    last = _fallback_last_warn.get(key)
    if last is None or (now - last[0]) >= _FALLBACK_WARN_WINDOW_S:
        if last is not None and last[1] > 0:
            log.warning(
                "telemetry_latest: provenance missing edge_id/region_id, "
                "falling back to env (asset=%s) [%d more suppressed in last %ds]",
                asset_id, last[1], int(_FALLBACK_WARN_WINDOW_S),
            )
        else:
            log.warning(
                "telemetry_latest: provenance missing edge_id/region_id, "
                "falling back to env (asset=%s)", asset_id,
            )
        _fallback_last_warn[key] = (now, 0)
    else:
        _fallback_last_warn[key] = (last[0], last[1] + 1)


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset = decoded.get("asset") or {}
    asset_id = asset.get("asset_id") or key
    if not asset_id:
        return None

    provenance = decoded.get("provenance") or {}

    # ADR-0023 §Projector: source edge_id/region_id from message-field with
    # env-default fallback. The WARN is rate-limited (see _warn_fallback).
    msg_edge_id = provenance.get("edge_id")
    msg_region_id = provenance.get("region_id")
    if not msg_edge_id or not msg_region_id:
        _warn_fallback(ORIGIN_EDGE_ID, asset_id)
    edge_id = msg_edge_id or ORIGIN_EDGE_ID
    region_id = msg_region_id or ORIGIN_REGION_ID

    row = {
        "asset_id": asset_id,
        "edge_id": edge_id,
        "region_id": region_id,
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
