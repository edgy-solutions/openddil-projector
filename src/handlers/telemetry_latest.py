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
from lifecycle_status import (OPERATIONAL_STATUS_DESTROYED,
                               OPERATIONAL_STATUS_REMOVED,
                               is_dis_destroyed_signal,
                               operational_status_from_op_state)
from persistence import Write

from .base import (now_utc, parse_timestamp, releasability_from,
                   resolve_origin_or_derive)

TABLE = "telemetry_latest_state"

_POSTURE_STATUS_PREFIX = "POSTURE_STATUS_"


def _posture_status_value(op_state: dict[str, Any]) -> str:
    """ADR-0044 amendment ("posture, a third column"). op_state's
    posture_status is the proto enum's full name string (e.g.
    "POSTURE_STATUS_MOVING"), or absent -- including_default_value_fields
    is off (see decoders/proto.py), so the zero value POSTURE_STATUS_
    UNSPECIFIED is indistinguishable from the field never having been set
    at all. Both map to 'unspecified' here, which is also the column's
    own default and the edge state machine's cold-start value, so there is
    no distinction being lost. Every other enum column here (power_state,
    functional_mode, health_state) stores the proto's full enum-name
    string as-is; posture_status is the one column whose CHECK constraint
    (schema.hcl) expects the short lower-case form, so it is mapped here."""
    raw = op_state.get("posture_status")
    if not raw:
        return "unspecified"
    if raw.startswith(_POSTURE_STATUS_PREFIX):
        return raw[len(_POSTURE_STATUS_PREFIX):].lower()
    return raw.lower()


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

    # Phase 5: operational_state 3-axis breakout. Producers that emit the
    # field (customer-overlay sensor branch via subsystem_state decomposition;
    # future DIS / AFSim / VRForces adapters) populate this block;
    # producers that don't (legacy Unit telemetry, capability-only assets)
    # leave it absent — read as `None` so postgres stores NULL and the
    # SPA's GROUND DIAGNOSTICS panel renders "—" for each axis.
    op_state = decoded.get("operational_state") or {}

    # ADR-0044 lifecycle slice 1: this projector instance's own clock, used
    # for every "as of right now, from THIS reader's point of view" stamp
    # below. Reusing one value keeps reporting_status_at, operational_status_at
    # and updated_at from this single message consistent with each other;
    # it deliberately does NOT flow into last_sample_at, which stays the
    # leaf's own sample_time so a severed relay hop freezes it correctly
    # (see reporting_sweep.py).
    now = now_utc()

    # ADR-0044 lifecycle slice "A": prefer the operational_status field
    # itself over the slice-1 fallback. `field_claim` remembers WHICH path
    # produced the claim, because the two paths write different shapes below
    # (see the status-only branch's comment) — a fallback-derived "destroyed"
    # still comes off a DIS Entity State PDU that also carries this entity's
    # own kinematics, so it stays on the ordinary full-row path exactly as
    # slice 1 shipped it; only a genuine field claim can trigger status-only.
    op_status_claim = operational_status_from_op_state(op_state)
    field_claim = op_status_claim is not None
    if not field_claim and is_dis_destroyed_signal(op_state, provenance):
        op_status_claim = OPERATIONAL_STATUS_DESTROYED

    # ADR-0044 lifecycle slice "A", §3b: a STATUS-ONLY record. No kinematics
    # AND a genuine operational_status claim is the shape a signal that is
    # NOT the entity reporting its own position takes — the case this slice
    # is built for is a simulation manager's Remove Entity PDU (ADR
    # §Alignment: "not decoded at all" today, but the column and this branch
    # exist ahead of that decoder so the write path is ready when it lands).
    # A Remove Entity does not come from the entity, so this branch must NOT
    # write reporting_status/_at (that column answers "did the ASSET report",
    # and this message isn't the asset reporting), last_sample_at,
    # kinematics, sustainment, provenance, the identity columns, or the
    # operational_state axes — doing so would blank a living row's last
    # known position and labels on the word of an authority (ADR §4:
    # "operational status is owned" by whoever received the actual signal,
    # not invented by whoever relays it) that never claimed to know them.
    if not kinematics and field_claim:
        status_only_row = {
            "asset_id": asset_id,
            **origin,
            **releasability_from(provenance),
            "operational_status": op_status_claim,
            "operational_status_at": now,
            "updated_at": now,
        }
        # A Remove Entity (operational_status == "removed") must not create a
        # fleet member. The upstream kind gate is stateless — it admits every
        # removal by PDU type alone, with no knowledge of whether this
        # asset_id has ever been seen — so resolution against existing
        # asset ids has to happen here, at write time: "update" can only
        # touch a row that already exists, so a removal for an unknown
        # asset_id is a no-op instead of an INSERT (see PostgresPool
        # build_sql/execute for the "update" mode itself). Every other
        # status-only claim still upserts as before.
        mode = "update" if op_status_claim == OPERATIONAL_STATUS_REMOVED else "upsert"
        return Write(
            table=TABLE,
            mode=mode,
            key_columns=["asset_id"],
            # No None survives to here in practice (asset_id/origin/claim/
            # timestamps are never None on this branch, and releasability_from
            # already omits its own keys rather than nulling them) — filtered
            # anyway so a future field added to this dict fails safe instead
            # of silently nulling a column this record has no business
            # touching.
            row={k: v for k, v in status_only_row.items() if v is not None},
        )

    row = {
        "asset_id": asset_id,
        **origin,
        # ADR-0029 §3: carried from the ingress stamp, never derived here.
        **releasability_from(provenance),
        "platform_variant": asset.get("platform_variant"),
        "callsign": asset.get("callsign"),
        # AssetSubsystem enum name (e.g. "ASSET_SUBSYSTEM_SENSOR"). The JSON
        # decode omits an UNSPECIFIED value, so absent -> None -> NULL =
        # the record describes the platform itself.
        "subsystem": asset.get("subsystem"),
        # ForceAffiliation enum -> its string name (e.g. "FORCE_FRIENDLY").
        "force_id": asset.get("force"),
        "kinematics": kinematics,
        "sustainment": decoded.get("sustainment"),
        "provenance": provenance,
        "last_sample_at": parse_timestamp(provenance.get("sample_time")),
        "schema_revision": int(decoded.get("schema_revision", 0) or 0),
        # Phase 5: operational_state columns. Enum fields are stored as
        # the proto's full enum-name strings ("POWER_STATE_OPERATE" etc.) —
        # protobuf JSON decoders default to that representation, and the
        # SPA's pill labels parse the same form. Boolean fields pass
        # through as native bool / None.
        "power_state":           op_state.get("power_state"),
        "functional_mode":       op_state.get("functional_mode"),
        "health_state":          op_state.get("health_state"),
        "actively_receiving":    op_state.get("actively_receiving"),
        "actively_transmitting": op_state.get("actively_transmitting"),
        # ADR-0044 amendment ("posture, a third column"). Decided ONCE, at
        # the edge tier's state machine; this handler stores the decided
        # value unchanged, on every full-row record (same tier at every
        # echelon runs this same handler on the bridged topic, so every
        # tier ends up storing the edge's decision). Unlike
        # operational_status below, there is no "omit to preserve a prior
        # terminal value" concern -- posture is never a one-way claim, so
        # it is always written, the same way reporting_status always is.
        "posture_status": _posture_status_value(op_state),
        "posture_since": parse_timestamp(op_state.get("posture_since")),
        # ADR-0044 lifecycle slice 1: reporting_status moves on the arrival
        # of a record, unconditionally — any message, from any producer,
        # means this reader just heard from the asset. Never gated on
        # health/damage: a destroyed asset that is still transmitting is
        # "destroyed" + "reporting" at once (ADR §2's "whole argument"), so
        # this key is set on every call, with no condition attached.
        "reporting_status": "reporting",
        "reporting_status_at": now,
        "updated_at": now,
    }

    # ADR-0044 lifecycle slice 1 (slice "A": now sourced from op_status_claim,
    # computed above from the field first and the DIS fallback second, rather
    # than re-testing is_dis_destroyed_signal here). operational_status is
    # the one column that must NOT be written on every message — only on an
    # actual signal about the asset. Omitting the keys (not writing
    # "operational") when there is no such signal is what makes silence, or
    # an ordinary non-destroyed update, leave a prior terminal value alone;
    # Postgres's UPSERT only touches columns present in this dict (see
    # PostgresPool.build_sql), so omission is the mechanism, not an
    # afterthought.
    if op_status_claim is not None:
        row["operational_status"] = op_status_claim
        row["operational_status_at"] = now

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={"kinematics", "sustainment", "provenance"},
    )
