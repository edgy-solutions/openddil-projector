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

Change-only writes (opt-in, off by default). Every full-row write sets
`provenance` (which carries `sample_time`), `last_sample_at`,
`reporting_status_at` and `updated_at`, and all four change on every
message, so every message is an UPDATE. Electric in `replica=default` logs,
per UPDATE, the key plus the changed columns (source-verified for 1.6.2,
`LogItems.from_change`), so an asset that has not changed still costs one
shape-log entry per message per shape. With TELEMETRY_CHANGE_ONLY_HEARTBEAT_S
> 0 this handler returns None for a message that changes nothing but
freshness, and refreshes freshness on a heartbeat instead. Writing a column
subset is deliberately not done: Electric already logs only the changed
columns, and a partial upsert could recreate a reset- or prune-deleted row
with NULL identity and kinematics.

A write is returned (and the per-asset cache refreshed) when any of these
holds: no cache entry (cold start, or the status-only branch dropped it); an
operational_status claim; a `deactivated` -> `operational` revive would
apply; the fingerprint differs; the position moved at least the deadband; or
the cached write is at least H old.

The fingerprint is the row minus the four freshness/status stamps,
`provenance.sample_time` and `provenance.ingest_time` (both move on every
message), and the motion leaves of `kinematics`: `position.ecef`,
`position.valid_at`, any lat/lon on `position` or inside `position.wgs84`,
all of `velocity` and `attitude.euler`. Velocity and yaw are the same motion
as the position, so they are written whenever the position is and at the
latest on the heartbeat: under the deadband they can lag by up to H.
Everything else counts, exactly: `position.quality`, `attitude.reference`,
the rest of `wgs84` (altitude), any other kinematics or provenance key.

TELEMETRY_CHANGE_ONLY_HEARTBEAT_S must be at most half of
reporting_sweep.STALE_AFTER_S (ValueError otherwise): while an asset reports
with inter-message gap G, the stored last_sample_at is at most H + G old, and
the sweep flips it at STALE_AFTER_S. TELEMETRY_POSITION_DEADBAND_M is a JSON
object of metres per platform_variant, with an optional "*" fallback; an
unknown variant with no "*" compares exactly. The chart builds it from
movement.standards.<variant>.positionDeadbandM, with "*" from
movement.positionDeadbandDefaultM, and sets both values only when its
projector.telemetryChangeOnly.heartbeatS is above 0. Distance, from the last
written position (so slow drift still writes once it accumulates past the
deadband): 3-D Euclidean metres over `position.ecef.{x,y,z}.value` when both
sides have all three with unit "m" or none; else great-circle metres from
lat/lon; else an exact compare of `position` without `valid_at`. A deadband
of 0 also takes the exact compare, so a re-stamped valid_at alone never
counts as movement. The suppression metric reads `unchanged` when position
(without valid_at), velocity and attitude all equal the last written ones,
and `deadband` otherwise.

Ordering caveat: the cache is filled when the Write is returned, not after
Postgres accepts it. A non-retryable write error (main.py logs and skips)
leaves the store behind the cache until the next heartbeat. That bound, H, is
also the stated bound for a reset- or prune-deleted row: it reappears within H.
The cache is in-process, keyed by asset_id and cold after a restart.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import reporting_sweep
from edge_assignment import extract_wgs84, great_circle_km
from lifecycle_status import (OPERATIONAL_STATUS_DEACTIVATED,
                               OPERATIONAL_STATUS_DESTROYED,
                               OPERATIONAL_STATUS_OPERATIONAL,
                               OPERATIONAL_STATUS_REMOVED,
                               is_dis_destroyed_signal,
                               operational_status_from_op_state)
from metrics import TELEMETRY_WRITES_SUPPRESSED
from persistence import Revive, Write

from .base import (now_utc, parse_timestamp, releasability_from,
                   resolve_origin_or_derive)

TABLE = "telemetry_latest_state"

_POSTURE_STATUS_PREFIX = "POSTURE_STATUS_"

# Columns that move on every message without the asset having changed.
_FRESHNESS_COLUMNS = ("last_sample_at", "reporting_status_at", "updated_at",
                      "operational_status_at")

_heartbeat_s = 0.0
_deadband_m: dict[str, float] = {}


@dataclass
class _Cached:
    fingerprint: str
    position: Any
    at: datetime
    op_status: Optional[str]
    motion: str


# asset_id -> what the last full-row Write this process returned carried.
_cache: dict[str, _Cached] = {}


def configure() -> None:
    """(Re)read the change-only env. Called at import; tests call it again
    after monkeypatching the environment. Raises ValueError on a bad value so
    a misconfigured deployment fails at startup, not on the first message."""
    global _heartbeat_s, _deadband_m
    raw_h = os.getenv("TELEMETRY_CHANGE_ONLY_HEARTBEAT_S", "0")
    try:
        heartbeat = float(raw_h)
    except ValueError:
        raise ValueError(
            f"TELEMETRY_CHANGE_ONLY_HEARTBEAT_S={raw_h!r} is not a number")
    if heartbeat < 0:
        raise ValueError(
            f"TELEMETRY_CHANGE_ONLY_HEARTBEAT_S={heartbeat} is negative")
    stale = reporting_sweep.STALE_AFTER_S
    if heartbeat > stale / 2:
        raise ValueError(
            f"TELEMETRY_CHANGE_ONLY_HEARTBEAT_S={heartbeat} exceeds half of "
            f"REPORTING_STALE_AFTER_S={stale}: a reporting asset's stored "
            f"last_sample_at could go stale before the next heartbeat")
    raw_d = os.getenv("TELEMETRY_POSITION_DEADBAND_M", "{}")
    try:
        parsed = json.loads(raw_d)
    except ValueError as exc:
        raise ValueError(f"TELEMETRY_POSITION_DEADBAND_M is not JSON: {exc}")
    if not isinstance(parsed, dict):
        raise ValueError("TELEMETRY_POSITION_DEADBAND_M must be a JSON object")
    deadband: dict[str, float] = {}
    for variant, metres in parsed.items():
        if isinstance(metres, bool) or not isinstance(metres, (int, float)):
            raise ValueError(
                f"TELEMETRY_POSITION_DEADBAND_M[{variant!r}] is not a number")
        if metres < 0:
            raise ValueError(
                f"TELEMETRY_POSITION_DEADBAND_M[{variant!r}]={metres} is negative")
        deadband[str(variant)] = float(metres)
    _heartbeat_s = heartbeat
    _deadband_m = deadband


configure()


_LATLON_KEYS = ("lat", "lon", "latitude", "longitude")


def _strip_latlon(d: Any) -> Any:
    return ({k: v for k, v in d.items() if k not in _LATLON_KEYS}
            if isinstance(d, dict) else d)


def _fingerprint(row: dict[str, Any]) -> str:
    fp = {k: v for k, v in row.items() if k not in _FRESHNESS_COLUMNS}
    prov = fp.get("provenance")
    if isinstance(prov, dict):
        fp["provenance"] = {k: v for k, v in prov.items()
                            if k not in ("sample_time", "ingest_time")}
    kin = fp.get("kinematics")
    if isinstance(kin, dict):
        kin = {k: v for k, v in kin.items() if k != "velocity"}
        att = kin.get("attitude")
        if isinstance(att, dict):
            kin["attitude"] = {k: v for k, v in att.items() if k != "euler"}
        pos = kin.get("position")
        if isinstance(pos, dict):
            # Only the motion leaves leave; quality and any other key stay,
            # and so does whatever sits beside lat/lon inside wgs84 (altitude).
            pos = {k: v for k, v in pos.items()
                   if k not in ("ecef", "valid_at", "wgs84")
                   and k not in _LATLON_KEYS}
            if isinstance(kin["position"].get("wgs84"), dict):
                pos["wgs84"] = _strip_latlon(kin["position"]["wgs84"])
            kin["position"] = pos
        fp["kinematics"] = kin
    return json.dumps(fp, sort_keys=True, default=str)


def _motion(kin: Any) -> str:
    """Position (minus valid_at), velocity and attitude, canonical. Equal to
    the last written one means the message carried no motion at all."""
    if not isinstance(kin, dict):
        return "null"
    pos = kin.get("position")
    if isinstance(pos, dict):
        pos = {k: v for k, v in pos.items() if k != "valid_at"}
    return json.dumps([pos, kin.get("velocity"), kin.get("attitude")],
                      sort_keys=True, default=str)


def _ecef_m(position: Any) -> Optional[tuple[float, float, float]]:
    if not isinstance(position, dict):
        return None
    ecef = position.get("ecef")
    if not isinstance(ecef, dict):
        return None
    out = []
    for axis in ("x", "y", "z"):
        q = ecef.get(axis)
        if not isinstance(q, dict) or q.get("unit", "m") != "m":
            return None
        v = q.get("value")
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return None
        out.append(float(v))
    return (out[0], out[1], out[2])


def _position_no_stamp(position: Any) -> Any:
    if not isinstance(position, dict):
        return position
    return {k: v for k, v in position.items() if k != "valid_at"}


def _moved(old: Any, new: Any, deadband_m: float) -> bool:
    """True when the position moved at least the deadband from the last
    written one. Order: 3-D ECEF metres, else great-circle metres, else (and
    always when the deadband is 0) an exact compare of `position` without its
    valid_at stamp, which is re-stamped on every message."""
    if deadband_m > 0:
        e0, e1 = _ecef_m(old), _ecef_m(new)
        if e0 is not None and e1 is not None:
            return math.dist(e0, e1) >= deadband_m
        lat0, lon0 = extract_wgs84({"position": old})
        lat1, lon1 = extract_wgs84({"position": new})
        if None not in (lat0, lon0, lat1, lon1):
            return great_circle_km(lat0, lon0, lat1, lon1) * 1000.0 >= deadband_m
    return _position_no_stamp(old) != _position_no_stamp(new)


def _change_only(asset_id: str, write: Write, op_status_claim: Optional[str],
                 variant: Optional[str], now: datetime) -> Write | None:
    row = write.row
    fp = _fingerprint(row)
    kin = row.get("kinematics")
    pos = kin.get("position") if isinstance(kin, dict) else None
    motion = _motion(kin)
    prev = _cache.get(asset_id)
    revive = write.revive
    revive_applies = (revive is not None and prev is not None
                      and prev.op_status in revive.from_values)
    deadband = _deadband_m.get(variant or "", _deadband_m.get("*", 0.0))
    if (prev is None or op_status_claim is not None or revive_applies
            or prev.fingerprint != fp
            or (now - prev.at).total_seconds() >= _heartbeat_s
            or _moved(prev.position, pos, deadband)):
        op_status = prev.op_status if prev is not None else None
        if op_status_claim is not None:
            op_status = op_status_claim
        elif revive_applies:
            op_status = revive.to_value
        _cache[asset_id] = _Cached(fp, pos, now, op_status, motion)
        return write
    # Suppressed: the fingerprint is equal, so the only possible difference
    # is motion under the deadband (or none at all).
    reason = "unchanged" if prev.motion == motion else "deadband"
    TELEMETRY_WRITES_SUPPRESSED.labels(reason=reason).inc()
    return None


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
    # future DIS / AFSim / co-located simulator adapters) populate this block;
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
        # Change-only: this write moves operational_status without the row's
        # other columns, so what the cache fingerprinted no longer describes
        # the stored row. Drop it; the next full row writes.
        _cache.pop(asset_id, None)
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
    # afterthought. The one exception is `deactivated`, which is reversible
    # (ADR-0044): a record that carries the asset's own kinematics and no
    # terminal claim proves the asset is back, so `revive` (below) returns
    # `deactivated` to `operational` in the same upsert.
    revive = None
    if op_status_claim is not None:
        row["operational_status"] = op_status_claim
        row["operational_status_at"] = now
    elif kinematics:
        # Only `deactivated` is in from_values, deliberately. `destroyed`:
        # a destroyed entity keeps transmitting, so appearance proves
        # nothing. `removed`: a Remove Entity has no undo on the wire.
        # Both stay put until a reset empties the stores.
        revive = Revive(
            column="operational_status",
            from_values=(OPERATIONAL_STATUS_DEACTIVATED,),
            to_value=OPERATIONAL_STATUS_OPERATIONAL,
            at_column="operational_status_at",
            at_from="updated_at",
        )

    write = Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={"kinematics", "sustainment", "provenance"},
        revive=revive,
    )
    if _heartbeat_s > 0:
        return _change_only(asset_id, write, op_status_claim,
                            asset.get("platform_variant"), now)
    return write
