"""Handler: effector-events -> effector_launch, the launch-record table.

Unlike every other handler in this package, this one is NOT pure: it is
registered with `mode: "custom"` in projector_config.yaml, so main.py calls
it as `await handle(key, decoded, pool)` instead of `handle(key, decoded)`.
Two of its three operations (Fire and Detonation) genuinely need the pool,
not just a `Write`; the third (Resupply Received) uses the pool for the same
admission read and an ordinary append:

  * Fire needs a READ (is `launcher_urn` an admitted asset?) before it can
    decide whether to write at all.
  * Detonation is update-only and must tell apart "no matching Fire" from
    "a real result replacing an already-timed-out row" from "a duplicate of
    an already-terminal row" -- see `PostgresPool.apply_effector_detonation`
    for why that needs the row's OLD terminal_state, not just a Write.

Fire's own write, once admitted, is an ordinary `Write(mode="append", ...)`
through the ordinary `pool.execute()` path -- no bespoke SQL needed there.

Three operations, then: Fire (-> effector_launch), Detonation (updates that
row), and Resupply Received (-> effector_resupply, one row per munition type).

Wire shape (confirmed against openddil-sensor-ingest/dis_ingestor.py
`_extract_fire` / `_extract_detonation` / `_munition_type_dict`): `pdu_type`,
`event_urn` ("dis-event:site:application:eventNumber"), `launcher_urn`,
`munition_urn` (or null), `target_urn` (or null), `munition_type` (dict with keys kind/domain/country/
category/subcategory/specific/extra -- DIS 1278.1's own prose names for the
EntityType 7-tuple, not opendis's attribute spelling), `quantity`,
`detonation_result` (Detonation only), `ingest_timestamp`, and a nested
`provenance: {edge_id, region_id, originator_nation?, releasable_to?}` --
the launch's own labels, stamped at ingress by the dis-effector mapping
from its launcher's releasability.yaml row (ADR-0029 §3), not derived here.
`releasability_from` reads them exactly like any other record's
provenance: present when the launcher is declared, absent when it is not
-- the same absent-stays-absent rule as telemetry_latest's DIS path.
"""
from __future__ import annotations

import logging
from typing import Any

from metrics import (EFFECTOR_LATE_TERMINAL, EFFECTOR_REFUSED, EFFECTOR_REPLAYED,
                     EFFECTOR_RESUPPLY_ROWS, EFFECTOR_RESUPPLY_SEEN,
                     MUNITION_TRACK_JOINED, MUNITION_TRACK_TERMINAL)
from persistence import PostgresPool, Write

from .base import (now_utc, parse_timestamp, refuse_row, releasability_from,
                    resolve_provenance_from_dict)

log = logging.getLogger("projector.handlers.effector_launch")

TABLE = "effector_launch"
RESUPPLY_TABLE = "effector_resupply"
LAUNCHER_LIFECYCLE_TABLE = "telemetry_latest_state"
LAUNCHER_LIFECYCLE_COLUMN = "asset_id"

# DIS detonationResult enum -> this store's small terminal-state vocabulary.
# There is deliberately no "miss" anywhere: DIS carries no such event, so an
# in-flight round that never gets a Detonation is unresolved (the timeout
# sweep), never missed -- absence is not an outcome.
_TERMINAL_MAP = {
    1: "entity_impact",
    3: "ground_impact",
    2: "detonated",
    4: "detonated",
    5: "detonated",
    6: "dud",
}


def terminal_state_for(detonation_result: Any) -> str:
    """DIS detonationResult int -> terminal_state. Unknown/unmapped codes
    fall to 'other' rather than being refused -- an out-of-range enum value
    is still a real detonation, just not one this vocabulary has a sharper
    name for."""
    try:
        code = int(detonation_result)
    except (TypeError, ValueError):
        return "other"
    return _TERMINAL_MAP.get(code, "other")


def munition_type_key(munition_type: dict[str, Any] | None) -> str:
    """The DIS 7-tuple dict (`_munition_type_dict`'s shape) ->
    "k.d.c.cat.sub.spec.extra", the dotted-tuple string the migration's
    `munition_type` column stores. Same key order the migration/schema
    comment documents."""
    d = munition_type or {}
    parts = (
        d.get("kind", 0), d.get("domain", 0), d.get("country", 0),
        d.get("category", 0), d.get("subcategory", 0), d.get("specific", 0),
        d.get("extra", 0),
    )
    return ".".join(str(int(p)) for p in parts)


async def _handle_fire(decoded: dict[str, Any], pool: PostgresPool) -> None:
    event_urn = decoded.get("event_urn")
    launcher_urn = decoded.get("launcher_urn")
    if not event_urn or not launcher_urn:
        refuse_row("effector_launch", "missing_key",
                   f"fire with event_urn={event_urn!r} launcher_urn={launcher_urn!r}")
        return

    admitted = await pool.row_exists(
        LAUNCHER_LIFECYCLE_TABLE, LAUNCHER_LIFECYCLE_COLUMN, launcher_urn
    )
    if not admitted:
        EFFECTOR_REFUSED.labels(reason="unknown_launcher").inc()
        log.info("REFUSED Fire %s: launcher %s is not an admitted asset",
                  event_urn, launcher_urn)
        return

    provenance = decoded.get("provenance") or {}
    row = {
        "event_urn": event_urn,
        "launcher_asset_id": launcher_urn,
        "munition_asset_id": decoded.get("munition_urn"),
        "munition_type": munition_type_key(decoded.get("munition_type")),
        "quantity": int(decoded.get("quantity", 0)),
        "target_asset_id": decoded.get("target_urn"),
        "launched_at": parse_timestamp(decoded.get("ingest_timestamp")) or now_utc(),
        # Labels from the launcher, as tactical_events does (ADR-0022
        # origin provenance + ADR-0029 releasability; both resolved from
        # the same nested `provenance` dict telemetry_windows.py reads).
        **resolve_provenance_from_dict(provenance, launcher_urn, "effector_launch"),
        **releasability_from(provenance),
    }
    write = Write(table=TABLE, mode="append", key_columns=["event_urn"], row=row)
    await pool.execute(write)
    # The munition's track may have arrived first (ADR-0049); give it its
    # launcher and labels. Nothing to join when the Fire names no munition.
    if row["munition_asset_id"]:
        joined = await pool.join_munition_track_launcher(
            munition_asset_id=row["munition_asset_id"],
            launcher_asset_id=launcher_urn,
            event_urn=event_urn,
            edge_id=row.get("edge_id"),
            region_id=row.get("region_id"),
            originator_nation=row.get("originator_nation"),
            releasable_to=row.get("releasable_to"),
            now=now_utc(),
        )
        if joined:
            MUNITION_TRACK_JOINED.labels(via="fire").inc()


async def _handle_detonation(decoded: dict[str, Any], pool: PostgresPool) -> None:
    event_urn = decoded.get("event_urn")
    if not event_urn:
        refuse_row("effector_launch", "missing_key", "detonation with no event_urn")
        return

    terminal_state = terminal_state_for(decoded.get("detonation_result"))
    terminated_at = parse_timestamp(decoded.get("ingest_timestamp")) or now_utc()
    new_result = int(decoded.get("detonation_result", 0) or 0)

    outcome, old_result = await pool.apply_effector_detonation(
        event_urn=event_urn,
        terminal_state=terminal_state,
        detonation_result=new_result,
        terminated_at=terminated_at,
        now=now_utc(),
    )
    if outcome == "no_fire":
        EFFECTOR_REFUSED.labels(reason="no_fire").inc()
        log.info("REFUSED Detonation %s: no matching Fire row", event_urn)
    elif outcome == "conflicting":
        EFFECTOR_REFUSED.labels(reason="conflicting_detonation").inc()
        log.warning("REFUSED Detonation %s: conflicting detonation_result "
                    "(existing=%s, new=%s) -- row left unchanged",
                    event_urn, old_result, new_result)
    elif outcome == "replayed":
        EFFECTOR_REPLAYED.inc()
        log.info("Detonation %s replayed: result %s already applied to this "
                  "row (e.g. a second projector instance admitting the same "
                  "Fire/Detonation pair) -- not a refusal", event_urn, new_result)
    elif outcome == "late":
        EFFECTOR_LATE_TERMINAL.inc()
        log.info("Detonation %s resolved a row the timeout sweep had already "
                  "marked unresolved (late_terminal)", event_urn)
    # "updated" is the normal case -- nothing to count beyond the write itself.

    # The munition's track terminates whatever the effector_launch outcome
    # was (ADR-0049): the track is keyed by the munition, and the Fire row
    # may be absent or already terminal. Key: the record's own munition_urn,
    # else the munition of this event's Fire row (resolved in the pool).
    track = await pool.terminate_munition_track(
        munition_asset_id=decoded.get("munition_urn"),
        event_urn=event_urn,
        terminal_state=terminal_state,
        detonation_result=new_result,
        terminated_at=terminated_at,
        now=now_utc(),
    )
    if track in ("terminated", "late"):
        MUNITION_TRACK_TERMINAL.labels(state=terminal_state).inc()
        if track == "late":
            log.info("Detonation %s resolved a munition track the silence "
                      "sweep had already marked silent (late_terminal)",
                      event_urn)


async def _handle_resupply(decoded: dict[str, Any], pool: PostgresPool) -> None:
    event_urn = decoded.get("event_urn")
    launcher_urn = decoded.get("launcher_urn")
    if not event_urn or not launcher_urn:
        refuse_row("effector_resupply", "missing_key",
                   f"resupply with event_urn={event_urn!r} launcher_urn={launcher_urn!r}")
        return

    admitted = await pool.row_exists(
        LAUNCHER_LIFECYCLE_TABLE, LAUNCHER_LIFECYCLE_COLUMN, launcher_urn
    )
    if not admitted:
        EFFECTOR_REFUSED.labels(reason="unknown_launcher").inc()
        log.info("REFUSED Resupply %s: launcher %s is not an admitted asset",
                  event_urn, launcher_urn)
        return

    supplies = decoded.get("supplies")
    if not isinstance(supplies, list) or not supplies:
        refuse_row("effector_resupply", "no_supplies",
                   f"resupply {event_urn} carries no supply lines")
        return

    # Lines with the same munition key are SUMMED into one row (the PK is
    # event_urn + munition_type). Summing then flooring equals fusion's
    # sequential floor in effector_supply.apply_resupply for non-negative
    # quantities, so the stored ledger replays to the same remaining.
    totals: dict[str, int] = {}
    for line in supplies:
        qty = line.get("quantity") if isinstance(line, dict) else None
        # The DIS supply quantity is a float32 on the wire, so a whole count
        # arrives as 1.0. Whole-number floats are counts; a fractional one is
        # refused here (fusion would truncate it), never rounded silently.
        if isinstance(qty, float) and qty.is_integer():
            qty = int(qty)
        if isinstance(qty, bool) or not isinstance(qty, int) or qty < 0:
            refuse_row("effector_resupply", "bad_quantity",
                       f"resupply {event_urn} line quantity={qty!r}")
            continue
        mt_key = munition_type_key(line.get("munition_type"))
        totals[mt_key] = totals.get(mt_key, 0) + qty

    provenance = decoded.get("provenance") or {}
    received_at = parse_timestamp(decoded.get("ingest_timestamp")) or now_utc()
    for mt_key, qty in totals.items():
        row = {
            "event_urn": event_urn,
            "launcher_asset_id": launcher_urn,
            "supplier_asset_id": decoded.get("supplier_urn"),
            "munition_type": mt_key,
            "quantity": qty,
            "received_at": received_at,
            **resolve_provenance_from_dict(provenance, launcher_urn, "effector_resupply"),
            **releasability_from(provenance),
        }
        # ON CONFLICT DO NOTHING (append): a replayed record is a no-op.
        await pool.execute(Write(table=RESUPPLY_TABLE, mode="append",
                                 key_columns=["event_urn", "munition_type"],
                                 row=row))
        EFFECTOR_RESUPPLY_ROWS.inc()


async def handle(key: str, decoded: dict[str, Any], pool: PostgresPool) -> None:
    """Entry point main.py's `mode: "custom"` dispatch calls. Not a pure
    `Handler` (see module docstring) -- owns its own Postgres I/O."""
    pdu_type = decoded.get("pdu_type")
    if pdu_type == "fire":
        await _handle_fire(decoded, pool)
    elif pdu_type == "detonation":
        await _handle_detonation(decoded, pool)
    elif pdu_type == "resupply_received":
        EFFECTOR_RESUPPLY_SEEN.inc()
        await _handle_resupply(decoded, pool)
    else:
        refuse_row("effector_launch", "unknown_pdu_type", str(pdu_type))
