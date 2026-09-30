"""ADR-0044 lifecycle slice 1 — pure helpers for the two independent,
timestamped status columns on the asset row.

Deliberately pure / no I/O, following the precedent of
`persistence.PostgresPool.build_sql`: both the destroyed-signal test and the
staleness test are the kind of thing that must be exercised with fabricated
inputs and an injected clock, never a sleeping test and never a live
Postgres. Anything that touches Kafka, Postgres or wall-clock time lives in
`handlers/telemetry_latest.py` (message arrival) and `reporting_sweep.py`
(periodic staleness sweep) instead, which call these functions.

Why two functions and not one "compute lifecycle" entry point: the ADR's
whole argument (§2) is that operational_status and reporting_status answer
different questions and must be able to move independently — a "destroyed"
asset that is still transmitting is not a contradiction to detect and
resolve, it is the exact state the two-column model exists to hold. Merging
the two computations back into one function would silently reintroduce the
coupling the ADR is against.
"""
from __future__ import annotations

from datetime import datetime

# The Bloblang DIS mapping (openddil-demo/dynamic-mappings/sim-dis-mapping.yaml)
# stamps this exact string into provenance.source_protocol for every DIS
# Entity State PDU it decodes, and collapses `damage = DESTROYED` into
# operational_state.health_state = HEALTH_STATE_FAILED before the event
# reaches this projector — there is no separate raw "damage" field on the
# wire today (checked: not in telemetry.proto, not threaded through by the
# mapping). HEALTH_STATE_FAILED alone is not a safe proxy for "destroyed":
# non-DIS producers (customer/Unit telemetry) can also report a FAILED
# health_state for an ordinary fault, and collapsing that into
# operational_status=destroyed would just move Finding 1's overloading
# problem from `health_state` to `operational_status` instead of fixing it.
# Gating on source_protocol keeps "destroyed" tied to the actual signal this
# slice was asked to honor: a DIS damage=DESTROYED report, specifically.
DIS_SOURCE_PROTOCOL_PREFIX = "DIS/IEEE-1278.1"

OPERATIONAL_STATUS_OPERATIONAL = "operational"
OPERATIONAL_STATUS_DESTROYED = "destroyed"
OPERATIONAL_STATUS_DEACTIVATED = "deactivated"
OPERATIONAL_STATUS_REMOVED = "removed"

REPORTING_STATUS_REPORTING = "reporting"
REPORTING_STATUS_NOT_REPORTING = "not_reporting"

# ADR-0044 §1 rule 3: "only an asset in a terminal status is eligible for
# retention". These three, and only these three, are the values the prune
# predicate (persistence.postgres.build_prune_sql) may treat as prunable.
# "operational" is deliberately absent — a still-operational asset is never
# prunable regardless of age, and REPORTING_STATUS values never belong in
# this tuple at all (reporting_status must never be a prune input; ADR §2,
# "different mechanisms answering different questions").
TERMINAL_OPERATIONAL_STATUSES = (
    OPERATIONAL_STATUS_DESTROYED,
    OPERATIONAL_STATUS_DEACTIVATED,
    OPERATIONAL_STATUS_REMOVED,
)

# proto enum-name string (OperationalStatus, telemetry.proto) -> column
# value. UNSPECIFIED and OPERATIONAL both map to None: per the proto's own
# comment, both are claims a producer makes, but neither is a claim this
# column persists — OPERATIONAL is the DB default already (nothing to
# write), and UNSPECIFIED is "no claim" by definition. Any string this dict
# doesn't recognize (absent field, a future enum value we don't know about
# yet) also falls through to None via .get()'s default, on the same
# "no claim, omit" logic is_dis_destroyed_signal already applies below.
_OPERATIONAL_STATUS_ENUM_TO_COLUMN = {
    "OPERATIONAL_STATUS_DESTROYED": OPERATIONAL_STATUS_DESTROYED,
    "OPERATIONAL_STATUS_DEACTIVATED": OPERATIONAL_STATUS_DEACTIVATED,
    "OPERATIONAL_STATUS_REMOVED": OPERATIONAL_STATUS_REMOVED,
}


def operational_status_from_op_state(op_state: dict) -> str | None:
    """The operational_status column value this message's `operational_state.
    operational_status` field (telemetry.proto's `OperationalStatus` enum,
    ADR-0044 lifecycle slice "A") asserts — or None when it asserts nothing.

    None covers every non-claim case identically, on purpose: the field is
    absent (producer not yet emitting it — see `is_dis_destroyed_signal` for
    the pre-field-adoption fallback signal), UNSPECIFIED (explicitly no
    claim), OPERATIONAL (a claim, but not one this column persists — see the
    module-level dict's comment), or an enum-name string this dict does not
    recognize. A caller must treat None as "omit the key", never as
    "operational" — writing "operational" here would move the column on a
    non-claim, exactly what ADR §1's "withdrawal cannot be inferred from
    silence" forbids in the other direction.
    """
    return _OPERATIONAL_STATUS_ENUM_TO_COLUMN.get((op_state or {}).get("operational_status"))


def is_dis_destroyed_signal(op_state: dict, provenance: dict) -> bool:
    """True if this message is a DIS-sourced report of the asset's own
    destruction.

    Not a proxy for "unhealthy" in general — only DIS damage=DESTROYED, as
    collapsed by the sim mapping into health_state=HEALTH_STATE_FAILED, and
    only when this event actually came off a DIS wire (source_protocol
    prefix). A caller must only ever ADD operational_status/
    operational_status_at to a row when this returns True, and must OMIT
    both keys otherwise (never write "operational" back over a prior
    "destroyed" — silence, a non-DIS message, or any other signal must not
    move this column; see ADR §2, "changed only by a signal about the
    asset").
    """
    if op_state.get("health_state") != "HEALTH_STATE_FAILED":
        return False
    source_protocol = (provenance or {}).get("source_protocol") or ""
    return source_protocol.startswith(DIS_SOURCE_PROTOCOL_PREFIX)


def compute_reporting_status(
    last_sample_at: datetime | None,
    now: datetime,
    stale_after_s: float,
) -> str:
    """The reporting_status a single reading tier would assign right now,
    given only ITS OWN view of when this asset last reported and ITS OWN
    clock.

    Pure and reader-relative by construction (ADR §4): pass a tier's local
    `last_sample_at` (from that tier's own Postgres row) and that tier's own
    `now`, and get that tier's own answer back. Two tiers computing this
    with different (last_sample_at, now) pairs — e.g. an edge that is still
    receiving locally vs. an HQ whose last forwarded sample froze when the
    edge->HQ link was severed — are expected to disagree; that disagreement
    is the property this slice exists to prove, not a bug to reconcile.

    `last_sample_at is None` (never sampled) reads as not_reporting: there
    is no reported knowledge of the asset to be "reporting" about.
    """
    if last_sample_at is None:
        return REPORTING_STATUS_NOT_REPORTING
    elapsed_s = (now - last_sample_at).total_seconds()
    if elapsed_s > stale_after_s:
        return REPORTING_STATUS_NOT_REPORTING
    return REPORTING_STATUS_REPORTING
