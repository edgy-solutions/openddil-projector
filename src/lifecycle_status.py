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

REPORTING_STATUS_REPORTING = "reporting"
REPORTING_STATUS_NOT_REPORTING = "not_reporting"


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
