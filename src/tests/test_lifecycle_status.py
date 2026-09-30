"""ADR-0044 lifecycle slice 1 — pure-function unit tests.

No Kafka, no Postgres, no sleeping: every "elapsed time" case injects both
`last_sample_at` and `now` directly, per SPEC-lifecycle-slice-1.md's
acceptance check 1 ("time must be injectable").
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lifecycle_status import (TERMINAL_OPERATIONAL_STATUSES,
                               compute_reporting_status,
                               is_dis_destroyed_signal,
                               operational_status_from_op_state)

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
STALE_AFTER_S = 30


# -- is_dis_destroyed_signal --------------------------------------------------

def test_dis_destroyed_signal_true_for_dis_failed_health_state():
    op_state = {"health_state": "HEALTH_STATE_FAILED"}
    provenance = {"source_protocol": "DIS/IEEE-1278.1-binary"}
    assert is_dis_destroyed_signal(op_state, provenance) is True


def test_dis_destroyed_signal_false_for_non_dis_failed_health_state():
    """A customer/Unit-telemetry FAILED health_state is an ordinary fault,
    not a DIS damage=DESTROYED report — gating on source_protocol is what
    keeps "destroyed" from becoming Finding 1's overloaded field all over
    again, just moved from health_state onto operational_status."""
    op_state = {"health_state": "HEALTH_STATE_FAILED"}
    provenance = {"source_protocol": "proprietary-v1"}
    assert is_dis_destroyed_signal(op_state, provenance) is False


def test_dis_destroyed_signal_false_for_dis_non_failed_health_state():
    op_state = {"health_state": "HEALTH_STATE_DEGRADED"}
    provenance = {"source_protocol": "DIS/IEEE-1278.1-binary"}
    assert is_dis_destroyed_signal(op_state, provenance) is False


def test_dis_destroyed_signal_false_for_missing_operational_state():
    assert is_dis_destroyed_signal({}, {"source_protocol": "DIS/IEEE-1278.1"}) is False


# -- compute_reporting_status --------------------------------------------------

def test_reporting_status_fresh_sample_is_reporting():
    now = T0 + timedelta(seconds=10)
    assert compute_reporting_status(T0, now, STALE_AFTER_S) == "reporting"


def test_reporting_status_exactly_at_threshold_is_reporting():
    now = T0 + timedelta(seconds=STALE_AFTER_S)
    assert compute_reporting_status(T0, now, STALE_AFTER_S) == "reporting"


def test_reporting_status_past_threshold_is_not_reporting():
    now = T0 + timedelta(seconds=STALE_AFTER_S + 1)
    assert compute_reporting_status(T0, now, STALE_AFTER_S) == "not_reporting"


def test_reporting_status_never_sampled_is_not_reporting():
    assert compute_reporting_status(None, T0, STALE_AFTER_S) == "not_reporting"


# -- Case 2: edge-01's uplink severed, all 8 ATL assets behind it ------------

def test_case2_two_readers_disagree_on_the_same_asset_at_the_same_instant():
    """ADR §4: reporting_status is computed per reading tier, never shared.

    Same asset, same instant, two local views:
      - edge-01's own Postgres: the asset is still transmitting TO EDGE-01
        (only the edge->HQ uplink is severed) so edge-01's own
        last_sample_at for it is fresh against edge-01's own clock.
      - HQ's own Postgres: nothing has been forwarded since the uplink was
        severed, so HQ's stored last_sample_at for the same asset is frozen
        at the moment of severance while HQ's own clock keeps advancing.

    If both readers produced the same verdict here, staleness would have
    been computed once and shared — exactly what ADR §4 forbids. This is
    that disagreement, in one test, for one representative ATL asset
    (dis:1:1:1000) standing in for all 8 behind the severed link.
    """
    severed_at = T0
    now = T0 + timedelta(minutes=5)  # comfortably past STALE_AFTER_S=30s

    # edge-01's own view: sensor->edge link is untouched, so edge-01 keeps
    # receiving and keeps its own last_sample_at fresh relative to its own now.
    edge_last_sample_at = now - timedelta(seconds=5)
    edge_view = compute_reporting_status(edge_last_sample_at, now, STALE_AFTER_S)

    # HQ's own view: the last sample HQ ever received for this asset is
    # whatever arrived before the uplink was severed; Provenance.sample_time
    # is the leaf's own timestamp and is never rewritten by a relay hop, so
    # HQ's stored value simply stops moving.
    hq_last_sample_at = severed_at
    hq_view = compute_reporting_status(hq_last_sample_at, now, STALE_AFTER_S)

    assert edge_view == "reporting"
    assert hq_view == "not_reporting"
    assert edge_view != hq_view


# -- operational_status_from_op_state (ADR-0044 lifecycle slice "A") ---------

def test_operational_status_maps_destroyed():
    assert operational_status_from_op_state(
        {"operational_status": "OPERATIONAL_STATUS_DESTROYED"}) == "destroyed"


def test_operational_status_maps_deactivated():
    assert operational_status_from_op_state(
        {"operational_status": "OPERATIONAL_STATUS_DEACTIVATED"}) == "deactivated"


def test_operational_status_maps_removed():
    assert operational_status_from_op_state(
        {"operational_status": "OPERATIONAL_STATUS_REMOVED"}) == "removed"


def test_operational_status_unspecified_is_no_claim():
    assert operational_status_from_op_state(
        {"operational_status": "OPERATIONAL_STATUS_UNSPECIFIED"}) is None


def test_operational_status_operational_is_no_claim():
    """OPERATIONAL is a claim the producer makes, but not one this column
    persists — it is the DB default already (see lifecycle_status.py's
    module-level dict comment). Writing "operational" here would be
    indistinguishable from a handler moving the column on an ordinary
    update, which ADR §1 forbids in the other direction (destroyed ->
    operational via silence/an unrelated message)."""
    assert operational_status_from_op_state(
        {"operational_status": "OPERATIONAL_STATUS_OPERATIONAL"}) is None


def test_operational_status_absent_field_is_no_claim():
    """Producer not yet emitting the field at all — the pre-field-adoption
    case `is_dis_destroyed_signal`'s fallback exists for."""
    assert operational_status_from_op_state({}) is None


def test_operational_status_unrecognized_string_is_no_claim():
    assert operational_status_from_op_state(
        {"operational_status": "SOME_FUTURE_ENUM_VALUE"}) is None


def test_terminal_operational_statuses_are_exactly_the_three_endpoints():
    """Pinned so a future edit can't silently add "operational" (never
    prunable at any age, ADR §1 rule 3) or drop one of the three without a
    test noticing — this tuple is a direct input to the prune predicate."""
    assert TERMINAL_OPERATIONAL_STATUSES == ("destroyed", "deactivated", "removed")
