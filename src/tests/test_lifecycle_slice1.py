"""ADR-0044 lifecycle slice 1 — SPEC-lifecycle-slice-1.md Case 1.

Simulates the 14-asset fleet across the three timesteps in the spec's table,
using the REAL production code paths for both ways a row can change:

  - message arrival: `handlers.telemetry_latest.handle()`, the actual
    handler, called with hand-built decoded dicts (same style as
    test_handlers.py — no live Kafka/Postgres).
  - staleness: `lifecycle_status.compute_reporting_status()`, the actual
    pure function the production `reporting_sweep_loop` calls, applied here
    with an injected `now` at each checkpoint instead of a real timer.

A `_Row` dict standing in for one asset's Postgres row is updated using the
same partial-column-write rule Postgres actually applies (see
`PostgresPool.build_sql`): a key ABSENT from a handler's `Write.row` leaves
that column untouched. This is what proves the omission in
handlers/telemetry_latest.py actually protects operational_status, using
the same handler code the projector runs — not a reimplementation of it.

Time must be injectable per the spec's acceptance check 1: nothing here
sleeps; T0/T0+60s/T0+20min are all fixed datetimes.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from handlers import get_handler
from lifecycle_status import compute_reporting_status

STALE_AFTER_S = 30  # matches reporting_sweep.py's own default

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T0_PLUS_60S = T0 + timedelta(seconds=60)
T0_PLUS_20MIN = T0 + timedelta(minutes=20)

ATLANTIAN_IDS = [f"dis:1:1:{n}" for n in range(1000, 1008)]  # 8 assets
BORDURIAN_IDS = [f"dis:2:1:{n}" for n in range(1000, 1006)]  # 6 assets
ALL_IDS = ATLANTIAN_IDS + BORDURIAN_IDS  # 14 assets

STOPPED_ID = "dis:1:1:1003"    # stops transmitting at T0, no further signal
DESTROYED_ID = "dis:1:1:1005"  # sends damage=DESTROYED at T0+45s, then stops

TELEMETRY_LATEST = get_handler("telemetry_latest")


def _message(asset_id: str, sample_time: datetime, *, destroyed: bool = False) -> dict:
    """A decoded EntityTelemetryEvent for a DIS entity, matching the shape
    test_handlers.py's DIS-path tests use. `destroyed=True` is the
    health_state=HEALTH_STATE_FAILED collapse the sim's Bloblang mapping
    already performs for damage=DESTROYED (see lifecycle_status.py's
    module docstring for why that collapse, not a new wire field, is the
    real signal this slice reads)."""
    return {
        "asset": {"asset_id": asset_id, "force": "FORCE_FRIENDLY"},
        "operational_state": {
            "health_state": "HEALTH_STATE_FAILED" if destroyed else "HEALTH_STATE_NOMINAL",
        },
        "provenance": {
            "producer_id": "dis-ingestor-binary",
            "source_protocol": "DIS/IEEE-1278.1-binary",
            "sample_time": sample_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "schema_revision": 1,
    }


def _new_row() -> dict:
    # The column DEFAULTs a fresh INSERT gets for keys the handler omits
    # (schema.hcl: operational_status DEFAULT 'operational', reporting_status
    # DEFAULT 'reporting') — not a re-decision, a mirror of the DB default so
    # this simulation's baseline matches what Postgres would actually store.
    return {"operational_status": "operational", "reporting_status": "reporting",
            "last_sample_at": None}


def _apply_message(row: dict, asset_id: str, sample_time: datetime, *, destroyed: bool = False) -> None:
    write = TELEMETRY_LATEST(asset_id, _message(asset_id, sample_time, destroyed=destroyed))
    assert write is not None
    # Partial-column UPSERT: only keys present in write.row are touched —
    # exactly PostgresPool.build_sql's ON CONFLICT DO UPDATE SET semantics.
    row.update(write.row)


def _apply_staleness_sweep(row: dict, now: datetime) -> None:
    # Mirrors reporting_sweep.py: only ever moves reporting -> not_reporting,
    # never the reverse (recovery is the arrival path's job), and never
    # touches operational_status.
    if row["reporting_status"] == "not_reporting":
        return
    verdict = compute_reporting_status(row["last_sample_at"], now, STALE_AFTER_S)
    if verdict == "not_reporting":
        row["reporting_status"] = "not_reporting"
        row["reporting_status_at"] = now


def _build_fleet() -> dict[str, dict]:
    return {asset_id: _new_row() for asset_id in ALL_IDS}


def _counts(fleet: dict[str, dict]) -> dict[str, int]:
    return {
        "total": len(fleet),
        "operational": sum(1 for r in fleet.values() if r["operational_status"] == "operational"),
        "destroyed": sum(1 for r in fleet.values() if r["operational_status"] == "destroyed"),
        "reporting": sum(1 for r in fleet.values() if r["reporting_status"] == "reporting"),
        "not_reporting": sum(1 for r in fleet.values() if r["reporting_status"] == "not_reporting"),
    }


def _run_case1_up_to(checkpoint: datetime) -> dict[str, dict]:
    """Replay every message up through `checkpoint`, then sweep staleness
    at `checkpoint`. Every asset except STOPPED_ID/DESTROYED_ID keeps
    reporting on a steady cadence (well under STALE_AFTER_S) so the table's
    "everyone else is fine" baseline holds without asserting anything about
    them individually."""
    fleet = _build_fleet()

    # T0: everyone reports in, including the two assets that will later stop.
    for asset_id in ALL_IDS:
        _apply_message(fleet[asset_id], asset_id, T0)

    if checkpoint >= T0_PLUS_60S:
        # dis:1:1:1005 sends its damage=DESTROYED report partway through the
        # first minute, then goes quiet like dis:1:1:1003 already has.
        _apply_message(fleet[DESTROYED_ID], DESTROYED_ID, T0 + timedelta(seconds=45),
                        destroyed=True)
        # Everyone else keeps reporting normally (steady cadence <
        # STALE_AFTER_S) so they are still fresh at every checkpoint below.
        for asset_id in ALL_IDS:
            if asset_id in (STOPPED_ID, DESTROYED_ID):
                continue
            _apply_message(fleet[asset_id], asset_id, T0_PLUS_60S)
        for asset_id in ALL_IDS:
            _apply_staleness_sweep(fleet[asset_id], T0_PLUS_60S)

    if checkpoint >= T0_PLUS_20MIN:
        for asset_id in ALL_IDS:
            if asset_id in (STOPPED_ID, DESTROYED_ID):
                continue
            _apply_message(fleet[asset_id], asset_id, T0_PLUS_20MIN)
        for asset_id in ALL_IDS:
            _apply_staleness_sweep(fleet[asset_id], T0_PLUS_20MIN)

    return fleet


# -- Case 1's table, at each of the three timesteps --------------------------

def test_case1_t0_everyone_operational_and_reporting():
    fleet = _run_case1_up_to(T0)
    counts = _counts(fleet)
    assert counts == {"total": 14, "operational": 14, "destroyed": 0,
                       "reporting": 14, "not_reporting": 0}
    assert (fleet[DESTROYED_ID]["operational_status"],
            fleet[DESTROYED_ID]["reporting_status"]) == ("operational", "reporting")
    assert (fleet[STOPPED_ID]["operational_status"],
            fleet[STOPPED_ID]["reporting_status"]) == ("operational", "reporting")


def test_case1_t0_plus_60s_one_destroyed_one_not_reporting():
    fleet = _run_case1_up_to(T0_PLUS_60S)
    counts = _counts(fleet)
    assert counts == {"total": 14, "operational": 13, "destroyed": 1,
                       "reporting": 13, "not_reporting": 1}
    assert fleet[DESTROYED_ID]["operational_status"] == "destroyed"
    assert fleet[STOPPED_ID]["reporting_status"] == "not_reporting"


def test_case1_t0_plus_20min_destroyed_asset_now_also_not_reporting():
    fleet = _run_case1_up_to(T0_PLUS_20MIN)
    counts = _counts(fleet)
    assert counts == {"total": 14, "operational": 13, "destroyed": 1,
                       "reporting": 12, "not_reporting": 2}
    assert (fleet[DESTROYED_ID]["operational_status"],
            fleet[DESTROYED_ID]["reporting_status"]) == ("destroyed", "not_reporting")
    assert (fleet[STOPPED_ID]["operational_status"],
            fleet[STOPPED_ID]["reporting_status"]) == ("operational", "not_reporting")


def test_case1_fleet_total_is_14_at_every_timestep():
    """No deletes on the asset path (ADR §1) — a shrinking total means
    something was deleted, not just reclassified."""
    for checkpoint in (T0, T0_PLUS_60S, T0_PLUS_20MIN):
        assert len(_run_case1_up_to(checkpoint)) == 14


# -- The two assertions the spec says carry the whole design -----------------

def test_destroyed_and_reporting_pair_is_representable_at_t0_plus_60s():
    """ADR's 'whole argument': the second an asset tells you it was hit, it
    is destroyed AND still reporting. A one-field model cannot hold this
    pair; if this fails, the two columns are not actually independent and
    the slice has failed regardless of every other number in this file."""
    fleet = _run_case1_up_to(T0_PLUS_60S)
    row = fleet[DESTROYED_ID]
    assert row["operational_status"] == "destroyed"
    assert row["reporting_status"] == "reporting"


def test_silence_never_moves_operational_status_dis_1_1_1003():
    """dis:1:1:1003 was never reported damaged — it was merely not heard
    from. A test that lets silence move operational_status is asserting
    the eviction mistake ADR-0044 exists to forbid: this is the explicit
    negative assertion the spec asks for, at every timestep, not merely an
    absence of coverage."""
    for checkpoint in (T0, T0_PLUS_60S, T0_PLUS_20MIN):
        fleet = _run_case1_up_to(checkpoint)
        assert fleet[STOPPED_ID]["operational_status"] == "operational"
