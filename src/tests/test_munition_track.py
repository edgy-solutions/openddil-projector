"""Tests for the munition_track handler, its Fire back-fill, the Detonation
terminal and the silence sweep (ADR-0049).

Same discipline as test_effector_launch.py: no live Postgres. `_TrackPool`
is an in-memory stand-in that mirrors the decisions the real pool methods
make in SQL (insert-with-join, newer-vs-older sample, terminal is final,
silent is overwritable), so these tests pin the handler wiring and the
decision table; the SQL text itself is pinned separately below.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import metrics
from handlers import effector_launch, munition_track
from persistence import PostgresPool, Write

T0 = datetime(2026, 10, 15, 12, 0, 0, tzinfo=timezone.utc)
URN = "dis:1:1:5001"


class _TrackPool:
    def __init__(self):
        self.tracks: dict[str, dict] = {}
        self.launches: list[dict] = []

    # -- calls effector_launch.py makes before the track side ----------------
    async def row_exists(self, table, column, value):
        return True

    async def execute(self, write: Write):
        r = write.row
        self.launches.append({**r, "terminal_state": None})

    async def apply_effector_detonation(self, **kw):
        for ln in self.launches:
            if ln["event_urn"] == kw["event_urn"]:
                return "updated", None
        return "no_fire", None

    # -- the munition_track methods ------------------------------------------
    async def upsert_munition_track(self, *, munition_asset_id, munition_type,
                                    ecef, sample_time, edge_id, region_id, now):
        t = self.tracks.get(munition_asset_id)
        if t is None:
            mine = [ln for ln in self.launches
                    if ln.get("munition_asset_id") == munition_asset_id]
            ln = max(mine, key=lambda r: r["launched_at"]) if mine else None
            self.tracks[munition_asset_id] = {
                "launcher_asset_id": ln["launcher_asset_id"] if ln else None,
                "event_urn": ln["event_urn"] if ln else None,
                "munition_type": munition_type,
                "ecef": ecef,
                "first_seen_at": sample_time,
                "last_sample_at": sample_time,
                "last_heard_at": now,
                "terminal_state": None,
                "detonation_result": None,
                "terminated_at": None,
                "late_terminal": False,
                "edge_id": (ln or {}).get("edge_id") or edge_id,
                "region_id": (ln or {}).get("region_id") or region_id,
                "originator_nation": (ln or {}).get("originator_nation"),
                "releasable_to": (ln or {}).get("releasable_to"),
            }
            return "inserted", ln is not None
        unlabelled = t["launcher_asset_id"] is None
        if t["terminal_state"] is not None:
            return "after_terminal", unlabelled and self._heal(munition_asset_id)
        t["last_heard_at"] = now
        if sample_time > t["last_sample_at"]:
            t["ecef"] = ecef
            t["last_sample_at"] = sample_time
            return "updated", unlabelled and self._heal(munition_asset_id)
        return "stale", unlabelled and self._heal(munition_asset_id)

    async def join_munition_track_launcher(self, *, munition_asset_id,
                                           launcher_asset_id, event_urn,
                                           edge_id, region_id,
                                           originator_nation, releasable_to,
                                           now):
        t = self.tracks.get(munition_asset_id)
        if t is None or t["launcher_asset_id"] is not None:
            return 0
        t.update(launcher_asset_id=launcher_asset_id, event_urn=event_urn,
                 edge_id=edge_id or t["edge_id"],
                 region_id=region_id or t["region_id"],
                 originator_nation=originator_nation,
                 releasable_to=releasable_to)
        return 1

    async def terminate_munition_track(self, *, munition_asset_id, event_urn,
                                       terminal_state, detonation_result,
                                       terminated_at, now):
        key = munition_asset_id
        if not key:
            key = next((ln.get("munition_asset_id") for ln in self.launches
                        if ln["event_urn"] == event_urn), None)
        t = self.tracks.get(key) if key else None
        if t is None:
            return "skipped"
        self._heal(key)
        if t["terminal_state"] not in (None, "silent"):
            return "replayed"
        late = t["terminal_state"] == "silent"
        t.update(terminal_state=terminal_state,
                 detonation_result=detonation_result,
                 terminated_at=terminated_at, late_terminal=late)
        return "late" if late else "terminated"

    async def heal_munition_track_launchers(self, *, now):
        n = 0
        for key, t in self.tracks.items():
            if t["launcher_asset_id"] is None and self._heal(key):
                n += 1
        return n

    def _heal(self, key):
        t = self.tracks.get(key)
        mine = [ln for ln in self.launches
                if ln.get("munition_asset_id") == key]
        if t is None or t["launcher_asset_id"] is not None or not mine:
            return False
        ln = max(mine, key=lambda r: r["launched_at"])
        t.update(launcher_asset_id=ln["launcher_asset_id"],
                 event_urn=ln["event_urn"],
                 edge_id=ln.get("edge_id") or t["edge_id"],
                 region_id=ln.get("region_id") or t["region_id"],
                 originator_nation=ln.get("originator_nation"),
                 releasable_to=ln.get("releasable_to"))
        return True

    async def sweep_munition_track_silence(self, *, silence_s, now):
        n = 0
        for t in self.tracks.values():
            if (t["terminal_state"] is None
                    and t["last_heard_at"] < now - timedelta(seconds=silence_s)):
                t.update(terminal_state="silent", terminated_at=now)
                n += 1
        return n


def _rec(urn=URN, t=T0, x=1.0, **over):
    rec = {
        "munition_urn": urn,
        "munition_type": {"kind": 2, "domain": 9, "country": 225,
                          "category": 2, "subcategory": 1, "specific": 1,
                          "extra": 0},
        "location_ecef": {"x": x, "y": 2.0, "z": 3.0},
        "linear_velocity_ecef": {"x": 0.0, "y": 0.0, "z": 0.0},
        "force_id": 1,
        "marking": "M1",
        "sample_time": t.isoformat(),
        "provenance": {"edge_id": "edge-01", "region_id": "region-01"},
    }
    rec.update(over)
    return rec


def _fire(urn=URN, event="dis-event:1:58:1", launcher="dis:1:58:1001"):
    return {
        "pdu_type": "fire", "event_urn": event, "launcher_urn": launcher,
        "munition_urn": urn, "target_urn": None,
        "munition_type": {"kind": 2, "domain": 9, "country": 225,
                          "category": 2, "subcategory": 1, "specific": 1,
                          "extra": 0},
        "quantity": 1, "ingest_timestamp": "2026-10-15T11:59:00Z",
        "provenance": {"edge_id": "edge-fire", "region_id": "region-fire",
                       "originator_nation": "ATL", "releasable_to": ["BDR"]},
    }


def _det(event="dis-event:1:58:1", urn=URN, result=1):
    return {
        "pdu_type": "detonation", "event_urn": event, "munition_urn": urn,
        "detonation_result": result, "ingest_timestamp": "2026-10-15T12:00:30Z",
        "provenance": {"edge_id": "edge-01", "region_id": "region-01"},
    }


def _count(counter, **labels):
    return counter.labels(**labels)._value.get()


# -- upsert ------------------------------------------------------------------

async def test_insert_without_launch_row_leaves_launcher_null():
    pool = _TrackPool()
    before = _count(metrics.MUNITION_TRACK_UPSERTS, outcome="inserted")
    await munition_track.handle(URN, _rec(), pool)
    t = pool.tracks[URN]
    assert t["launcher_asset_id"] is None and t["event_urn"] is None
    assert t["munition_type"] == "2.9.225.2.1.1.0"
    assert t["edge_id"] == "edge-01"
    assert t["originator_nation"] is None
    assert _count(metrics.MUNITION_TRACK_UPSERTS, outcome="inserted") == before + 1


async def test_insert_with_launch_row_copies_launcher_and_labels():
    pool = _TrackPool()
    await effector_launch.handle("k", _fire(), pool)
    before = _count(metrics.MUNITION_TRACK_JOINED, via="track")
    await munition_track.handle(URN, _rec(), pool)
    t = pool.tracks[URN]
    assert t["launcher_asset_id"] == "dis:1:58:1001"
    assert t["event_urn"] == "dis-event:1:58:1"
    assert t["edge_id"] == "edge-fire"
    assert t["originator_nation"] == "ATL"
    assert t["releasable_to"] == ["BDR"]
    assert _count(metrics.MUNITION_TRACK_JOINED, via="track") == before + 1


async def test_fire_after_track_backfills_launcher():
    pool = _TrackPool()
    await munition_track.handle(URN, _rec(), pool)
    assert pool.tracks[URN]["launcher_asset_id"] is None
    before = _count(metrics.MUNITION_TRACK_JOINED, via="fire")
    await effector_launch.handle("k", _fire(), pool)
    t = pool.tracks[URN]
    assert t["launcher_asset_id"] == "dis:1:58:1001"
    assert t["releasable_to"] == ["BDR"]
    assert _count(metrics.MUNITION_TRACK_JOINED, via="fire") == before + 1
    # A second Fire cannot re-home an already-joined track.
    await effector_launch.handle("k", _fire(event="dis-event:1:58:2",
                                            launcher="dis:1:58:9999"), pool)
    assert pool.tracks[URN]["launcher_asset_id"] == "dis:1:58:1001"
    assert _count(metrics.MUNITION_TRACK_JOINED, via="fire") == before + 1


async def test_newer_sample_advances_older_only_touches_heard():
    pool = _TrackPool()
    await munition_track.handle(URN, _rec(t=T0, x=1.0), pool)
    await munition_track.handle(URN, _rec(t=T0 + timedelta(seconds=1), x=5.0), pool)
    t = pool.tracks[URN]
    assert t["ecef"][0] == 5.0
    heard = t["last_heard_at"]
    before = _count(metrics.MUNITION_TRACK_UPSERTS, outcome="stale")
    await munition_track.handle(URN, _rec(t=T0 - timedelta(seconds=5), x=9.0), pool)
    assert t["ecef"][0] == 5.0
    assert t["last_sample_at"] == T0 + timedelta(seconds=1)
    assert t["last_heard_at"] >= heard
    assert _count(metrics.MUNITION_TRACK_UPSERTS, outcome="stale") == before + 1


async def test_sample_after_terminal_writes_nothing():
    pool = _TrackPool()
    await munition_track.handle(URN, _rec(), pool)
    await effector_launch.handle("k", _det(), pool)
    snap = dict(pool.tracks[URN])
    before = _count(metrics.MUNITION_TRACK_UPSERTS, outcome="after_terminal")
    await munition_track.handle(URN, _rec(t=T0 + timedelta(seconds=9), x=7.0), pool)
    assert pool.tracks[URN] == snap
    assert _count(metrics.MUNITION_TRACK_UPSERTS, outcome="after_terminal") == before + 1


@pytest.mark.parametrize("over,reason", [
    ({"munition_urn": None}, "no_munition_urn"),
    ({"location_ecef": None}, "no_location"),
    ({"location_ecef": {"x": 1.0, "y": 2.0}}, "no_location"),
    ({"sample_time": "not-a-time"}, "bad_sample_time"),
    ({"sample_time": None}, "bad_sample_time"),
])
async def test_malformed_record_refused(over, reason):
    pool = _TrackPool()
    before = _count(metrics.MUNITION_TRACK_REFUSED, reason=reason)
    await munition_track.handle("k", _rec(**over), pool)
    assert pool.tracks == {}
    assert _count(metrics.MUNITION_TRACK_REFUSED, reason=reason) == before + 1


# -- detonation terminal -----------------------------------------------------

async def test_detonation_terminates_by_munition_urn():
    pool = _TrackPool()
    await munition_track.handle(URN, _rec(), pool)
    before = _count(metrics.MUNITION_TRACK_TERMINAL, state="ground_impact")
    # No matching Fire row at all: effector_launch says no_fire, the track
    # still terminates.
    await effector_launch.handle("k", _det(result=3), pool)
    t = pool.tracks[URN]
    assert t["terminal_state"] == "ground_impact"
    assert t["detonation_result"] == 3
    assert t["terminated_at"] is not None and t["late_terminal"] is False
    assert _count(metrics.MUNITION_TRACK_TERMINAL, state="ground_impact") == before + 1


async def test_detonation_terminates_via_event_urn_fallback():
    pool = _TrackPool()
    await effector_launch.handle("k", _fire(), pool)
    await munition_track.handle(URN, _rec(), pool)
    await effector_launch.handle("k", _det(urn=None, result=1), pool)
    assert pool.tracks[URN]["terminal_state"] == "entity_impact"


async def test_detonation_with_no_key_is_skipped():
    pool = _TrackPool()
    await munition_track.handle(URN, _rec(), pool)
    await effector_launch.handle("k", _det(urn=None, event="dis-event:9:9:9"), pool)
    assert pool.tracks[URN]["terminal_state"] is None


async def test_detonation_over_silent_sets_late_terminal():
    pool = _TrackPool()
    await munition_track.handle(URN, _rec(), pool)
    await pool.sweep_munition_track_silence(
        silence_s=12, now=pool.tracks[URN]["last_heard_at"] + timedelta(seconds=60))
    assert pool.tracks[URN]["terminal_state"] == "silent"
    await effector_launch.handle("k", _det(result=1), pool)
    t = pool.tracks[URN]
    assert t["terminal_state"] == "entity_impact"
    assert t["late_terminal"] is True


async def test_detonation_replay_writes_nothing():
    pool = _TrackPool()
    await munition_track.handle(URN, _rec(), pool)
    await effector_launch.handle("k", _det(result=1), pool)
    snap = dict(pool.tracks[URN])
    before = _count(metrics.MUNITION_TRACK_TERMINAL, state="entity_impact")
    await effector_launch.handle("k", _det(result=1), pool)
    await effector_launch.handle("k", _det(result=3), pool)  # different claim: still nothing
    assert pool.tracks[URN] == snap
    assert _count(metrics.MUNITION_TRACK_TERMINAL, state="entity_impact") == before


# -- silence sweep -----------------------------------------------------------

async def test_sweep_marks_only_stale_nonterminal_and_spares_effector_launch():
    pool = _TrackPool()
    await effector_launch.handle("k", _fire(), pool)
    launch_before = [dict(ln) for ln in pool.launches]
    now = T0 + timedelta(seconds=100)
    for urn, heard, state in [
        ("dis:1:1:6001", now - timedelta(seconds=60), None),   # stale, in flight
        ("dis:1:1:6002", now - timedelta(seconds=2), None),    # fresh
        ("dis:1:1:6003", now - timedelta(seconds=60), "dud"),  # stale but terminal
    ]:
        pool.tracks[urn] = {"last_heard_at": heard, "terminal_state": state,
                            "terminated_at": None}
    assert await pool.sweep_munition_track_silence(silence_s=12, now=now) == 1
    assert pool.tracks["dis:1:1:6001"]["terminal_state"] == "silent"
    assert pool.tracks["dis:1:1:6002"]["terminal_state"] is None
    assert pool.tracks["dis:1:1:6003"]["terminal_state"] == "dud"
    assert pool.launches == launch_before


def test_sweep_sql_reads_only_arrival_clock_and_only_munition_track():
    sql = PostgresPool.build_munition_track_silence_sweep_sql()
    assert sql.startswith('UPDATE "munition_track"')
    assert '"last_heard_at" <' in sql
    assert '"terminal_state" IS NULL' in sql
    assert "last_sample_at" not in sql
    assert "effector_launch" not in sql
    assert "DELETE" not in sql.upper()


def test_terminal_vocabulary_is_effector_map_plus_silent():
    """The stack migration's CHECK allows exactly these states."""
    states = {effector_launch.terminal_state_for(c) for c in range(-1, 8)}
    assert states | {"silent"} == {"entity_impact", "ground_impact", "detonated",
                                    "dud", "other", "silent"}


# -- launcher heal: a missed join converges (race outcome) --------------------

def _race(pool, *, urn=URN, launcher="dis:1:58:1001", event="dis-event:1:58:1"):
    """A track row with a NULL launcher and a launch row present, as if the
    insert's join and the Fire's back-fill both missed. Built by hand, never
    through the join paths."""
    pool.tracks[urn] = {
        "launcher_asset_id": None, "event_urn": None,
        "munition_type": "2.9.225.2.1.1.0", "ecef": (1.0, 2.0, 3.0),
        "first_seen_at": T0, "last_sample_at": T0,
        "last_heard_at": T0, "terminal_state": None,
        "detonation_result": None, "terminated_at": None,
        "late_terminal": False, "edge_id": "edge-01", "region_id": "region-01",
        "originator_nation": None, "releasable_to": None,
    }
    pool.launches.append({
        "event_urn": event, "launcher_asset_id": launcher,
        "munition_asset_id": urn, "launched_at": T0 - timedelta(seconds=1),
        "edge_id": "edge-fire", "region_id": "region-fire",
        "originator_nation": "ATL", "releasable_to": ["BDR"],
        "terminal_state": None,
    })


def _labelled(t):
    assert t["launcher_asset_id"] == "dis:1:58:1001"
    assert t["event_urn"] == "dis-event:1:58:1"
    assert t["edge_id"] == "edge-fire" and t["region_id"] == "region-fire"
    assert t["originator_nation"] == "ATL"
    assert t["releasable_to"] == ["BDR"]


async def test_heal_on_newer_sample():
    pool = _TrackPool()
    _race(pool)
    before = _count(metrics.MUNITION_TRACK_JOINED, via="track")
    await munition_track.handle(URN, _rec(t=T0 + timedelta(seconds=1)), pool)
    _labelled(pool.tracks[URN])
    assert _count(metrics.MUNITION_TRACK_JOINED, via="track") == before + 1


async def test_heal_on_stale_sample():
    pool = _TrackPool()
    _race(pool)
    before = _count(metrics.MUNITION_TRACK_JOINED, via="track")
    await munition_track.handle(URN, _rec(t=T0 - timedelta(seconds=5)), pool)
    _labelled(pool.tracks[URN])
    assert _count(metrics.MUNITION_TRACK_JOINED, via="track") == before + 1


async def test_heal_on_sample_after_terminal_keeps_state():
    pool = _TrackPool()
    _race(pool)
    pool.tracks[URN].update(terminal_state="dud", terminated_at=T0)
    await munition_track.handle(URN, _rec(t=T0 + timedelta(seconds=9)), pool)
    t = pool.tracks[URN]
    _labelled(t)
    assert t["terminal_state"] == "dud" and t["terminated_at"] == T0
    assert t["last_sample_at"] == T0


async def test_heal_on_detonation():
    pool = _TrackPool()
    _race(pool)
    await effector_launch.handle("k", _det(result=1), pool)
    t = pool.tracks[URN]
    assert t["terminal_state"] == "entity_impact"
    _labelled(t)


async def test_sweep_pass_labels_terminal_null_row_and_never_rehomes():
    from munition_track_sweep import sweep_pass
    pool = _TrackPool()
    _race(pool)                                   # terminal, NULL launcher
    pool.tracks[URN].update(terminal_state="dud", terminated_at=T0)
    other = "dis:1:1:5002"
    _race(pool, urn=other, launcher="dis:1:58:2001", event="dis-event:1:58:2")
    pool.tracks[other]["launcher_asset_id"] = "dis:1:58:1111"   # joined to A
    pool.tracks[other]["event_urn"] = "dis-event:1:58:7"
    pool.launches[-1]["launched_at"] = T0 + timedelta(seconds=30)  # newer, names B
    before = _count(metrics.MUNITION_TRACK_JOINED, via="heal")
    await sweep_pass(pool)
    t = pool.tracks[URN]
    _labelled(t)
    assert t["terminal_state"] == "dud"
    assert pool.tracks[other]["launcher_asset_id"] == "dis:1:58:1111"
    assert pool.tracks[other]["event_urn"] == "dis-event:1:58:7"
    assert _count(metrics.MUNITION_TRACK_JOINED, via="heal") == before + 1


async def test_null_row_with_no_launch_stays_null():
    from munition_track_sweep import sweep_pass
    pool = _TrackPool()
    await munition_track.handle(URN, _rec(), pool)
    await munition_track.handle(URN, _rec(t=T0 + timedelta(seconds=1)), pool)
    await effector_launch.handle("k", _det(result=1), pool)
    await sweep_pass(pool)
    t = pool.tracks[URN]
    assert t["launcher_asset_id"] is None and t["event_urn"] is None
    assert t["edge_id"] == "edge-01"


def test_heal_sql_shape():
    for scoped in (True, False):
        sql = PostgresPool.build_munition_track_launcher_heal_sql(scoped)
        assert sql.startswith('UPDATE "munition_track"')
        assert "effector_launch" in sql
        assert "DISTINCT ON" in sql
        assert '"launcher_asset_id" IS NULL' in sql
        assert "DELETE" not in sql.upper()
        assert sql.count("UPDATE") == 1 and "INSERT" not in sql.upper()
        assert ('"munition_asset_id" = $1' in sql) is scoped
    assert "$2" in PostgresPool.build_munition_track_launcher_heal_sql(True)
    assert "$2" not in PostgresPool.build_munition_track_launcher_heal_sql(False)
