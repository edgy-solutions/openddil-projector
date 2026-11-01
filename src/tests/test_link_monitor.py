"""link_status is written on change, with a slow liveness heartbeat; the
traffic band is held before it changes the row. Pure: no Kafka, no postgres."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import link_heartbeat as lh
import link_monitor as lm

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
D = 15.0
HB = 30.0
HOLD = 10.0


class _Wall:
    t = T0

    def __call__(self):
        return self.t


def _hb(link, traffic="ACTIVE"):
    return lh.encode_heartbeat(link, T0, traffic, 0)


def _mon(declared=(), links=("e1",)):
    return lm.LinkState(list(links), list(declared), D, 0.0, wall_clock=_Wall(), traffic_hold_s=HOLD)


def _drive(mon, planner, ticks, feed):
    """Tick every 2 s over `ticks`; `feed(t)` returns {link: traffic} heartbeats
    arriving at t (before the tick). Returns [(t, link, kind, write)]."""
    out = []
    for t in ticks:
        for link, traffic in feed(t).items():
            mon.ingest(_hb(link, traffic), t)
        for built in mon.rows(t):
            plan = planner.plan(built, t)
            if plan is None:
                continue
            kind, write = plan
            planner.record(built, kind, t)
            out.append((t, built.row["id"], kind, write))
    return out


def test_steady_60s_one_change_write_then_heartbeats_at_30_and_60():
    mon, planner = _mon(), lm.LinkWritePlanner(HB)
    ev = _drive(mon, planner, range(0, 61, 2), lambda t: {"e1": "ACTIVE"})
    assert [(t, k) for t, _, k, _ in ev] == [(0, "change"), (30, "heartbeat"), (60, "heartbeat")]


def test_heartbeat_write_columns_are_exactly_the_four():
    mon, planner = _mon(), lm.LinkWritePlanner(HB)
    ev = _drive(mon, planner, range(0, 33, 2), lambda t: {"e1": "ACTIVE"})
    w = [w for _, _, k, w in ev if k == "heartbeat"][0]
    assert w.mode == "update" and w.key_columns == ["id"]
    assert set(w.row) == {"id", "updated_at", "last_heartbeat_at", "heartbeat_age_s", "bridge_lag"}
    full = [w for _, _, k, w in ev if k == "change"][0]
    assert full.mode == "upsert"


def test_reachability_flip_is_a_change_write_on_that_tick():
    mon, planner = _mon(), lm.LinkWritePlanner(HB)
    ev = _drive(mon, planner, range(0, 41, 2), lambda t: {"e1": "ACTIVE"} if t <= 10 else {})
    changes = [(t, w.row["link_state"]) for t, _, k, w in ev if k == "change"]
    assert changes[0] == (0, "up")
    # last arrival at 10; down once silence exceeds D (15) -> first tick > 25
    assert changes[1][1] == "down" and changes[1][0] == 26
    assert len(changes) == 2


def test_single_unspecified_between_active_is_not_a_change():
    mon, planner = _mon(), lm.LinkWritePlanner(HB)
    seq = {0: "ACTIVE", 2: "ACTIVE", 4: "UNSPECIFIED", 6: "ACTIVE", 8: "ACTIVE", 20: "ACTIVE"}
    ev = _drive(mon, planner, range(0, 21, 2), lambda t: {"e1": seq[t]} if t in seq else {})
    assert [k for _, _, k, _ in ev] == ["change"]
    assert ev[0][3].row["traffic"] == "active"


def test_active_then_idle_held_past_hold_is_one_change_write():
    mon, planner = _mon(), lm.LinkWritePlanner(HB)
    ev = _drive(mon, planner, range(0, 41, 2),
                lambda t: {"e1": "ACTIVE" if t < 10 else "IDLE"})
    changes = [(t, w.row["traffic"], w.row["link_state"]) for t, _, k, w in ev if k == "change"]
    assert changes[0][1:] == ("active", "up")
    # IDLE first reported at 10, held at >= 20
    assert changes[1] == (20, "idle", "idle")
    assert len(changes) == 2


def test_idle_shorter_than_hold_never_changes_row():
    mon, planner = _mon(), lm.LinkWritePlanner(HB)
    seq = lambda t: {"e1": "IDLE" if 8 <= t <= 14 else "ACTIVE"}
    ev = _drive(mon, planner, range(0, 31, 2), seq)
    assert [k for _, _, k, _ in ev if k == "change"] == ["change"]


def test_declared_idle_classifies_on_held_band():
    mon = _mon(declared=["e1"])
    mon.ingest(_hb("e1", "ACTIVE"), 1.0)
    assert mon.rows(2.0)[0].row["link_state"] == "up"
    mon.ingest(_hb("e1", "UNSPECIFIED"), 3.0)   # inside the hold: still ACTIVE
    assert mon.rows(4.0)[0].row["link_state"] == "up"
    for t in (5.0, 7.0, 9.0, 11.0, 13.5):
        mon.ingest(_hb("e1", "UNSPECIFIED"), t)
    row = mon.rows(14.0)[0].row                  # held 11 s: now idle
    assert row["link_state"] == "idle" and row["declared_idle"] is True


def test_failed_write_is_retried_as_due_kind():
    planner = lm.LinkWritePlanner(HB)
    mon = _mon()
    mon.ingest(_hb("e1"), 0.0)
    built = mon.rows(0.0)[0]
    assert planner.plan(built, 0.0)[0] == "change"
    # write failed: not recorded, so the next tick is still a change write
    assert planner.plan(mon.rows(2.0)[0], 2.0)[0] == "change"
    planner.record(built, "change", 2.0)
    assert planner.plan(mon.rows(4.0)[0], 4.0) is None
    planner.forget("e1")                          # update matched no row
    assert planner.plan(mon.rows(6.0)[0], 6.0)[0] == "change"
