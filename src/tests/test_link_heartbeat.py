"""Link heartbeat (edge side) and arrival-measured link_status (HQ side).

Written before the modules exist: the first run fails at import, which is
the RED this file records.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

import edge_buffer_monitor as ebm
import link_heartbeat as lh
import link_monitor as lm


# -- TrafficMeter ----------------------------------------------------------

def test_meter_unspecified_before_one_window():
    m = ebm.TrafficMeter(idle_after_s=30)
    assert m.observe(100.0, 10) == "UNSPECIFIED"
    assert m.observe(110.0, 20) == "UNSPECIFIED"  # advanced, but window not elapsed


def test_meter_active_on_advance_after_window():
    m = ebm.TrafficMeter(idle_after_s=30)
    m.observe(100.0, 10)
    assert m.observe(131.0, 11) == "ACTIVE"


def test_meter_idle_after_flat_window():
    m = ebm.TrafficMeter(idle_after_s=30)
    m.observe(100.0, 10)
    assert m.observe(120.0, 10) == "UNSPECIFIED"
    assert m.observe(130.5, 10) == "IDLE"


def test_meter_idle_then_active_again():
    m = ebm.TrafficMeter(idle_after_s=30)
    m.observe(0.0, 5)
    assert m.observe(31.0, 5) == "IDLE"
    assert m.observe(32.0, 6) == "ACTIVE"


def test_meter_none_is_unspecified_and_does_not_start_window():
    m = ebm.TrafficMeter(idle_after_s=30)
    assert m.observe(0.0, None) == "UNSPECIFIED"
    assert m.observe(100.0, 7) == "UNSPECIFIED"  # first reading starts the window
    assert m.observe(131.0, 7) == "IDLE"
    assert m.observe(132.0, None) == "UNSPECIFIED"


# -- codec -----------------------------------------------------------------

def test_codec_round_trip():
    ts = datetime(2026, 10, 8, 12, 0, 1, tzinfo=timezone.utc)
    raw = lh.encode_heartbeat("edge-01", ts, "IDLE", 42)
    hb = lh.decode_heartbeat(raw)
    assert hb.link_id == "edge-01"
    assert hb.emitted_at == ts
    assert hb.traffic == "IDLE"
    assert hb.bridge_lag == 42


def test_codec_unknown_lag_and_unspecified():
    raw = lh.encode_heartbeat("t1", datetime.now(timezone.utc), "UNSPECIFIED", -1)
    hb = lh.decode_heartbeat(raw)
    assert hb.traffic == "UNSPECIFIED" and hb.bridge_lag == -1


@pytest.mark.parametrize("raw", [b"\xff\xff\xff", b"", b"not protobuf at all"])
def test_decode_malformed_raises(raw):
    with pytest.raises(lh.MalformedHeartbeat):
        lh.decode_heartbeat(raw)


# -- classifier + tracker --------------------------------------------------

D = 15.0


def test_unknown_during_warmup():
    t = lm.LinkTracker(D, start=0.0)
    assert t.reach(10.0) == "unknown"
    assert lm.classify("unknown", "ACTIVE", False) == "unknown"


def test_warmup_then_down_without_heartbeat():
    t = lm.LinkTracker(D, start=0.0)
    assert t.reach(15.0) == "unknown"   # uptime <= down_after
    assert t.reach(15.1) == "down"


def test_fresh_up_idle():
    t = lm.LinkTracker(D, start=0.0)
    t.on_arrival(1.0)
    assert t.reach(2.0) == "fresh"
    assert lm.classify("fresh", "ACTIVE", False) == "up"
    assert lm.classify("fresh", "UNSPECIFIED", False) == "up"
    assert lm.classify("fresh", "IDLE", False) == "idle"


def test_stale_goes_down_strictly_after_threshold():
    t = lm.LinkTracker(D, start=0.0)
    t.on_arrival(1.0)
    assert t.reach(16.0) == "fresh"
    assert t.reach(16.1) == "down"


def test_restore_needs_three_fresh_arrivals_within_down_after():
    t = lm.LinkTracker(D, start=0.0, restore_arrivals=3, fresh_max_age_s=D)
    t.on_arrival(1.0, 0.5)
    assert t.reach(30.0) == "down"
    t.on_arrival(31.0, 0.5)             # first arrival after a long gap
    assert t.reach(31.5) == "down"
    t.on_arrival(33.0, 0.5)
    assert t.reach(33.5) == "down"
    t.on_arrival(35.0, 0.5)
    assert t.reach(35.5) == "fresh"


def test_restore_count_resets_on_wide_gap():
    t = lm.LinkTracker(D, start=0.0, restore_arrivals=3, fresh_max_age_s=D)
    t.on_arrival(1.0, 0.5)
    assert t.reach(30.0) == "down"
    t.on_arrival(31.0, 0.5)
    t.on_arrival(33.0, 0.5)
    t.on_arrival(50.0, 0.5)             # gap 17 > down_after: count restarts at 1
    assert t.reach(50.5) == "down"
    t.on_arrival(52.0, 0.5)
    assert t.reach(52.5) == "down"
    t.on_arrival(54.0, 0.5)
    assert t.reach(54.5) == "fresh"


def test_never_seen_after_warmup_needs_three_arrivals():
    t = lm.LinkTracker(D, start=0.0, restore_arrivals=3, fresh_max_age_s=D)
    assert t.reach(20.0) == "down"
    t.on_arrival(21.0, 0.5)
    t.on_arrival(23.0, 0.5)
    assert t.reach(23.5) == "down"
    t.on_arrival(25.0, 0.5)
    assert t.reach(25.5) == "fresh"


def test_late_arrival_without_tick_still_needs_restore():
    t = lm.LinkTracker(D, start=0.0)
    t.on_arrival(1.0)
    t.on_arrival(40.0)                  # no reach() ran in between
    assert t.reach(40.5) == "down"


def test_declared_idle_cases():
    assert lm.classify("fresh", "IDLE", True) == "idle"
    assert lm.classify("fresh", "UNSPECIFIED", True) == "idle"
    assert lm.classify("fresh", "ACTIVE", True) == "up"
    assert lm.classify("down", "ACTIVE", True) == "down"
    assert lm.classify("down", "IDLE", True) == "down"


# -- restore hysteresis (freshness by emitted_at) --------------------------

T0 = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)


class _Wall:
    """Injectable wall clock, the counterpart of the monotonic `now`."""

    def __init__(self) -> None:
        self.t = T0

    def __call__(self) -> datetime:
        return self.t


def _mon(wall):
    return lm.LinkState(["e1"], [], D, 0.0, wall_clock=wall)


def _hb(wall, age_s):
    """A heartbeat emitted `age_s` before the wall clock now (negative = future)."""
    return lh.encode_heartbeat("e1", wall.t - timedelta(seconds=age_s), "ACTIVE", 0)


def _state(mon, now):
    return mon.rows(now)[0].row["link_state"]


def _down_monitor():
    wall = _Wall()
    mon = _mon(wall)
    assert _state(mon, 20.0) == "down"      # never seen after warm-up
    return mon, wall


def test_backlog_burst_stays_down_then_fresh_restores():
    mon, wall = _down_monitor()
    for i, age in enumerate(range(60, 40, -2)):     # 10 heartbeats, 60..42 s old
        mon.ingest(_hb(wall, age), 21.0 + i * 0.01)
    assert _state(mon, 21.5) == "down"
    mon.ingest(_hb(wall, 0.5), 22.0)
    mon.ingest(_hb(wall, 0.5), 24.0)
    assert _state(mon, 24.5) == "down"
    mon.ingest(_hb(wall, 0.5), 26.0)
    assert _state(mon, 26.5) == "up"


def test_stale_between_fresh_resets_streak():
    mon, wall = _down_monitor()
    mon.ingest(_hb(wall, 0.5), 21.0)
    mon.ingest(_hb(wall, 0.5), 23.0)
    mon.ingest(_hb(wall, 50.0), 25.0)               # stale: streak back to 0
    mon.ingest(_hb(wall, 0.5), 27.0)
    mon.ingest(_hb(wall, 0.5), 29.0)
    assert _state(mon, 29.5) == "down"
    mon.ingest(_hb(wall, 0.5), 31.0)
    assert _state(mon, 31.5) == "up"


def test_clock_skew_tolerance_is_absolute():
    mon, wall = _down_monitor()
    for i in range(3):                              # child ahead by 5 s: fresh
        mon.ingest(_hb(wall, -5.0), 21.0 + 2 * i)
    assert _state(mon, 27.5) == "up"

    mon, wall = _down_monitor()
    for i in range(3):                              # child ahead by 20 s: not fresh
        mon.ingest(_hb(wall, -20.0), 21.0 + 2 * i)
    assert _state(mon, 27.5) == "down"


def test_missing_emitted_at_restores_by_arrival_and_warns_once(caplog):
    caplog.set_level(logging.WARNING, logger="projector.link_monitor")
    mon, _ = _down_monitor()
    raw = lh.encode_heartbeat("e1", datetime.fromtimestamp(0, timezone.utc), "ACTIVE", 0)
    for i in range(3):
        mon.ingest(raw, 21.0 + 2 * i)
    assert _state(mon, 27.5) == "up"
    warns = [r for r in caplog.records if "without emitted_at" in r.getMessage()]
    assert len(warns) == 1


def test_restore_logs_streak_max_age_and_skipped(caplog):
    caplog.set_level(logging.INFO, logger="projector.link_monitor")
    mon, wall = _down_monitor()
    for i in range(4):
        mon.ingest(_hb(wall, 60.0), 21.0 + i * 0.01)
    for i, age in enumerate((0.5, 1.5, 1.0)):
        mon.ingest(_hb(wall, age), 22.0 + 2 * i)
    msgs = [r.getMessage() for r in caplog.records if "restored" in r.getMessage()]
    assert len(msgs) == 1
    assert "e1" in msgs[0] and "streak=3" in msgs[0]
    assert "max_age_s=1.5" in msgs[0] and "stale_skipped=4" in msgs[0]


# -- row builder -----------------------------------------------------------

def test_row_unknown_never_seen():
    w = lm.build_row("edge-01", "unknown", None, False, now_mono=5.0)
    r = w.row
    assert w.table == "link_status" and w.mode == "upsert" and w.key_columns == ["id"]
    assert r["id"] == "edge-01" and r["link_state"] == "unknown"
    assert r["traffic"] == "unspecified"
    assert r["heartbeat_age_s"] is None and r["last_heartbeat_at"] is None
    assert r["bridge_lag"] == -1 and r["declared_idle"] is False
    assert isinstance(r["updated_at"], datetime)


def test_row_up_with_payload():
    ts = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    last = lm.LastSeen(arrival=10.0, emitted_at=ts, traffic="ACTIVE", bridge_lag=3)
    r = lm.build_row("edge-01", "up", last, False, now_mono=11.5).row
    assert r["link_state"] == "up" and r["traffic"] == "active"
    assert r["heartbeat_age_s"] == pytest.approx(1.5)
    assert r["last_heartbeat_at"] == ts and r["bridge_lag"] == 3


def test_row_idle_declared_and_down():
    ts = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    last = lm.LastSeen(arrival=0.0, emitted_at=ts, traffic="IDLE", bridge_lag=-1)
    idle = lm.build_row("e3", "idle", last, True, now_mono=2.0).row
    assert idle["link_state"] == "idle" and idle["traffic"] == "idle" and idle["declared_idle"] is True
    down = lm.build_row("e1", "down", last, False, now_mono=60.0).row
    assert down["link_state"] == "down" and down["heartbeat_age_s"] == pytest.approx(60.0)


# -- ingest: malformed / unexpected ---------------------------------------

def _val(c, **labels):
    return (c.labels(**labels) if labels else c)._value.get()


def test_ingest_counts_malformed_and_unexpected():
    from metrics import LINK_HEARTBEATS_REJECTED, LINK_HEARTBEATS_RECEIVED
    mon = lm.LinkState(expected=["edge-01"], declared_idle=[], down_after_s=D, start=0.0)
    m0 = _val(LINK_HEARTBEATS_REJECTED, reason="malformed")
    u0 = _val(LINK_HEARTBEATS_REJECTED, reason="unexpected")
    r0 = _val(LINK_HEARTBEATS_RECEIVED, link_id="edge-01")

    mon.ingest(b"\xff\xff", now_mono=1.0)
    mon.ingest(lh.encode_heartbeat("stranger", datetime.now(timezone.utc), "ACTIVE", 0), now_mono=1.0)
    mon.ingest(lh.encode_heartbeat("edge-01", datetime.now(timezone.utc), "ACTIVE", 0), now_mono=1.0)

    assert _val(LINK_HEARTBEATS_REJECTED, reason="malformed") == m0 + 1
    assert _val(LINK_HEARTBEATS_REJECTED, reason="unexpected") == u0 + 1
    assert _val(LINK_HEARTBEATS_RECEIVED, link_id="edge-01") == r0 + 1
    rows = mon.rows(now_mono=2.0)
    assert [w.row["id"] for w in rows] == ["edge-01"]
    assert rows[0].row["link_state"] == "up"


def test_rows_cover_every_expected_id_and_declared_idle():
    mon = lm.LinkState(expected=["a", "b"], declared_idle=["b"], down_after_s=D, start=0.0)
    mon.ingest(lh.encode_heartbeat("b", datetime.now(timezone.utc), "UNSPECIFIED", -1), now_mono=1.0)
    rows = {w.row["id"]: w.row for w in mon.rows(now_mono=2.0)}
    assert rows["a"]["link_state"] == "unknown"
    assert rows["b"]["link_state"] == "idle" and rows["b"]["declared_idle"] is True


# -- gating ----------------------------------------------------------------

def test_flags_default_off(monkeypatch):
    monkeypatch.delenv("LINK_HEARTBEAT_ENABLED", raising=False)
    monkeypatch.delenv("LINK_MONITOR_ENABLED", raising=False)
    assert ebm.link_heartbeat_enabled() is False
    assert lm.link_monitor_enabled() is False
    monkeypatch.setenv("LINK_HEARTBEAT_ENABLED", "true")
    monkeypatch.setenv("LINK_MONITOR_ENABLED", "TRUE")
    assert ebm.link_heartbeat_enabled() is True
    assert lm.link_monitor_enabled() is True


def test_heartbeat_not_started_when_off(monkeypatch):
    monkeypatch.delenv("LINK_HEARTBEAT_ENABLED", raising=False)

    async def go():
        return ebm.start_link_heartbeat()

    assert asyncio.run(go()) is None


def test_heartbeat_not_started_without_link_id(monkeypatch, caplog):
    monkeypatch.setenv("LINK_HEARTBEAT_ENABLED", "true")
    monkeypatch.setattr(ebm, "LINK_ID", "")

    async def go():
        return ebm.start_link_heartbeat()

    assert asyncio.run(go()) is None
    assert any("LINK_ID" in r.getMessage() for r in caplog.records)


def test_main_starts_neither_when_flags_off(monkeypatch):
    import main
    started = []

    async def fake(pool):
        started.append(1)

    monkeypatch.setattr(main, "edge_buffer_loop", fake)
    monkeypatch.setattr(main, "link_monitor_loop", fake)
    monkeypatch.setenv("BUFFER_MONITOR_ENABLED", "false")
    monkeypatch.delenv("LINK_MONITOR_ENABLED", raising=False)

    async def go():
        tasks = main.start_monitor_tasks(object())
        await asyncio.gather(*tasks)
        return tasks

    assert asyncio.run(go()) == []
    assert started == []

    monkeypatch.setenv("LINK_MONITOR_ENABLED", "true")
    assert len(asyncio.run(go())) == 1 and started == [1]
