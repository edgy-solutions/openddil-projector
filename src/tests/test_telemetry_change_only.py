"""Change-only writes for telemetry_latest_state (opt-in; off is today's output).

The handler keeps a module-level cache and reads its env through
`telemetry_latest.configure()`, so every test runs inside `cfg`, which clears
the cache and re-reads the (restored) environment afterwards.
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import pytest
from prometheus_client import REGISTRY

import reporting_sweep
from handlers import telemetry_latest as tl
from persistence import Revive, Write

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
M_PER_DEG_LAT = 6371.0088 * 1000.0 * 3.141592653589793 / 180.0  # matches great_circle_km


class Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return T0 + timedelta(seconds=self.t)


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(tl, "now_utc", c.now)
    return c


@pytest.fixture
def cfg(monkeypatch):
    """Returns enable(h, deadband=None). Environment starts with the feature off."""
    monkeypatch.delenv("TELEMETRY_CHANGE_ONLY_HEARTBEAT_S", raising=False)
    monkeypatch.delenv("TELEMETRY_POSITION_DEADBAND_M", raising=False)
    tl.configure()
    tl._cache.clear()

    def enable(h, deadband=None):
        monkeypatch.setenv("TELEMETRY_CHANGE_ONLY_HEARTBEAT_S", str(h))
        if deadband is not None:
            monkeypatch.setenv("TELEMETRY_POSITION_DEADBAND_M",
                               deadband if isinstance(deadband, str) else json.dumps(deadband))
        tl.configure()

    yield enable
    monkeypatch.undo()
    tl.configure()
    tl._cache.clear()


def msg(asset="A1", t=0.0, north_m=0.0, variant="V1", health="HEALTH_STATE_OK",
        prov=None, op=None, kin=True, altitude=None):
    position = {"wgs84": {"lat": {"value": 50.0 + north_m / M_PER_DEG_LAT, "unit": "deg"},
                          "lon": {"value": 4.0, "unit": "deg"}}}
    if altitude is not None:
        position["wgs84"]["alt"] = {"value": altitude, "unit": "m"}
    operational = {"health_state": health, **(op or {})}
    d = {
        "asset": {"asset_id": asset, "platform_variant": variant,
                  "callsign": "C", "force": "FORCE_FRIENDLY"},
        "operational_state": operational,
        "provenance": {"edge_id": "edge-a", "region_id": "region-a",
                       "producer_id": "p1",
                       "sample_time": (T0 + timedelta(seconds=t)).isoformat(),
                       **(prov or {})},
        "schema_revision": 2,
    }
    if kin:
        d["kinematics"] = {"position": position, "speed": {"value": 1.0}}
    return d


def sent(clock, t, **kw):
    clock.t = t
    return tl.handle(kw.get("asset", "A1"), msg(t=t, **kw))


def _count(reason):
    return REGISTRY.get_sample_value(
        "projector_telemetry_writes_suppressed_total", {"reason": reason}) or 0.0


# -- off is today ------------------------------------------------------------

def test_off_returns_a_write_per_message_identical_to_the_plain_row(cfg, clock):
    writes = [sent(clock, 5.0 * i) for i in range(50)]
    assert all(w is not None for w in writes)
    for i, w in enumerate(writes):
        now = T0 + timedelta(seconds=5.0 * i)
        m = msg(t=5.0 * i)
        expected = Write(
            table="telemetry_latest_state", mode="upsert", key_columns=["asset_id"],
            row={
                "asset_id": "A1", "edge_id": "edge-a", "region_id": "region-a",
                "platform_variant": "V1", "callsign": "C", "subsystem": None,
                "force_id": "FORCE_FRIENDLY",
                "kinematics": m["kinematics"], "sustainment": None,
                "provenance": m["provenance"], "last_sample_at": now,
                "schema_revision": 2,
                "power_state": None, "functional_mode": None,
                "health_state": "HEALTH_STATE_OK",
                "actively_receiving": None, "actively_transmitting": None,
                "posture_status": "unspecified", "posture_since": None,
                "reporting_status": "reporting", "reporting_status_at": now,
                "updated_at": now,
            },
            jsonb_columns={"kinematics", "sustainment", "provenance"},
            revive=Revive(column="operational_status",
                          from_values=("deactivated",), to_value="operational",
                          at_column="operational_status_at", at_from="updated_at"),
        )
        got = w
        # releasability keys come from the same helper in both; compare the rest.
        for k in ("releasable_to", "originator_nation"):
            if k in got.row:
                expected.row[k] = got.row[k]
        assert got == expected
    assert tl._cache == {}


# -- old versus new on a stream ------------------------------------------------

H = 15.0
GAP = 5.0
N_MSG = 720  # one hour at 5 s: t = 0, 5, ..., 3595
STATIONARY = [f"S{i}" for i in range(6)]
MOVERS = ["M0", "M1"]
JITTER = "J0"
CHANGER = "C0"
ASSETS = STATIONARY + MOVERS + [JITTER, CHANGER]


def _stream_msg(asset, k):
    t = GAP * k
    if asset in MOVERS:
        return msg(asset, t, north_m=20.0 * k)
    if asset == JITTER:
        return msg(asset, t, north_m=0.5 * (k % 2))
    if asset == CHANGER:
        return msg(asset, t, health="HEALTH_STATE_DEGRADED" if t >= 1800 else "HEALTH_STATE_OK")
    return msg(asset, t)


def _apply(store, w):
    store.setdefault(w.row["asset_id"], {}).update(copy.deepcopy(w.row))


def _run(clock, check_age=None):
    store: dict = {}
    counts = {a: 0 for a in ASSETS}
    for k in range(N_MSG):
        clock.t = GAP * k
        for a in ASSETS:
            if check_age is not None and a in store:
                age = (clock.now() - store[a]["last_sample_at"]).total_seconds()
                assert age <= check_age, (a, k, age)
            w = tl.handle(a, _stream_msg(a, k))
            if w is not None:
                counts[a] += 1
                _apply(store, w)
    return store, counts


def test_stream_old_versus_new(cfg, clock):
    old_store, old_counts = _run(clock)
    assert sum(old_counts.values()) == 7200                      # (a)
    assert all(c == N_MSG for c in old_counts.values())

    tl._cache.clear()
    cfg(H, {"*": 2.0})
    sup_u, sup_d = _count("unchanged"), _count("deadband")
    new_store, new_counts = _run(clock, check_age=H + GAP)       # (d) stored age <= H + 5 s

    # (b) Derivation. Messages for an asset sit at t = 0, 5, ..., 3595 (720 of
    # them). The first is a cold-cache write at t=0; thereafter a quiet asset
    # is written when now - last_write >= 15 s, i.e. every third message, at
    # t = 15, 30, ..., 3585 (= 15 * 239). That is 1 + 239 = 240 writes; the
    # spec's "about 241" counts 3600/15 + 1, but the hour ends at t=3595, so
    # the 3600 boundary is never reached.
    #  - stationary: 240 each.
    #  - movers: 20 m per message, over the 2 m deadband every time: 720 each.
    #  - jitterer: alternates 0 m / 0.5 m north, always < 2 m from the last
    #    written position, so it is a pure heartbeat stream: 240.
    #  - health changer: the change lands at t=1800 (message 360), which is
    #    itself a heartbeat boundary (1800 = 15 * 120), so the changed write
    #    and the heartbeat write are the same write: still 240. A change off
    #    a boundary would cost one more.
    for a in STATIONARY:
        assert new_counts[a] == 240, a
    for a in MOVERS:
        assert new_counts[a] == 720, a
    assert new_counts[JITTER] == 240
    assert new_counts[CHANGER] == 240
    assert sum(new_counts.values()) == 6 * 240 + 2 * 720 + 240 + 240 == 3360

    # Metric. Suppressed = 7200 - 3360 = 3840. Stationary and changer
    # suppress only unchanged (6 * 480 + 480 = 3360). The jitterer's 480
    # suppressed messages are k = 3m+1 and 3m+2 for m = 0..239; the cache holds
    # the position of the write at k = 3m, whose parity is m's, so k = 3m+2 has
    # the same position (unchanged) and k = 3m+1 the other one (deadband):
    # 240 each. Totals: unchanged 3360 + 240 = 3600, deadband 240.
    assert _count("unchanged") - sup_u == 3600
    assert _count("deadband") - sup_d == 240

    # (c) Final stores agree except the freshness columns (within H) and the
    # jitterer's position (within the deadband).
    fresh = {"last_sample_at", "reporting_status_at", "updated_at"}
    last_t = T0 + timedelta(seconds=GAP * (N_MSG - 1))
    for a in ASSETS:
        o, n = old_store[a], new_store[a]
        for col in fresh:
            assert 0 <= (o[col] - n[col]).total_seconds() <= H, (a, col)
        assert o["last_sample_at"] == last_t
        skip = fresh | {"provenance"}
        if a == JITTER:
            skip |= {"kinematics"}
        assert {c: v for c, v in o.items() if c not in skip} == \
               {c: v for c, v in n.items() if c not in skip}, a
        assert ({k: v for k, v in o["provenance"].items() if k != "sample_time"} ==
                {k: v for k, v in n["provenance"].items() if k != "sample_time"})
    assert new_store[CHANGER]["health_state"] == "HEALTH_STATE_DEGRADED"
    lat = lambda r: r["kinematics"]["position"]["wgs84"]["lat"]["value"]
    assert abs(lat(old_store[JITTER]) - lat(new_store[JITTER])) * M_PER_DEG_LAT <= 2.0


# -- the rules -----------------------------------------------------------------

def test_sample_time_only_provenance_change_is_suppressed(cfg, clock):
    cfg(H)
    assert sent(clock, 0) is not None
    before = _count("unchanged")
    assert sent(clock, 5) is None
    assert _count("unchanged") == before + 1


def test_other_provenance_change_is_written(cfg, clock):
    cfg(H)
    sent(clock, 0)
    assert sent(clock, 5, prov={"producer_id": "p2"}) is not None


def test_op_status_claim_always_written(cfg, clock):
    cfg(H)
    claim = {"operational_status": "OPERATIONAL_STATUS_DEACTIVATED"}
    assert sent(clock, 0, op=claim) is not None
    assert sent(clock, 1, op=claim) is not None
    assert sent(clock, 2, op=claim) is not None


def test_revive_after_deactivated_then_plain(cfg, clock):
    cfg(H)
    sent(clock, 0, op={"operational_status": "OPERATIONAL_STATUS_DEACTIVATED"})
    w = sent(clock, 1)  # plain kinematics, nothing else changed except the claim key
    assert w is not None and w.revive is not None
    assert sent(clock, 2) is None  # revived: cache now says operational


def test_revive_applies_even_when_row_is_otherwise_identical(cfg, clock):
    cfg(H)
    sent(clock, 0)
    # Seed the cache as if a deactivated claim had been the last write.
    tl._cache["A1"].op_status = "deactivated"
    assert sent(clock, 1) is not None


def test_status_only_drops_the_cache(cfg, clock):
    cfg(H)
    sent(clock, 0)
    assert sent(clock, 5) is None
    clock.t = 6
    so = tl.handle("A1", msg(t=6, kin=False, op={"operational_status": "OPERATIONAL_STATUS_DEACTIVATED"}))
    assert so is not None and "kinematics" not in so.row
    assert "A1" not in tl._cache
    assert sent(clock, 7) is not None


def test_cold_cache_writes(cfg, clock):
    cfg(H)
    assert sent(clock, 0, asset="X") is not None
    tl._cache.clear()  # a restart
    assert sent(clock, 1, asset="X") is not None


def test_heartbeat_exactly_at_h_writes(cfg, clock):
    cfg(H)
    sent(clock, 0)
    assert sent(clock, 14.999) is None
    assert sent(clock, 15) is not None
    assert sent(clock, 29.999) is None
    assert sent(clock, 30) is not None


def test_deadband_per_variant_and_star_fallback(cfg, clock):
    cfg(H, {"A": 5.0, "*": 2.0})
    for v in ("A", "B"):
        sent(clock, 0, asset=v, variant=v)
    before = _count("deadband")
    assert sent(clock, 1, asset="A", variant="A", north_m=3.0) is None   # under 5 m
    assert sent(clock, 1, asset="B", variant="B", north_m=3.0) is not None  # "*" = 2 m
    assert _count("deadband") == before + 1
    assert sent(clock, 2, asset="A", variant="A", north_m=5.5) is not None  # 5.5 m from cached 0


def test_unknown_variant_without_star_is_exact(cfg, clock):
    cfg(H, {"A": 5.0})
    sent(clock, 0, variant="Z")
    assert sent(clock, 1, variant="Z", north_m=0.0) is None
    assert sent(clock, 2, variant="Z", north_m=0.01) is not None


def test_altitude_change_is_never_suppressed_by_the_horizontal_deadband(cfg, clock):
    cfg(H, {"*": 50.0})
    sent(clock, 0, altitude=100.0)
    assert sent(clock, 1, altitude=100.0, north_m=1.0) is None
    assert sent(clock, 2, altitude=900.0) is not None


def test_bad_config_raises(cfg, monkeypatch):
    stale = reporting_sweep.STALE_AFTER_S
    with pytest.raises(ValueError) as e:
        cfg(stale / 2 + 0.01)
    assert str(stale) in str(e.value) and "TELEMETRY_CHANGE_ONLY_HEARTBEAT_S" in str(e.value)
    with pytest.raises(ValueError):
        cfg(H, "{not json")
    with pytest.raises(ValueError):
        cfg(H, {"*": -1.0})
    with pytest.raises(ValueError):
        cfg(H, "[1]")
    with pytest.raises(ValueError):
        cfg(H, {"*": "wide"})
    cfg(stale / 2, {})  # the limit itself is allowed


# -- metric ----------------------------------------------------------------------

def test_metric_labels_move_as_predicted(cfg, clock):
    cfg(H, {"*": 2.0})
    u0, d0 = _count("unchanged"), _count("deadband")
    sent(clock, 0)
    sent(clock, 1)                  # unchanged
    sent(clock, 2, north_m=1.0)     # deadband
    sent(clock, 3, north_m=1.0)     # equal to the message before, but cache holds t=0's position
    assert _count("unchanged") == u0 + 1
    assert _count("deadband") == d0 + 2
    sent(clock, 4, north_m=2.5)     # written, no metric
    assert (_count("unchanged"), _count("deadband")) == (u0 + 1, d0 + 2)
