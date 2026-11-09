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


X0 = 6_378_137.0


def msg(asset="A1", t=0.0, dx=0.0, variant="V1", health="HEALTH_STATE_OK",
        prov=None, op=None, kin=True, quality="dis", ref="REF_A",
        vel=None, yaw=None, frozen=False, unit="m", shape="ecef",
        altitude=None, north_m=0.0, ingest_lag=0.2):
    """A telemetry message in the real row shape: ECEF position with a
    valid_at stamp, ECEF velocity, euler yaw. valid_at, sample_time and
    ingest_time advance with `t` on every message; velocity and yaw do too
    unless `frozen` (a stationary asset) or given explicitly. shape="wgs84"
    is the lat/lon fallback shape."""
    stamp = (T0 + timedelta(seconds=t)).isoformat()
    if shape == "ecef":
        position = {"ecef": {"x": {"unit": unit, "value": X0 + dx},
                             "y": {"unit": unit, "value": 1000.0},
                             "z": {"unit": unit, "value": 2000.0}},
                    "quality": {"source": quality}, "valid_at": stamp}
    else:
        position = {"wgs84": {"lat": {"value": 50.0 + north_m / M_PER_DEG_LAT, "unit": "deg"},
                              "lon": {"value": 4.0, "unit": "deg"}},
                    "valid_at": stamp}
        if altitude is not None:
            position["wgs84"]["alt"] = {"value": altitude, "unit": "m"}
    if vel is None:
        vel = 0.0 if frozen else 1.0 + 0.01 * t
    if yaw is None:
        yaw = 0.0 if frozen else 0.1 + 0.001 * t
    operational = {"health_state": health, **(op or {})}
    d = {
        "asset": {"asset_id": asset, "platform_variant": variant,
                  "callsign": "C", "force": "FORCE_FRIENDLY"},
        "operational_state": operational,
        "provenance": {"edge_id": "edge-a", "region_id": "region-a",
                       "producer_id": "p1", "classification": "U",
                       "source_protocol": "dis",
                       "sample_time": stamp,
                       "ingest_time": (T0 + timedelta(seconds=t + ingest_lag)).isoformat(),
                       **(prov or {})},
        "schema_revision": 2,
    }
    if kin:
        d["kinematics"] = {
            "position": position,
            "velocity": {"ecef": {"x": vel, "y": 0.5, "unit": "m/s"}},
            "attitude": {"euler": {"yaw": {"unit": "rad", "value": yaw},
                                   "roll": {"unit": "rad"}, "pitch": {"unit": "rad"}},
                         "reference": ref},
            "speed": {"value": 1.0}}
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
        return msg(asset, t, dx=20.0 * k)
    if asset == JITTER:
        return msg(asset, t, dx=0.5 * (k % 2))
    if asset == CHANGER:
        return msg(asset, t, frozen=True,
                   health="HEALTH_STATE_DEGRADED" if t >= 1800 else "HEALTH_STATE_OK")
    return msg(asset, t, frozen=True)


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

    # Metric. Suppressed = 7200 - 3360 = 3840. Stationary and changer are
    # frozen in velocity and yaw, so they suppress only unchanged (6 * 480 +
    # 480 = 3360). The jitterer's velocity and yaw advance on every message,
    # so each of its 480 suppressed messages differs from the last written
    # motion: deadband 480.
    assert _count("unchanged") - sup_u == 3360
    assert _count("deadband") - sup_d == 480

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
        def _cmp(r):
            r = {c: v for c, v in r.items() if c not in skip}
            if "kinematics" in r:  # valid_at is a per-message stamp
                r["kinematics"] = copy.deepcopy(r["kinematics"])
                r["kinematics"]["position"].pop("valid_at")
            return r
        assert _cmp(o) == _cmp(n), a
        stamps = ("sample_time", "ingest_time")
        assert ({k: v for k, v in o["provenance"].items() if k not in stamps} ==
                {k: v for k, v in n["provenance"].items() if k not in stamps})
    assert new_store[CHANGER]["health_state"] == "HEALTH_STATE_DEGRADED"
    px = lambda r: r["kinematics"]["position"]["ecef"]["x"]["value"]
    assert abs(px(old_store[JITTER]) - px(new_store[JITTER])) <= 2.0


# -- the rules -----------------------------------------------------------------

def test_sample_time_only_provenance_change_is_suppressed(cfg, clock):
    cfg(H)
    assert sent(clock, 0, frozen=True) is not None
    before = _count("unchanged")
    assert sent(clock, 5, frozen=True) is None
    assert _count("unchanged") == before + 1


def test_ingest_time_only_change_is_suppressed(cfg, clock):
    cfg(H)
    sent(clock, 0, frozen=True)
    assert sent(clock, 5, frozen=True, ingest_lag=3.0) is None


def test_valid_at_only_change_is_suppressed(cfg, clock):
    cfg(H, {"*": 10.0})
    sent(clock, 0, frozen=True)
    assert sent(clock, 5, frozen=True) is None  # valid_at advanced with t


def test_velocity_only_change_suppressed_then_written_at_heartbeat(cfg, clock):
    cfg(H, {"*": 10.0})
    sent(clock, 0)
    assert sent(clock, 5, vel=9.0) is None
    assert sent(clock, 14.9, vel=9.0) is None
    w = sent(clock, 15, vel=9.0)
    assert w is not None and w.row["kinematics"]["velocity"]["ecef"]["x"] == 9.0


def test_yaw_only_change_suppressed_then_written_at_heartbeat(cfg, clock):
    cfg(H, {"*": 10.0})
    sent(clock, 0)
    assert sent(clock, 5, yaw=2.0) is None
    w = sent(clock, 15, yaw=2.0)
    assert w is not None
    assert w.row["kinematics"]["attitude"]["euler"]["yaw"]["value"] == 2.0


def test_position_quality_change_is_written(cfg, clock):
    cfg(H, {"*": 10.0})
    sent(clock, 0)
    assert sent(clock, 1, quality="degraded") is not None


def test_attitude_reference_change_is_written(cfg, clock):
    cfg(H, {"*": 10.0})
    sent(clock, 0)
    assert sent(clock, 1, ref="REF_B") is not None


def test_other_provenance_change_is_written(cfg, clock):
    cfg(H)
    sent(clock, 0)
    assert sent(clock, 5, prov={"producer_id": "p2"}) is not None
    assert sent(clock, 6, prov={"originator_nation": "XX"}) is not None


def test_ecef_move_at_or_over_the_deadband_is_written_just_under_is_not(cfg, clock):
    cfg(H, {"*": 10.0})
    sent(clock, 0)
    assert sent(clock, 1, dx=9.99) is None
    assert sent(clock, 2, dx=10.0) is not None
    assert sent(clock, 3, dx=10.0 + 9.99) is None
    assert sent(clock, 4, dx=10.0 + 10.01) is not None


def test_distance_is_three_dimensional(cfg, clock):
    cfg(H, {"*": 10.0})
    sent(clock, 0)
    m = msg(t=1)
    pos = m["kinematics"]["position"]["ecef"]
    pos["x"]["value"] += 6.0
    pos["y"]["value"] += 8.0  # 10 m in 3-D, 6 m in x alone
    clock.t = 1
    assert tl.handle("A1", m) is not None


def test_sub_deadband_steps_accumulate_from_the_last_written_position(cfg, clock):
    cfg(H, {"*": 10.0})
    sent(clock, 0)
    for i, x in enumerate((3.0, 6.0, 9.0), start=1):
        assert sent(clock, i, dx=x) is None
    assert sent(clock, 4, dx=12.0) is not None   # 12 m from the write at 0
    assert sent(clock, 5, dx=15.0) is None       # 3 m from the write at 12


def test_deadband_zero_suppresses_valid_at_alone_and_writes_an_ecef_change(cfg, clock):
    cfg(H)
    sent(clock, 0, frozen=True)
    assert sent(clock, 1, frozen=True) is None
    assert sent(clock, 2, frozen=True, dx=0.001) is not None


def test_non_metre_unit_falls_through_to_the_exact_compare(cfg, clock):
    cfg(H, {"*": 1000.0})
    sent(clock, 0, unit="km", frozen=True)
    assert sent(clock, 1, unit="km", frozen=True) is None              # valid_at only
    assert sent(clock, 2, unit="km", frozen=True, dx=0.5) is not None  # not a distance


def test_latlon_fallback_uses_great_circle_and_keeps_altitude_exact(cfg, clock):
    cfg(H, {"*": 50.0})
    sent(clock, 0, shape="wgs84", altitude=100.0, frozen=True)
    assert sent(clock, 1, shape="wgs84", altitude=100.0, north_m=10.0, frozen=True) is None
    assert sent(clock, 2, shape="wgs84", altitude=100.0, north_m=60.0, frozen=True) is not None
    assert sent(clock, 3, shape="wgs84", altitude=900.0, north_m=60.0, frozen=True) is not None


def test_op_status_claim_always_written(cfg, clock):
    cfg(H)
    claim = {"operational_status": "OPERATIONAL_STATUS_DEACTIVATED"}
    assert sent(clock, 0, op=claim) is not None
    assert sent(clock, 1, op=claim) is not None
    assert sent(clock, 2, op=claim) is not None


def test_revive_after_deactivated_then_plain(cfg, clock):
    cfg(H)
    sent(clock, 0, op={"operational_status": "OPERATIONAL_STATUS_DEACTIVATED"})
    w = sent(clock, 1, frozen=True)  # plain kinematics, nothing else changed except the claim key
    assert w is not None and w.revive is not None
    assert sent(clock, 2, frozen=True) is None  # revived: cache now says operational


def test_revive_applies_even_when_row_is_otherwise_identical(cfg, clock):
    cfg(H)
    sent(clock, 0)
    # Seed the cache as if a deactivated claim had been the last write.
    tl._cache["A1"].op_status = "deactivated"
    assert sent(clock, 1) is not None


def test_status_only_drops_the_cache(cfg, clock):
    cfg(H)
    sent(clock, 0, frozen=True)
    assert sent(clock, 5, frozen=True) is None
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
    sent(clock, 0, frozen=True)
    assert sent(clock, 14.999, frozen=True) is None
    assert sent(clock, 15, frozen=True) is not None
    assert sent(clock, 29.999, frozen=True) is None
    assert sent(clock, 30, frozen=True) is not None


def test_deadband_per_variant_and_star_fallback(cfg, clock):
    cfg(H, {"A": 5.0, "*": 2.0})
    for v in ("A", "B"):
        sent(clock, 0, asset=v, variant=v)
    before = _count("deadband")
    assert sent(clock, 1, asset="A", variant="A", dx=3.0) is None   # under 5 m
    assert sent(clock, 1, asset="B", variant="B", dx=3.0) is not None  # "*" = 2 m
    assert _count("deadband") == before + 1
    assert sent(clock, 2, asset="A", variant="A", dx=5.5) is not None  # 5.5 m from cached 0


def test_unknown_variant_without_star_is_exact(cfg, clock):
    cfg(H, {"A": 5.0})
    sent(clock, 0, variant="Z", frozen=True)
    assert sent(clock, 1, variant="Z", frozen=True) is None  # stamps only
    assert sent(clock, 2, variant="Z", frozen=True, dx=0.01) is not None


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
    sent(clock, 0, frozen=True)
    sent(clock, 1, frozen=True)               # nothing but stamps: unchanged
    sent(clock, 2, frozen=True, dx=1.0)       # position moved under the deadband
    sent(clock, 3, frozen=True, dx=1.0)       # cache still holds dx=0: deadband
    sent(clock, 4, frozen=True, vel=5.0)      # velocity alone: deadband
    sent(clock, 5, frozen=True, yaw=1.0)      # yaw alone: deadband
    assert _count("unchanged") == u0 + 1
    assert _count("deadband") == d0 + 4
    sent(clock, 6, frozen=True, dx=2.5)       # written, no metric
    assert (_count("unchanged"), _count("deadband")) == (u0 + 1, d0 + 4)


# -- replay from a measured spread of per-message steps -------------------------

STEPS_M = [1.42, 1.68, 2.34, 3.21, 5.06, 5.34, 6.61, 9.52, 19.06, 23.41,
           39.70, 40.05, 41.25, 51.74, 60.01, 65.64, 98.30]
R_GAP = 5.9
R_N = 611  # k = 0..610: the last message, at 3599 s, is inside the hour
R_H = 15.0

# Derivation. After a write at message j, the next write lands on the first
# message k > j where (k - j) * step >= D or (k - j) * 5.9 >= 15. Since
# 2 * 5.9 = 11.8 < 15 <= 17.7 = 3 * 5.9, that gap is at most 3 messages, so an
# asset writes every m = min(3, ceil(D / step)) messages, after the cold write
# at k = 0: writes = 1 + floor(610 / m).
#   D = 10:  steps 1.42 .. 3.21 (m = 3): 204 each; 5.06, 5.34, 6.61, 9.52
#            (m = 2): 306 each; 19.06 and up (m = 1): 611 each.
#            4 * 204 + 4 * 306 + 9 * 611 = 7539.
#   D = 50:  1.42 .. 23.41 (10 assets, m = 3): 204; 39.70, 40.05, 41.25
#            (m = 2): 306; 51.74 and up (4 assets, m = 1): 611.
#            10 * 204 + 3 * 306 + 4 * 611 = 5402.
#   D = 100: 1.42 .. 41.25 (13 assets, m = 3): 204; 51.74, 60.01, 65.64 and
#            98.30 (m = 2, two steps pass 100 m): 306.
#            13 * 204 + 4 * 306 = 3876.
# Slowest (1.42 m) is bound by the heartbeat at every D: 204. Fastest (98.30):
# 611 at D = 10 and 50, 306 at D = 100 (one message is 98.3 m, short of 100).
EXPECTED = {
    10.0: ([204] * 4 + [306] * 4 + [611] * 9, 7539),
    50.0: ([204] * 10 + [306] * 3 + [611] * 4, 5402),
    100.0: ([204] * 13 + [306] * 4, 3876),
}


def _replay(clock, check_age=None):
    store: dict = {}
    counts = [0] * len(STEPS_M)
    for k in range(R_N):
        clock.t = R_GAP * k
        for i, step in enumerate(STEPS_M):
            a = f"R{i}"
            if check_age is not None and a in store:
                age = (clock.now() - store[a]["last_sample_at"]).total_seconds()
                assert age <= check_age, (a, k, age)
            w = tl.handle(a, msg(a, clock.t, dx=step * k))
            if w is not None:
                counts[i] += 1
                _apply(store, w)
    return store, counts


def test_replay_off_writes_every_message(cfg, clock):
    _, counts = _replay(clock)
    assert counts == [R_N] * 17
    assert sum(counts) == 10387  # 17 * 611


@pytest.mark.parametrize("d", [10.0, 50.0, 100.0])
def test_replay_on_exact_counts(cfg, clock, d):
    cfg(R_H, {"*": d})
    store, counts = _replay(clock, check_age=R_H + R_GAP)
    assert R_H + R_GAP < reporting_sweep.STALE_AFTER_S
    per_asset, total = EXPECTED[d]
    assert counts == per_asset
    assert sum(counts) == total
    assert counts[0] == 204                       # slowest: heartbeat-bound
    assert counts[-1] == (306 if d == 100.0 else 611)  # fastest
    for i, step in enumerate(STEPS_M):
        stored = store[f"R{i}"]["kinematics"]["position"]["ecef"]["x"]["value"]
        assert abs(stored - (X0 + step * (R_N - 1))) <= d
