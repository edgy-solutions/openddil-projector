"""Reachability-based `hq_link_severed`.

The toxiproxy probe this module replaces only existed on the tiers whose
link happened to run through toxiproxy; most tiers' real link is a relay
(bridge/uplink) to their parent, which commits its consumer-group offsets
only after the parent acks. An advance of the relay group's committed
offsets IS a completed exchange with the parent — that's the signal
`LinkReachability` turns into an up/severed decision, with hysteresis so a
single missed heartbeat doesn't flap the UI.

These tests exercise `LinkReachability` in isolation — no asyncio, no
Kafka, no Postgres — the same pattern as `test_prune_targets.py`. They are
written before the class exists; the first collection of this file is
expected to fail at import, which is the RED this file records.
"""
from __future__ import annotations

import pytest

import edge_buffer_monitor


def test_toxiproxy_probe_removed():
    """Behavioural red, independent of the new class: the toxiproxy probe
    must be gone, not just unused."""
    assert not hasattr(edge_buffer_monitor, "_probe_hq_link_severed")
    assert not hasattr(edge_buffer_monitor, "TOXIPROXY_API_URL")
    assert not hasattr(edge_buffer_monitor, "HQ_LINK_PROXY")


# ---------------------------------------------------------------------------
# Everything below needs LinkReachability, which does not exist yet.
# ---------------------------------------------------------------------------

def _LR(*args, **kwargs):
    """Resolved lazily (not at module import) so that collecting this file
    does not itself raise before `test_toxiproxy_probe_removed` gets a
    chance to run and fail for its own, behavioural reason."""
    return edge_buffer_monitor.LinkReachability(*args, **kwargs)


def _run(events, *, sever_after_s, restore_exchanges=2):
    """events: list of (now, committed_sum, lag) observations, in order."""
    state = _LR(sever_after_s=sever_after_s, restore_exchanges=restore_exchanges)
    history = []
    for now, committed_sum, lag in events:
        state.observe(now, committed_sum, lag)
        history.append(state.severed)
    return state, history


def test_steady_advance_never_severs():
    """Case 1: relay advances every 1s, observed every 2s, sever_after=7.
    Up after the first advance; never severed over 120s. The very first
    observation only sets the baseline (lag != 0 here, so no evidence yet);
    every observation after that sees the sum advance."""
    events = []
    now = 0.0
    committed = 0
    while now <= 120.0:
        committed += 2  # ~1s cadence worth of advance between 2s observations
        events.append((now, committed, 1))
        now += 2.0
    state, history = _run(events, sever_after_s=7.0)
    assert history[0] is None  # baseline only, no evidence yet
    assert all(h is False for h in history[1:])
    assert state.severed is False


def test_steady_advance_up_from_second_observation():
    """First observation only sets the baseline (no prior sum to compare
    against) unless lag == 0; the second observation sees the advance."""
    state = _LR(sever_after_s=7.0)
    state.observe(0.0, 100, 3)
    assert state.severed is None  # baseline only, no evidence yet
    state.observe(2.0, 102, 3)
    assert state.severed is False  # sum advanced -> exchange


def test_frozen_sum_with_climbing_lag_severs_after_threshold():
    """Case 2: proxy-style cut — committed_sum frozen, lag climbing. Still
    up at age == threshold; severed at the first observation with
    age > threshold."""
    state = _LR(sever_after_s=7.0)
    state.observe(0.0, 500, 0)  # baseline; lag==0 counts as an exchange
    assert state.severed is False
    state.observe(2.0, 500, 2)   # age=2
    assert state.severed is False
    state.observe(4.0, 500, 4)   # age=4
    assert state.severed is False
    state.observe(6.0, 500, 6)   # age=6
    assert state.severed is False
    state.observe(7.0, 500, 7)   # age==7, exactly threshold -> still up
    assert state.severed is False
    state.observe(9.0, 500, 9)   # age=9 > 7 -> severed
    assert state.severed is True


def test_probe_failure_severs_the_same_way():
    """Case 3: committed_sum None (probe failed) behaves like no evidence —
    it severs on the same age math as a frozen sum."""
    state = _LR(sever_after_s=7.0)
    state.observe(0.0, 500, 1)
    assert state.severed is None
    state.observe(2.0, 502, 1)  # one real exchange -> up
    assert state.severed is False
    state.observe(4.0, None, None)   # probe failure, no evidence, no rebase
    state.observe(6.0, None, None)
    state.observe(9.0, None, None)   # age since last exchange (t=2) is 7 -> not yet
    assert state.severed is False
    state.observe(9.5, None, None)   # age = 7.5 > 7 -> severed
    assert state.severed is True


def test_restore_requires_two_exchanges_within_threshold():
    """Case 4: restoring from severed needs `restore_exchanges` (2)
    consecutive exchanges, each gap <= sever_after_s. One exchange then a
    long silence stays severed; two exchanges within threshold go up."""
    state = _LR(sever_after_s=7.0, restore_exchanges=2)
    state.observe(0.0, 100, 1)
    state.observe(2.0, 100, 1)   # frozen
    state.observe(10.0, 100, 1)  # age=10 > 7 -> severed
    assert state.severed is True

    # One exchange, then silence past the threshold: stays severed.
    state.observe(12.0, 102, 1)   # exchange #1 since severed
    assert state.severed is True
    state.observe(20.0, 102, 1)   # 8s of silence since that exchange -> gap too big
    assert state.severed is True
    state.observe(22.0, 104, 1)   # exchange, but count restarts at 1 (previous gap exceeded threshold)
    assert state.severed is True

    # Two exchanges within threshold of each other: restores.
    state.observe(24.0, 106, 1)   # exchange #2, gap=2 <= 7 -> restore_exchanges satisfied
    assert state.severed is False


def test_group_reset_is_not_evidence():
    """Case 5: committed_sum decreasing (group reset / topics recreated)
    rebases the baseline rather than counting as an exchange."""
    state = _LR(sever_after_s=7.0)
    state.observe(0.0, 1000, 1)
    state.observe(2.0, 1002, 1)   # exchange -> up
    assert state.severed is False
    state.observe(4.0, 10, 1)     # reset: sum dropped -> rebase, not evidence
    assert state.severed is False  # age since t=2 exchange is only 2s, still up
    state.observe(12.0, 10, 1)    # frozen at the rebased value; age since t=2 is 10 > 7
    assert state.severed is True


def test_lag_zero_counts_as_exchange_without_advance():
    """Case 6: lag == 0 is evidence even when committed_sum hasn't moved
    (a caught-up relay can sit at a stable committed sum). The very first
    observation reports lag==0, which is itself evidence, resolving the
    warm-up immediately."""
    state = _LR(sever_after_s=7.0)
    state.observe(0.0, 500, 0)   # lag==0 -> exchange at t=0, up
    assert state.severed is False
    state.observe(2.0, 500, 1)   # sum frozen, lag != 0 -> no new evidence
    state.observe(7.0, 500, 1)   # age since t=0 exchange is 7, not > 7
    assert state.severed is False
    state.observe(7.1, 500, 1)   # age = 7.1 > 7 -> severed
    assert state.severed is True


def test_warmup_is_undecided_until_exchange_or_timeout():
    """Case 7: severed is None (undecided) until either an exchange occurs
    or age exceeds the threshold with none."""
    state = _LR(sever_after_s=7.0)
    assert state.severed is None  # no observations yet
    state.observe(0.0, 100, 3)   # baseline only; lag != 0, no evidence
    assert state.severed is None
    state.observe(3.0, 100, 3)   # still frozen, age=3 <= 7
    assert state.severed is None
    state.observe(7.9, 100, 3)   # age=7.9 > 7, no exchange ever -> severed
    assert state.severed is True


def test_warmup_resolves_up_on_first_exchange():
    state = _LR(sever_after_s=7.0)
    state.observe(0.0, 100, 3)
    assert state.severed is None
    state.observe(1.0, 101, 3)   # advance -> up, resolves the warm-up
    assert state.severed is False


def test_last_exchange_age():
    state = _LR(sever_after_s=7.0)
    assert state.last_exchange_age(0.0) is None  # never observed
    state.observe(0.0, 100, 3)
    assert state.last_exchange_age(5.0) == 5.0  # no exchange yet -> age from start
    state.observe(2.0, 102, 3)  # exchange at t=2
    assert state.last_exchange_age(9.0) == 7.0


def test_hub_edge_cadence_defaults_never_severs():
    """Case 8: hub-attached edge cadence — relay advances every 5s,
    observed every 2s, defaults E=5 H=2 -> threshold 14. Never severed."""
    threshold = edge_buffer_monitor.default_sever_after_s(
        exchange_period_s=5.0, probe_interval_s=2.0
    )
    assert threshold == 14.0
    state = _LR(sever_after_s=threshold)
    now = 0.0
    committed = 0
    last_advance = 0.0
    while now <= 120.0:
        # Advance only on ticks that line up with the 5s relay cadence.
        if now - last_advance >= 5.0 or now == 0.0:
            committed += 1
            last_advance = now
        state.observe(now, committed, 0 if now == last_advance else 1)
        assert state.severed is not True, f"severed at t={now}"
        now += 2.0


def test_default_sever_after_derivation_and_env_override(monkeypatch):
    """Case 9: default threshold = 2*(E+H); override via env is honoured
    by the module-level constant computed at import time."""
    assert edge_buffer_monitor.default_sever_after_s(5.0, 2.0) == 14.0
    assert edge_buffer_monitor.default_sever_after_s(1.5, 2.0) == pytest.approx(7.0)

    import importlib
    monkeypatch.setenv("LINK_SEVER_AFTER_S", "42")
    reloaded = importlib.reload(edge_buffer_monitor)
    assert reloaded.LINK_SEVER_AFTER_S == 42.0
    # Undo the env override now (inside the test, not at fixture teardown)
    # so the restoring reload below picks up the un-overridden default —
    # later test files that import this module must see normal defaults.
    monkeypatch.undo()
    importlib.reload(edge_buffer_monitor)
