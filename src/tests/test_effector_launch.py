"""Unit tests for the effector_launch handler (launch-record projector).

Unlike every other handler, `effector_launch.handle` is async and takes the
pool directly (mode "custom"). These tests use a small fake pool (duck-typed
against the two methods the handler calls: `row_exists` and
`apply_effector_detonation`) rather than a live Postgres — same "pure,
no live infra" discipline `test_handlers.py` uses for the other handlers,
extended minimally for the one handler that genuinely needs I/O.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import metrics
from effector_declared_load import DeclaredLoadConfigError, _validate_and_flatten
from handlers import effector_launch
from handlers.effector_launch import munition_type_key, terminal_state_for
from persistence import Write


# -- terminal map --------------------------------------------------------------

@pytest.mark.parametrize("code,expected", [
    (1, "entity_impact"),
    (3, "ground_impact"),
    (2, "detonated"),
    (4, "detonated"),
    (5, "detonated"),
    (6, "dud"),
    (0, "other"),
    (99, "other"),
    (-1, "other"),
])
def test_terminal_state_for(code, expected):
    assert terminal_state_for(code) == expected


def test_terminal_state_for_never_returns_miss():
    """There is no 'miss' anywhere in this vocabulary — absence is not an
    outcome. Pinned across the whole domain, not just the mapped codes."""
    for code in range(-5, 20):
        assert terminal_state_for(code) != "miss"


def test_terminal_state_for_non_numeric_falls_to_other():
    assert terminal_state_for(None) == "other"
    assert terminal_state_for("not-a-number") == "other"


# -- munition_type_key ---------------------------------------------------------

def test_munition_type_key_dotted_order():
    d = {"kind": 2, "domain": 9, "country": 225, "category": 2,
         "subcategory": 1, "specific": 1, "extra": 0}
    assert munition_type_key(d) == "2.9.225.2.1.1.0"


def test_munition_type_key_missing_dict_defaults_to_zeros():
    assert munition_type_key(None) == "0.0.0.0.0.0.0"
    assert munition_type_key({}) == "0.0.0.0.0.0.0"


# -- fire: admitted vs unknown_launcher ----------------------------------------

class _FakePool:
    """Duck-typed against the two PostgresPool methods effector_launch.py
    calls. `executed` records every Write passed to `execute`."""

    def __init__(self, *, admitted: bool = True, detonation_outcome: str = "updated",
                detonation_old_result: int | None = None):
        self._admitted = admitted
        self._detonation_outcome = detonation_outcome
        self._detonation_old_result = detonation_old_result
        self.executed: list[Write] = []
        self.detonation_calls: list[dict] = []

    async def row_exists(self, table, column, value):
        assert table == "telemetry_latest_state"
        assert column == "asset_id"
        return self._admitted

    async def execute(self, write: Write):
        self.executed.append(write)
        return None

    async def apply_effector_detonation(self, **kwargs):
        self.detonation_calls.append(kwargs)
        return self._detonation_outcome, self._detonation_old_result


def _fire(event_urn="dis-event:1:58:1", launcher_urn="dis:1:58:1001"):
    return {
        "pdu_type": "fire",
        "event_urn": event_urn,
        "launcher_urn": launcher_urn,
        "target_urn": None,
        "munition_type": {"kind": 2, "domain": 9, "country": 225,
                           "category": 2, "subcategory": 1, "specific": 1,
                           "extra": 0},
        "quantity": 1,
        "ingest_timestamp": "2026-10-06T00:00:00Z",
        "provenance": {"edge_id": "edge-01", "region_id": "region-01"},
    }


async def test_fire_admitted_writes_append_row():
    pool = _FakePool(admitted=True)
    before = metrics.EFFECTOR_REFUSED.labels(reason="unknown_launcher")._value.get()
    await effector_launch.handle("dis:1:58:1001", _fire(), pool)
    assert len(pool.executed) == 1
    write = pool.executed[0]
    assert write.table == "effector_launch"
    assert write.mode == "append"
    assert write.key_columns == ["event_urn"]
    assert write.row["event_urn"] == "dis-event:1:58:1"
    assert write.row["launcher_asset_id"] == "dis:1:58:1001"
    assert write.row["munition_type"] == "2.9.225.2.1.1.0"
    assert write.row["edge_id"] == "edge-01"
    assert write.row["region_id"] == "region-01"
    after = metrics.EFFECTOR_REFUSED.labels(reason="unknown_launcher")._value.get()
    assert after == before  # unchanged — this Fire was admitted


async def test_fire_writes_munition_asset_id_when_named():
    pool = _FakePool(admitted=True)
    fire = _fire()
    fire["munition_urn"] = "dis:1:1:2001"
    await effector_launch.handle("dis:1:58:1001", fire, pool)
    assert pool.executed[0].row["munition_asset_id"] == "dis:1:1:2001"


async def test_fire_writes_null_munition_asset_id_when_none():
    pool = _FakePool(admitted=True)
    fire = _fire()
    fire["munition_urn"] = None
    await effector_launch.handle("dis:1:58:1001", fire, pool)
    assert pool.executed[0].row["munition_asset_id"] is None


async def test_fire_unknown_launcher_refuses_and_writes_nothing():
    pool = _FakePool(admitted=False)
    before = metrics.EFFECTOR_REFUSED.labels(reason="unknown_launcher")._value.get()
    await effector_launch.handle("dis:1:58:2001", _fire(launcher_urn="dis:1:58:2001"), pool)
    assert pool.executed == []
    after = metrics.EFFECTOR_REFUSED.labels(reason="unknown_launcher")._value.get()
    assert after == before + 1


# -- detonation: no_fire / late / replayed / conflicting / normal --------------

def _detonation(event_urn="dis-event:1:58:1", detonation_result=1):
    return {
        "pdu_type": "detonation",
        "event_urn": event_urn,
        "launcher_urn": "dis:1:58:1001",
        "target_urn": None,
        "munition_type": {"kind": 2, "domain": 9, "country": 225,
                           "category": 2, "subcategory": 1, "specific": 1,
                           "extra": 0},
        "quantity": 1,
        "detonation_result": detonation_result,
        "ingest_timestamp": "2026-10-06T00:00:10Z",
        "provenance": {"edge_id": "edge-01", "region_id": "region-01"},
    }


async def test_detonation_no_fire_refuses():
    pool = _FakePool(detonation_outcome="no_fire")
    before = metrics.EFFECTOR_REFUSED.labels(reason="no_fire")._value.get()
    await effector_launch.handle("k", _detonation(), pool)
    assert len(pool.detonation_calls) == 1
    call = pool.detonation_calls[0]
    assert call["event_urn"] == "dis-event:1:58:1"
    assert call["terminal_state"] == "entity_impact"  # detonation_result=1
    after = metrics.EFFECTOR_REFUSED.labels(reason="no_fire")._value.get()
    assert after == before + 1


async def test_detonation_late_increments_late_terminal():
    pool = _FakePool(detonation_outcome="late")
    before = metrics.EFFECTOR_LATE_TERMINAL._value.get()
    await effector_launch.handle("k", _detonation(detonation_result=3), pool)
    assert pool.detonation_calls[0]["terminal_state"] == "ground_impact"
    after = metrics.EFFECTOR_LATE_TERMINAL._value.get()
    assert after == before + 1


async def test_detonation_replayed_same_result_not_a_refusal():
    """Same detonation_result the row already has (the second projector
    instance to apply the same real Detonation) -- counted as a replay, not
    a refusal."""
    pool = _FakePool(detonation_outcome="replayed", detonation_old_result=6)
    before_replayed = metrics.EFFECTOR_REPLAYED._value.get()
    before_refused = metrics.EFFECTOR_REFUSED.labels(reason="conflicting_detonation")._value.get()
    await effector_launch.handle("k", _detonation(detonation_result=6), pool)
    assert pool.detonation_calls[0]["terminal_state"] == "dud"
    assert metrics.EFFECTOR_REPLAYED._value.get() == before_replayed + 1
    assert metrics.EFFECTOR_REFUSED.labels(reason="conflicting_detonation")._value.get() == before_refused


async def test_detonation_conflicting_result_refuses():
    """Different detonation_result than the row already has -- two
    different termination claims for the same event_urn; refused, logged
    at WARN (not asserted here), row left unchanged."""
    pool = _FakePool(detonation_outcome="conflicting", detonation_old_result=1)
    before = metrics.EFFECTOR_REFUSED.labels(reason="conflicting_detonation")._value.get()
    await effector_launch.handle("k", _detonation(detonation_result=3), pool)
    assert pool.detonation_calls[0]["terminal_state"] == "ground_impact"
    after = metrics.EFFECTOR_REFUSED.labels(reason="conflicting_detonation")._value.get()
    assert after == before + 1


async def test_detonation_updated_counts_nothing():
    pool = _FakePool(detonation_outcome="updated")
    before_no_fire = metrics.EFFECTOR_REFUSED.labels(reason="no_fire")._value.get()
    before_conflicting = metrics.EFFECTOR_REFUSED.labels(reason="conflicting_detonation")._value.get()
    before_late = metrics.EFFECTOR_LATE_TERMINAL._value.get()
    before_replayed = metrics.EFFECTOR_REPLAYED._value.get()
    await effector_launch.handle("k", _detonation(), pool)
    assert metrics.EFFECTOR_REFUSED.labels(reason="no_fire")._value.get() == before_no_fire
    assert metrics.EFFECTOR_REFUSED.labels(reason="conflicting_detonation")._value.get() == before_conflicting
    assert metrics.EFFECTOR_LATE_TERMINAL._value.get() == before_late
    assert metrics.EFFECTOR_REPLAYED._value.get() == before_replayed


# -- declared-load parse refusals ----------------------------------------------

def test_declared_load_valid_asset_and_variant():
    rows = _validate_and_flatten({
        "asset": {"dis:1:58:1001": {"2.9.225.2.1.1.0": 8}},
        "variant": {"M1A1": {"2.9.225.2.1.1.0": 8}, "AH-64E-V6": {"2.9.225.2.1.1.0": 4}},
    })
    assert ("dis:1:58:1001", "asset", "2.9.225.2.1.1.0", 8) in rows
    assert ("M1A1", "variant", "2.9.225.2.1.1.0", 8) in rows
    assert ("AH-64E-V6", "variant", "2.9.225.2.1.1.0", 4) in rows
    assert len(rows) == 3


def test_declared_load_empty_blocks_empty_table():
    assert _validate_and_flatten({}) == []
    assert _validate_and_flatten({"asset": {}, "variant": {}}) == []


def test_declared_load_negative_count_refuses_naming_the_entry():
    with pytest.raises(DeclaredLoadConfigError) as exc:
        _validate_and_flatten({"variant": {"M1A1": {"2.9.225.2.1.1.0": -1}}})
    assert "variant.M1A1.2.9.225.2.1.1.0" in str(exc.value)


def test_declared_load_malformed_munition_key_refuses_naming_the_entry():
    with pytest.raises(DeclaredLoadConfigError) as exc:
        _validate_and_flatten({"variant": {"M1A1": {"not-a-munition-key": 4}}})
    assert "M1A1.not-a-munition-key" in str(exc.value)


def test_declared_load_non_integer_count_refuses():
    with pytest.raises(DeclaredLoadConfigError):
        _validate_and_flatten({"asset": {"A": {"2.9.225.2.1.1.0": "eight"}}})
    with pytest.raises(DeclaredLoadConfigError):
        _validate_and_flatten({"asset": {"A": {"2.9.225.2.1.1.0": True}}})


# -- asset key beats variant key -----------------------------------------------
# effector_launcher_counts has no Python-side builder (it is a static view
# in the migration/schema.hcl, unlike build_sql/build_prune_sql) — pinned at
# the SQL-text level instead, same idiom test_persistence.py uses for
# build_staleness_sweep_sql's UPDATE-not-DELETE pin.

def test_effector_launcher_counts_view_coalesces_asset_before_variant():
    migration = Path(__file__).resolve().parents[3] / "openddil-stack" / "schema" / \
        "migrations" / "20261006000000_effector_launch.sql"
    sql = migration.read_text(encoding="utf-8")
    assert 'COALESCE("dl_asset"."declared", "dl_variant"."declared")' in sql
    assert '"dl_asset"."key_kind" = \'asset\'' in sql
    assert '"dl_variant"."key_kind" = \'variant\'' in sql
