"""failure-episodes -> failure_episodes (ADR-0051 slice 3a, projector side).

First write wins (S7): same key + same body = duplicate, same key + a different
body = a counted conflict, the stored row never changes.

The golden record is a CONTRACT COPY of the cm-service file
src/tests/golden/failure_episode_record.json; its sha256 is pinned below. If
cm-service changes the wire record, this copy and the pin change with it.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from config import NEVER_PRUNED_TABLES, Mapping, load_config
from handlers import get_handler
from handlers.base import refused_rows
from handlers.failure_episodes import body_sha256, handle
from handlers.failure_episodes_withheld import handle as handle_withheld
from main import build_prune_targets
from metrics import FAILURE_EPISODE_WRITES
from persistence.postgres import PostgresPool, PostgresWriteError

GOLDEN = Path(__file__).parent / "golden" / "failure_episode_record.json"
# CONTRACT COPY of cm-service src/tests/golden/failure_episode_withheld.json.
GOLDEN_WITHHELD = Path(__file__).parent / "golden" / "failure_episode_withheld.json"
GOLDEN_WITHHELD_SHA256 = "68bee17ae03e4bdd02a935c1f6d86ca2a2dae91bc1c90f24c63293c309e9f214"
GOLDEN_SHA256 = "cc42f7199b7ca7a4f1f3f2a034075a935ec9e406063afeeedacc6e516a6dbfbb"
# body_sha256 of the golden record: canonical JSON, sorted keys, no spaces.
GOLDEN_BODY_SHA256 = "592552ae855a014217e36bd84adfe2f3e5762cbffa51c03dea8661b8f8681f87"
CONFIG_YAML = Path(__file__).parent.parent / "config" / "projector_config.yaml"

# S1 migration 20261113000000_failure_episodes.sql (stack 616f036), copied.
MIGRATION_SQL = """
CREATE TABLE "public"."failure_episodes" (
  "episode_id" text NOT NULL,
  "detected_at" timestamptz NOT NULL,
  "asset_id" text NOT NULL,
  "component" text NOT NULL DEFAULT '',
  "fault_code" text NOT NULL DEFAULT '',
  "cleared_at" timestamptz NOT NULL,
  "cleared_by" text NOT NULL DEFAULT '',
  "resolution" text NOT NULL DEFAULT '',
  "sources" jsonb NOT NULL DEFAULT '[]',
  "baseline_id" text NOT NULL DEFAULT '',
  "mod_compliance" jsonb NOT NULL DEFAULT '[]',
  "originator_nation" text NOT NULL,
  "releasable_to" text[] NOT NULL,
  "label_key" text NULL,
  "label" jsonb NULL,
  "edge_id" text NOT NULL DEFAULT '',
  "region_id" text NULL,
  "body_sha256" text NOT NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("episode_id", "detected_at")
);
"""
WITHHELD_SQL = """
CREATE TABLE "public"."failure_episodes_withheld" (
  "body_sha256" text NOT NULL,
  "stage" text NOT NULL,
  "reason" text NOT NULL,
  "asset_id" text NOT NULL DEFAULT '',
  "episode_id" text NOT NULL DEFAULT '',
  "detected_at" timestamptz NULL,
  "body" jsonb NOT NULL,
  "edge_id" text NOT NULL DEFAULT '',
  "region_id" text NULL,
  "first_seen_at" timestamptz NOT NULL DEFAULT now(),
  "last_seen_at" timestamptz NOT NULL DEFAULT now(),
  "times_seen" bigint NOT NULL DEFAULT 1,
  PRIMARY KEY ("body_sha256", "stage", "reason")
);
"""
MIGRATION_COLUMNS = re.findall(r'^\s+"(\w+)" ', MIGRATION_SQL, re.M)
KEY = ("episode_id", "detected_at")


def golden() -> dict:
    return json.loads(GOLDEN.read_bytes())


def rec(n: int) -> dict:
    r = golden()
    r["episode_id"] = f"ep-t-{n:05d}"
    r["detected_at_ns"] = 1700000000000000000 + n * 1_000_000_000
    r["cleared_at_ns"] = r["detected_at_ns"] + 100_000_000_000
    return r


class FakePool:
    """First-write-wins stand-in for PostgresPool.insert_failure_episode."""

    def __init__(self):
        self.rows: dict[tuple, dict] = {}
        self.inserts = 0
        self.withheld: dict[tuple, dict] = {}
        self.raise_on_insert: Exception | None = None
        self.policy: str | None = None
        self.policy_reads = 0

    async def fetch_label_policy_id(self) -> str | None:
        self.policy_reads += 1
        return self.policy

    async def upsert_failure_episode_withheld(self, row: dict) -> None:
        k = (row["body_sha256"], row["stage"], row["reason"])
        if k in self.withheld:
            self.withheld[k]["times_seen"] += 1
        else:
            self.withheld[k] = {**row, "times_seen": 1}

    async def insert_failure_episode(self, row: dict) -> str:
        self.inserts += 1
        if self.raise_on_insert is not None:
            raise self.raise_on_insert
        k = tuple(row[c] for c in KEY)
        if k not in self.rows:
            self.rows[k] = dict(row)
            return "inserted"
        if self.rows[k]["body_sha256"] == row["body_sha256"]:
            return "duplicate"
        return "conflict"


def outcome_count(outcome: str) -> float:
    return FAILURE_EPISODE_WRITES.labels(outcome=outcome)._value.get()


async def project(pool: FakePool, record: dict):
    await handle(record["asset_id"], record, pool)


async def written(record: dict) -> dict:
    p = FakePool()
    await project(p, record)
    assert len(p.rows) == 1
    return next(iter(p.rows.values()))


def test_golden_is_the_contract_copy():
    assert hashlib.sha256(GOLDEN.read_bytes()).hexdigest() == GOLDEN_SHA256


def test_migration_columns_parsed():
    assert len(MIGRATION_COLUMNS) == 19 and MIGRATION_COLUMNS[0] == "episode_id"


async def test_golden_record_one_write_every_column():
    r = await written(golden())
    assert sorted(r) == sorted(MIGRATION_COLUMNS)
    assert r["episode_id"] == "ep-t-0001"
    assert r["detected_at"] == datetime(2023, 11, 14, 22, 15, 0, tzinfo=timezone.utc)
    assert r["cleared_at"] == datetime(2023, 11, 14, 22, 16, 40, tzinfo=timezone.utc)
    assert r["asset_id"] == "asset-t-0001"
    assert (r["component"], r["fault_code"]) == ("tr_module", "F-1001")
    assert (r["cleared_by"], r["resolution"]) == ("operator-b", "swapped module")
    assert r["sources"] == golden()["sources"]
    assert r["mod_compliance"] == golden()["mod_compliance"]
    assert r["baseline_id"] == "BL-T-1"
    assert r["originator_nation"] == "ATL" and r["releasable_to"] == ["BDR"]
    assert (r["edge_id"], r["region_id"]) == ("edge-a", "region-a")


async def test_ns_to_microseconds_is_deterministic():
    r = golden()
    r["detected_at_ns"] = 1700000100123456789
    a, b = await written(r), await written(r)
    assert a["detected_at"] == b["detected_at"]
    assert a["detected_at"].microsecond == 123456


def test_registered():
    assert asyncio.iscoroutinefunction(handle)
    assert get_handler("failure_episodes") is handle


async def test_replay_same_record_is_a_duplicate_row_unchanged():
    p = FakePool()
    dup0, ins0 = outcome_count("duplicate"), outcome_count("inserted")
    await project(p, golden())
    before = dict(next(iter(p.rows.values())))
    await project(p, golden())
    assert len(p.rows) == 1 and next(iter(p.rows.values())) == before
    assert outcome_count("inserted") == ins0 + 1
    assert outcome_count("duplicate") == dup0 + 1


async def test_changed_body_is_a_counted_conflict_row_unchanged(caplog):
    p = FakePool()
    await project(p, golden())
    before = dict(next(iter(p.rows.values())))
    changed = golden()
    changed["resolution"] = "something else entirely"
    c0 = outcome_count("conflict")
    with caplog.at_level(logging.WARNING, logger="projector.handlers.failure_episodes"):
        await project(p, changed)
    assert len(p.rows) == 1 and next(iter(p.rows.values())) == before
    assert before["resolution"] == "swapped module"
    assert outcome_count("conflict") == c0 + 1
    warnings = [r for r in caplog.records if "CONFLICT" in r.getMessage()]
    assert len(warnings) == 1
    assert body_sha256(changed)[:12] in warnings[0].getMessage()
    assert "ep-t-0001" in warnings[0].getMessage()


async def test_conflict_warning_names_both_hashes_and_is_rate_limited(caplog):
    from persistence.postgres import StoredHashOutcome

    class P(FakePool):
        async def insert_failure_episode(self, row):
            return StoredHashOutcome("conflict", "ab" * 32)

    r = golden()
    r["episode_id"] = "ep-rate-limit"
    with caplog.at_level(logging.WARNING, logger="projector.handlers.failure_episodes"):
        await handle("k", r, P())
        await handle("k", r, P())
    msgs = [x.getMessage() for x in caplog.records if "CONFLICT" in x.getMessage()]
    assert len(msgs) == 1
    assert "abababababab" in msgs[0] and body_sha256(r)[:12] in msgs[0]


async def test_1001_distinct_records_1001_rows():
    p = FakePool()
    for n in range(1001):
        await project(p, rec(n))
    assert len(p.rows) == 1001


async def test_backfill_set_applied_twice_n_rows():
    p = FakePool()
    batch = [rec(n) for n in range(50)]
    for _ in range(2):
        for r in batch:
            await project(p, r)
    assert len(p.rows) == 50


async def test_live_close_and_file_copy_one_row():
    p = FakePool()
    await project(p, golden())
    await project(p, json.loads(json.dumps(golden())))  # same episode, other route
    assert len(p.rows) == 1


def test_insert_sql_is_do_nothing_never_do_update():
    sql = PostgresPool.build_failure_episode_insert_sql(["episode_id", "detected_at", "body_sha256"])
    assert 'ON CONFLICT ("episode_id", "detected_at") DO NOTHING' in sql
    assert "DO UPDATE" not in sql
    assert 'INSERT INTO "failure_episodes"' in sql


def test_body_hash_ignores_key_order_and_whitespace():
    g = golden()
    reordered = dict(reversed(list(g.items())))
    spaced = json.loads(json.dumps(g, indent=4))
    assert body_sha256(g) == body_sha256(reordered) == body_sha256(spaced)
    assert body_sha256(g) == GOLDEN_BODY_SHA256
    changed = golden()
    changed["resolution"] = "x"
    assert body_sha256(changed) != body_sha256(g)


async def test_empty_releasable_to_is_a_label():
    r = golden()
    r["releasable_to"] = []
    assert (await written(r))["releasable_to"] == []


async def test_blank_region_is_null():
    r = golden()
    r["region_id"] = ""
    assert (await written(r))["region_id"] is None


async def test_row_carries_the_wire_body_hash():
    assert (await written(golden()))["body_sha256"] == GOLDEN_BODY_SHA256


class _Conn:
    """asyncpg connection stand-in: raises OSError n times, then answers."""

    def __init__(self, fail: int, status: str, stored: str | None = None):
        self.fail, self.status, self.stored, self.calls = fail, status, stored, 0

    async def execute(self, sql, *args):
        self.calls += 1
        if self.fail:
            self.fail -= 1
            raise OSError("connection reset")
        return self.status

    async def fetchval(self, sql, *args):
        return self.stored


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *a):
        return False


def _pool_over(conn) -> PostgresPool:
    p = PostgresPool("postgresql://unused")
    p._pool = type("P", (), {"acquire": lambda self: _Acquire(conn)})()
    p._backoff = lambda attempt: 0
    return p


def _row() -> dict:
    return {"episode_id": "e", "detected_at": datetime.now(timezone.utc),
            "body_sha256": "h"}


async def test_transient_errors_retry_then_insert():
    conn = _Conn(fail=2, status="INSERT 0 1")
    assert await _pool_over(conn).insert_failure_episode(_row()) == "inserted"
    assert conn.calls == 3


async def test_real_pool_duplicate_and_conflict_from_stored_hash():
    assert await _pool_over(_Conn(0, "INSERT 0 0", "h")).insert_failure_episode(_row()) == "duplicate"
    got = await _pool_over(_Conn(0, "INSERT 0 0", "other")).insert_failure_episode(_row())
    assert got == "conflict" and got.stored_sha256 == "other"


async def test_other_postgres_error_raises_write_error():
    import asyncpg

    class Bad(_Conn):
        async def execute(self, sql, *args):
            raise asyncpg.PostgresError("check violation")

    with pytest.raises(PostgresWriteError):
        await _pool_over(Bad(0, "")).insert_failure_episode(_row())


@pytest.mark.parametrize("mutate, reason", [
    (lambda r: r.pop("detected_at_ns"), "missing_detected_at"),
    (lambda r: r.update(detected_at_ns=0), "missing_detected_at"),
    (lambda r: r.pop("cleared_at_ns"), "missing_cleared_at"),
    (lambda r: r.update(episode_id=""), "missing_episode_id"),
    (lambda r: r.pop("asset_id"), "missing_asset_id"),
    (lambda r: r.update(originator_nation=""), "unlabelled"),
    (lambda r: r.update(releasable_to="BDR"), "bad_releasable_to"),
    (lambda r: r.pop("releasable_to"), "missing_releasable_to"),
    (lambda r: r.update(releasable_to=None), "missing_releasable_to"),
])
async def test_refusals_counted(mutate, reason):
    name = f"failure_episodes:{reason}"
    before = refused_rows().get(name, 0)
    r = golden()
    mutate(r)
    p = FakePool()
    w0 = outcome_count("withheld")
    await handle("k", r, p)
    assert p.inserts == 0 and not p.rows
    assert refused_rows().get(name, 0) == before + 1
    # kept, not dropped: one withheld row, stage projector, with the reason
    assert len(p.withheld) == 1
    w = next(iter(p.withheld.values()))
    assert (w["stage"], w["reason"]) == ("projector", reason)
    assert w["body"] == r and w["body_sha256"] == body_sha256(r)
    assert outcome_count("withheld") == w0 + 1


async def test_refusal_fills_what_parses_and_defaults_the_rest():
    r = golden()
    r["originator_nation"] = ""
    p = FakePool()
    await handle("k", r, p)
    w = next(iter(p.withheld.values()))
    assert (w["asset_id"], w["episode_id"]) == ("asset-t-0001", "ep-t-0001")
    assert w["detected_at"] == datetime(2023, 11, 14, 22, 15, 0, tzinfo=timezone.utc)
    assert (w["edge_id"], w["region_id"]) == ("edge-a", "region-a")
    p2 = FakePool()
    await handle("k", {"x": 1}, p2)
    w = next(iter(p2.withheld.values()))
    assert (w["asset_id"], w["episode_id"], w["detected_at"], w["edge_id"],
            w["region_id"]) == ("", "", None, "", None)


async def test_write_error_keeps_a_withheld_row():
    p = FakePool()
    p.raise_on_insert = PostgresWriteError("check violation")
    w0 = outcome_count("withheld")
    await handle("k", golden(), p)
    assert not p.rows and len(p.withheld) == 1
    w = next(iter(p.withheld.values()))
    assert (w["stage"], w["reason"]) == ("projector", "write_error")
    assert w["episode_id"] == "ep-t-0001" and w["body"] == golden()
    assert outcome_count("withheld") == w0 + 1


async def test_other_exceptions_are_not_swallowed():
    p = FakePool()
    p.raise_on_insert = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        await handle("k", golden(), p)
    assert not p.withheld


async def test_withheld_write_failure_raises():
    class P(FakePool):
        async def upsert_failure_episode_withheld(self, row):
            raise PostgresWriteError("nope")

    r = golden()
    r["originator_nation"] = ""
    with pytest.raises(PostgresWriteError):
        await handle("k", r, P())


# -- lk1 label_key + label at write (S7 change D) ---------------------------

VENDOR = Path(__file__).parent.parent / "vendor" / "openddil_labels"
VECTORS = json.loads((VENDOR / "vectors" / "lk1.json").read_bytes())
KEYED = [v for v in VECTORS if v["label_key"] is not None]


def test_vendored_labels_match_their_pin():
    lines = (VENDOR / "PIN").read_text().splitlines()
    assert len(lines) == 3
    assert "openddil-contracts" in lines[0] and "0552b81" in lines[0]
    for line, rel in ((lines[1], "lk1.py"), (lines[2], "vectors/lk1.json")):
        name, sha = line.split()
        assert name == rel
        assert hashlib.sha256((VENDOR / rel).read_bytes()).hexdigest() == sha


def test_vendored_vectors_equal_the_vendored_generator():
    from vendor.openddil_labels import lk1
    assert lk1.generate_vectors() == VECTORS


async def _labelled(policy, nation, release) -> dict:
    r = golden()
    r["originator_nation"] = nation
    if release is None:
        del r["releasable_to"]
    else:
        r["releasable_to"] = release
    p = FakePool()
    p.policy = policy
    await handle("k", r, p)
    return p, r


@pytest.mark.parametrize("v", [v for v in KEYED if v["releasable_to"] is not None],
                         ids=lambda v: v["name"])
async def test_every_keyed_vector_writes_its_key_and_label(v):
    p, r = await _labelled(v["policy_id"], v["originator_nation"], v["releasable_to"])
    assert len(p.rows) == 1, "vector was refused: %s" % list(p.withheld.values())
    row = next(iter(p.rows.values()))
    assert row["label_key"] == v["label_key"]
    assert row["label"] == v["label"]
    # the hash is still over the wire record, not the label
    assert row["body_sha256"] == body_sha256(r)


async def test_absent_releasable_to_is_refused_not_labelled():
    # the existing refusal (missing_releasable_to) still fires before any key
    p, _ = await _labelled("example:policy-1", "ATL", None)
    assert not p.rows and len(p.withheld) == 1
    assert next(iter(p.withheld.values()))["reason"] == "missing_releasable_to"


async def test_no_policy_writes_null_key_and_label_and_still_inserts():
    i0 = outcome_count("inserted")
    p, _ = await _labelled(None, "ATL", ["BDR"])
    row = next(iter(p.rows.values()))
    assert row["label_key"] is None and row["label"] is None
    assert outcome_count("inserted") == i0 + 1


async def test_declared_empty_and_set_semantics_give_the_vector_keys():
    by = {v["name"]: v for v in VECTORS}
    p, _ = await _labelled("example:policy-1", "ATL", [])
    assert next(iter(p.rows.values()))["label_key"] == by["atl_empty"]["label_key"]
    p2, _ = await _labelled("example:policy-1", "ATL", ["BDR", "ATL", "BDR"])
    k2 = next(iter(p2.rows.values()))["label_key"]
    assert k2 == by["atl_bdr_atl"]["label_key"]
    p3, _ = await _labelled("example:policy-1", "ATL", ["ATL", "BDR", "ATL"])
    assert next(iter(p3.rows.values()))["label_key"] == by["atl_atl_bdr_atl"]["label_key"]
    assert by["atl_empty"]["label_key"] != by["atl_absent"]["label_key"]


async def test_unlabelled_close_is_withheld_and_compile_is_never_reached(monkeypatch):
    import handlers.failure_episodes as fe

    def boom(*a):
        raise AssertionError("compile_lk1 reached")

    monkeypatch.setattr(fe, "compile_lk1", boom)
    r = golden()
    r["originator_nation"] = ""
    p = FakePool()
    p.policy = "example:policy-1"
    await handle("k", r, p)
    assert not p.rows and p.inserts == 0
    assert len(p.withheld) == 1
    w = next(iter(p.withheld.values()))
    assert (w["stage"], w["reason"]) == ("projector", "unlabelled")


async def test_policy_is_read_once_per_process_and_cached_on_the_pool():
    p = FakePool()
    p.policy = "example:policy-1"
    for n in range(3):
        await project(p, rec(n))
    assert p.policy_reads == 1
    row = next(iter(p.rows.values()))
    assert row["label_key"].startswith("lk1:")


async def test_fetch_label_policy_id_table_missing_row_missing_and_present():
    class C:
        def __init__(self, regclass, policy):
            self.regclass, self.policy, self.sql = regclass, policy, []

        async def fetchval(self, sql, *a):
            self.sql.append(sql)
            return self.regclass if "to_regclass" in sql else self.policy

    c = C(None, "ignored")
    assert await _pool_over(c).fetch_label_policy_id() is None
    assert len(c.sql) == 1
    assert await _pool_over(C("label_policy", None)).fetch_label_policy_id() is None
    assert await _pool_over(C("label_policy", "example:policy-1")).fetch_label_policy_id() == "example:policy-1"


def test_withheld_sql_counts_a_repeat():
    sql = PostgresPool.build_failure_episode_withheld_sql(["body_sha256", "stage", "reason"])
    assert 'ON CONFLICT ("body_sha256", "stage", "reason")' in sql
    assert 'DO UPDATE SET "times_seen" = t."times_seen" + 1' in sql
    assert '"last_seen_at" = now()' in sql


async def test_real_pool_withheld_upsert_retries_and_binds_body_as_json():
    seen = []

    class C(_Conn):
        async def execute(self, sql, *args):
            self.calls += 1
            if self.fail:
                self.fail -= 1
                raise OSError("reset")
            seen.append(args)

    conn = C(1, "")
    row = {"body_sha256": "h", "stage": "projector", "reason": "r",
           "body": {"a": 1}}
    await _pool_over(conn).upsert_failure_episode_withheld(row)
    assert conn.calls == 2 and seen[0][3] == '{"a": 1}'


# -- the withheld topic ----------------------------------------------------

def withheld_golden() -> dict:
    return json.loads(GOLDEN_WITHHELD.read_bytes())


def test_withheld_golden_is_the_contract_copy():
    assert hashlib.sha256(GOLDEN_WITHHELD.read_bytes()).hexdigest() == GOLDEN_WITHHELD_SHA256


def test_withheld_handler_registered_and_mapped():
    assert get_handler("failure_episodes_withheld") is handle_withheld
    cfg = load_config(CONFIG_YAML)
    m = next(m for m in cfg.mappings if m.table == "failure_episodes_withheld")
    assert (m.topic, m.handler, m.consumer_group, m.decode_as, m.mode) == (
        "failure-episodes-withheld", "failure_episodes_withheld",
        "projector-failure-episodes-withheld", "json", "custom")
    assert m.asset_ttl_hours is None


async def test_withheld_golden_writes_one_row_replay_counts():
    p = FakePool()
    g = withheld_golden()
    w0 = outcome_count("withheld")
    await handle_withheld("asset-t-0001", g, p)
    assert len(p.withheld) == 1
    w = next(iter(p.withheld.values()))
    assert (w["stage"], w["reason"]) == ("emit", "unlabelled")
    assert w["episode_id"] == "ep-t-0001" and w["asset_id"] == "asset-t-0001"
    assert w["detected_at"] == datetime(2023, 11, 14, 22, 15, 0, tzinfo=timezone.utc)
    assert (w["edge_id"], w["region_id"]) == ("edge-a", "region-a")
    assert w["body"] == g and w["body_sha256"] == body_sha256(g)
    assert w["times_seen"] == 1
    await handle_withheld("asset-t-0001", withheld_golden(), p)
    assert len(p.withheld) == 1 and next(iter(p.withheld.values()))["times_seen"] == 2
    assert outcome_count("withheld") == w0 + 2


@pytest.mark.parametrize("drop", ["reason", "stage"])
async def test_withheld_without_stage_or_reason_is_malformed(drop):
    g = withheld_golden()
    del g[drop]
    p = FakePool()
    await handle_withheld("k", g, p)
    w = next(iter(p.withheld.values()))
    assert (w["stage"], w["reason"]) == ("projector", "malformed_withheld")
    assert w["body"] == g


async def test_withheld_null_detected_at_when_zero():
    g = withheld_golden()
    g["detected_at_ns"] = 0
    p = FakePool()
    await handle_withheld("k", g, p)
    assert next(iter(p.withheld.values()))["detected_at"] is None


# -- never pruned ------------------------------------------------------------

def test_never_pruned_table_set():
    assert "failure_episodes" in NEVER_PRUNED_TABLES


def test_env_ttl_does_not_reach_failure_episodes(monkeypatch):
    monkeypatch.setenv("PROJECTOR_ASSET_TTL_HOURS", "24")
    cfg = load_config(CONFIG_YAML)
    m = next(m for m in cfg.mappings if m.table == "failure_episodes")
    assert m.asset_ttl_hours is None
    assert m.consumer_group == "projector-failure-episodes"
    assert m.topic == "failure-episodes" and m.mode == "custom"
    # the env still reaches an ordinary upsert mapping
    assert any(x.asset_ttl_hours == 24 for x in cfg.mappings)
    assert "failure_episodes" not in [t[0] for t in build_prune_targets(cfg.mappings)]


def test_build_prune_targets_excludes_even_a_handbuilt_ttl():
    m = Mapping(topic="failure-episodes", handler="failure_episodes",
                table="failure_episodes", consumer_group="g",
                decode_as="json", mode="upsert", asset_ttl_hours=24)
    assert build_prune_targets([m]) == []


def test_yaml_ttl_on_failure_episodes_is_a_load_error(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "mappings:\n"
        "  - topic: failure-episodes\n    handler: failure_episodes\n"
        "    table: failure_episodes\n    consumer_group: g\n"
        "    decode_as: json\n    mode: upsert\n    asset_ttl_hours: 24\n")
    with pytest.raises(ValueError, match="never pruned"):
        load_config(p)


# -- real Postgres (skipped locally, failed under CI when unset) -------------

DSN = os.environ.get("PROJECTOR_TEST_PG_DSN")


def _require_dsn() -> str:
    if DSN:
        return DSN
    if os.environ.get("CI"):
        pytest.fail("PROJECTOR_TEST_PG_DSN is unset under CI; the upsert would go unchecked")
    pytest.skip("PROJECTOR_TEST_PG_DSN unset; real-Postgres failure_episodes test not run")


@pytest.fixture
async def pool():
    import asyncpg
    dsn = _require_dsn()
    schema = f"fe_test_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(dsn)
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    sep = "&" if "?" in dsn else "?"
    p = PostgresPool(f"{dsn}{sep}search_path={schema}")
    await p.connect()
    async with p._pool.acquire() as conn:
        await conn.execute((MIGRATION_SQL + WITHHELD_SQL).replace('"public".', ""))
    try:
        yield p
    finally:
        await p.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()


async def _count(pool) -> int:
    async with pool._pool.acquire() as conn:
        return await conn.fetchval('SELECT count(*) FROM "failure_episodes"')


async def test_real_pg_replay_one_row(pool):
    for _ in range(2):
        await handle("k", golden(), pool)
    assert await _count(pool) == 1
    async with pool._pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM "failure_episodes"')
    assert row["releasable_to"] == ["BDR"]
    assert json.loads(row["sources"]) == golden()["sources"]


async def test_real_pg_1001_distinct_twice(pool):
    for _ in range(2):
        for n in range(1001):
            await handle("k", rec(n), pool)
    assert await _count(pool) == 1001


async def test_real_pg_label_columns_with_and_without_a_policy(pool):
    await handle("k", rec(1), pool)
    async with pool._pool.acquire() as conn:
        row = await conn.fetchrow('SELECT "label_key", "label" FROM "failure_episodes"')
    assert row["label_key"] is None and row["label"] is None
    async with pool._pool.acquire() as conn:
        await conn.execute('CREATE TABLE "label_policy" ("policy_id" text NOT NULL)')
        await conn.execute("INSERT INTO \"label_policy\" VALUES ('example:policy-1')")
    assert await pool.fetch_label_policy_id() == "example:policy-1"
    pool._label_policy_loaded = False
    await handle("k", rec(2), pool)
    async with pool._pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT "label_key", "label" FROM "failure_episodes" WHERE "episode_id" = $1',
            "ep-t-00002")
    assert row["label_key"].startswith("lk1:") and json.loads(row["label"])["v"] == 1


async def test_real_pg_withheld_replay_counts_and_kept_on_refusal(pool):
    r = golden()
    r["originator_nation"] = ""
    for _ in range(3):
        await handle("k", r, pool)
    assert await _count(pool) == 0
    async with pool._pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM "failure_episodes_withheld"')
    assert len(rows) == 1
    assert (rows[0]["stage"], rows[0]["reason"], rows[0]["times_seen"]) == (
        "projector", "unlabelled", 3)
    assert json.loads(rows[0]["body"]) == r


async def test_real_pg_withheld_topic_record(pool):
    await handle_withheld("k", withheld_golden(), pool)
    async with pool._pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM "failure_episodes_withheld"')
    assert (row["stage"], row["reason"], row["episode_id"]) == ("emit", "unlabelled", "ep-t-0001")


async def test_real_pg_conflict_keeps_the_first_row(pool):
    await handle("k", golden(), pool)
    changed = golden()
    changed["resolution"] = "different"
    await handle("k", changed, pool)
    assert await _count(pool) == 1
    async with pool._pool.acquire() as conn:
        row = await conn.fetchrow('SELECT "resolution", "body_sha256" FROM "failure_episodes"')
    assert row["resolution"] == "swapped module"
    assert row["body_sha256"] == GOLDEN_BODY_SHA256


@pytest.mark.parametrize("policy,nation,release,reason", [
    ("bad policy!", "ATL", ["BDR"], "label_policy_invalid"),
    ("example:policy-1", "atl", ["BDR"], "label_invalid"),
    ("example:policy-1", "ATL", ["BDR", "x"], "label_invalid"),
])
async def test_label_that_does_not_compile_is_withheld_never_written(
        policy, nation, release, reason):
    r = golden()
    r["originator_nation"] = nation
    r["releasable_to"] = release
    p = FakePool()
    p.policy = policy
    w0 = outcome_count("withheld")
    await handle("k", r, p)
    assert p.inserts == 0 and not p.rows
    assert len(p.withheld) == 1
    w = next(iter(p.withheld.values()))
    assert (w["stage"], w["reason"]) == ("projector", reason)
    assert w["body"] == r and w["body_sha256"] == body_sha256(r)
    assert outcome_count("withheld") == w0 + 1


async def test_no_policy_with_lowercase_nation_still_inserts_unkeyed():
    r = golden()
    r["originator_nation"] = "atl"
    p = FakePool()
    w0 = outcome_count("withheld")
    await handle("k", r, p)
    assert len(p.rows) == 1 and not p.withheld
    row = next(iter(p.rows.values()))
    assert row["label_key"] is None and row["label"] is None
    assert outcome_count("withheld") == w0
