"""failure-episodes -> failure_episodes (ADR-0051 slice 3a, projector side).

The golden record is a CONTRACT COPY of the cm-service file
src/tests/golden/failure_episode_record.json; its sha256 is pinned below. If
cm-service changes the wire record, this copy and the pin change with it.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from config import NEVER_PRUNED_TABLES, Mapping, load_config
from handlers import get_handler
from handlers.base import refused_rows
from handlers.failure_episodes import handle
from main import build_prune_targets
from persistence import Write
from persistence.postgres import PostgresPool

GOLDEN = Path(__file__).parent / "golden" / "failure_episode_record.json"
GOLDEN_SHA256 = "cc42f7199b7ca7a4f1f3f2a034075a935ec9e406063afeeedacc6e516a6dbfbb"
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
  "releasable_to" text[] NOT NULL DEFAULT '{}',
  "edge_id" text NOT NULL DEFAULT '',
  "region_id" text NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("episode_id", "detected_at")
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


class FakeStore:
    """Dict keyed on the Write's key-column values, upsert semantics."""

    def __init__(self):
        self.rows: dict[tuple, dict] = {}

    def apply(self, write: Write):
        assert write.mode == "upsert"
        k = tuple(write.row[c] for c in write.key_columns)
        self.rows[k] = dict(write.row)


def project(store: FakeStore, record: dict):
    w = handle(record["asset_id"], record)
    assert w is not None
    store.apply(w)


def test_golden_is_the_contract_copy():
    assert hashlib.sha256(GOLDEN.read_bytes()).hexdigest() == GOLDEN_SHA256


def test_migration_columns_parsed():
    assert len(MIGRATION_COLUMNS) == 16 and MIGRATION_COLUMNS[0] == "episode_id"


def test_golden_record_one_write_every_column():
    w = handle("asset-t-0001", golden())
    assert w.table == "failure_episodes" and w.mode == "upsert"
    assert tuple(w.key_columns) == KEY
    assert sorted(w.row) == sorted(MIGRATION_COLUMNS)
    assert w.jsonb_columns == {"sources", "mod_compliance"}
    r = w.row
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


def test_ns_to_microseconds_is_deterministic():
    r = golden()
    r["detected_at_ns"] = 1700000100123456789
    a, b = handle("k", r), handle("k", r)
    assert a.row["detected_at"] == b.row["detected_at"]
    assert a.row["detected_at"].microsecond == 123456


def test_registered():
    assert get_handler("failure_episodes") is handle


def test_replay_same_record_one_row():
    s = FakeStore()
    project(s, golden())
    project(s, golden())
    assert len(s.rows) == 1


def test_1001_distinct_records_1001_rows():
    s = FakeStore()
    for n in range(1001):
        project(s, rec(n))
    assert len(s.rows) == 1001


def test_backfill_set_applied_twice_n_rows():
    s = FakeStore()
    batch = [rec(n) for n in range(50)]
    for _ in range(2):
        for r in batch:
            project(s, r)
    assert len(s.rows) == 50


def test_live_close_and_file_copy_one_row():
    live = golden()
    from_file = json.loads(json.dumps(golden()))  # same episode, other route
    s = FakeStore()
    project(s, live)
    project(s, from_file)
    assert len(s.rows) == 1


def test_build_sql_multi_column_conflict():
    sql = PostgresPool.build_sql(handle("k", golden()))
    assert 'ON CONFLICT ("episode_id", "detected_at") DO UPDATE SET' in sql
    assert 'INSERT INTO "failure_episodes"' in sql


def test_empty_releasable_to_is_a_label():
    r = golden()
    r["releasable_to"] = []
    w = handle("k", r)
    assert w is not None and w.row["releasable_to"] == []


def test_blank_region_is_null():
    r = golden()
    r["region_id"] = ""
    assert handle("k", r).row["region_id"] is None


@pytest.mark.parametrize("mutate, reason", [
    (lambda r: r.pop("detected_at_ns"), "missing_detected_at"),
    (lambda r: r.update(detected_at_ns=0), "missing_detected_at"),
    (lambda r: r.pop("cleared_at_ns"), "missing_cleared_at"),
    (lambda r: r.update(episode_id=""), "missing_episode_id"),
    (lambda r: r.pop("asset_id"), "missing_asset_id"),
    (lambda r: r.update(originator_nation=""), "unlabelled"),
    (lambda r: r.update(releasable_to="BDR"), "bad_releasable_to"),
])
def test_refusals_counted(mutate, reason):
    name = f"failure_episodes:{reason}"
    before = refused_rows().get(name, 0)
    r = golden()
    mutate(r)
    assert handle("k", r) is None
    assert refused_rows().get(name, 0) == before + 1


# -- never pruned ------------------------------------------------------------

def test_never_pruned_table_set():
    assert "failure_episodes" in NEVER_PRUNED_TABLES


def test_env_ttl_does_not_reach_failure_episodes(monkeypatch):
    monkeypatch.setenv("PROJECTOR_ASSET_TTL_HOURS", "24")
    cfg = load_config(CONFIG_YAML)
    m = next(m for m in cfg.mappings if m.table == "failure_episodes")
    assert m.asset_ttl_hours is None
    assert m.consumer_group == "projector-failure-episodes"
    assert m.topic == "failure-episodes" and m.mode == "upsert"
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
        await conn.execute(MIGRATION_SQL.replace('"public".', ""))
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
        await pool.execute(handle("k", golden()))
    assert await _count(pool) == 1
    async with pool._pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM "failure_episodes"')
    assert row["releasable_to"] == ["BDR"]
    assert json.loads(row["sources"]) == golden()["sources"]


async def test_real_pg_1001_distinct_twice(pool):
    for _ in range(2):
        for n in range(1001):
            await pool.execute(handle("k", rec(n)))
    assert await _count(pool) == 1001
