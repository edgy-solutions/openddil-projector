"""Record-labelled band rows against a REAL Postgres.

The labelled append is one statement (a CTE reading telemetry_latest_state,
inserting into tactical_events only when the record carries a nation). The
string test in test_persistence.py pins its text; only a server can show
that the parameters bind to the target columns and that the four outcomes
come back as counted. This drives the real `PostgresPool.execute`.

Tables are created in a throwaway schema (the pool's SQL names them bare, so
the DSN carries `search_path`); the column lists are the stack's
tactical_events and the two label columns of telemetry_latest_state.

DSN: PROJECTOR_TEST_PG_DSN. Unset locally -> skipped. Unset under CI -> FAILED.
"""
from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from handlers import get_handler
from persistence.postgres import PostgresPool

DSN = os.environ.get("PROJECTOR_TEST_PG_DSN")


def _require_dsn() -> str:
    if DSN:
        return DSN
    if os.environ.get("CI"):
        pytest.fail("PROJECTOR_TEST_PG_DSN is unset under CI; the band labels would go unchecked")
    pytest.skip("PROJECTOR_TEST_PG_DSN unset; real-Postgres band label test not run")


@pytest.fixture
async def pool():
    dsn = _require_dsn()
    schema = f"band_test_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(dsn)
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    sep = "&" if "?" in dsn else "?"
    p = PostgresPool(f"{dsn}{sep}search_path={schema}")
    await p.connect()
    async with p._pool.acquire() as conn:
        await conn.execute(
            'CREATE TABLE "telemetry_latest_state" ('
            'asset_id text PRIMARY KEY, originator_nation text NULL, '
            'releasable_to text[] NULL)'
        )
        await conn.execute(
            'CREATE TABLE "tactical_events" ('
            'id text PRIMARY KEY, edge_id text NOT NULL, region_id text NOT NULL, '
            'source text NOT NULL, type text NOT NULL, subject text NOT NULL, '
            'severity text NULL, time timestamptz NOT NULL, data jsonb NOT NULL, '
            'ingested_at timestamptz NOT NULL DEFAULT now(), '
            'originator_nation text NULL, releasable_to text[] NULL)'
        )
    try:
        yield p
    finally:
        await p.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()


def _write(event_id="ce-b1", asset="asset-a"):
    return get_handler("tactical_events")(asset, {
        "id": event_id, "source": "/openddil/faust-edge",
        "type": "openddil.element.band", "subject": asset,
        "time": "2026-11-07T03:00:00Z",
        "data": {"edge_id": "edge-a", "region_id": "region-a",
                 "originator_nation": "BDR", "band": "amber"},
    })


async def _record(pool, asset, nation, releasable):
    async with pool._pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO "telemetry_latest_state" VALUES ($1, $2, $3)',
            asset, nation, releasable)


async def _rows(pool):
    async with pool._pool.acquire() as conn:
        return [dict(r) for r in await conn.fetch(
            'SELECT id, originator_nation, releasable_to, data FROM "tactical_events"')]


async def test_no_record_is_refused_and_writes_nothing(pool):
    assert await pool.execute(_write()) == "no_label_record"
    assert await _rows(pool) == []


async def test_unlabelled_record_is_refused_and_writes_nothing(pool):
    await _record(pool, "asset-a", None, None)
    assert await pool.execute(_write()) == "label_record_unlabelled"
    await _record(pool, "asset-b", "", ["BDR"])
    assert await pool.execute(_write("ce-b2", "asset-b")) == "label_record_unlabelled"
    assert await _rows(pool) == []


async def test_labelled_record_labels_the_row_then_a_replay_is_a_duplicate(pool):
    await _record(pool, "asset-a", "ATL", None)
    assert await pool.execute(_write()) == "inserted"
    rows = await _rows(pool)
    assert len(rows) == 1
    # The record's nation, not the one carried in data; NULL releasable -> [].
    assert rows[0]["originator_nation"] == "ATL"
    assert rows[0]["releasable_to"] == []
    assert await pool.execute(_write()) == "duplicate"
    assert len(await _rows(pool)) == 1
