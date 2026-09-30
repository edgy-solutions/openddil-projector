"""The reporting sweep against a REAL Postgres (ADR-0044 §1).

The string-shape tests in test_persistence.py passed while the sweep failed on
every pass in every deployment: Postgres inferred `$2` in
`"<sample>" < $2 - ($3 || ' seconds')::interval` as an interval, so the
comparison was timestamptz < interval and the UPDATE never ran. Only a
server that plans the statement can see that, so this module executes the
sweep through `PostgresPool.sweep_reporting_staleness` — the same binding
the service uses — against a throwaway table.

DSN: PROJECTOR_TEST_PG_DSN. Unset locally → skipped. Unset under CI → FAILED,
because a skip there would read as green while checking nothing.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from persistence import Write
from persistence.postgres import PostgresPool

DSN = os.environ.get("PROJECTOR_TEST_PG_DSN")


def _require_dsn() -> str:
    if DSN:
        return DSN
    if os.environ.get("CI"):
        pytest.fail("PROJECTOR_TEST_PG_DSN is unset under CI; the sweep would go unchecked")
    pytest.skip("PROJECTOR_TEST_PG_DSN unset; real-Postgres sweep test not run")


@pytest.fixture
async def pool_and_table():
    pool = PostgresPool(_require_dsn())
    await pool.connect()
    table = f"sweep_test_{uuid.uuid4().hex[:12]}"
    async with pool._pool.acquire() as conn:
        await conn.execute(
            f'CREATE TABLE "{table}" ('
            'asset_id text PRIMARY KEY, '
            'last_sample_at timestamptz NOT NULL, '
            "reporting_status text NOT NULL DEFAULT 'reporting', "
            'reporting_status_at timestamptz)'
        )
    try:
        yield pool, table
    finally:
        async with pool._pool.acquire() as conn:
            await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        await pool.close()


async def _seed(pool, table, now):
    async with pool._pool.acquire() as conn:
        await conn.executemany(
            f'INSERT INTO "{table}" (asset_id, last_sample_at) VALUES ($1, $2)',
            [("stale", now - timedelta(seconds=120)),
             ("fresh", now - timedelta(seconds=5))],
        )


async def _statuses(pool, table):
    async with pool._pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT asset_id, reporting_status, reporting_status_at FROM "{table}" ORDER BY asset_id')
    return {r["asset_id"]: (r["reporting_status"], r["reporting_status_at"]) for r in rows}


async def _sweep(pool, table, now):
    return await pool.sweep_reporting_staleness(
        table,
        sample_column="last_sample_at",
        reporting_column="reporting_status",
        reporting_at_column="reporting_status_at",
        not_reporting_value="not_reporting",
        stale_after_s=30.0,
        now=now,
    )


async def test_sweep_flags_only_the_stale_row(pool_and_table):
    pool, table = pool_and_table
    now = datetime.now(timezone.utc)
    await _seed(pool, table, now)

    touched = await _sweep(pool, table, now)

    assert touched == 1
    got = await _statuses(pool, table)
    assert got["stale"] == ("not_reporting", now)
    assert got["fresh"] == ("reporting", None)


async def test_sweep_is_a_noop_on_an_already_flagged_row(pool_and_table):
    pool, table = pool_and_table
    now = datetime.now(timezone.utc)
    await _seed(pool, table, now)
    await _sweep(pool, table, now)

    later = now + timedelta(seconds=10)
    assert await _sweep(pool, table, later) == 0
    got = await _statuses(pool, table)
    # reporting_status_at is not bumped by a repeated sweep
    assert got["stale"] == ("not_reporting", now)


async def test_sweep_never_deletes(pool_and_table):
    pool, table = pool_and_table
    now = datetime.now(timezone.utc)
    await _seed(pool, table, now)
    await _sweep(pool, table, now + timedelta(hours=48))
    assert set(await _statuses(pool, table)) == {"stale", "fresh"}


# -- Write mode "update" (removal-for-unknown-asset must not create a row) --
#
# The string-shape test (test_persistence.py) pins the SQL text; only a real
# server proves the semantic this mode exists for — that an UPDATE with no
# matching row genuinely creates nothing, versus e.g. a typo'd WHERE clause
# that happens to also return 0 for the wrong reason.

@pytest.fixture
async def update_pool_and_table():
    pool = PostgresPool(_require_dsn())
    await pool.connect()
    table = f"update_test_{uuid.uuid4().hex[:12]}"
    async with pool._pool.acquire() as conn:
        await conn.execute(
            f'CREATE TABLE "{table}" ('
            "asset_id text PRIMARY KEY, "
            "operational_status text)"
        )
    try:
        yield pool, table
    finally:
        async with pool._pool.acquire() as conn:
            await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        await pool.close()


async def test_update_mode_on_missing_key_returns_zero_and_creates_no_row(update_pool_and_table):
    pool, table = update_pool_and_table
    write = Write(
        table=table,
        mode="update",
        key_columns=["asset_id"],
        row={"asset_id": "unknown-asset", "operational_status": "removed"},
    )
    rows_affected = await pool.execute(write)
    assert rows_affected == 0
    async with pool._pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT * FROM "{table}" WHERE asset_id = $1', "unknown-asset")
    assert row is None


async def test_update_mode_on_existing_row_returns_one(update_pool_and_table):
    pool, table = update_pool_and_table
    async with pool._pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{table}" (asset_id, operational_status) VALUES ($1, $2)',
            "known-asset", "operational",
        )
    write = Write(
        table=table,
        mode="update",
        key_columns=["asset_id"],
        row={"asset_id": "known-asset", "operational_status": "removed"},
    )
    rows_affected = await pool.execute(write)
    assert rows_affected == 1
    async with pool._pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT operational_status FROM "{table}" WHERE asset_id = $1', "known-asset")
    assert row["operational_status"] == "removed"
