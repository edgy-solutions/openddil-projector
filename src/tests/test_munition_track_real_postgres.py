"""munition_track launcher heal against a REAL Postgres (ADR-0049).

The in-memory pool in test_munition_track.py mirrors the decisions; only a
server can show that the SQL itself converges the race outcome: the track
insert read no launch row, the Fire's back-fill found no track row, and the
track stayed NULL. This drives the real `PostgresPool` methods.

Tables are created in a throwaway schema (the pool's SQL names them bare, so
the DSN carries `search_path`); the column lists are the ones these methods
use, copied from the stack migrations 20261015000000_munition_track.sql and
20261006000000 / 20261008020000 effector_launch.

DSN: PROJECTOR_TEST_PG_DSN. Unset locally -> skipped. Unset under CI -> FAILED.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from persistence import Write
from persistence.postgres import PostgresPool

DSN = os.environ.get("PROJECTOR_TEST_PG_DSN")
URN = "dis:1:1:5001"
T0 = datetime(2026, 10, 15, 12, 0, 0, tzinfo=timezone.utc)


def _require_dsn() -> str:
    if DSN:
        return DSN
    if os.environ.get("CI"):
        pytest.fail("PROJECTOR_TEST_PG_DSN is unset under CI; the heal would go unchecked")
    pytest.skip("PROJECTOR_TEST_PG_DSN unset; real-Postgres munition_track test not run")


@pytest.fixture
async def pool():
    dsn = _require_dsn()
    schema = f"mt_test_{uuid.uuid4().hex[:12]}"
    admin = await asyncpg.connect(dsn)
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    sep = "&" if "?" in dsn else "?"
    p = PostgresPool(f"{dsn}{sep}search_path={schema}")
    await p.connect()
    async with p._pool.acquire() as conn:
        await conn.execute(
            'CREATE TABLE "munition_track" ('
            'munition_asset_id text PRIMARY KEY, launcher_asset_id text NULL, '
            'event_urn text NULL, munition_type text NOT NULL, '
            'ecef_x double precision NOT NULL, ecef_y double precision NOT NULL, '
            'ecef_z double precision NOT NULL, first_seen_at timestamptz NOT NULL, '
            'last_sample_at timestamptz NOT NULL, last_heard_at timestamptz NOT NULL, '
            'terminal_state text NULL, detonation_result int NULL, '
            'terminated_at timestamptz NULL, late_terminal boolean NOT NULL DEFAULT false, '
            'edge_id text NULL, region_id text NULL, originator_nation text NULL, '
            'releasable_to text[] NULL, updated_at timestamptz NOT NULL DEFAULT now(), '
            'trail_ecef double precision[] NULL)'
        )
        await conn.execute(
            'CREATE TABLE "effector_launch" ('
            'event_urn text PRIMARY KEY, launcher_asset_id text NOT NULL, '
            'munition_asset_id text NULL, munition_type text NOT NULL, '
            'quantity int NOT NULL, launched_at timestamptz NOT NULL, '
            'edge_id text NULL, region_id text NULL, originator_nation text NULL, '
            'releasable_to text[] NULL, updated_at timestamptz NOT NULL DEFAULT now())'
        )
    try:
        yield p
    finally:
        await p.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()


async def _sample(pool, t, urn=URN):
    return await pool.upsert_munition_track(
        munition_asset_id=urn, munition_type="2.9.225.2.1.1.0",
        ecef=(1.0, 2.0, 3.0), sample_time=t, edge_id="edge-01",
        region_id="region-01", now=t)


async def _launch(pool, *, urn=URN, event="dis-event:1:58:1",
                  launcher="dis:1:58:1001", at=T0 - timedelta(seconds=1)):
    """The launch row only -- the join is deliberately not called."""
    await pool.execute(Write(
        table="effector_launch", mode="append", key_columns=["event_urn"],
        row={"event_urn": event, "launcher_asset_id": launcher,
             "munition_asset_id": urn, "munition_type": "2.9.225.2.1.1.0",
             "quantity": 1, "launched_at": at, "edge_id": "edge-fire",
             "region_id": "region-fire", "originator_nation": "ATL",
             "releasable_to": ["BDR"]}))


async def _track(pool, urn=URN):
    async with pool._pool.acquire() as conn:
        return dict(await conn.fetchrow(
            'SELECT * FROM "munition_track" WHERE munition_asset_id = $1', urn))


def _assert_labelled(r):
    assert r["launcher_asset_id"] == "dis:1:58:1001"
    assert r["event_urn"] == "dis-event:1:58:1"
    assert r["edge_id"] == "edge-fire" and r["region_id"] == "region-fire"
    assert r["originator_nation"] == "ATL"
    assert r["releasable_to"] == ["BDR"]


async def test_next_sample_heals_a_missed_join(pool):
    assert await _sample(pool, T0) == ("inserted", False)
    await _launch(pool)
    assert (await _track(pool))["launcher_asset_id"] is None
    outcome, joined = await _sample(pool, T0 + timedelta(seconds=1))
    assert (outcome, joined) == ("updated", True)
    _assert_labelled(await _track(pool))
    # Joined now: a further sample does not report a join.
    assert await _sample(pool, T0 + timedelta(seconds=2)) == ("updated", False)


async def test_stale_sample_heals(pool):
    await _sample(pool, T0)
    await _launch(pool)
    assert await _sample(pool, T0 - timedelta(seconds=5)) == ("stale", True)
    _assert_labelled(await _track(pool))


async def test_sample_after_terminal_heals_without_changing_state(pool):
    await _sample(pool, T0)
    await pool.terminate_munition_track(
        munition_asset_id=URN, event_urn="dis-event:9:9:9", terminal_state="dud",
        detonation_result=6, terminated_at=T0, now=T0)
    await _launch(pool)                        # race outcome on a terminal row
    assert await _sample(pool, T0 + timedelta(seconds=3)) == ("after_terminal", True)
    r = await _track(pool)
    _assert_labelled(r)
    assert r["terminal_state"] == "dud" and r["last_sample_at"] == T0


async def test_detonation_terminates_and_labels(pool):
    await _sample(pool, T0)
    await _launch(pool)
    res = await pool.terminate_munition_track(
        munition_asset_id=URN, event_urn="dis-event:1:58:1",
        terminal_state="entity_impact", detonation_result=1,
        terminated_at=T0 + timedelta(seconds=4), now=T0 + timedelta(seconds=4))
    assert res == "terminated"
    r = await _track(pool)
    _assert_labelled(r)
    assert r["terminal_state"] == "entity_impact"


async def test_sweep_heal_labels_terminal_row_and_never_rehomes(pool):
    await _sample(pool, T0)
    await pool.terminate_munition_track(
        munition_asset_id=URN, event_urn="x", terminal_state="dud",
        detonation_result=6, terminated_at=T0, now=T0)
    await _launch(pool)                        # race outcome, row is terminal
    other = "dis:1:1:5002"
    await _launch(pool, urn=other, event="dis-event:1:58:7",
                  launcher="dis:1:58:1111")
    await _sample(pool, T0, urn=other)         # joins A at insert
    await _launch(pool, urn=other, event="dis-event:1:58:8",
                  launcher="dis:1:58:2222", at=T0 + timedelta(seconds=30))
    assert await pool.heal_munition_track_launchers(now=T0 + timedelta(seconds=40)) == 1
    r = await _track(pool)
    _assert_labelled(r)
    assert r["terminal_state"] == "dud"
    o = await _track(pool, other)
    assert o["launcher_asset_id"] == "dis:1:58:1111"
    assert o["event_urn"] == "dis-event:1:58:7"
    # Idempotent.
    assert await pool.heal_munition_track_launchers(now=T0 + timedelta(seconds=41)) == 0


async def test_no_launch_stays_null(pool):
    await _sample(pool, T0)
    await _sample(pool, T0 + timedelta(seconds=1))
    assert await pool.heal_munition_track_launchers(now=T0) == 0
    assert (await _track(pool))["launcher_asset_id"] is None


# -- trail_ecef (ADR-0049) ----------------------------------------------------

TRAIL_URN = "dis:9:9:1"


def _triples(flat):
    return [tuple(flat[i:i + 3]) for i in range(0, len(flat), 3)]


async def _move(pool, n, urn=TRAIL_URN):
    return await pool.upsert_munition_track(
        munition_asset_id=urn, munition_type="2.9.225.2.1.1.0",
        ecef=(float(n), 2.0, 3.0), sample_time=T0 + timedelta(seconds=n),
        edge_id="edge-01", region_id="region-01", now=T0 + timedelta(seconds=n))


async def test_trail_grows_and_ends_at_the_latest_ecef(pool):
    assert await _move(pool, 0) == ("inserted", False)
    for n in (1, 2, 3):
        assert (await _move(pool, n))[0] == "updated"
    r = await _track(pool, TRAIL_URN)
    assert len(_triples(r["trail_ecef"])) == 4
    assert _triples(r["trail_ecef"])[-1] == (r["ecef_x"], r["ecef_y"], r["ecef_z"])


async def test_stale_sample_leaves_the_trail_alone(pool):
    await _move(pool, 5)
    await _move(pool, 6)
    before = (await _track(pool, TRAIL_URN))["trail_ecef"]
    assert (await _move(pool, 3))[0] == "stale"
    after = await _track(pool, TRAIL_URN)
    assert after["trail_ecef"] == before and after["ecef_x"] == 6.0


async def test_trail_caps_at_ten_triples(pool):
    await _move(pool, 0)
    for n in range(1, 13):
        await _move(pool, n)
    trail = (await _track(pool, TRAIL_URN))["trail_ecef"]
    assert len(_triples(trail)) == 10
    assert _triples(trail)[0][0] == 3.0 and _triples(trail)[-1][0] == 12.0


@pytest.fixture
async def pool_no_trail_column(pool):
    """The same table as a bundle image that predates the migration has it."""
    async with pool._pool.acquire() as conn:
        await conn.execute('ALTER TABLE "munition_track" DROP COLUMN "trail_ecef"')
    pool._trail_supported = None  # forget the probe made against the full table
    return pool


async def test_table_without_trail_column_still_works(pool_no_trail_column, caplog):
    pool = pool_no_trail_column
    with caplog.at_level("WARNING", logger="projector.postgres"):
        assert await _move(pool, 0) == ("inserted", False)
        assert (await _move(pool, 1))[0] == "updated"
        assert (await _move(pool, 0))[0] == "stale"
    r = await _track(pool, TRAIL_URN)
    assert "trail_ecef" not in r and r["ecef_x"] == 1.0
    assert sum("trail_ecef" in m for m in caplog.messages) == 1  # cached False, one probe
