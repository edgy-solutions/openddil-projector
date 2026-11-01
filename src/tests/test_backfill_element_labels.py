"""Backfill of element-row labels: SQL text, argument rules and the HALT path,
against a fake connection (no database in this suite)."""
from __future__ import annotations

import io

import pytest

import backfill_element_labels as bf

STATE, TELE = "asset_element_state", "asset_element_telemetry"


class FakeTx:
    def __init__(self):
        self.events = []

    async def start(self):
        self.events.append("start")

    async def commit(self):
        self.events.append("commit")

    async def rollback(self):
        self.events.append("rollback")


class FakeConn:
    def __init__(self, stampable, updated):
        self.stampable = stampable
        self.updated = updated
        self.executed = []
        self.tx = FakeTx()

    def transaction(self):
        return self.tx

    async def execute(self, sql, *args):
        self.executed.append(sql)
        if sql.startswith("UPDATE"):
            return f"UPDATE {self.updated[sql.split(chr(34))[1]]}"
        return "LOCK TABLE"

    async def fetchrow(self, sql, *args):
        n = self.stampable[sql.split(' FROM "')[1].split('"')[0]]
        return {"unlabelled": n + 1, "stampable": n, "no_record": 1, "record_unlabelled": 0}

    async def fetch(self, sql, *args):
        return []


def test_update_sql_stamps_only_unlabelled_rows_from_labelled_records():
    sql = bf.build_update_sql(STATE)
    assert "SET originator_nation = t.originator_nation, releasable_to = t.releasable_to" in sql
    assert 'FROM "telemetry_latest_state" t' in sql
    assert "originator_nation IS NULL" in sql and "t.originator_nation IS NOT NULL" in sql
    assert "updated_at" not in sql


def test_lock_covers_both_tables_in_share_row_exclusive():
    assert bf.LOCK_SQL == ("LOCK TABLE asset_element_state, asset_element_telemetry "
                           "IN SHARE ROW EXCLUSIVE MODE")


def test_apply_requires_both_expectations_and_dry_run_is_default():
    with pytest.raises(SystemExit):
        bf.parse_args(["--apply", "--expect-state", "1"])
    with pytest.raises(SystemExit):
        bf.parse_args(["--expect-state", "1"])
    assert not bf.parse_args([]).apply
    a = bf.parse_args(["--apply", "--expect-state", "4", "--expect-telemetry", "2"])
    assert (a.expect_state, a.expect_telemetry) == (4, 2)


async def test_count_mismatch_halts_before_any_update():
    conn = FakeConn({STATE: 5, TELE: 2}, {STATE: 5, TELE: 2})
    out = io.StringIO()
    assert await bf.apply(conn, {STATE: 4, TELE: 2}, out) == 3
    assert out.getvalue().strip() == f"HALT expected=4 found=5 table={STATE}"
    assert conn.tx.events == ["start", "rollback"]
    assert not any(s.startswith("UPDATE") for s in conn.executed)


async def test_rowcount_mismatch_halts_and_rolls_back():
    conn = FakeConn({STATE: 4, TELE: 2}, {STATE: 4, TELE: 3})
    out = io.StringIO()
    assert await bf.apply(conn, {STATE: 4, TELE: 2}, out) == 3
    assert f"HALT expected=2 found=3 table={TELE}" in out.getvalue()
    assert conn.tx.events == ["start", "rollback"]


async def test_matching_counts_commit_and_report():
    conn = FakeConn({STATE: 4, TELE: 2}, {STATE: 4, TELE: 2})
    out = io.StringIO()
    assert await bf.apply(conn, {STATE: 4, TELE: 2}, out) == 0
    lines = out.getvalue().splitlines()
    assert lines[:2] == [f"applied table={STATE} rows=4", f"applied table={TELE} rows=2"]
    assert conn.tx.events == ["start", "commit"]
    assert conn.executed[0] == bf.LOCK_SQL
