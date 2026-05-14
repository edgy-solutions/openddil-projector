"""Unit tests for the persistence layer — pure SQL building, no live DB."""
from __future__ import annotations

import json

from persistence import Write
from persistence.postgres import PostgresPool


def test_upsert_sql_has_on_conflict_do_update():
    write = Write(
        table="asset_cm_state",
        mode="upsert",
        key_columns=["asset_id"],
        row={"asset_id": "A1", "lifecycle": "LIFECYCLE_ACTIVE",
             "discrepancies": []},
        jsonb_columns={"discrepancies"},
    )
    sql = PostgresPool.build_sql(write)
    assert 'INSERT INTO "asset_cm_state"' in sql
    assert "ON CONFLICT (\"asset_id\") DO UPDATE SET" in sql
    # key column is NOT in the update set
    assert '"asset_id" = EXCLUDED."asset_id"' not in sql
    # non-key columns ARE
    assert '"lifecycle" = EXCLUDED."lifecycle"' in sql
    assert '"discrepancies" = EXCLUDED."discrepancies"' in sql
    # three columns -> three placeholders
    assert "$1, $2, $3" in sql


def test_append_sql_is_do_nothing():
    write = Write(
        table="tactical_events",
        mode="append",
        key_columns=["id"],
        row={"id": "ce-1", "source": "cm", "type": "x", "subject": "A1",
             "data": {}},
        jsonb_columns={"data"},
    )
    sql = PostgresPool.build_sql(write)
    assert 'INSERT INTO "tactical_events"' in sql
    assert 'ON CONFLICT ("id") DO NOTHING' in sql
    assert "DO UPDATE" not in sql


def test_composite_key_conflict_target():
    write = Write(
        table="some_table",
        mode="upsert",
        key_columns=["a", "b"],
        row={"a": 1, "b": 2, "c": 3},
    )
    sql = PostgresPool.build_sql(write)
    assert 'ON CONFLICT ("a", "b") DO UPDATE SET "c" = EXCLUDED."c"' in sql


def test_jsonb_columns_are_json_dumped():
    write = Write(
        table="t",
        mode="upsert",
        key_columns=["id"],
        row={"id": "A1", "blob": [{"k": "v"}], "plain": "text"},
        jsonb_columns={"blob"},
    )
    values = PostgresPool._bind_values(write)
    # blob serialised, plain untouched
    assert values == ["A1", json.dumps([{"k": "v"}]), "text"]


def test_jsonb_none_passes_through_as_none():
    write = Write(
        table="t", mode="upsert", key_columns=["id"],
        row={"id": "A1", "blob": None}, jsonb_columns={"blob"},
    )
    assert PostgresPool._bind_values(write) == ["A1", None]
