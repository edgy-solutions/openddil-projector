"""Unit tests for the persistence layer — pure SQL building, no live DB."""
from __future__ import annotations

import json

import pytest

from persistence import Revive, Write
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


def test_update_sql_is_plain_update_by_key():
    """mode='update' — a plain UPDATE that can only touch a row that
    already exists (no ON CONFLICT / no INSERT at all), for a claim that
    must never be able to create a row (e.g. a Remove Entity for an unknown
    asset_id; see handlers/telemetry_latest.py)."""
    write = Write(
        table="telemetry_latest_state",
        mode="update",
        key_columns=["asset_id"],
        row={"asset_id": "A1", "operational_status": "removed",
             "operational_status_at": "2026-09-30T00:00:00Z",
             "updated_at": "2026-09-30T00:00:00Z"},
    )
    sql = PostgresPool.build_sql(write)
    assert sql == (
        'UPDATE "telemetry_latest_state" SET '
        '"operational_status" = $2, "operational_status_at" = $3, '
        '"updated_at" = $4 '
        'WHERE "asset_id" = $1'
    )
    assert "INSERT" not in sql
    assert "ON CONFLICT" not in sql


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


def test_staleness_sweep_sql_is_update_not_delete():
    """ADR-0044 §1: no deletes on the asset path. The staleness sweep must
    be an UPDATE — this pins that down at the SQL-text level so a future
    edit can't turn it into a DELETE without a test noticing."""
    sql = PostgresPool.build_staleness_sweep_sql(
        "telemetry_latest_state",
        sample_column="last_sample_at",
        reporting_column="reporting_status",
        reporting_at_column="reporting_status_at",
    )
    assert sql.strip().upper().startswith("UPDATE")
    assert "DELETE" not in sql.upper()
    assert '"telemetry_latest_state"' in sql
    assert '"reporting_status" = $1' in sql
    assert '"reporting_status_at" = $2' in sql
    # only rows not already flagged are touched, and only by staleness
    assert '"last_sample_at" < $2::timestamptz' in sql
    assert '"reporting_status" != $1' in sql


# -- build_prune_sql (ADR-0044 §1 rule 3) -------------------------------------

def test_prune_sql_non_asset_id_keyed_is_age_only():
    """A table not keyed by asset_id (e.g. inventory_items, keyed by "id")
    reproduces the pre-ADR age-only predicate byte-for-byte — nothing to
    join a terminal-status check against."""
    sql = PostgresPool.build_prune_sql(
        "inventory_items", "updated_at", asset_id_keyed=False)
    assert sql == (
        'DELETE FROM "inventory_items" WHERE '
        '"updated_at" < now() - ($1 || \' hours\')::interval'
    )
    assert "operational_status" not in sql
    assert "reporting_status" not in sql


def test_prune_sql_lifecycle_table_checks_its_own_terminal_status():
    sql = PostgresPool.build_prune_sql(
        "telemetry_latest_state", "updated_at", asset_id_keyed=True)
    assert '"updated_at" < now()' in sql
    assert '"operational_status" IN (' in sql
    for status in ("destroyed", "deactivated", "removed"):
        assert f"'{status}'" in sql
    assert "'operational'" not in sql
    assert "reporting_status" not in sql
    assert "EXISTS" not in sql


def test_prune_sql_dependent_asset_table_joins_lifecycle_table():
    """asset_cm_state has no operational_status column of its own — it
    EXISTS-joins telemetry_latest_state on asset_id instead."""
    sql = PostgresPool.build_prune_sql(
        "asset_cm_state", "updated_at", asset_id_keyed=True)
    assert 'DELETE FROM "asset_cm_state"' in sql
    assert '"updated_at" < now()' in sql
    assert 'EXISTS (SELECT 1 FROM "telemetry_latest_state"' in sql
    assert 'lc."asset_id" = t."asset_id"' in sql
    assert 'lc."operational_status" IN (' in sql
    for status in ("destroyed", "deactivated", "removed"):
        assert f"'{status}'" in sql
    assert "reporting_status" not in sql


def test_prune_sql_terminal_predicate_present_for_asset_tables_only():
    """Pins the asset_id_keyed=True/False split at the SQL-text level: the
    terminal-status predicate must appear for asset tables and must NOT
    appear when a table isn't asset_id-keyed."""
    asset_sql = PostgresPool.build_prune_sql(
        "asset_logistics_status", "updated_at", asset_id_keyed=True)
    non_asset_sql = PostgresPool.build_prune_sql(
        "inventory_items", "updated_at", asset_id_keyed=False)
    assert "operational_status" in asset_sql
    assert "operational_status" not in non_asset_sql


# -- Write.revive (ADR-0044: `deactivated` is reversible) ---------------------

def _revive_write(**over):
    kw = dict(
        table="t",
        mode="upsert",
        key_columns=["asset_id"],
        row={"asset_id": "A1", "x": 1, "updated_at": "now"},
        revive=Revive(
            column="operational_status",
            from_values=("deactivated",),
            to_value="operational",
            at_column="operational_status_at",
            at_from="updated_at",
        ),
    )
    kw.update(over)
    return Write(**kw)


def test_revive_upsert_sql_is_two_cases_on_the_old_row():
    sql = PostgresPool.build_sql(_revive_write())
    assert sql == (
        'INSERT INTO "t" ("asset_id", "x", "updated_at") '
        "VALUES ($1, $2, $3) "
        'ON CONFLICT ("asset_id") DO UPDATE SET '
        '"x" = EXCLUDED."x", "updated_at" = EXCLUDED."updated_at", '
        '"operational_status_at" = CASE WHEN "t"."operational_status" IN ($4) '
        'THEN EXCLUDED."updated_at" ELSE "t"."operational_status_at" END, '
        '"operational_status" = CASE WHEN "t"."operational_status" IN ($4) '
        'THEN $5 ELSE "t"."operational_status" END'
    )


def test_revive_params_are_numbered_after_the_row_and_bound_in_order():
    write = _revive_write(revive=Revive(
        column="operational_status",
        from_values=("deactivated", "parked"),
        to_value="operational",
        at_column="operational_status_at",
        at_from="updated_at",
    ))
    sql = PostgresPool.build_sql(write)
    assert 'IN ($4, $5) THEN EXCLUDED."updated_at"' in sql
    assert "THEN $6 ELSE" in sql
    # never interpolated
    assert "deactivated" not in sql and "'operational'" not in sql
    assert PostgresPool._bind_values(write) == [
        "A1", 1, "now", "deactivated", "parked", "operational",
    ]


def test_no_revive_leaves_sql_and_binds_unchanged():
    write = _revive_write(revive=None)
    assert "CASE" not in PostgresPool.build_sql(write)
    assert PostgresPool._bind_values(write) == ["A1", 1, "now"]


def test_revive_outside_upsert_is_refused():
    for mode in ("append", "update"):
        with pytest.raises(ValueError):
            PostgresPool.build_sql(_revive_write(mode=mode))


def test_revive_column_that_is_also_a_row_key_is_refused():
    for extra in ("operational_status", "operational_status_at"):
        row = {"asset_id": "A1", "updated_at": "now", extra: "x"}
        with pytest.raises(ValueError):
            PostgresPool.build_sql(_revive_write(row=row))


def test_revive_at_from_missing_from_row_is_refused():
    with pytest.raises(ValueError):
        PostgresPool.build_sql(_revive_write(row={"asset_id": "A1", "x": 1}))
