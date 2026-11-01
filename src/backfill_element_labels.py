"""One-time backfill: label element rows written before labels were stamped.

Runs inside the projector pod with the pod's own store environment
(POSTGRES_DSN or PG*):

    python /app/src/backfill_element_labels.py [--dry-run]
    python /app/src/backfill_element_labels.py --apply --expect-state N --expect-telemetry M

The label source is telemetry_latest_state, the asset's ingress stamp. Rows
whose asset has no record, or an unlabelled one, are left NULL and reported;
this script never invents a label. Prints counts and asset ids only, never
connection settings.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any

import asyncpg

from persistence.postgres import StoreNotConfigured, store_dsn_from_env

TABLES = ("asset_element_state", "asset_element_telemetry")
RECORD = "telemetry_latest_state"
HALT_EXIT = 3


def build_count_sql(table: str) -> str:
    """Unlabelled rows of `table`, split by what the asset's record offers. Pure."""
    return (
        f'SELECT count(*) AS unlabelled, '
        f'count(*) FILTER (WHERE t.asset_id IS NOT NULL AND t.originator_nation IS NOT NULL) AS stampable, '
        f'count(*) FILTER (WHERE t.asset_id IS NULL) AS no_record, '
        f'count(*) FILTER (WHERE t.asset_id IS NOT NULL AND t.originator_nation IS NULL) AS record_unlabelled '
        f'FROM "{table}" e LEFT JOIN "{RECORD}" t ON t.asset_id = e.asset_id '
        f'WHERE e.originator_nation IS NULL')


def build_breakdown_sql(table: str) -> str:
    """Per-asset unlabelled row counts with the record's nation and release list. Pure."""
    return (
        f'SELECT e.asset_id, count(*) AS rows, t.asset_id IS NOT NULL AS has_record, '
        f't.originator_nation, t.releasable_to '
        f'FROM "{table}" e LEFT JOIN "{RECORD}" t ON t.asset_id = e.asset_id '
        f'WHERE e.originator_nation IS NULL '
        f'GROUP BY e.asset_id, t.asset_id, t.originator_nation, t.releasable_to '
        f'ORDER BY e.asset_id')


def build_update_sql(table: str) -> str:
    """Stamp unlabelled rows from a labelled record; updated_at is untouched. Pure."""
    return (
        f'UPDATE "{table}" SET originator_nation = t.originator_nation, '
        f'releasable_to = t.releasable_to FROM "{RECORD}" t '
        f'WHERE "{table}".asset_id = t.asset_id AND "{table}".originator_nation IS NULL '
        f'AND t.originator_nation IS NOT NULL')


LOCK_SQL = f"LOCK TABLE {', '.join(TABLES)} IN SHARE ROW EXCLUSIVE MODE"


def _record_text(row: Any) -> str:
    if not row["has_record"]:
        return "none"
    if not row["originator_nation"]:
        return "unlabelled"
    return f"{row['originator_nation']}:{list(row['releasable_to'] or [])}"


async def report(conn: Any, out: Any) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for table in TABLES:
        c = dict(await conn.fetchrow(build_count_sql(table)))
        counts[table] = c
        print(f"table={table} unlabelled={c['unlabelled']} stampable={c['stampable']} "
              f"no_record={c['no_record']} record_unlabelled={c['record_unlabelled']}",
              file=out)
        for r in await conn.fetch(build_breakdown_sql(table)):
            print(f"asset={r['asset_id']} table={table} rows={r['rows']} "
                  f"record={_record_text(r)}", file=out)
    return counts


async def apply(conn: Any, expect: dict[str, int], out: Any) -> int:
    """Stamp in ONE transaction; returns the exit code (3 on a halt)."""
    tx = conn.transaction()
    await tx.start()
    try:
        await conn.execute(LOCK_SQL)
        for table in TABLES:
            found = (await conn.fetchrow(build_count_sql(table)))["stampable"]
            if found != expect[table]:
                await tx.rollback()
                print(f"HALT expected={expect[table]} found={found} table={table}", file=out)
                return HALT_EXIT
        done: dict[str, int] = {}
        for table in TABLES:
            status = await conn.execute(build_update_sql(table))
            done[table] = int(status.rsplit(" ", 1)[-1])
            if done[table] != expect[table]:
                await tx.rollback()
                print(f"HALT expected={expect[table]} found={done[table]} table={table}",
                      file=out)
                return HALT_EXIT
    except BaseException:
        await tx.rollback()
        raise
    await tx.commit()
    for table in TABLES:
        print(f"applied table={table} rows={done[table]}", file=out)
    await report(conn, out)
    return 0


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Stamp element rows that have no labels from the asset's "
                    "telemetry_latest_state record. Dry run by default.")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="print counts per table and per asset; write nothing (default)")
    mode.add_argument("--apply", action="store_true",
                      help="stamp in one locked transaction; needs both --expect-* values")
    ap.add_argument("--expect-state", type=int, metavar="N",
                    help="rows expected stampable in asset_element_state")
    ap.add_argument("--expect-telemetry", type=int, metavar="M",
                    help="rows expected stampable in asset_element_telemetry")
    args = ap.parse_args(argv)
    if args.apply and (args.expect_state is None or args.expect_telemetry is None):
        ap.error("--apply requires --expect-state and --expect-telemetry")
    if not args.apply and (args.expect_state is not None or args.expect_telemetry is not None):
        ap.error("--expect-* is only valid with --apply")
    return args


async def run(args: argparse.Namespace, out: Any = sys.stdout) -> int:
    try:
        dsn = store_dsn_from_env()
    except StoreNotConfigured as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    conn = await asyncpg.connect(dsn)
    try:
        if args.apply:
            return await apply(conn, {"asset_element_state": args.expect_state,
                                      "asset_element_telemetry": args.expect_telemetry}, out)
        await report(conn, out)
        return 0
    finally:
        await conn.close()


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
