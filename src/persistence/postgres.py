"""asyncpg connection pool + UPSERT / INSERT / prune helpers.

A handler produces a `Write` describing what to persist; `PostgresPool`
turns it into parameterised SQL. JSONB columns are passed as Python objects
and serialised here, so handlers never touch SQL or json.dumps.

Retry policy: a failed write is retried with exponential backoff until it
succeeds. The Kafka offset is committed by the caller only after `execute`
returns, so a Postgres outage stalls the consumer (correct — at-least-once)
rather than dropping the message.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any

import asyncpg

from lifecycle_status import TERMINAL_OPERATIONAL_STATUSES

log = logging.getLogger("projector.postgres")

# ADR-0044 §1 rule 3 / §4: the table other per-asset tables' prune
# predicates join against for "is this asset_id's operational_status
# terminal?". Not configurable — every per-asset table shares one lifecycle
# system of record, and a second one to point at would just be a second
# answer to the question ADR-0044's Alignment section says is still open
# about *who* may assert it, not about *where* the assertion lives once made.
LIFECYCLE_TABLE = "telemetry_latest_state"


@dataclass
class Write:
    """What a handler wants persisted.

    table     — target table name
    mode      — "upsert" (ON CONFLICT DO UPDATE by `key_columns`),
                "append" (plain INSERT; `key_columns` still names the PK so
                a duplicate CloudEvent id is a no-op via ON CONFLICT DO NOTHING),
                or "update" (plain UPDATE by `key_columns`; creates no row —
                a no-op, not an insert, when no row matches the key. For a
                claim that must be able to modify an existing asset without
                ever being able to conjure a new one, such as a Remove
                Entity for an asset_id this projector has no prior record
                of — see handlers/telemetry_latest.py's status-only branch)
    key_columns — primary-key column(s); the conflict target
    row       — column -> value. Values destined for jsonb columns are
                plain Python lists/dicts; `jsonb_columns` says which.
    jsonb_columns — names in `row` that must be json.dumps'd before binding
    """

    table: str
    mode: str
    key_columns: list[str]
    row: dict[str, Any]
    jsonb_columns: set[str] = field(default_factory=set)


class PostgresPool:
    """Owns the asyncpg pool and executes Writes with retry."""

    def __init__(
        self,
        dsn: str,
        *,
        retry_base_seconds: float = 0.5,
        retry_max_seconds: float = 30.0,
    ) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None
        self._retry_base = retry_base_seconds
        self._retry_max = retry_max_seconds

    async def connect(self) -> None:
        """Open the pool. Retries until Postgres is reachable."""
        attempt = 0
        while True:
            try:
                self._pool = await asyncpg.create_pool(
                    self._dsn, min_size=2, max_size=10
                )
                log.info("postgres pool ready")
                return
            except (OSError, asyncpg.PostgresError) as exc:
                delay = self._backoff(attempt)
                log.warning(
                    "postgres connect failed (attempt %d): %s — retrying in %.1fs",
                    attempt + 1,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                attempt += 1

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    def _backoff(self, attempt: int) -> float:
        return min(self._retry_base * (2 ** attempt), self._retry_max)

    # -- SQL building -------------------------------------------------------

    @staticmethod
    def build_sql(write: Write) -> str:
        """Return the parameterised SQL for a Write. Pure — unit-testable."""
        cols = list(write.row.keys())
        placeholders = [f"${i + 1}" for i in range(len(cols))]
        col_list = ", ".join(f'"{c}"' for c in cols)
        val_list = ", ".join(placeholders)
        conflict = ", ".join(f'"{c}"' for c in write.key_columns)

        if write.mode == "append":
            # A replayed CloudEvent (same id) is a harmless no-op.
            return (
                f'INSERT INTO "{write.table}" ({col_list}) '
                f"VALUES ({val_list}) "
                f"ON CONFLICT ({conflict}) DO NOTHING"
            )

        if write.mode == "update":
            # Plain UPDATE — cannot create a row (a no-op when no row
            # matches `key_columns`, never an insert). Reuse `placeholders`
            # (one per `cols` position) rather than renumbering, because
            # `_bind_values` binds values in that same `cols`/`row` order —
            # a key column keeps whatever placeholder its row position
            # already got, whether it lands in SET or WHERE below.
            set_clause = ", ".join(
                f'"{c}" = {placeholders[i]}'
                for i, c in enumerate(cols)
                if c not in write.key_columns
            )
            where_clause = " AND ".join(
                f'"{c}" = {placeholders[i]}'
                for i, c in enumerate(cols)
                if c in write.key_columns
            )
            return (
                f'UPDATE "{write.table}" SET {set_clause} '
                f"WHERE {where_clause}"
            )

        # upsert: overwrite every non-key column on conflict.
        updates = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in cols
            if c not in write.key_columns
        )
        return (
            f'INSERT INTO "{write.table}" ({col_list}) '
            f"VALUES ({val_list}) "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        )

    @staticmethod
    def _bind_values(write: Write) -> list[Any]:
        values: list[Any] = []
        for col, val in write.row.items():
            if col in write.jsonb_columns and val is not None:
                values.append(json.dumps(val))
            else:
                values.append(val)
        return values

    # -- execution ----------------------------------------------------------

    async def execute(self, write: Write) -> int | None:
        """Execute a Write, retrying on transient failure until it succeeds.

        Returns the rows-affected count for mode "update" (asyncpg's status
        string, e.g. "UPDATE 0" / "UPDATE 1", parsed the same way
        `prune_older_than` and `sweep_reporting_staleness` already parse
        their own DELETE/UPDATE counts). Other modes are unchanged: no
        caller of "upsert"/"append" reads a return value today, so this
        stays None for them rather than guessing at a meaning.
        """
        if self._pool is None:
            raise RuntimeError("PostgresPool.execute called before connect()")
        sql = self.build_sql(write)
        values = self._bind_values(write)
        attempt = 0
        while True:
            try:
                async with self._pool.acquire() as conn:
                    result = await conn.execute(sql, *values)
                if write.mode == "update":
                    try:
                        return int(result.split()[-1])
                    except (ValueError, IndexError):  # pragma: no cover
                        return 0
                return None
            except (OSError, asyncpg.PostgresConnectionError) as exc:
                # Transient — Postgres restarting, network blip. Retry.
                delay = self._backoff(attempt)
                log.warning(
                    "postgres write to %s failed (attempt %d): %s — retry in %.1fs",
                    write.table,
                    attempt + 1,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                attempt += 1
            except asyncpg.PostgresError as exc:
                # Data/constraint error — retrying won't help. Surface it so
                # the caller can log-and-skip (do NOT commit the offset on a
                # raise; the caller decides). Re-raise as a clear type.
                raise PostgresWriteError(
                    f"non-retryable write to {write.table}: {exc}"
                ) from exc

    @staticmethod
    def build_staleness_sweep_sql(
        table: str,
        *,
        sample_column: str,
        reporting_column: str,
        reporting_at_column: str,
    ) -> str:
        """UPDATE, never DELETE — a row that passes out of the staleness
        window flips its reporting_status; it does not disappear (ADR-0044
        §1, "no deletes... anywhere, on the asset path"). Pure — unit
        testable without a live Postgres, same as `build_sql`.

        $1 = the "not_reporting" value, $2 = this reader's own now, $3 =
        this reader's own stale_after_s. Only rows not already flagged are
        touched, so a repeated sweep over an already-stale row is a no-op
        rather than repeatedly bumping reporting_at_column.

        `$2::timestamptz` is load-bearing: uncast, Postgres infers $2 from
        `$2 - interval` as an interval, and the comparison becomes
        timestamptz < interval, which fails on every pass. Only a real server
        can see that (test_sweep_real_postgres.py).
        """
        return (
            f'UPDATE "{table}" SET '
            f'"{reporting_column}" = $1, "{reporting_at_column}" = $2 '
            f'WHERE "{sample_column}" < $2::timestamptz - ($3 || \' seconds\')::interval '
            f'AND "{reporting_column}" != $1'
        )

    async def sweep_reporting_staleness(
        self,
        table: str,
        *,
        sample_column: str,
        reporting_column: str,
        reporting_at_column: str,
        not_reporting_value: str,
        stale_after_s: float,
        now: Any,
    ) -> int:
        """Flip rows whose sample is older than `stale_after_s`, as measured
        by THIS pool's own `now`, to `not_reporting_value`. Returns rows
        touched.

        Deliberately does not also flip stale rows back to "reporting" —
        recovery is the arrival path's job (a handler writes
        reporting_status="reporting" unconditionally on every message; see
        handlers/telemetry_latest.py). This sweep only ever moves in the
        stale direction, which is what makes it safe to run independently,
        on its own interval, per reading tier (ADR §4).
        """
        if self._pool is None:
            raise RuntimeError(
                "PostgresPool.sweep_reporting_staleness before connect()"
            )
        sql = self.build_staleness_sweep_sql(
            table,
            sample_column=sample_column,
            reporting_column=reporting_column,
            reporting_at_column=reporting_at_column,
        )
        async with self._pool.acquire() as conn:
            result = await conn.execute(sql, not_reporting_value, now, str(stale_after_s))
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):  # pragma: no cover
            return 0

    @staticmethod
    def build_prune_sql(table: str, time_column: str, *,
                         asset_id_keyed: bool) -> str:
        """Return the parameterised prune DELETE for `table`. Pure — unit
        testable without a live Postgres, same as `build_sql`.

        $1 = the TTL, in hours (bound as text and cast, same as before this
        ADR — `prune_older_than` passes `str(hours)`).

        ADR-0044 §1 rule 3: age alone is no longer a sufficient predicate for
        any table that carries a per-asset operational_status, because "only
        a terminal status is eligible for retention" — a quiet-but-still-
        operational asset must never be pruned merely for being old
        (rule 3's "withdrawal cannot be inferred from silence", extended from
        reporting_status to age). `asset_id_keyed=False` is the escape hatch
        for the tables this doesn't apply to (see NON_ASSET_ID_KEYED comment
        at the call site in main.py) and reproduces the pre-ADR age-only SQL
        byte-for-byte, so nothing changes for them.

        `table == LIFECYCLE_TABLE` (telemetry_latest_state itself) checks its
        OWN operational_status column. Every other asset_id-keyed table has
        no such column of its own — it was never part of what this ADR two-
        columned — so it EXISTS-joins telemetry_latest_state on asset_id
        instead. Both branches read reporting_status nowhere: reporting_status
        must never be a prune input (ADR §2 — it answers a different question
        than "is this asset gone").
        """
        age_predicate = (
            f'"{time_column}" < now() - ($1 || \' hours\')::interval'
        )
        if not asset_id_keyed:
            return f'DELETE FROM "{table}" WHERE {age_predicate}'

        terminal_list = ", ".join(f"'{s}'" for s in TERMINAL_OPERATIONAL_STATUSES)
        if table == LIFECYCLE_TABLE:
            return (
                f'DELETE FROM "{table}" WHERE {age_predicate} '
                f'AND "operational_status" IN ({terminal_list})'
            )
        return (
            f'DELETE FROM "{table}" AS t WHERE t.{age_predicate} '
            f'AND EXISTS (SELECT 1 FROM "{LIFECYCLE_TABLE}" AS lc '
            f'WHERE lc."asset_id" = t."asset_id" '
            f'AND lc."operational_status" IN ({terminal_list}))'
        )

    async def prune_older_than(self, table: str, time_column: str,
                               hours: float, *,
                               asset_id_keyed: bool = False) -> int:
        """Delete rows eligible for retention per `build_prune_sql`. Returns
        rows deleted."""
        if self._pool is None:
            raise RuntimeError("PostgresPool.prune_older_than before connect()")
        sql = self.build_prune_sql(table, time_column,
                                    asset_id_keyed=asset_id_keyed)
        async with self._pool.acquire() as conn:
            result = await conn.execute(sql, str(hours))
        # asyncpg returns e.g. "DELETE 12"
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):  # pragma: no cover
            return 0


class PostgresWriteError(Exception):
    """A non-retryable Postgres write error — caller logs-and-skips."""
