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

log = logging.getLogger("projector.postgres")


@dataclass
class Write:
    """What a handler wants persisted.

    table     — target table name
    mode      — "upsert" (ON CONFLICT DO UPDATE by `key_columns`) or
                "append" (plain INSERT; `key_columns` still names the PK so
                a duplicate CloudEvent id is a no-op via ON CONFLICT DO NOTHING)
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

    async def execute(self, write: Write) -> None:
        """Execute a Write, retrying on transient failure until it succeeds."""
        if self._pool is None:
            raise RuntimeError("PostgresPool.execute called before connect()")
        sql = self.build_sql(write)
        values = self._bind_values(write)
        attempt = 0
        while True:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(sql, *values)
                return
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

    async def prune_older_than(self, table: str, time_column: str,
                               hours: int) -> int:
        """Delete rows older than `hours`. Returns rows deleted."""
        if self._pool is None:
            raise RuntimeError("PostgresPool.prune_older_than before connect()")
        sql = (
            f'DELETE FROM "{table}" '
            f"WHERE \"{time_column}\" < now() - ($1 || ' hours')::interval"
        )
        async with self._pool.acquire() as conn:
            result = await conn.execute(sql, str(hours))
        # asyncpg returns e.g. "DELETE 12"
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):  # pragma: no cover
            return 0


class PostgresWriteError(Exception):
    """A non-retryable Postgres write error — caller logs-and-skips."""
