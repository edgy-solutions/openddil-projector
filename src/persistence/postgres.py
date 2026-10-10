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


@dataclass(frozen=True)
class Revive:
    """A conditional return from one status value to another, on upsert.

    column      - the status column that may be rewritten
    from_values - the only values of `column` that are rewritten; any other
                  existing value is left exactly as it is
    to_value    - what `column` becomes when it held one of `from_values`
    at_column   - the timestamp column stamped when `column` is rewritten
    at_from     - the `row` key whose value `at_column` takes on a rewrite

    Everything is bound as a parameter, never interpolated. On INSERT (no
    existing row) a Revive adds nothing: the column default already applies.
    """

    column: str
    from_values: tuple[str, ...]
    to_value: str
    at_column: str
    at_from: str


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
    revive    — upsert only: on conflict, move `revive.column` from one of
                `revive.from_values` to `revive.to_value` (stamping
                `revive.at_column` from the row's `revive.at_from`), else
                leave both columns as they were. Neither column may also be
                a key in `row`: the CASE must be their only assignment.
    """

    table: str
    mode: str
    key_columns: list[str]
    row: dict[str, Any]
    jsonb_columns: set[str] = field(default_factory=set)
    revive: Revive | None = None


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
        revive = write.revive
        if revive is not None:
            if write.mode != "upsert":
                raise ValueError("Write.revive is only valid for mode 'upsert'")
            if revive.column in write.row or revive.at_column in write.row:
                raise ValueError(
                    "Write.revive columns must not also be keys in row: "
                    "the CASE must be their only assignment"
                )
            if revive.at_from not in write.row:
                raise ValueError(
                    f"Write.revive.at_from {revive.at_from!r} is not a key in row"
                )
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
        if revive is not None:
            # from_values / to_value are bound after the row's own
            # placeholders (see _bind_values), never interpolated. Both
            # CASEs read the pre-update row (Postgres evaluates every SET
            # expression against the old row), so the status test in the
            # timestamp CASE still sees the old status and their order
            # here does not matter.
            n = len(cols)
            in_params = ", ".join(
                f"${n + 1 + i}" for i in range(len(revive.from_values))
            )
            to_param = f"${n + 1 + len(revive.from_values)}"
            t, c, a = write.table, revive.column, revive.at_column
            case = f'"{t}"."{c}" IN ({in_params})'
            revive_sql = (
                f'"{a}" = CASE WHEN {case} THEN EXCLUDED."{revive.at_from}" '
                f'ELSE "{t}"."{a}" END, '
                f'"{c}" = CASE WHEN {case} THEN {to_param} '
                f'ELSE "{t}"."{c}" END'
            )
            updates = f"{updates}, {revive_sql}" if updates else revive_sql
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
        if write.revive is not None:
            values.extend(write.revive.from_values)
            values.append(write.revive.to_value)
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

    # -- effector_launch (launch-record admission/termination) -------------
    #
    # `effector_launch` is the one table in this projector whose handler owns
    # its own Postgres I/O (config.Mapping mode "custom" — see handlers/
    # effector_launch.py and main.py's dispatch branch for it), because two
    # of its three operations cannot be expressed as a single pure Write:
    #
    #   * Fire needs a READ before it decides whether to write at all (the
    #     launcher-admission check) — `row_exists` below, a generic version
    #     of the EXISTS-join idiom `build_prune_sql` already uses.
    #   * Detonation needs to know the row's OLD terminal_state to tell
    #     apart "no matching Fire" / "a real result replacing an
    #     already-timed-out row" / "a duplicate of an already-terminal row"
    #     / "the normal case" — a plain UPDATE's rows-affected count (which
    #     `execute()` already reports for mode="update", so that ability did
    #     not need adding) can only ever distinguish zero from non-zero, not
    #     which of the three non-zero cases happened. `apply_effector_
    #     detonation` below reads the old value with SELECT ... FOR UPDATE
    #     and decides inside the same transaction.
    #
    # Fire's own write, once admitted, needs nothing bespoke — it is a plain
    # `Write(mode="append", ...)` through the existing `execute()`/`build_sql`
    # path above, same ON CONFLICT DO NOTHING idempotency every other
    # append-mode handler gets for free.

    @staticmethod
    def build_row_exists_sql(table: str, column: str) -> str:
        """Pure — unit testable without a live Postgres, same as `build_sql`.
        $1 = the value to look for."""
        return f'SELECT EXISTS (SELECT 1 FROM "{table}" WHERE "{column}" = $1)'

    async def row_exists(self, table: str, column: str, value: Any) -> bool:
        """Generic admission-style check: does any row have `column` = value?
        Used by the effector_launch Fire path to confirm the launcher is a
        known asset (a `telemetry_latest_state` row exists for it) before
        admitting the launch."""
        if self._pool is None:
            raise RuntimeError("PostgresPool.row_exists before connect()")
        sql = self.build_row_exists_sql(table, column)
        async with self._pool.acquire() as conn:
            return bool(await conn.fetchval(sql, value))

    async def apply_effector_detonation(
        self,
        *,
        event_urn: str,
        terminal_state: str,
        detonation_result: int,
        terminated_at: Any,
        now: Any,
    ) -> tuple[str, int | None]:
        """Apply a Detonation to `effector_launch` by event_urn. Returns
        (outcome, old_detonation_result) where outcome is one of:

          "updated"     — the row was in flight (terminal_state IS NULL);
                          now resolved with a real result.
          "late"        — the row was already 'unresolved' (the timeout
                          sweep fired first); the real result replaces it
                          and late_terminal is set true. A termination
                          event outranks a timeout inference.
          "replayed"    — the row was already terminal with a real result,
                          and that result is the SAME detonation_result
                          this call is carrying -- the same Detonation
                          reaching the store a second time (e.g. an edge
                          projector and the HQ projector both admitting the
                          same Fire and then both applying the same
                          Detonation). Left unchanged; not a refusal.
          "conflicting" — the row was already terminal with a real result,
                          and that result is a DIFFERENT detonation_result
                          -- two different termination claims for the same
                          event_urn. Left unchanged; a real problem, unlike
                          "replayed".
          "no_fire"     — no row exists for this event_urn; nothing to
                          update.

        old_detonation_result is the row's previous value (None if no row
        existed), for a caller that wants to log what the new result
        conflicted with.

        SELECT ... FOR UPDATE then a conditional UPDATE in the same
        transaction: these outcomes depend on the row's OLD terminal_state
        (and, for the terminal case, its OLD detonation_result), which a
        single WHERE-matched UPDATE's rows-affected count cannot
        distinguish (see the module-level comment above this method).
        """
        if self._pool is None:
            raise RuntimeError(
                "PostgresPool.apply_effector_detonation before connect()"
            )
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    'SELECT "terminal_state", "detonation_result" '
                    'FROM "effector_launch" WHERE "event_urn" = $1 FOR UPDATE',
                    event_urn,
                )
                if row is None:
                    return "no_fire", None
                old_state = row["terminal_state"]
                old_result = row["detonation_result"]
                if old_state is not None and old_state != "unresolved":
                    if old_result == detonation_result:
                        return "replayed", old_result
                    return "conflicting", old_result
                late = old_state == "unresolved"
                await conn.execute(
                    'UPDATE "effector_launch" SET '
                    '"terminal_state" = $1, "detonation_result" = $2, '
                    '"terminated_at" = $3, "late_terminal" = $4, '
                    '"updated_at" = $5 '
                    'WHERE "event_urn" = $6',
                    terminal_state, detonation_result, terminated_at,
                    late, now, event_urn,
                )
                return ("late" if late else "updated"), old_result

    @staticmethod
    def build_effector_timeout_sweep_sql() -> str:
        """UPDATE, never DELETE — same discipline as
        `build_staleness_sweep_sql` (ADR-0044 rule 3, "no deletes on the
        asset path"; effector_launch is not asset-ttl-pruned at all — see
        projector_config.yaml's effector-events entry — but the "never
        delete a launch row" rule is the same one in spirit: expended must
        never decrease).

        $1 = this reader's own now, $2 = EFFECTOR_TERMINAL_TIMEOUT_S. Only
        rows still in flight (terminal_state IS NULL) and past the timeout
        are touched."""
        return (
            'UPDATE "effector_launch" SET '
            '"terminal_state" = \'unresolved\', "terminated_at" = $1, '
            '"updated_at" = $1 '
            'WHERE "terminal_state" IS NULL '
            'AND "launched_at" < $1::timestamptz - ($2 || \' seconds\')::interval'
        )

    async def sweep_effector_timeouts(self, *, timeout_s: float, now: Any) -> int:
        """Mark in-flight rows past EFFECTOR_TERMINAL_TIMEOUT_S as
        'unresolved'. Returns rows touched."""
        if self._pool is None:
            raise RuntimeError("PostgresPool.sweep_effector_timeouts before connect()")
        sql = self.build_effector_timeout_sweep_sql()
        async with self._pool.acquire() as conn:
            result = await conn.execute(sql, now, str(timeout_s))
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):  # pragma: no cover
            return 0

    async def replace_effector_declared_load(
        self, rows: list[tuple[str, str, str, int]]
    ) -> None:
        """Replace `effector_declared_load`'s entire contents in one
        transaction (startup-only; see handlers/effector_declared_load.py).
        Empty `rows` empties the table, so every `remaining` in
        `effector_launcher_counts` then reads NULL rather than 0."""
        if self._pool is None:
            raise RuntimeError(
                "PostgresPool.replace_effector_declared_load before connect()"
            )
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute('DELETE FROM "effector_declared_load"')
                if rows:
                    await conn.executemany(
                        'INSERT INTO "effector_declared_load" '
                        '("load_key", "key_kind", "munition_type", "declared") '
                        "VALUES ($1, $2, $3, $4)",
                        rows,
                    )

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
