"""OpenDDIL Projector — entrypoint.

One async consumer task per configured topic. Each task drains a batch from
Kafka, decodes + maps each message to a `Write`, persists the batch to
Postgres, then commits the consumed offsets. Offsets advance ONLY after the
batch is durable — at-least-once delivery.

Batch-level coalescing provides the rate limiting: within one drained batch,
compacted-topic messages are deduped by key (latest wins) before writing, so
an asset spamming updates costs one UPSERT, not N. Offsets stay contiguous
within the batch, so dedup never risks skipping another key's message.

Background tasks: a retention pruner for append-mode tables, a consumer-
lag gauge updater, and (ADR-0044 lifecycle slice 1) a reporting-status
staleness sweep over telemetry_latest_state, evaluated on this instance's
own clock and interval only (see reporting_sweep.py). SIGHUP reloads config
(adds/removes consumers, refreshes settings) without dropping unchanged
connections.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition

from config import Config, Mapping, load_config
from edge_assignment import configure_from_config as configure_edge_assignment
from decoders import (
    DecodeError,
    ProtoDecoder,
    decode_cloudevent,
    decode_json,
)
from edge_buffer_monitor import edge_buffer_loop
from handlers import get_handler
from metrics import (
    DECODE_ERRORS,
    MESSAGES_CONSUMED,
    POSTGRES_ERRORS,
    REMOVAL_UNKNOWN_ASSET_DROPPED,
    ROWS_PRUNED,
    TOPIC_LAG,
    UPSERTS,
    start_metrics_server,
)
from persistence import PostgresPool
from persistence.postgres import PostgresWriteError
from reporting_sweep import reporting_sweep_loop

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("projector")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-edge:9092")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgres://postgres:password@postgres-hq:5432/openddil",
)
BATCH_MAX_MESSAGES = int(os.getenv("BATCH_MAX_MESSAGES", "500"))
BATCH_MAX_WAIT_MS = int(os.getenv("BATCH_MAX_WAIT_MS", "200"))

# Schema-drift logging: log an unknown decode-failure reason once per
# (topic, reason), not on every message.
_logged_decode_reasons: set[tuple[str, str]] = set()

# Same discipline for partition-level Kafka errors: once per
# (topic, error-code). UNKNOWN_TOPIC_OR_PART otherwise repeats on every
# poll for a topic absent from this broker, burying real errors.
_logged_kafka_errors: set[tuple[str, int]] = set()


class ConsumerWorker:
    """Drains one Kafka topic into one Postgres table."""

    def __init__(self, mapping: Mapping, pool: PostgresPool) -> None:
        self.mapping = mapping
        self._pool = pool
        self._handler = get_handler(mapping.handler)
        # Decoder dispatch by the config's `decode_as`:
        #   "cloudevents.json" -> JSON CloudEvents envelope
        #   "json"             -> plain json.dumps(...) payload
        #   anything else      -> a fully-qualified proto message name
        if mapping.decode_as == "cloudevents.json":
            self._decode = decode_cloudevent
        elif mapping.decode_as == "json":
            self._decode = decode_json
        else:
            self._decode = ProtoDecoder(mapping.decode_as).decode
        self._consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BROKERS,
                "group.id": mapping.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,  # we commit after the DB write
                # asset-element-telemetry envelopes carry the full per-
                # asset element tree (~5MB for MRAD at 23k elements);
                # default 1MB fetch.message.max.bytes would reject.
                # Topic-level max.message.bytes must agree.
                "fetch.message.max.bytes": 16 * 1024 * 1024,
                "message.max.bytes": 16 * 1024 * 1024,
            }
        )
        self._consumer.subscribe([mapping.topic])
        self._running = True

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception:  # noqa: BLE001 - shutdown best-effort
            pass

    # -- batch drain --------------------------------------------------------

    def _drain_batch(self) -> list[Any]:
        """Blocking — collect up to BATCH_MAX_MESSAGES, or whatever arrives
        within BATCH_MAX_WAIT_MS. Runs in an executor thread."""
        batch: list[Any] = []
        # First poll waits up to the full window; subsequent polls are
        # near-instant to scoop whatever else is already buffered.
        msg = self._consumer.poll(BATCH_MAX_WAIT_MS / 1000.0)
        while msg is not None and len(batch) < BATCH_MAX_MESSAGES:
            batch.append(msg)
            msg = self._consumer.poll(0)
        return batch

    # -- processing ---------------------------------------------------------

    def _coalesce(self, batch: list[Any]) -> list[Any]:
        """For compacted (upsert) topics, keep only the last message per key
        within the batch. Append topics keep every message."""
        if self.mapping.mode != "upsert":
            return batch
        # dict preserves insertion order; re-inserting a key moves the value
        # but Python keeps original position — fine, we only need last value.
        by_key: dict[Any, Any] = {}
        for msg in batch:
            by_key[msg.key()] = msg
        return list(by_key.values())

    async def _persist(self, msg: Any) -> None:
        topic = self.mapping.topic
        raw = msg.value()
        key = msg.key().decode("utf-8") if msg.key() else ""

        try:
            decoded = self._decode(raw)
        except DecodeError as exc:
            reason = type(exc).__name__
            sig = (topic, str(exc)[:80])
            if sig not in _logged_decode_reasons:
                _logged_decode_reasons.add(sig)
                log.warning("decode error on %s: %s (logged once)", topic, exc)
            DECODE_ERRORS.labels(topic=topic, reason=reason).inc()
            return

        try:
            write = self._handler(key, decoded)
        except Exception as exc:  # noqa: BLE001 - handler bug must not crash consumer
            log.error("handler %s raised on %s: %s",
                      self.mapping.handler, topic, exc)
            DECODE_ERRORS.labels(topic=topic, reason="handler_exception").inc()
            return

        if write is None:
            # Handler chose to skip (e.g. no asset_id). Not an error.
            return

        try:
            rows_affected = await self._pool.execute(write)
        except PostgresWriteError as exc:
            # Non-retryable (constraint violation, bad data). Log-and-skip;
            # transient errors are retried inside execute() and never reach here.
            log.error("postgres write skipped for %s: %s", write.table, exc)
            POSTGRES_ERRORS.labels(operation=f"write:{write.table}").inc()
            return
        if write.mode == "update" and rows_affected == 0:
            # A removal (or any other "update"-mode write) for an asset_id
            # with no existing row — the "update" mode's whole point is that
            # this is a no-op, not an insert. Not a UPSERT: no row was
            # written.
            log.info("removal for unknown asset %s dropped", key)
            REMOVAL_UNKNOWN_ASSET_DROPPED.labels(table=write.table).inc()
            return
        UPSERTS.labels(table=write.table).inc()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        log.info(
            "consumer started: topic=%s -> table=%s (group=%s, mode=%s)",
            self.mapping.topic, self.mapping.table,
            self.mapping.consumer_group, self.mapping.mode,
        )
        while self._running:
            batch = await loop.run_in_executor(None, self._drain_batch)
            if not batch:
                continue

            # Surface partition-level Kafka errors; drop the error frames.
            real: list[Any] = []
            for msg in batch:
                err = msg.error()
                if err is None:
                    real.append(msg)
                elif err.code() != KafkaError._PARTITION_EOF:
                    # Warn-once per (topic, error-code).
                    #
                    # UNKNOWN_TOPIC_OR_PART repeats on every poll for a topic
                    # that does not exist on this broker, which buries real
                    # errors under thousands of identical lines. That is
                    # normal and expected for a TIER-SCOPED projector: a tier
                    # legitimately does not carry root-only rollup topics.
                    #
                    # The root fix is the tier-scoped PROJECTOR_CONFIG (a tier
                    # simply does not list topics its broker lacks) — this is
                    # the belt to that braces, so a misconfiguration is still
                    # visible ONCE rather than either silenced or drowned.
                    key = (self.mapping.topic, err.code())
                    if key not in _logged_kafka_errors:
                        _logged_kafka_errors.add(key)
                        log.warning("kafka error on %s: %s "
                                    "(further identical errors suppressed)",
                                    self.mapping.topic, err)
            if not real:
                continue

            MESSAGES_CONSUMED.labels(topic=self.mapping.topic).inc(len(real))
            for msg in self._coalesce(real):
                await self._persist(msg)

            # Whole batch durable (or individually logged-and-skipped) —
            # commit the consumed offsets. Synchronous commit in executor.
            try:
                await loop.run_in_executor(None, self._consumer.commit)
            except Exception as exc:  # noqa: BLE001
                log.warning("offset commit failed on %s: %s — will retry "
                            "next batch", self.mapping.topic, exc)

        self.close()
        log.info("consumer stopped: %s", self.mapping.topic)

    # -- lag ----------------------------------------------------------------

    def update_lag_gauge(self) -> None:
        """Sum (high watermark - committed) across assigned partitions."""
        try:
            assignment = self._consumer.assignment()
            if not assignment:
                return
            committed = self._consumer.committed(assignment, timeout=5)
            total = 0
            for tp in committed:
                lo, hi = self._consumer.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition), timeout=5
                )
                pos = tp.offset if tp.offset >= 0 else lo
                total += max(0, hi - pos)
            TOPIC_LAG.labels(topic=self.mapping.topic).set(total)
        except Exception as exc:  # noqa: BLE001 - lag is best-effort telemetry
            log.debug("lag probe failed for %s: %s", self.mapping.topic, exc)


# ADR-0044 §1 rule 3: upsert tables in the asset_ttl_hours set whose row
# does NOT carry its own asset_id column keyed 1:1 with an asset (checked
# against each handler's own `Write.key_columns` in src/handlers/) can't be
# joined against telemetry_latest_state's per-asset operational_status, so
# they are left on the pre-ADR age-only predicate rather than guessed at.
# inventory_items is keyed by "id" (a deterministic `<asset_id>:<layer_name>`
# composite — see asset_element_inventory.py); every other table in today's
# asset_ttl_hours set (asset_cm_state, asset_logistics_status,
# telemetry_latest_state, asset_telemetry_windows, asset_capability_state,
# asset_element_telemetry) is keyed by plain `key_columns=["asset_id"]`.
NON_ASSET_ID_KEYED_UPSERT_TABLES = frozenset({"inventory_items"})

# Default hourly, as before this env var existed. Overridable so a compose
# run can exercise a full prune pass on a human timescale (paired with
# PROJECTOR_ASSET_TTL_HOURS set to a fraction of an hour) instead of waiting
# out a real 3600s sleep to see ADR-0044's terminal-status gate fire.
PRUNE_INTERVAL_S = float(os.getenv("PROJECTOR_PRUNE_INTERVAL_S", "3600"))


def build_prune_targets(
    mappings: list[Mapping],
) -> list[tuple[str, str, float, bool]]:
    """(table, time_column, hours, asset_id_keyed) tuples, in the order
    `prune_loop` must run them in one pass. Pure — no asyncio, no pool —
    split out of `prune_loop` specifically so the ADR-0044 dependents-
    before-`telemetry_latest_state` ordering is unit-testable the same way
    `PostgresPool.build_sql`/`build_prune_sql` are: fabricated `Mapping`
    objects in, an ordered list out, no DB.
    """
    append_tables = [
        (m.table, "time", m.retention_hours, False)
        for m in mappings
        if m.mode == "append" and m.retention_hours
    ]
    upsert_tables = [
        (m.table, "updated_at", m.asset_ttl_hours,
         m.table not in NON_ASSET_ID_KEYED_UPSERT_TABLES)
        for m in mappings
        if m.mode == "upsert" and m.asset_ttl_hours
    ]
    # Dependents-before-lifecycle-table within the upsert group (see
    # prune_loop's docstring). Stable sort: only telemetry_latest_state's
    # relative position moves, to last.
    from persistence.postgres import LIFECYCLE_TABLE
    upsert_tables.sort(key=lambda t: t[0] == LIFECYCLE_TABLE)
    return append_tables + upsert_tables


async def prune_loop(pool: PostgresPool, mappings: list[Mapping]) -> None:
    """Retention pruning, on PROJECTOR_PRUNE_INTERVAL_S (default hourly).
    Two flavors of cleanup share the loop:

    * APPEND mode (e.g. tactical_events) -- rolling-window event log;
      drops rows where `time` is older than retention_hours. Unaffected
      by ADR-0044: these are event logs, not asset lifecycle state.
    * UPSERT mode with asset_ttl_hours set (e.g. telemetry_latest_state,
      asset_cm_state, ...) -- bounds postgres growth across long-
      running demos where every asset_id ever seen would otherwise
      accumulate. Drops rows where `updated_at` is older than
      asset_ttl_hours AND (ADR-0044 §1 rule 3) that asset_id's
      operational_status in telemetry_latest_state is terminal -- see
      `PostgresPool.build_prune_sql`. An asset that is old but still
      just "operational" (or "operational" and merely not_reporting;
      reporting_status is never a prune input) is never eligible, at any
      age -- that is the whole content of the amendment this slice
      implements. The 5-tier liveness model on the frontend already
      hides assets at the operator level; this TTL is the separate
      long-tail cap so postgres doesn't grow without bound. Different
      window than the frontend's LOST threshold by design -- postgres is
      the long memory, the SPA filters for what operators want to see.

    Rollup tables (region_*) are aggregates whose updated_at is bumped
    whenever the underlying assets churn; they shouldn't be aged out by
    a static TTL. Leave asset_ttl_hours unset in their mappings.

    Ordering within a pass: telemetry_latest_state is pruned LAST among the
    asset_id-keyed upsert tables. It is the row every other table's terminal-
    status predicate joins against (`build_prune_sql`'s EXISTS check); pruning
    it first would delete the evidence a dependent needs to qualify for
    pruning IN THE SAME PASS, silently pushing that dependent's prune to the
    next interval instead of this one.
    """
    all_targets = build_prune_targets(mappings)
    if not all_targets:
        return
    append_count = sum(1 for _, key_col, *_ in all_targets if key_col == "time")
    log.info("prune_loop: %d append target(s), %d upsert target(s)",
             append_count, len(all_targets) - append_count)
    while True:
        await asyncio.sleep(PRUNE_INTERVAL_S)
        for table, key_col, hours, asset_id_keyed in all_targets:
            try:
                deleted = await pool.prune_older_than(
                    table, key_col, hours, asset_id_keyed=asset_id_keyed)
                if deleted:
                    ROWS_PRUNED.labels(table=table).inc(deleted)
                    log.info("pruned %d rows from %s (> %gh old by %s%s)",
                             deleted, table, hours, key_col,
                             ", terminal operational_status" if asset_id_keyed else "")
            except Exception as exc:  # noqa: BLE001
                log.warning("prune failed for %s: %s", table, exc)


async def lag_loop(workers: list[ConsumerWorker]) -> None:
    """Refresh the topic-lag gauge every 15s."""
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(15)
        for w in workers:
            await loop.run_in_executor(None, w.update_lag_gauge)


async def main() -> None:
    config: Config = load_config()
    if not config.mappings:
        log.error("no mappings in projector config — nothing to do")
        sys.exit(1)

    # Install the edge-assignment strategy for customer-feed handlers
    # (telemetry_latest, capability_state, logistics_status) before the
    # consumers start — those handlers import-time depend on it.
    configure_edge_assignment(config.edge_assignment)
    log.info(
        "edge_assignment configured: strategy=%s",
        (config.edge_assignment or {}).get("strategy") or "none",
    )

    port = start_metrics_server()
    log.info("metrics server on :%d", port)

    pool = PostgresPool(
        POSTGRES_DSN,
        retry_base_seconds=config.settings.postgres_retry_base_seconds,
        retry_max_seconds=config.settings.postgres_retry_max_seconds,
    )
    await pool.connect()

    workers = [ConsumerWorker(m, pool) for m in config.mappings]

    # SIGHUP: reload config. For Phase 4a this re-reads the file and logs the
    # delta; consumers for unchanged mappings keep running. Adding/removing a
    # mapping at runtime restarts the process via the supervisor — documented
    # as a known limitation in README.
    def _on_sighup() -> None:
        try:
            new = load_config()
            old_topics = {m.topic for m in config.mappings}
            new_topics = {m.topic for m in new.mappings}
            log.info(
                "SIGHUP: config reloaded. added=%s removed=%s "
                "(consumer add/remove requires restart)",
                sorted(new_topics - old_topics),
                sorted(old_topics - new_topics),
            )
        except Exception as exc:  # noqa: BLE001
            log.error("SIGHUP reload failed, keeping current config: %s", exc)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    try:
        loop.add_signal_handler(signal.SIGHUP, _on_sighup)
    except (NotImplementedError, AttributeError):
        pass  # SIGHUP not available (Windows) — fine, dev only
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, AttributeError):
            pass

    tasks = [asyncio.create_task(w.run()) for w in workers]
    tasks.append(asyncio.create_task(prune_loop(pool, config.mappings)))
    tasks.append(asyncio.create_task(lag_loop(workers)))
    # ADR-0044 lifecycle slice 1: reporting_status staleness sweep. Runs
    # unconditionally on every projector instance — each tier's own sweep
    # over its own Postgres is what makes reporting_status a per-reader
    # fact instead of a shared one (ADR §4); there is no "primary" sweeper
    # to gate, unlike edge_buffer_loop below.
    tasks.append(asyncio.create_task(reporting_sweep_loop(pool)))
    # Phase 4c.5: edge->HQ DDIL link/buffer monitor.
    # ADR-0023 Phase 6a: with 3 projector instances (one per edge cluster),
    # only one should run the buffer monitor — they all write to the same
    # edge_buffer_status row otherwise. Gate via BUFFER_MONITOR_ENABLED env
    # ("true" by default to preserve single-instance behavior; set "false"
    # on the additional per-edge projector instances). Multi-edge buffer
    # monitoring (per-edge bridge-group lag) is 6c rewire territory.
    if os.getenv("BUFFER_MONITOR_ENABLED", "true").lower() == "true":
        tasks.append(asyncio.create_task(edge_buffer_loop(pool)))

    log.info("projector running: %d topic consumers", len(workers))
    await stop_event.wait()

    log.info("shutdown signal received — draining")
    for w in workers:
        w.stop()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()
    log.info("projector stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main())
