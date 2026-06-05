"""OpenDDIL Projector — entrypoint.

One async consumer task per configured topic. Each task drains a batch from
Kafka, decodes + maps each message to a `Write`, persists the batch to
Postgres, then commits the consumed offsets. Offsets advance ONLY after the
batch is durable — at-least-once delivery.

Batch-level coalescing provides the rate limiting: within one drained batch,
compacted-topic messages are deduped by key (latest wins) before writing, so
an asset spamming updates costs one UPSERT, not N. Offsets stay contiguous
within the batch, so dedup never risks skipping another key's message.

Background tasks: a retention pruner for append-mode tables, and a consumer-
lag gauge updater. SIGHUP reloads config (adds/removes consumers, refreshes
settings) without dropping unchanged connections.
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
    ROWS_PRUNED,
    TOPIC_LAG,
    UPSERTS,
    start_metrics_server,
)
from persistence import PostgresPool
from persistence.postgres import PostgresWriteError

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
            await self._pool.execute(write)
        except PostgresWriteError as exc:
            # Non-retryable (constraint violation, bad data). Log-and-skip;
            # transient errors are retried inside execute() and never reach here.
            log.error("postgres write skipped for %s: %s", write.table, exc)
            POSTGRES_ERRORS.labels(operation=f"write:{write.table}").inc()
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
                    log.warning("kafka error on %s: %s",
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


async def prune_loop(pool: PostgresPool, mappings: list[Mapping]) -> None:
    """Hourly retention pruning. Two flavors of cleanup share the loop:

    * APPEND mode (e.g. tactical_events) -- rolling-window event log;
      drops rows where `time` is older than retention_hours.
    * UPSERT mode with asset_ttl_hours set (e.g. telemetry_latest_state,
      asset_cm_state, ...) -- bounds postgres growth across long-
      running demos where every asset_id ever seen would otherwise
      accumulate. Drops rows where `updated_at` is older than
      asset_ttl_hours. The 5-tier liveness model on the frontend
      already hides assets at the operator level; this TTL is the
      separate long-tail cap so postgres doesn't grow without bound.
      Different window than the frontend's LOST threshold by design
      -- postgres is the long memory, the SPA filters for what
      operators want to see.

    Rollup tables (region_*) are aggregates whose updated_at is bumped
    whenever the underlying assets churn; they shouldn't be aged out by
    a static TTL. Leave asset_ttl_hours unset in their mappings.
    """
    append_tables = [
        (m.table, "time", m.retention_hours)
        for m in mappings
        if m.mode == "append" and m.retention_hours
    ]
    upsert_tables = [
        (m.table, "updated_at", m.asset_ttl_hours)
        for m in mappings
        if m.mode == "upsert" and m.asset_ttl_hours
    ]
    all_targets = append_tables + upsert_tables
    if not all_targets:
        return
    log.info("prune_loop: %d append target(s), %d upsert target(s)",
             len(append_tables), len(upsert_tables))
    while True:
        await asyncio.sleep(3600)
        for table, key_col, hours in all_targets:
            try:
                deleted = await pool.prune_older_than(table, key_col, hours)
                if deleted:
                    ROWS_PRUNED.labels(table=table).inc(deleted)
                    log.info("pruned %d rows from %s (> %dh old by %s)",
                             deleted, table, hours, key_col)
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
