# OpenDDIL Projector

Kafka → Postgres projector. Consumes OpenDDIL pipeline topics and UPSERTs
them into Postgres read-model tables that ElectricSQL exposes as Shapes to
the UI.

Introduced in Phase 4a. This is the **single** Kafka→Postgres path — there
is no per-topic projector and no Faust HTTP endpoint. One service, many
subscriptions.

## What it does

```
Kafka topic ──► decoder ──► handler ──► Postgres UPSERT ──► ElectricSQL Shape ──► UI
```

The projector is a **dumb pipe**: decode the message, map its fields onto a
table row, UPSERT. No business logic — severity computation, discrepancy
analysis, and fusion all happen upstream.

## Topic → table mapping

| Topic | Table | Mode | Decode as |
|-------|-------|------|-----------|
| `asset-cm-state` | `asset_cm_state` | upsert | `openddil.configuration.v1.AsMaintainedConfiguration` |
| `asset-logistics-status` | `asset_logistics_status` | upsert | `openddil.logistics.v1.AssetLogisticsStatusUpdate` |
| `telemetry-latest-state` | `telemetry_latest_state` | upsert | `openddil.telemetry.v1.EntityTelemetryEvent` |
| `tactical-events` | `tactical_events` | append | CloudEvents JSON |
| `asset-telemetry-windows` | `asset_telemetry_windows` | upsert | `openddil.logistics.v1.WindowedTelemetry` |

The mapping lives in [`src/config/projector_config.yaml`](src/config/projector_config.yaml)
and is **hot-reloadable via SIGHUP**.

## Behaviors

- **At-least-once delivery** — a Kafka offset is committed only after the
  Postgres write is durable. On Postgres-down, the consumer retries with
  exponential backoff; it does not crash and does not drop messages.
- **Schema-drift tolerant** — a decode error or an unrecognized field is
  logged once per `(topic, field)` and skipped. One malformed message never
  stalls the pipeline.
- **Rate limiting** — if one `asset_id` exceeds `rate_limit_per_sec` updates
  per second, updates are coalesced in a small per-asset buffer before the
  UPSERT.
- **Append-only retention** — `tactical_events` rows older than
  `retention_hours` are pruned hourly by a background task.

## Configuration

[`src/config/projector_config.yaml`](src/config/projector_config.yaml):

```yaml
mappings:
  - topic: asset-cm-state
    handler: cm_state
    consumer_group: projector-cm-state
    decode_as: openddil.configuration.v1.AsMaintainedConfiguration
    mode: upsert
```

Environment variables:

| Var | Default | Purpose |
|-----|---------|---------|
| `KAFKA_BROKERS` | `redpanda-edge:9092` | Kafka bootstrap servers |
| `POSTGRES_DSN` | `postgres://postgres:password@postgres-hq:5432/openddil` | Postgres connection string |
| `PROJECTOR_CONFIG` | `/app/src/config/projector_config.yaml` | Config file path |
| `METRICS_PORT` | `8084` | Prometheus port |
| `RATE_LIMIT_PER_SEC` | `10` | Per-asset UPSERT coalescing threshold |
| `LOG_LEVEL` | `INFO` | Log level |

## Metrics

Prometheus on `:8084`:

- `projector_messages_consumed_total{topic}`
- `projector_upserts_total{table}`
- `projector_decode_errors_total{topic,reason}`
- `projector_postgres_errors_total{operation}`
- `projector_topic_lag{topic}` — consumer-group lag gauge

## Running

Via `openddil-demo`'s compose (recommended):

```bash
docker compose -f openddil-demo/docker-compose.yml -f openddil-demo/docker-compose.override.yml up -d openddil-projector
```

Standalone (requires a reachable Kafka + Postgres + the generated proto
stubs on `PYTHONPATH`):

```bash
PYTHONPATH=../openddil-contracts/gen/python:src python src/main.py
```

## Tests

```bash
pip install -e ".[dev]"
pytest
```

Handlers and decoders are pure functions — unit tests need no live Kafka or
Postgres.

## License

MIT — see [LICENSE](LICENSE).
