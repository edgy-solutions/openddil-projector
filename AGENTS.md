# AGENTS.md — OpenDDIL Projector

Guidelines and safety constraints for AI agents working in this repository.

## Repository Scope

This repo contains the **Kafka → Postgres projector** for OpenDDIL — a
single generic service that consumes pipeline topics and UPSERTs them into
Postgres read-model tables. ElectricSQL then exposes those tables as Shapes
to the UI. One service, many subscriptions; new topics are added via a YAML
mapping entry + a handler, not a new service.

Introduced in Phase 4a (see ADR-0016 for the single-projector decision).

## What You CAN Do

- **Add a new topic → table mapping** in `src/config/projector_config.yaml`
  plus a handler in `src/handlers/`. Register the handler in
  `src/handlers/__init__.py`.
- **Add a new decoder** in `src/decoders/` for a new wire format.
- **Tune backpressure / rate-limiting / retention** parameters — they are
  config-driven, not hardcoded.
- **Add metrics** to `src/main.py` — keep the `projector_*` prefix.
- **Add tests** in `src/tests/` — handlers and decoders are pure functions
  and must stay unit-testable without a live Kafka or Postgres.

## What You MUST NOT Do

- ❌ **Never let a decode error or an unknown field crash the consumer.**
  Schema drift is expected. Log once per (topic, field), increment
  `projector_decode_errors_total`, and skip. The pipeline must not stall
  because one message is malformed.
- ❌ **Never commit a Kafka offset before the Postgres write succeeds.**
  At-least-once delivery is the contract; offsets advance only after the
  UPSERT is durable. On Postgres-down, retry with backoff — do not drop.
- ❌ **Never put business logic in the projector.** It is a dumb pipe:
  decode → map fields → UPSERT. Severity computation, discrepancy analysis,
  fusion — all of that lives upstream in cm-service / logistics-fusion /
  faust-edge. The projector only moves bytes into rows.
- ❌ **Never bake generated proto stubs into the image.** They are mounted
  from `openddil-contracts/gen/python` at runtime (see the Dockerfile note),
  same as cm-service and faust-edge.
- ❌ **Never write to `audit_log`.** That table is HQ-only and is not in
  the Electric publication. The projector only touches the read-model
  tables it is configured for.

## Topic → Table Mapping

| Topic | Table | Mode | Decoder |
|-------|-------|------|---------|
| `asset-cm-state` | `asset_cm_state` | upsert | `AsMaintainedConfiguration` |
| `asset-logistics-status` | `asset_logistics_status` | upsert | `AssetLogisticsStatusUpdate` |
| `telemetry-latest-state` | `telemetry_latest_state` | upsert | `EntityTelemetryEvent` |
| `tactical-events` | `tactical_events` | append | CloudEvents JSON |
| `asset-telemetry-windows` | `asset_telemetry_windows` | upsert | `WindowedTelemetry` |

Compacted topics → `upsert` mode (UPSERT by primary key). The
`tactical-events` stream → `append` mode with a TTL pruner.

## Configuration

`src/config/projector_config.yaml` is the source of truth for the
topic→table mapping. It is **hot-reloadable via SIGHUP** — editing the
file and sending SIGHUP re-reads it without dropping Kafka connections for
unchanged mappings.

## Metrics

Prometheus on `:8084`:
- `projector_messages_consumed_total{topic}`
- `projector_upserts_total{table}`
- `projector_decode_errors_total{topic,reason}`
- `projector_postgres_errors_total{operation}`
- `projector_topic_lag{topic}` (gauge)

## Docker Compose Conventions (cross-repo rule)

When consumed by `openddil-demo/docker-compose.yml`:
- The base compose references `image: ghcr.io/edgy-solutions/openddil/projector:latest`.
  It MUST NOT contain a `build:` directive for the projector.
- `openddil-demo/docker-compose.override.yml` has the matching
  `build: { context: ../openddil-projector }` and the
  `openddil-contracts/gen/python` source mount.
- **When you change the Dockerfile or pyproject.toml here**, publish a new
  image to `ghcr.io/edgy-solutions/openddil/projector:latest` so the base
  compose works for non-developer consumers.

## Tests

`pytest` from the repo root runs:
- `src/tests/test_decoders.py` — proto + CloudEvents decoding, schema-drift tolerance
- `src/tests/test_handlers.py` — field mapping for each of the five handlers
- `src/tests/test_persistence.py` — UPSERT SQL generation

Handlers and decoders are pure functions — no live Kafka/Postgres in unit tests.

## Documentation Maintenance

After ANY structural change, update:
1. `README.md` — service overview, topic→table table, env vars.
2. `llms.txt` — high-level summary for downstream LLM context.
3. `docs/topology.md` — the topic→table mapping diagram.
4. `.cursorrules` — only if new conventions are introduced.
5. This file — only if new safety constraints apply.
