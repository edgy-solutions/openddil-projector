# OpenDDIL Projector — Topology

The projector is the **read-path bridge**: it consumes the internal Kafka
(Redpanda) topics that the OpenDDIL services produce, and writes them into
the Postgres read-model tables that ElectricSQL exposes to the UI as
Shapes. It is the single, generic, config-driven projector decided in
[ADR-0019](../../openddil-contracts/decisions/ADR-0019-single-kafka-postgres-projector.md)
— one service, not one service per topic.

```
 producers                Kafka (redpanda)        openddil-projector            Postgres (postgres-hq)        ElectricSQL        UI
 ─────────                ────────────────        ──────────────────            ──────────────────────        ───────────        ──
 cm-service          ──>  asset-cm-state      ──> cm_state handler          ──> asset_cm_state            ──┐
 logistics-fusion    ──>  asset-logistics-... ──> logistics_status handler  ──> asset_logistics_status      │
 faust-edge          ──>  telemetry-latest-...──> telemetry_latest handler  ──> telemetry_latest_state      ├─> Shapes ──> hooks
 cm-svc + logistics  ──>  tactical-events     ──> tactical_events handler   ──> tactical_events             │
 faust-edge          ──>  asset-telemetry-... ──> telemetry_windows handler ──> asset_telemetry_windows   ──┘
 (internal monitor)                               edge_buffer_monitor       ──> edge_buffer_status        ──> Shape ──> useEdgeBuffer
```

## Topic → table mapping

The mapping is **declarative** — it lives in
[`src/config/projector_config.yaml`](../src/config/projector_config.yaml)
and is hot-reloadable via `SIGHUP`. Adding a topic to the UI is: a new
entry there + a handler in `src/handlers/` + a table in the Atlas schema
(`openddil-stack/schema/schema.hcl`).

| Kafka topic | Decoder (`decode_as`) | Handler | Postgres table | Mode | Consumer group |
|---|---|---|---|---|---|
| `asset-cm-state` | `json` | `cm_state` | `asset_cm_state` | upsert | `projector-cm-state` |
| `asset-logistics-status` | `openddil.logistics.v1.AssetLogisticsStatusUpdate` | `logistics_status` | `asset_logistics_status` | upsert | `projector-logistics-status` |
| `telemetry-latest-state` | `openddil.telemetry.v1.EntityTelemetryEvent` | `telemetry_latest` | `telemetry_latest_state` | upsert | `projector-telemetry-latest` |
| `tactical-events` | `cloudevents.json` | `tactical_events` | `tactical_events` | append (24h retention) | `projector-tactical-events` |
| `asset-telemetry-windows` | `openddil.logistics.v1.WindowedTelemetry` | `telemetry_windows` | `asset_telemetry_windows` | upsert | `projector-telemetry-windows` |

Notes:

- **`asset-cm-state` is JSON, not protobuf.** cm-service emits
  `json.dumps(...)` with integer enum values and `*_ns` integer
  timestamps. The `cm_state` handler normalises it; the deliberate
  inconsistency is recorded in
  [ADR-0018](../../openddil-contracts/decisions/ADR-0018-asset-cm-state-wire-format-inconsistency.md).
- **`upsert` vs `append`.** Compacted topics (keyed by `asset_id`) map to
  UPSERT-by-PK tables — latest state wins. The `tactical-events` stream
  maps to an append-only log with `ON CONFLICT (id) DO NOTHING` (a
  replayed CloudEvent is a harmless no-op) and a 24h retention pruner.
- **One consumer group per topic** so consumer-group lag is per-topic and
  diagnosable in isolation.
- **At-least-once.** A batch is drained, decoded, mapped, and written to
  Postgres; the Kafka offset is committed **only after** the write is
  durable. A Postgres outage stalls the consumer rather than dropping a
  message.

## Decoders

`decode_as` in the config selects one of three decoders
(`src/decoders/`):

- a **fully-qualified proto message name** → `ProtoDecoder` (protobuf
  binary → dict),
- `cloudevents.json` → `decode_cloudevent` (JSON CloudEvents envelope),
- `json` → `decode_json` (a plain `json.dumps(...)` payload).

## Handlers

A handler is a **pure function** `(kafka_key, decoded_dict) -> Write | None`
— no I/O, no Kafka, no Postgres — which makes every handler unit-testable
in isolation (`src/tests/test_handlers.py`). It normalises the proto-JSON
quirks (RFC3339 vs `*_ns` timestamps, Duration strings, enum names) and
returns a `Write` describing the row. `None` means "skip" (e.g. no
`asset_id`), which is not an error.

### Origin-node provenance ([ADR-0022](../../openddil-contracts/decisions/ADR-0022-hierarchical-aggregation-is-the-architecture.md))

Every per-asset handler stamps each `Write.row` with `edge_id` /
`region_id` via `handlers/base.py::origin_provenance()`. OpenDDIL is
hierarchical streaming aggregation (edge → regional → HQ); the projector
runs **single-tier today**, so these are constant defaults (`edge-01` /
`region-01`, env-overridable via `OPENDDIL_EDGE_ID` / `OPENDDIL_REGION_ID`).
They are written **explicitly** rather than left to the DB column default:
the projector stays provenance-aware *in shape* even while flat, so when
the hierarchy phase lands only the value source changes — not the handler
signature or the row schema.

## The edge-buffer monitor

Beyond the topic consumers, the projector runs one background task —
`src/edge_buffer_monitor.py` — that is **not** a topic→table projection.
It probes the `bridge-group` consumer-group lag on `redpanda-edge` and the
toxiproxy `hq-link` proxy state every ~2s, and writes the singleton
`edge_buffer_status` row. That row is the real, honestly-backed edge→HQ
buffer depth and link state the UI's buffer/link widgets read (via
`useEdgeBuffer`) — see
[ADR-0021](../../openddil-contracts/decisions/ADR-0021-edge-hq-topology-is-load-bearing.md).

## Background tasks

- **Retention pruner** — hourly; deletes `tactical_events` rows older than
  `retention_hours`.
- **Lag gauge** — refreshes the per-topic consumer-lag Prometheus gauge
  every 15s.
- **Edge-buffer monitor** — see above.

## Schema ownership

The projector does **not** own the schema. The Postgres tables are defined
declaratively in `openddil-stack/schema/schema.hcl` and applied by Atlas
(`atlas-init` in the compose). The projector assumes the tables exist; a
new column is an Atlas migration first, then a handler change.
