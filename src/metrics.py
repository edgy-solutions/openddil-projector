"""Prometheus metrics for the projector. Served on :8084 (METRICS_PORT)."""
from __future__ import annotations

import os

from prometheus_client import Counter, Gauge, start_http_server

MESSAGES_CONSUMED = Counter(
    "projector_messages_consumed_total",
    "Kafka messages consumed",
    ["topic"],
)
UPSERTS = Counter(
    "projector_upserts_total",
    "Postgres rows written (UPSERT or append INSERT)",
    ["table"],
)
DECODE_ERRORS = Counter(
    "projector_decode_errors_total",
    "Messages that failed to decode and were skipped",
    ["topic", "reason"],
)
POSTGRES_ERRORS = Counter(
    "projector_postgres_errors_total",
    "Non-retryable Postgres write errors (message logged-and-skipped)",
    ["operation"],
)
TOPIC_LAG = Gauge(
    "projector_topic_lag",
    "Consumer-group lag (high watermark - committed position), summed over partitions",
    ["topic"],
)
ROWS_PRUNED = Counter(
    "projector_rows_pruned_total",
    "Append-mode rows deleted by the retention pruner",
    ["table"],
)
REMOVAL_UNKNOWN_ASSET_DROPPED = Counter(
    "projector_removal_unknown_asset_dropped_total",
    "Remove Entity claims for an asset_id with no row, dropped (no row created)",
    ["table"],
)
# Present at 0 from startup. A labelled counter has no series until a label
# is used, and then "nothing dropped yet" reads the same as "this build
# cannot drop".
REMOVAL_UNKNOWN_ASSET_DROPPED.labels(table="telemetry_latest_state")


def start_metrics_server() -> int:
    """Start the Prometheus HTTP endpoint. Returns the port it bound."""
    port = int(os.getenv("METRICS_PORT", "8084"))
    start_http_server(port)
    return port
