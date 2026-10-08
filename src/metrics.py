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

# -- effector_launch (launch-record admission/termination) -------------------
# Unprefixed -- a deliberate departure from this module's own `projector_*`
# convention, which every other counter here follows, so these names stay
# stable for dashboards built directly against them.
EFFECTOR_REFUSED = Counter(
    "effector_refused_total",
    "effector_launch rows this handler declined to write",
    ["reason"],
)
EFFECTOR_LATE_TERMINAL = Counter(
    "effector_late_terminal_total",
    "Detonations that resolved a row the timeout sweep had already marked unresolved",
)
EFFECTOR_UNRESOLVED = Counter(
    "effector_unresolved_total",
    "Rows the timeout sweep marked unresolved (no Detonation arrived in time)",
)
# A real Detonation for an event_urn two projector instances both admit the
# matching Fire for (e.g. an edge projector and the HQ projector, each
# writing the same shared store) lands twice. The second arrival to apply a
# given result is a harmless replay, not a refusal -- counted separately so
# it carries no signal about a genuine problem the way a refusal does.
EFFECTOR_REPLAYED = Counter(
    "effector_replayed_total",
    "Detonations that repeated a result already applied to an already-terminal row",
)
# A Resupply Received is not a launch: the launch table records launches only,
# and remaining-with-resupply lives in fusion. Counted here so it reads as
# seen-and-deliberately-skipped rather than refused.
EFFECTOR_RESUPPLY_SEEN = Counter(
    "effector_resupply_seen_total",
    "Resupply Received records seen by the effector_launch handler (no row written)",
)
# Present at 0 from startup, same reasoning as REMOVAL_UNKNOWN_ASSET_DROPPED
# above: a reason label with no series yet reads as "cannot happen", not
# "hasn't happened".
for _reason in ("unknown_launcher", "no_fire", "conflicting_detonation"):
    EFFECTOR_REFUSED.labels(reason=_reason)


def start_metrics_server() -> int:
    """Start the Prometheus HTTP endpoint. Returns the port it bound."""
    port = int(os.getenv("METRICS_PORT", "8084"))
    start_http_server(port)
    return port
