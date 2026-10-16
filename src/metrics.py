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
# A Resupply Received is not a launch, but it is a stored fact: the move panel
# replays the effector ledger to an instant and needs it. SEEN counts every
# resupply record the effector_launch handler sees (written or not); ROWS counts
# the effector_resupply rows actually written.
EFFECTOR_RESUPPLY_SEEN = Counter(
    "effector_resupply_seen_total",
    "Resupply Received records seen by the effector_launch handler (written or not)",
)
EFFECTOR_RESUPPLY_ROWS = Counter(
    "effector_resupply_rows_total",
    "effector_resupply rows written by the effector_launch handler",
)
# Present at 0 from startup, same reasoning as REMOVAL_UNKNOWN_ASSET_DROPPED
# above: a reason label with no series yet reads as "cannot happen", not
# "hasn't happened".
for _reason in ("unknown_launcher", "no_fire", "conflicting_detonation"):
    EFFECTOR_REFUSED.labels(reason=_reason)

# -- munition_track (in-flight munition tracks, ADR-0049) ---------------------
# Unprefixed for the same reason as the EFFECTOR_* names above. `silent` is a
# track state on this tier's own arrival clock, never a launch outcome
# (ADR-0048 section 4 is untouched).
MUNITION_TRACK_REFUSED = Counter(
    "munition_track_refused_total",
    "munition-tracks records this handler declined to write",
    ["reason"],
)
MUNITION_TRACK_UPSERTS = Counter(
    "munition_track_upserts_total",
    "munition_track sample upserts, by outcome",
    ["outcome"],
)
MUNITION_TRACK_JOINED = Counter(
    "munition_track_joined_total",
    "munition_track rows given a launcher, by which side arrived second",
    ["via"],
)
MUNITION_TRACK_TERMINAL = Counter(
    "munition_track_terminal_total",
    "munition_track rows terminated by a Detonation, by terminal state",
    ["state"],
)
MUNITION_TRACK_SILENT = Counter(
    "munition_track_silent_total",
    "munition_track rows the silence sweep marked silent",
)
for _reason in ("no_munition_urn", "no_location", "bad_sample_time"):
    MUNITION_TRACK_REFUSED.labels(reason=_reason)
for _outcome in ("inserted", "updated", "stale", "after_terminal"):
    MUNITION_TRACK_UPSERTS.labels(outcome=_outcome)
for _via in ("fire", "track", "heal"):
    MUNITION_TRACK_JOINED.labels(via=_via)

# -- link heartbeat / link_monitor --------------------------------------------
LINK_HEARTBEATS_PRODUCED = Counter(
    "projector_link_heartbeats_produced_total",
    "Link heartbeat records handed to this tier's own broker",
)
LINK_HEARTBEATS_RECEIVED = Counter(
    "projector_link_heartbeats_received_total",
    "Link heartbeats that arrived at HQ for an expected link id",
    ["link_id"],
)
# reason: malformed (undecodable or empty link_id) or unexpected (id not in
# LINK_EXPECTED_IDS). Present at 0 from startup, as above.
LINK_HEARTBEATS_REJECTED = Counter(
    "projector_link_heartbeats_rejected_total",
    "Link heartbeat payloads skipped by the HQ link monitor",
    ["reason"],
)
for _reason in ("malformed", "unexpected"):
    LINK_HEARTBEATS_REJECTED.labels(reason=_reason)


def start_metrics_server() -> int:
    """Start the Prometheus HTTP endpoint. Returns the port it bound."""
    port = int(os.getenv("METRICS_PORT", "8084"))
    start_http_server(port)
    return port
