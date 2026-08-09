"""Edge-buffer monitor (Phase 4c.5).

Probes the real edge->HQ DDIL link/buffer state and writes it to the
`edge_buffer_status` singleton row, which ElectricSQL exposes to the UI.

Two probes, every EDGE_BUFFER_PROBE_INTERVAL_S seconds:

  bridge_group_lag — the `bridge-group` consumer-group lag on
    redpanda-edge across raw-sensor-stream + tactical-events. The
    edge-hq-bridge commits offsets for that group only when its writes to
    redpanda-hq (through the toxiproxy hq-link) succeed; when the link is
    severed the writes fail, offsets stop advancing, and this lag climbs.
    It IS the real edge-buffer depth.

  hq_link_severed — whether the toxiproxy hq-link proxy currently has a
    timeout toxic applied.

If either probe cannot reach its dependency the row is still written with
`probe_healthy = False`, so the UI can show "probe down" instead of a
stale number presenting as real.
"""
from __future__ import annotations

import asyncio
import logging
import os
import urllib.error
import urllib.request

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from persistence import PostgresPool, Write

log = logging.getLogger("projector.edge_buffer")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-edge:9092")
BRIDGE_CONSUMER_GROUP = os.getenv("BRIDGE_CONSUMER_GROUP", "bridge-group")
BRIDGE_TOPICS = [
    t.strip()
    for t in os.getenv("BRIDGE_TOPICS", "raw-sensor-stream,tactical-events").split(",")
    if t.strip()
]
TOXIPROXY_API_URL = os.getenv("TOXIPROXY_API_URL", "http://toxiproxy:8475")
HQ_LINK_PROXY = os.getenv("HQ_LINK_PROXY", "hq-link")
PROBE_INTERVAL_S = float(os.getenv("EDGE_BUFFER_PROBE_INTERVAL_S", "2"))

# Sentinel written to bridge_group_lag when the probe could not produce a
# reading. Deliberately negative so it cannot be mistaken for a real lag by
# anything that renders the number — a UI showing "-1" prompts a question,
# a UI showing "0" ends one.
LAG_UNKNOWN = -1

# The probe runs every 2s. Without dedup a persistent misconfiguration would
# emit 30 warnings a minute forever, which trains readers to filter the log
# — the same way a permanent 0 trained them to trust the number.
_warned: set[str] = set()


def _warn_once(key: str, fmt: str, *args) -> None:
    if key not in _warned:
        _warned.add(key)
        log.warning(fmt + "  (further identical warnings suppressed)", *args)


class BridgeGroupAbsent(RuntimeError):
    """The configured consumer group has no committed offsets on the bridge
    topics — so its lag is UNKNOWN, not zero.

    This exists because the previous behaviour (return 0) made a broken
    lookup indistinguishable from a healthy, caught-up link. The monitor
    read 0 on every cluster for months while the bridge buffered normally,
    because the group name it queried had never existed. Nothing errored;
    the number was simply always plausible.

    THE DESIGN RULE THIS ENCODES: an instrument whose failure mode is
    indistinguishable from its healthy reading is not an instrument. A probe
    must fail DISTINGUISHABLY from its own zero.
    """


def _probe_bridge_lag() -> int:
    """Sum the bridge consumer group's lag across the bridge topics.

    Raises:
        BridgeGroupAbsent: the group has committed nothing on these topics.
            Callers must surface this as UNKNOWN — never as 0. A genuinely
            caught-up bridge HAS committed offsets and returns 0 through the
            normal path, so the two cases are distinguishable here and must
            stay distinguishable upstream.
        Exception: Kafka unreachable, timeouts, etc.
    """
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    futures = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(BRIDGE_CONSUMER_GROUP)]
    )
    result = futures[BRIDGE_CONSUMER_GROUP].result(timeout=10)
    committed: list[TopicPartition] = list(result.topic_partitions or [])
    # Only the bridge's topics; ignore anything else the group ever touched.
    committed = [tp for tp in committed if tp.topic in BRIDGE_TOPICS]
    if not committed:
        raise BridgeGroupAbsent(
            f"consumer group {BRIDGE_CONSUMER_GROUP!r} has no committed offsets "
            f"on {sorted(BRIDGE_TOPICS)}. Either the bridge has never run, or "
            f"BRIDGE_CONSUMER_GROUP is wrong — the bridge commits under "
            f"'bridge-group-<edge_id>', not the bare 'bridge-group'."
        )

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "edge-buffer-probe",
        "enable.auto.commit": False,
    })
    try:
        total = 0
        for tp in committed:
            lo, hi = consumer.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition), timeout=5
            )
            pos = tp.offset if tp.offset is not None and tp.offset >= 0 else lo
            total += max(0, hi - pos)
        return total
    finally:
        consumer.close()


def _probe_hq_link_severed() -> bool:
    """True if the toxiproxy hq-link proxy is severed. Raises on toxiproxy
    unreachable.

    The UI severs the link by DISABLING the proxy: toxiproxy then closes
    all connections and refuses new ones — an unambiguous, total sever.
    (A `downstream` timeout toxic would still let the produce reach
    redpanda-hq and only delay the ack, so it would not actually buffer.)
    A toxic being present is also treated as severed, for robustness."""
    import json

    url = f"{TOXIPROXY_API_URL}/proxies/{HQ_LINK_PROXY}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        proxy = json.loads(resp.read().decode("utf-8"))
    enabled = proxy.get("enabled", True)
    toxics = proxy.get("toxics") or []
    return (not enabled) or len(toxics) > 0


def _build_write(lag: int, severed: bool, healthy: bool) -> Write:
    from handlers.base import now_utc

    return Write(
        table="edge_buffer_status",
        mode="upsert",
        key_columns=["id"],
        row={
            "id": "edge",
            "bridge_group_lag": int(lag),
            "hq_link_severed": bool(severed),
            "probe_healthy": bool(healthy),
            "updated_at": now_utc(),
        },
    )


async def edge_buffer_loop(pool: PostgresPool) -> None:
    """Probe + write the edge_buffer_status row on a fixed interval."""
    loop = asyncio.get_running_loop()
    log.info(
        "edge-buffer monitor started: group=%s topics=%s interval=%.1fs",
        BRIDGE_CONSUMER_GROUP, BRIDGE_TOPICS, PROBE_INTERVAL_S,
    )
    while True:
        await asyncio.sleep(PROBE_INTERVAL_S)
        # LAG_UNKNOWN, not 0. Writing 0 here is what made a blind probe look
        # like a healthy caught-up link for months. Consumers of this row must
        # treat a negative lag as "no reading", and `probe_healthy` is false
        # alongside it.
        lag = LAG_UNKNOWN
        severed = False
        healthy = True
        try:
            lag = await loop.run_in_executor(None, _probe_bridge_lag)
        except BridgeGroupAbsent as exc:
            healthy = False
            lag = LAG_UNKNOWN
            # WARNING, and deduplicated — not debug. The previous code logged
            # probe failures at DEBUG, which is invisible at the default level,
            # so the one signal that would have revealed the wrong group name
            # was suppressed by default on every cluster.
            _warn_once("bridge-group-absent", "edge-buffer: %s", exc)
        except Exception as exc:  # noqa: BLE001 - probe failure is non-fatal
            healthy = False
            lag = LAG_UNKNOWN
            _warn_once("bridge-lag-probe", "edge-buffer: bridge-lag probe failed: %s", exc)
        try:
            severed = await loop.run_in_executor(None, _probe_hq_link_severed)
        except Exception as exc:  # noqa: BLE001
            healthy = False
            _warn_once("hq-link-probe", "edge-buffer: hq-link probe failed: %s", exc)

        try:
            await pool.execute(_build_write(lag, severed, healthy))
        except Exception as exc:  # noqa: BLE001 - never let the monitor crash
            log.warning("edge_buffer_status write failed: %s", exc)
