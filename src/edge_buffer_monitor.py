"""Edge-buffer monitor (Phase 4c.5).

Probes the real edge->HQ DDIL link/buffer state and writes it to the
`edge_buffer_status` singleton row, which ElectricSQL exposes to the UI.

Two probes, every EDGE_BUFFER_PROBE_INTERVAL_S seconds, both read off the
SAME bridge-group lookup (`_probe_bridge_lag`):

  bridge_group_lag — the `bridge-group` consumer-group lag on
    redpanda-edge across raw-sensor-stream + tactical-events. The
    edge-hq-bridge commits offsets for that group only when its writes to
    its parent succeed; when the link is severed the writes fail, offsets
    stop advancing, and this lag climbs. It IS the real edge-buffer depth.

  hq_link_severed — whether THIS TIER'S OWN UPLINK has completed no
    exchange with its parent for longer than LINK_SEVER_AFTER_S. The
    column keeps its historical name for schema compatibility (no Atlas
    migration here); what it now measures is reachability, not a toxiproxy
    flag.

    Why reachability, and why measured from the relay's commits rather
    than probed directly from this pod: the relay (bridge/uplink) commits
    its consumer-group offsets only after its parent acks the write, so an
    advance of the relay group's committed offsets across a heartbeat IS a
    completed exchange with the parent — the earlier toxiproxy-`enabled`
    probe only worked on the one tier whose link happened to run through
    toxiproxy, and read wrong (or not at all) everywhere else. A
    projector-side probe of the link itself would also be structurally
    wrong even where a proxy exists: a NetworkPolicy cut of a parent
    typically admits the child's relay traffic but not an arbitrary probe
    from the child's projector pod, so a projector-side check would read
    SEVERED while the relay's data kept flowing. `LinkReachability` turns
    the bridge-group's own commit cadence into the up/severed decision,
    with hysteresis so one missed heartbeat doesn't flap the UI.

If either probe cannot reach its dependency the row is still written with
`probe_healthy = False`, so the UI can show "probe down" instead of a
stale number presenting as real. During warm-up (no exchange observed yet
and the sever threshold hasn't elapsed either) `LinkReachability.severed`
is `None` — "cannot vouch" — and `probe_healthy` reads False for that row
too, for the same reason: a boolean with no honest "don't know" state
trains readers to trust whichever value happens to be the default.
"""
from __future__ import annotations

import asyncio
import logging
import os

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from persistence import PostgresPool, Write

log = logging.getLogger("projector.edge_buffer")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-edge:9092")
BRIDGE_CONSUMER_GROUP = os.getenv("BRIDGE_CONSUMER_GROUP", "bridge-group")

# WHAT THIS TIER CALLS ITS OUTBOUND LINK. A leaf's link is edge -> HQ; an
# intermediate's is region -> HQ, and its inbound side carries a subtree.
# Hardcoding "edge" put a leaf's label on a region's screen, which is the
# mode-confusion class: a panel that is correct about its numbers and wrong
# about whose numbers they are.
#
# Defaults to "edge" so a deployment that has not been told still renders
# what it always rendered, rather than a blank where a label used to be.
BUFFER_ROW_ID = os.getenv("BUFFER_ROW_ID", "edge")
BRIDGE_TOPICS = [
    t.strip()
    for t in os.getenv("BRIDGE_TOPICS", "raw-sensor-stream,tactical-events").split(",")
    if t.strip()
]
PROBE_INTERVAL_S = float(os.getenv("EDGE_BUFFER_PROBE_INTERVAL_S", "2"))

# How often the relay (bridge/uplink) commits its consumer-group offsets
# when the link is healthy and idle-ish. Measured at rest: tier relays
# 0.5-1.5s; the hub-attached edge's bridge 5.0s (its relay's default
# commit period). Each tier sets this to its own measured value; the
# default here is the common tier-relay figure, not the hub-edge one.
EXCHANGE_PERIOD_S = float(os.getenv("LINK_EXCHANGE_PERIOD_S", "5.0"))


def default_sever_after_s(exchange_period_s: float, probe_interval_s: float) -> float:
    """Derive the "no exchange in this long means severed" threshold from
    the two things that bound how late a real exchange can look: the
    relay's own commit period (E) and this monitor's heartbeat (H).

    2*(E+H) gives one full exchange period of slack plus one full
    heartbeat of slack on top of that — enough that a single slow commit
    or a single missed probe heartbeat never reads as severed, while a
    genuinely cut link is caught within two heartbeats of two exchange
    periods at the outside.
    """
    return 2.0 * (exchange_period_s + probe_interval_s)


# Overridable independently of the derivation above, for a tier whose
# measured commit behaviour doesn't fit the 2*(E+H) shape.
LINK_SEVER_AFTER_S = float(
    os.getenv("LINK_SEVER_AFTER_S", "")
    or default_sever_after_s(EXCHANGE_PERIOD_S, PROBE_INTERVAL_S)
)
LINK_RESTORE_EXCHANGES = int(os.getenv("LINK_RESTORE_EXCHANGES", "2"))

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


def _probe_bridge_lag() -> tuple[int, int]:
    """Sum the bridge consumer group's lag AND its committed offsets
    across the bridge topics, in one lookup.

    Returns:
        (lag, committed_sum) — committed_sum sums only the committed
        offsets that are >= 0 (a partition the group has never committed
        to contributes nothing, rather than poisoning the sum negative).
        An advance of committed_sum between two heartbeats is a completed
        exchange with the parent; see `LinkReachability`.

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
        total_lag = 0
        committed_sum = 0
        for tp in committed:
            lo, hi = consumer.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition), timeout=5
            )
            pos = tp.offset if tp.offset is not None and tp.offset >= 0 else lo
            total_lag += max(0, hi - pos)
            if tp.offset is not None and tp.offset >= 0:
                committed_sum += tp.offset
        return total_lag, committed_sum
    finally:
        consumer.close()


class LinkReachability:
    """Turns a sequence of (committed_sum, lag) observations into an
    up/severed decision. Pure, I/O-free — no Kafka, no Postgres, no clock
    of its own; the caller supplies `now`. Unit-tested directly in
    `src/tests/test_edge_buffer_reachability.py`.

    An "exchange" is evidence that this tier's relay actually reached its
    parent since the last observation:

      - committed_sum advanced past the previous baseline (the relay
        committed new offsets, which it only does after its parent acks), OR
      - lag == 0 (the relay is fully caught up right now, which is only
        possible if its last produce to the parent succeeded).

    committed_sum going DOWN (a consumer-group reset, or the bridge topics
    being recreated) is not evidence either way — it rebases the baseline
    so the drop itself can't manufacture a false "no exchange" gap.
    committed_sum of None (the probe failed) is likewise not evidence and
    does NOT rebase — the age since the last real exchange just keeps
    growing, exactly as if the relay had gone quiet.

    State is `None` (undecided / "cannot vouch") until the first exchange
    or until age exceeds `sever_after_s` with none seen. Once up, it flips
    to severed the first time age is STRICTLY greater than
    `sever_after_s`. Once severed, it only flips back after
    `restore_exchanges` consecutive exchanges, each no more than
    `sever_after_s` apart from the previous one — a single exchange after
    a long sever doesn't immediately read as healthy, which is the
    hysteresis that keeps a borderline link from flapping the UI every
    heartbeat.
    """

    def __init__(self, sever_after_s: float, restore_exchanges: int = 2) -> None:
        self._sever_after_s = sever_after_s
        self._restore_exchanges = restore_exchanges

        self._start_time: float | None = None
        self._baseline_sum: int | None = None
        self._last_exchange_time: float | None = None
        self._severed: bool | None = None  # None = undecided (warm-up)
        self._restore_count = 0

    def observe(self, now: float, committed_sum: int | None, lag: int | None) -> None:
        if self._start_time is None:
            self._start_time = now

        is_exchange = False
        if committed_sum is not None:
            # Only an INCREASE over the existing baseline is evidence. The
            # very first reading has no baseline to compare against (lag
            # == 0 is the only evidence a first reading can offer), and a
            # decrease (group reset / topics recreated) rebases silently
            # rather than counting either way.
            if self._baseline_sum is not None and committed_sum > self._baseline_sum:
                is_exchange = True
            self._baseline_sum = committed_sum
        # else: probe failed. No evidence, and — deliberately — no rebase:
        # the baseline and the age below are left exactly as they were, so
        # a run of failed probes reads the same as a relay gone quiet.
        if lag == 0:
            is_exchange = True

        if is_exchange:
            if self._severed is True:
                gap = (now - self._last_exchange_time) if self._last_exchange_time is not None else None
                self._restore_count = (
                    self._restore_count + 1 if gap is not None and gap <= self._sever_after_s else 1
                )
                if self._restore_count >= self._restore_exchanges:
                    self._severed = False
                    self._restore_count = 0
            else:
                self._severed = False
                self._restore_count = 0
            self._last_exchange_time = now
        else:
            age = self.last_exchange_age(now)
            if age is not None and age > self._sever_after_s and self._severed is not True:
                self._restore_count = 0
                self._severed = True

    @property
    def severed(self) -> bool | None:
        return self._severed

    def last_exchange_age(self, now: float) -> float | None:
        if self._start_time is None:
            return None
        anchor = self._last_exchange_time if self._last_exchange_time is not None else self._start_time
        return now - anchor


def _build_write(lag: int, severed: bool, healthy: bool) -> Write:
    from handlers.base import now_utc

    return Write(
        table="edge_buffer_status",
        mode="upsert",
        key_columns=["id"],
        row={
            "id": BUFFER_ROW_ID,
            "bridge_group_lag": int(lag),
            "hq_link_severed": bool(severed),
            "probe_healthy": bool(healthy),
            "updated_at": now_utc(),
        },
    )


async def edge_buffer_loop(pool: PostgresPool) -> None:
    """Probe + write the edge_buffer_status row on a fixed interval."""
    loop = asyncio.get_running_loop()
    reachability = LinkReachability(
        sever_after_s=LINK_SEVER_AFTER_S, restore_exchanges=LINK_RESTORE_EXCHANGES
    )
    # Logged once at loop start, not per-heartbeat — these are the three
    # numbers that decide how long a cut link takes to show SEVERED and
    # how long a restored one takes to show up again.
    log.info(
        "edge-buffer monitor started: group=%s topics=%s interval=%.1fs "
        "exchange_period=%.1fs sever_after=%.1fs restore_exchanges=%d",
        BRIDGE_CONSUMER_GROUP, BRIDGE_TOPICS, PROBE_INTERVAL_S,
        EXCHANGE_PERIOD_S, LINK_SEVER_AFTER_S, LINK_RESTORE_EXCHANGES,
    )
    while True:
        await asyncio.sleep(PROBE_INTERVAL_S)
        now = loop.time()
        # LAG_UNKNOWN, not 0. Writing 0 here is what made a blind probe look
        # like a healthy caught-up link for months. Consumers of this row must
        # treat a negative lag as "no reading", and `probe_healthy` is false
        # alongside it.
        lag = LAG_UNKNOWN
        committed_sum: int | None = None
        healthy = True
        try:
            lag, committed_sum = await loop.run_in_executor(None, _probe_bridge_lag)
        except BridgeGroupAbsent as exc:
            healthy = False
            lag = LAG_UNKNOWN
            committed_sum = None
            # WARNING, and deduplicated — not debug. The previous code logged
            # probe failures at DEBUG, which is invisible at the default level,
            # so the one signal that would have revealed the wrong group name
            # was suppressed by default on every cluster.
            _warn_once("bridge-group-absent", "edge-buffer: %s", exc)
        except Exception as exc:  # noqa: BLE001 - probe failure is non-fatal
            healthy = False
            lag = LAG_UNKNOWN
            committed_sum = None
            _warn_once("bridge-lag-probe", "edge-buffer: bridge-lag probe failed: %s", exc)

        # A failed probe feeds the reachability state None/None, same as a
        # relay that's gone quiet — see LinkReachability's docstring on why
        # that's deliberate and not a rebase.
        reachability.observe(now, committed_sum, lag if healthy else None)
        severed = reachability.severed
        probe_healthy = healthy and severed is not None

        try:
            await pool.execute(_build_write(lag, bool(severed), probe_healthy))
        except Exception as exc:  # noqa: BLE001 - never let the monitor crash
            log.warning("edge_buffer_status write failed: %s", exc)
