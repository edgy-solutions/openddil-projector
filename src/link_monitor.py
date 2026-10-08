"""HQ link monitor: per-link reachability measured by heartbeat ARRIVAL.

Each tier produces a LinkHeartbeat to its own broker; the relay forwards it
up the same path as the data. This module, running at HQ only, consumes
`link-heartbeat` there and writes one `link_status` row per expected link.

Why arrival at HQ, and not a probe from a pod: a NetworkPolicy cut of a
parent typically admits the relay's traffic but not an arbitrary connection
from some other pod, so a pod-side probe reads the path of the wrong pod --
SEVERED while data still flows, or UP while the relay is cut. A heartbeat
that rides the data path through the same relay either arrives or it does
not, and arrival is measured on HQ's own monotonic clock, so there is no
cross-site clock skew. The sender's `emitted_at` is informational only.

States (per link):
  unknown  no heartbeat yet and uptime <= down_after (warm-up)
  down     last arrival older than down_after, or none after warm-up;
           restore needs 2 consecutive arrivals, each <= down_after apart
  idle     fresh, and the sender reported its source topics idle
  up       fresh, and traffic ACTIVE or UNSPECIFIED (IDLE is a refinement
           that needs evidence, so an unmeasured link is not shown idle)
A declared-idle link reads idle while fresh unless traffic is ACTIVE (a
declaration never hides traffic), and still reads down when stale (it never
hides unreachability).
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime

from confluent_kafka import OFFSET_END, Consumer, TopicPartition

from link_heartbeat import MalformedHeartbeat, decode_heartbeat
from metrics import LINK_HEARTBEATS_RECEIVED, LINK_HEARTBEATS_REJECTED
from persistence import PostgresPool, Write

log = logging.getLogger("projector.link_monitor")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-edge:9092")
LINK_HEARTBEAT_TOPIC = "link-heartbeat"
LINK_DOWN_AFTER_S = float(os.getenv("LINK_DOWN_AFTER_S", "15"))
LINK_RESTORE_ARRIVALS = 2
UPSERT_INTERVAL_S = 2.0


def _csv(name: str) -> list[str]:
    return [t.strip() for t in os.getenv(name, "").split(",") if t.strip()]


def link_monitor_enabled() -> bool:
    return os.getenv("LINK_MONITOR_ENABLED", "false").lower() == "true"


class LinkTracker:
    """Reachability of one link from its arrival times. Pure: the caller
    supplies a monotonic `now`. `reach()` returns "unknown", "down" or
    "fresh"; down latches until `restore_arrivals` consecutive arrivals,
    each no more than `down_after_s` after the previous one."""

    def __init__(self, down_after_s: float, start: float, restore_arrivals: int = LINK_RESTORE_ARRIVALS) -> None:
        self._down_after_s = down_after_s
        self._start = start
        self._restore_arrivals = restore_arrivals
        self._last: float | None = None
        self._down = False
        self._restore_count = 0

    def on_arrival(self, now: float) -> None:
        gap = None if self._last is None else now - self._last
        # An arrival after a stale gap starts a restore even if no tick
        # noticed the staleness in between.
        if gap is not None and gap > self._down_after_s:
            self._down = True
        if self._down:
            self._restore_count = self._restore_count + 1 if gap is not None and gap <= self._down_after_s else 1
            if self._restore_count >= self._restore_arrivals:
                self._down = False
                self._restore_count = 0
        self._last = now

    def reach(self, now: float) -> str:
        if self._last is None:
            if now - self._start <= self._down_after_s:
                return "unknown"
            self._down = True
            return "down"
        if now - self._last > self._down_after_s:
            if not self._down:
                self._down = True
                self._restore_count = 0
            return "down"
        return "down" if self._down else "fresh"


def classify(reach: str, traffic: str, declared_idle: bool) -> str:
    """Final link_state from reachability, last-reported traffic and the
    declaration. `traffic` is the payload's name (ACTIVE / IDLE / UNSPECIFIED)."""
    if reach != "fresh":
        return reach
    if declared_idle:
        return "up" if traffic == "ACTIVE" else "idle"
    return "idle" if traffic == "IDLE" else "up"


@dataclass(frozen=True)
class LastSeen:
    arrival: float          # monotonic
    emitted_at: datetime
    traffic: str
    bridge_lag: int


def build_row(link_id: str, state: str, last: LastSeen | None, declared_idle: bool, now_mono: float) -> Write:
    from handlers.base import now_utc

    return Write(
        table="link_status",
        mode="upsert",
        key_columns=["id"],
        row={
            "id": link_id,
            "link_state": state,
            "traffic": last.traffic.lower() if last else "unspecified",
            "declared_idle": declared_idle,
            "heartbeat_age_s": (now_mono - last.arrival) if last else None,
            "last_heartbeat_at": last.emitted_at if last else None,
            "bridge_lag": last.bridge_lag if last else -1,
            "updated_at": now_utc(),
        },
    )


class LinkState:
    """All expected links' trackers and last payloads. No I/O."""

    def __init__(self, expected: list[str], declared_idle: list[str], down_after_s: float, start: float) -> None:
        self._expected = list(expected)
        self._declared = set(declared_idle)
        self._trackers = {i: LinkTracker(down_after_s, start) for i in self._expected}
        self._last: dict[str, LastSeen] = {}

    def ingest(self, raw: bytes, now_mono: float) -> None:
        try:
            hb = decode_heartbeat(raw)
        except MalformedHeartbeat as exc:
            LINK_HEARTBEATS_REJECTED.labels(reason="malformed").inc()
            log.debug("malformed link heartbeat skipped: %s", exc)
            return
        tracker = self._trackers.get(hb.link_id)
        if tracker is None:
            LINK_HEARTBEATS_REJECTED.labels(reason="unexpected").inc()
            return
        LINK_HEARTBEATS_RECEIVED.labels(link_id=hb.link_id).inc()
        tracker.on_arrival(now_mono)
        self._last[hb.link_id] = LastSeen(now_mono, hb.emitted_at, hb.traffic, hb.bridge_lag)

    def rows(self, now_mono: float) -> list[Write]:
        out = []
        for link_id in self._expected:
            last = self._last.get(link_id)
            declared = link_id in self._declared
            state = classify(
                self._trackers[link_id].reach(now_mono),
                last.traffic if last else "UNSPECIFIED",
                declared,
            )
            out.append(build_row(link_id, state, last, declared, now_mono))
        return out


def _open_consumer() -> Consumer:
    """Assign every partition of the heartbeat topic at the end; no group
    commits. Raises while the topic does not exist yet."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "link-monitor",
        "enable.auto.commit": False,
    })
    meta = consumer.list_topics(LINK_HEARTBEAT_TOPIC, timeout=10).topics.get(LINK_HEARTBEAT_TOPIC)
    if meta is None or meta.error is not None or not meta.partitions:
        consumer.close()
        raise RuntimeError(f"topic {LINK_HEARTBEAT_TOPIC} not available")
    consumer.assign([TopicPartition(LINK_HEARTBEAT_TOPIC, p, OFFSET_END) for p in meta.partitions])
    return consumer


async def _consume(state: LinkState) -> None:
    loop = asyncio.get_running_loop()
    consumer = None
    while True:
        try:
            if consumer is None:
                consumer = await loop.run_in_executor(None, _open_consumer)
            msg = await loop.run_in_executor(None, consumer.poll, 0.5)
            if msg is None:
                continue
            if msg.error():
                log.warning("link-heartbeat consume error: %s", msg.error())
                continue
            state.ingest(msg.value() or b"", loop.time())
        except asyncio.CancelledError:
            if consumer is not None:
                consumer.close()
            raise
        except Exception as exc:  # noqa: BLE001 - keep trying; rows go stale meanwhile
            log.warning("link-heartbeat consumer: %s", exc)
            await asyncio.sleep(2)


async def _upsert(state: LinkState, pool: PostgresPool) -> None:
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(UPSERT_INTERVAL_S)
        for write in state.rows(loop.time()):
            try:
                await pool.execute(write)
            except Exception as exc:  # noqa: BLE001 - never let the monitor crash
                log.warning("link_status write failed: %s", exc)


async def link_monitor_loop(pool: PostgresPool) -> None:
    expected = _csv("LINK_EXPECTED_IDS")
    declared = _csv("LINK_DECLARED_IDLE")
    loop = asyncio.get_running_loop()
    state = LinkState(expected, declared, LINK_DOWN_AFTER_S, loop.time())
    log.info(
        "link monitor started: expected=%s declared_idle=%s down_after=%.1fs",
        expected, declared, LINK_DOWN_AFTER_S,
    )
    await asyncio.gather(_consume(state), _upsert(state, pool))
