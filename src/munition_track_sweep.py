"""munition_track silence sweep (ADR-0049).

A track with no terminal_state is in flight. DIS carries no "miss" and a
munition that stops reporting may have simply left coverage, so a track that
goes quiet is marked `silent`, never given a launch outcome (ADR-0048
section 4 is untouched). Silence is judged on this tier's own arrival clock
(`last_heard_at`), never the edge's sample time, so a delayed backlog cannot
make a live track look dead. Same shape as effector_sweep.py: env-tunable
constants, a fixed-interval loop, and a sweep failure only warns.
"""
from __future__ import annotations

import asyncio
import logging
import os

from metrics import MUNITION_TRACK_JOINED, MUNITION_TRACK_SILENT
from persistence import PostgresPool

log = logging.getLogger("projector.munition_track_sweep")

TABLE = "munition_track"

# 12 s = DIS's default entity timeout (2.4 x the 5 s heartbeat).
MUNITION_TRACK_SILENCE_S = float(os.getenv("MUNITION_TRACK_SILENCE_S", "12"))
MUNITION_TRACK_SWEEP_INTERVAL_S = float(os.getenv("MUNITION_TRACK_SWEEP_INTERVAL_S", "2"))

_warned: set[str] = set()


def _warn_once(key: str, fmt: str, *args: object) -> None:
    if key not in _warned:
        _warned.add(key)
        log.warning(fmt + "  (further identical warnings suppressed)", *args)


async def sweep_pass(pool: PostgresPool) -> None:
    """One pass: the silence sweep, then the launcher heal. Each warns on
    failure and never raises."""
    from handlers.base import now_utc

    try:
        touched = await pool.sweep_munition_track_silence(
            silence_s=MUNITION_TRACK_SILENCE_S,
            now=now_utc(),
        )
        if touched:
            MUNITION_TRACK_SILENT.inc(touched)
            log.info("munition track sweep: %d track(s) -> silent", touched)
    except Exception as exc:  # noqa: BLE001 - never let the sweep crash
        _warn_once("munition-track-sweep-failed",
                   "munition track silence sweep failed: %s", exc)
    # A track whose launcher join lost a commit race converges here even if
    # it never gets another sample (terminal rows included).
    try:
        healed = await pool.heal_munition_track_launchers(now=now_utc())
        if healed:
            MUNITION_TRACK_JOINED.labels(via="heal").inc(healed)
            log.info("munition track sweep: %d track(s) labelled", healed)
    except Exception as exc:  # noqa: BLE001 - never let the sweep crash
        _warn_once("munition-track-heal-failed",
                   "munition track launcher heal failed: %s", exc)


async def munition_track_sweep_loop(pool: PostgresPool) -> None:
    """Mark in-flight tracks quiet for MUNITION_TRACK_SILENCE_S as 'silent',
    and heal launcher-less tracks, forever, on this instance's own interval
    and clock."""
    log.info(
        "munition track silence sweep started: table=%s silence=%.0fs interval=%.0fs",
        TABLE, MUNITION_TRACK_SILENCE_S, MUNITION_TRACK_SWEEP_INTERVAL_S,
    )
    while True:
        await asyncio.sleep(MUNITION_TRACK_SWEEP_INTERVAL_S)
        await sweep_pass(pool)
