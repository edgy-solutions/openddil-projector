"""effector_launch timeout sweep.

A Fire admitted into `effector_launch` has `terminal_state` NULL (in
flight) until a Detonation resolves it. DIS carries no "miss" event, so a
round that never gets a Detonation would stay NULL forever without this
sweep. Modeled on reporting_sweep.py: its own module, its own env-tunable
constants, a fixed-interval `asyncio.sleep` loop on this instance's own
clock, and a probe/write failure never crashes the loop -- only a
rate-limited warning.
"""
from __future__ import annotations

import asyncio
import logging
import os

from metrics import EFFECTOR_UNRESOLVED
from persistence import PostgresPool

log = logging.getLogger("projector.effector_sweep")

TABLE = "effector_launch"

# Defaults: 300s timeout, 15s sweep interval. Both env-overridable so a compose
# run can exercise the sweep on a human timescale (the demo wiring sets
# EFFECTOR_TERMINAL_TIMEOUT_S=60) instead of waiting out a real 300s.
EFFECTOR_TERMINAL_TIMEOUT_S = float(os.getenv("EFFECTOR_TERMINAL_TIMEOUT_S", "300"))
EFFECTOR_SWEEP_INTERVAL_S = float(os.getenv("EFFECTOR_SWEEP_INTERVAL_S", "15"))

_warned: set[str] = set()


def _warn_once(key: str, fmt: str, *args: object) -> None:
    if key not in _warned:
        _warned.add(key)
        log.warning(fmt + "  (further identical warnings suppressed)", *args)


async def effector_sweep_loop(pool: PostgresPool) -> None:
    """Mark effector_launch rows past EFFECTOR_TERMINAL_TIMEOUT_S as
    'unresolved', forever, on this instance's own interval and clock."""
    from handlers.base import now_utc

    log.info(
        "effector timeout sweep started: table=%s timeout=%.0fs interval=%.0fs",
        TABLE, EFFECTOR_TERMINAL_TIMEOUT_S, EFFECTOR_SWEEP_INTERVAL_S,
    )
    while True:
        await asyncio.sleep(EFFECTOR_SWEEP_INTERVAL_S)
        try:
            touched = await pool.sweep_effector_timeouts(
                timeout_s=EFFECTOR_TERMINAL_TIMEOUT_S,
                now=now_utc(),
            )
            if touched:
                EFFECTOR_UNRESOLVED.inc(touched)
                log.info("effector timeout sweep: %d row(s) -> unresolved", touched)
        except Exception as exc:  # noqa: BLE001 - never let the sweep crash
            _warn_once("effector-sweep-failed",
                       "effector timeout sweep failed: %s", exc)
