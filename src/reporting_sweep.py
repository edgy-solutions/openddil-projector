"""ADR-0044 lifecycle slice 1 — reporting-status staleness sweep.

A handler sets reporting_status="reporting" the moment a message arrives
(handlers/telemetry_latest.py), but nothing writes reporting_status back to
"not_reporting" when messages stop — the whole point of the two-column
model is that silence is a fact about US, not about the asset, so it cannot
be inferred inside a per-message handler that only runs when a message
DOES arrive. This loop is what actually observes the silence, on this
projector instance's own interval and its own clock, and never any other
tier's.

Scope for this slice: `telemetry_latest_state` only, the one table this
slice's compose cases exercise (see SPEC-lifecycle-slice-1.md "measure,
don't assume" — that is where the asset row for a DIS entity lives today).
The other four per-asset tables (asset_cm_state, asset_logistics_status,
asset_telemetry_windows, asset_capability_state) are not part of this slice
and are not swept here.

Modeled on edge_buffer_loop (edge_buffer_monitor.py): fixed interval,
never lets a probe/write failure crash the loop, logs a rate-limited
warning instead of retrying in a tight loop.
"""
from __future__ import annotations

import asyncio
import logging
import os

from persistence import PostgresPool

log = logging.getLogger("projector.reporting_sweep")

TABLE = "telemetry_latest_state"
SAMPLE_COLUMN = "last_sample_at"
REPORTING_COLUMN = "reporting_status"
REPORTING_AT_COLUMN = "reporting_status_at"
NOT_REPORTING_VALUE = "not_reporting"

# No standards body defines this number (checked: DIS/IEEE-1278.1 leaves
# entity-state timeout to the application). 30s matches the existing,
# already-shipped local heuristics this repo uses elsewhere for the same
# judgment call — assetTier.ts's stale_after_s and fusion's
# STALE_INPUT_SECONDS use the same order of magnitude for "how long before
# a quiet entity stops being presumed current" — without importing from
# either (frontend is out of bounds for this slice; fusion's constant is a
# different reading tier's own choice, per ADR §4, not a shared value).
STALE_AFTER_S = float(os.getenv("REPORTING_STALE_AFTER_S", "30"))
SWEEP_INTERVAL_S = float(os.getenv("REPORTING_SWEEP_INTERVAL_S", "5"))

_warned: set[str] = set()


def _warn_once(key: str, fmt: str, *args) -> None:
    if key not in _warned:
        _warned.add(key)
        log.warning(fmt + "  (further identical warnings suppressed)", *args)


async def reporting_sweep_loop(pool: PostgresPool) -> None:
    """Flip stale telemetry_latest_state rows to not_reporting, forever,
    on this instance's own interval and clock — never a global fact, never
    computed for another tier (ADR §4)."""
    from handlers.base import now_utc

    log.info(
        "reporting-status sweep started: table=%s stale_after=%.0fs interval=%.0fs",
        TABLE, STALE_AFTER_S, SWEEP_INTERVAL_S,
    )
    while True:
        await asyncio.sleep(SWEEP_INTERVAL_S)
        try:
            touched = await pool.sweep_reporting_staleness(
                TABLE,
                sample_column=SAMPLE_COLUMN,
                reporting_column=REPORTING_COLUMN,
                reporting_at_column=REPORTING_AT_COLUMN,
                not_reporting_value=NOT_REPORTING_VALUE,
                stale_after_s=STALE_AFTER_S,
                now=now_utc(),
            )
            if touched:
                log.info("reporting-status sweep: %d row(s) -> not_reporting", touched)
        except Exception as exc:  # noqa: BLE001 - never let the sweep crash
            _warn_once("reporting-sweep-failed", "reporting-status sweep failed: %s", exc)
