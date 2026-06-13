"""Handler registry.

Each handler module exposes a `handle(key, decoded) -> Write | None`
function. `get_handler` resolves the name from projector_config.yaml to the
function. Adding a topic = a new module here + a config entry.
"""
from __future__ import annotations

from . import (
    asset_element_telemetry,
    capability_state,
    cm_state,
    logistics_status,
    region_fleet_summary,
    region_top_factors,
    region_wear_trends,
    tactical_events,
    telemetry_latest,
    telemetry_windows,
)
from .base import Handler

_REGISTRY: dict[str, Handler] = {
    "cm_state": cm_state.handle,
    "logistics_status": logistics_status.handle,
    "telemetry_latest": telemetry_latest.handle,
    "tactical_events": tactical_events.handle,
    "telemetry_windows": telemetry_windows.handle,
    # ADR-0023 Phase 6b §B — regional rollups.
    "region_fleet_summary": region_fleet_summary.handle,
    "region_top_factors":   region_top_factors.handle,
    "region_wear_trends":   region_wear_trends.handle,
    # Recipe v3 Sub-phase E — customer-overlay capability snapshots.
    "capability_state":     capability_state.handle,
    # Phase 9 — openddil-logistics-sim per-element sub-component telemetry.
    "asset_element_telemetry": asset_element_telemetry.handle,
}


def get_handler(name: str) -> Handler:
    """Resolve a handler name to its function. Raises KeyError if unknown."""
    if name not in _REGISTRY:
        raise KeyError(
            f"unknown handler '{name}' — registered: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


__all__ = ["get_handler", "Handler"]
