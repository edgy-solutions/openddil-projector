"""Handler registry.

Each handler module exposes a `handle(key, decoded) -> Write | None`
function. `get_handler` resolves the name from projector_config.yaml to the
function. Adding a topic = a new module here + a config entry.
"""
from __future__ import annotations

from . import (
    cm_state,
    logistics_status,
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
}


def get_handler(name: str) -> Handler:
    """Resolve a handler name to its function. Raises KeyError if unknown."""
    if name not in _REGISTRY:
        raise KeyError(
            f"unknown handler '{name}' — registered: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


__all__ = ["get_handler", "Handler"]
