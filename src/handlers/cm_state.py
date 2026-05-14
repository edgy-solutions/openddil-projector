"""Handler: asset-cm-state -> asset_cm_state.

cm-service emits this topic as JSON (json.dumps of its internal payload),
NOT protobuf — confirmed against the running stack in Phase 4a. The JSON
shape differs from the AsMaintainedConfiguration proto in three ways the
handler normalises here:

  * timestamps are `*_ns` integer nanoseconds, not RFC3339 strings
  * enum fields (overall_status, lifecycle, last_alerted_status) are integer
    values, not string names — mapped back to names so the text columns and
    the UI get readable values
  * `manual_discrepancies` is present alongside `discrepancies` (ADR-0009
    keeps them separate); both are stored

Compacted topic, keyed by asset_id -> UPSERT. Nested lists are stored as
JSONB blobs verbatim.
"""
from __future__ import annotations

import logging
from typing import Any

from persistence import Write

from .base import now_utc, parse_ns_timestamp

log = logging.getLogger("projector.handler.cm_state")
TABLE = "asset_cm_state"

# Map cm-service's integer enum values back to their proto names. Imported
# best-effort: if the contracts stubs aren't on PYTHONPATH the handler still
# works, it just stores the integer as a string.
try:
    from openddil.configuration.v1 import as_maintained_pb2 as _pb

    _CONFIG_STATUS = _pb.ConfigurationStatus
    _LIFECYCLE = _pb.LifecycleState
except Exception:  # noqa: BLE001 - proto stubs optional at import time
    _CONFIG_STATUS = None
    _LIFECYCLE = None


def _enum_name(enum_type: Any, value: Any, default: str) -> str:
    """Integer enum value -> proto name. Pass-through if already a string;
    fall back to the raw value's string form if the lookup fails."""
    if value is None:
        return default
    if isinstance(value, str):
        return value
    if enum_type is None:
        return str(value)
    try:
        return enum_type.Name(int(value))
    except (ValueError, TypeError):
        return str(value)


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset_id = decoded.get("asset_id") or key
    if not asset_id:
        return None

    row = {
        "asset_id": asset_id,
        "baseline_id": decoded.get("baseline_id"),
        "lifecycle": _enum_name(
            _LIFECYCLE, decoded.get("lifecycle"), "LIFECYCLE_UNSPECIFIED"
        ),
        "overall_status": _enum_name(
            _CONFIG_STATUS, decoded.get("overall_status"),
            "CONFIG_STATUS_UNSPECIFIED",
        ),
        "last_alerted_status": (
            _enum_name(_CONFIG_STATUS, decoded["last_alerted_status"], "")
            if decoded.get("last_alerted_status") is not None
            else None
        ),
        "as_of": parse_ns_timestamp(decoded.get("as_of_ns")),
        "last_observed_at": parse_ns_timestamp(
            decoded.get("last_observed_at_ns")
        ),
        "installed": decoded.get("installed", []),
        "mod_status": decoded.get("mod_status", []),
        "discrepancies": decoded.get("discrepancies", []),
        "manual_discrepancies": decoded.get("manual_discrepancies", []),
        "updated_at": now_utc(),
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={
            "installed",
            "mod_status",
            "discrepancies",
            "manual_discrepancies",
        },
    )
