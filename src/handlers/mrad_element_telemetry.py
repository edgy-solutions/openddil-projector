"""Handler: mrad-element-telemetry -> mrad_element_telemetry.

Decodes the openddil-mrad-sim per-asset envelope:
  {
    "asset_id": "<asset_id>",
    "observed_at_ns": <int>,
    "elements": [
      {"element_id": ..., "layer_depth": ..., "layer_name": ...,
       "health": ..., "temp_c": ..., "load_pct": ...},
      ...
    ]
  }

Compacted topic keyed by asset_id -> UPSERT. The elements array is stored
verbatim as JSONB so the SensorArrayView can build its per-element lookup
without the projector having to expand to one-row-per-element (which would
multiply postgres write volume by ~115 per tick per asset for no benefit
on the read side).

Origin-node provenance: mrad-sim runs on HQ and emits no edge_id of its
own. The projector backfills via resolve_origin_or_derive(provenance=None,
asset_id, ...) -- same path capability_state uses for strike-only
launchers that have no position-bearing telemetry.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from persistence import Write

from .base import now_utc, resolve_origin_or_derive

TABLE = "mrad_element_telemetry"
HANDLER_LABEL = "mrad_element_telemetry"


def _observed_at_from_ns(observed_at_ns: Any) -> datetime:
    """Convert the sim's observed_at_ns (int wall-clock nanoseconds) to a
    timezone-aware datetime for postgres. Defensive about type because
    JSON's `int` parses cleanly but a stray string slips through if the
    publisher format ever changes."""
    try:
        ns = int(observed_at_ns)
        return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
    except (TypeError, ValueError):
        return now_utc()


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    asset_id = decoded.get("asset_id") or key
    if not asset_id:
        return None

    elements = decoded.get("elements") or []
    if not isinstance(elements, list):
        # A malformed publisher record -- drop silently rather than
        # corrupt the table with a non-array. The DEBUG log fires from
        # the projector base layer; no need to duplicate here.
        return None

    # mrad-sim has no edge attribution of its own. Resolve via the
    # registered edge_assignment strategy (asset_id_prefix / static
    # fallback) just like capability_state does for strike-only
    # launchers. provenance arg is None because the sim envelope has
    # no provenance block.
    prov = resolve_origin_or_derive(None, asset_id, HANDLER_LABEL)

    row = {
        "asset_id": asset_id,
        "elements": elements,
        "observed_at": _observed_at_from_ns(decoded.get("observed_at_ns")),
        "updated_at": now_utc(),
        **prov,
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["asset_id"],
        row=row,
        jsonb_columns={"elements"},
    )
