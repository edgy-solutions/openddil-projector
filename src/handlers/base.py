"""Handler interface + shared field-mapping helpers.

A handler is a pure function: it takes the Kafka message key and the decoded
message dict, and returns a `Write` (or None to skip). No I/O, no Kafka, no
Postgres — that makes every handler unit-testable in isolation.

The helpers here cover the recurring proto-JSON quirks:
  - Timestamps serialise as RFC3339 strings; Postgres timestamptz wants
    datetime objects.
  - Durations serialise as "3.5s" strings; we store whole seconds.
  - Enums serialise as their string names (use_integers_for_enums=False),
    which is exactly what the text columns want — stored as-is.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable

from persistence import Write

# A handler maps (key, decoded) -> Write or None.
Handler = Callable[[str, dict[str, Any]], "Write | None"]


# -- origin-node provenance (ADR-0022) ----------------------------------------
# OpenDDIL is hierarchical streaming aggregation: edge -> regional -> HQ. The
# topology is currently collapsed to a single flat tier, so these are constant
# defaults — but every per-asset Write the projector emits carries them
# explicitly. The projector stays provenance-aware *in shape* even while
# running flat: when the hierarchy phase lands, only the value source changes
# (a per-tier deployment env, or a field on the message), not the handler
# signature or the row schema. Retrofitting an echelon dimension after the
# shapes, rollups, and ALCS/EAGLE egress bridges already exist is the
# expensive path this avoids.
ORIGIN_EDGE_ID = os.getenv("OPENDDIL_EDGE_ID", "edge-01")
ORIGIN_REGION_ID = os.getenv("OPENDDIL_REGION_ID", "region-01")


def origin_provenance() -> dict[str, str]:
    """The origin-node columns every per-asset projection row carries.

    Constant today (single-tier); env-overridable for a future per-tier
    deployment. See ADR-0022 for why this stays explicit rather than leaning
    on the DB column default. Named `origin_*` to stay distinct from the
    telemetry-message `provenance` blob (producer_id / source_protocol /
    sample_time) that telemetry_latest_state also carries."""
    return {"edge_id": ORIGIN_EDGE_ID, "region_id": ORIGIN_REGION_ID}


def now_utc() -> datetime:
    """Current UTC time — handlers stamp `updated_at`/`ingested_at` with this
    so the UPSERT refreshes it on every write (a DB default only fires on
    INSERT, not on ON CONFLICT DO UPDATE)."""
    return datetime.now(timezone.utc)


def parse_timestamp(value: Any) -> datetime | None:
    """RFC3339 string (proto JSON Timestamp) -> aware datetime, or None.

    Tolerates already-parsed datetimes and the trailing 'Z'.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_ns_timestamp(value: Any) -> datetime | None:
    """Integer nanoseconds-since-epoch -> aware datetime, or None.

    cm-service serialises proto Timestamps as `*_ns` integers in its JSON
    payloads (not RFC3339 strings). 0 is treated as 'unset' -> None.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        ns = int(value)
    except (TypeError, ValueError):
        return None
    if ns <= 0:
        return None
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)


def duration_to_seconds(value: Any) -> int | None:
    """Proto JSON Duration ("3.5s", "120s") -> whole seconds, or None."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        return None
    text = value[:-1] if value.endswith("s") else value
    try:
        return int(float(text))
    except ValueError:
        return None


def first_present(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Return d[k] for the first key present and truthy-or-zero, else default.

    Proto JSON omits unset scalar fields entirely, so a missing key is normal.
    """
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


# -- Rate-limited fallback WARN helper (ADR-0023 Phase 6a/6b) -----------------
# Per-process throttle keyed by (handler_label, edge_id_we_fell_back_to,
# asset_id). One WARN per 60s per key, with a count of suppressed-in-window.
# Used by every per-asset handler that switched from env-default attribution
# to message-field provenance (Phase 6b §A).
import logging as _logging
import time as _time

_FALLBACK_WARN_WINDOW_S = 60.0
_fallback_last_warn: dict[tuple[str, str, str], tuple[float, int]] = {}


def warn_provenance_fallback(handler_label: str, edge_id: str,
                              asset_id: str) -> None:
    """Emit a WARN at most once per _FALLBACK_WARN_WINDOW_S per key, with
    suppressed-count in the next emission."""
    log = _logging.getLogger(handler_label)
    key = (handler_label, edge_id, asset_id)
    now = _time.monotonic()
    last = _fallback_last_warn.get(key)
    if last is None or (now - last[0]) >= _FALLBACK_WARN_WINDOW_S:
        if last is not None and last[1] > 0:
            log.warning(
                "provenance missing edge_id/region_id, falling back to env "
                "(asset=%s) [%d more suppressed in last %ds]",
                asset_id, last[1], int(_FALLBACK_WARN_WINDOW_S),
            )
        else:
            log.warning(
                "provenance missing edge_id/region_id, falling back to env "
                "(asset=%s)", asset_id,
            )
        _fallback_last_warn[key] = (now, 0)
    else:
        _fallback_last_warn[key] = (last[0], last[1] + 1)


def resolve_provenance_from_proto(provenance_msg: Any, asset_id: str,
                                    handler_label: str) -> dict[str, str]:
    """Read edge_id/region_id from a proto Provenance message; fall back to
    env defaults with rate-limited WARN when absent. Returns the dict
    callers spread into their Write.row."""
    msg_edge = getattr(provenance_msg, "edge_id", "") or ""
    msg_region = getattr(provenance_msg, "region_id", "") or ""
    if not msg_edge or not msg_region:
        warn_provenance_fallback(handler_label, ORIGIN_EDGE_ID, asset_id)
    return {
        "edge_id":   msg_edge or ORIGIN_EDGE_ID,
        "region_id": msg_region or ORIGIN_REGION_ID,
    }


def resolve_provenance_from_dict(provenance_dict: dict, asset_id: str,
                                   handler_label: str) -> dict[str, str]:
    """Read edge_id/region_id from a decoded message's `provenance` dict
    (proto-JSON shape). Same fallback semantics as the proto variant."""
    msg_edge = (provenance_dict or {}).get("edge_id") or ""
    msg_region = (provenance_dict or {}).get("region_id") or ""
    if not msg_edge or not msg_region:
        warn_provenance_fallback(handler_label, ORIGIN_EDGE_ID, asset_id)
    return {
        "edge_id":   msg_edge or ORIGIN_EDGE_ID,
        "region_id": msg_region or ORIGIN_REGION_ID,
    }


def resolve_provenance_from_top_level(envelope: dict, asset_id: str,
                                        handler_label: str) -> dict[str, str]:
    """Read edge_id/region_id from a JSON envelope's top-level keys (the
    cm-state shape — cm-service stamps these at the top, not nested in a
    provenance block, because asset-cm-state is JSON not proto per
    ADR-0018). Same fallback semantics."""
    msg_edge = (envelope or {}).get("edge_id") or ""
    msg_region = (envelope or {}).get("region_id") or ""
    if not msg_edge or not msg_region:
        warn_provenance_fallback(handler_label, ORIGIN_EDGE_ID, asset_id)
    return {
        "edge_id":   msg_edge or ORIGIN_EDGE_ID,
        "region_id": msg_region or ORIGIN_REGION_ID,
    }
