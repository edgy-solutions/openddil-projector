"""Handler: tactical-events -> tactical_events.

Decodes JSON CloudEvents (not protobuf). Append-only stream -> INSERT, with
ON CONFLICT (id) DO NOTHING so a replayed event is a harmless no-op. A
background pruner (see main.py) deletes rows older than retention_hours.

`severity` is not a CloudEvents context attribute; producers put it inside
`data`. We extract it best-effort so the UI can filter the feed without
parsing `data` itself.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import origin_provenance, parse_timestamp

TABLE = "tactical_events"


# Keys, in priority order, that a producer might use to express "how bad is
# this" inside `data`. cm-service config alerts use `current_status`;
# logistics alerts use `overall_severity`. Confirmed against the running
# stack in Phase 4a.
_SEVERITY_KEYS = (
    "severity",
    "current_status",
    "overall_severity",
    "overall_status",
)


def _extract_severity(envelope: dict[str, Any]) -> str | None:
    """Best-effort severity from the CloudEvent. Producers vary: some put it
    as a top-level extension attribute, most nest it in `data`."""
    if envelope.get("severity"):
        return str(envelope["severity"])
    data = envelope.get("data")
    if isinstance(data, dict):
        for key in _SEVERITY_KEYS:
            if data.get(key):
                return str(data[key])
    return None


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    event_id = decoded.get("id")
    if not event_id:
        # decode_cloudevent already enforces id/source/type, but guard anyway.
        return None

    row = {
        "id": event_id,
        **origin_provenance(),
        "source": decoded.get("source", ""),
        "type": decoded.get("type", ""),
        # `subject` is optional in CloudEvents; the OpenDDIL convention is
        # subject = asset_id. Fall back to the Kafka key if a producer omits it.
        "subject": decoded.get("subject") or key or "",
        "severity": _extract_severity(decoded),
        "time": parse_timestamp(decoded.get("time")),
        "data": decoded.get("data", {}),
    }
    # ingested_at left to the DB default (now()) — append rows never update.

    return Write(
        table=TABLE,
        mode="append",
        key_columns=["id"],
        row=row,
        jsonb_columns={"data"},
    )
