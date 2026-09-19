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

from .base import (parse_timestamp, refuse_row, releasability_from,
                   resolve_provenance_from_top_level)

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

    # ADR-0023 Phase 6b §A: CloudEvent producers (faust-edge anomalies,
    # cm-service config alerts) stamp edge_id/region_id into the `data`
    # block. Read from data dict with rate-limited env-default fallback.
    # AN EMPTY SUBJECT IS NOT A SUBJECT -- refuse, do not substitute.
    #
    # This line used to end `or ""`. A relay producing null-keyed messages
    # then wrote 18,562 rows with subject = "" into one region store from a
    # single burst on 2026-09-08, each with a fresh uuid so ON CONFLICT (id)
    # deduped nothing. Nobody saw them for ten days: the read path had never
    # carried real data, and the first client that loaded the region screen
    # made its PEP buffer 10 MiB and die.
    #
    # The relay bug is fixed. This refusal is what stops the NEXT one, from
    # anywhere in the chain, doing it again. A tactical event is ABOUT an
    # asset; one that cannot say which asset is not a degraded event, it is an
    # unanswerable one, and writing it with a placeholder makes the store
    # claim something the message never said.
    asset_subject = decoded.get("subject") or key or ""
    if not asset_subject:
        refuse_row("tactical_events", "empty subject",
                   f"type={decoded.get('type', '?')} id={event_id}")
        return None
    data = decoded.get("data") if isinstance(decoded.get("data"), dict) else {}
    row = {
        "id": event_id,
        **resolve_provenance_from_top_level(data, asset_subject, "tactical_events"),
        "source": decoded.get("source", ""),
        "type": decoded.get("type", ""),
        # `subject` is optional in CloudEvents; the OpenDDIL convention is
        # subject = asset_id, with the Kafka key as the fallback. Resolved and
        # VALIDATED once above -- reused here rather than recomputed, because
        # a second copy of the expression is a second copy of the rule, and
        # the refusal above would not apply to it.
        "subject": asset_subject,
        # ADR-0029 §3: CARRIED from the producer's `data`, never derived
        # here. A tactical event is about an asset and is exactly as
        # releasable as that asset — fusion reads the labels from the same
        # state that stamps asset_logistics_status, so the alert row and the
        # status row cannot disagree about who may see it.
        #
        # An event whose producer has not been taught to stamp arrives
        # unlabelled and deny-unlabeled hides it. That is the intended
        # direction: a visible alert about an asset the viewer may not see
        # is the leak, and an invisible one is a gap the completeness gate
        # counts.
        **releasability_from(data),
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
