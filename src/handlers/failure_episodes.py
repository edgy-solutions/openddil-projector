"""Handler: failure-episodes -> failure_episodes (ADR-0051 first slice).

Pure handler, `handle(key, decoded) -> Write | None`. Upsert keyed by
(episode_id, detected_at): a replay or a second backfill of the same episode
writes no second row. Never pruned (config.NEVER_PRUNED_TABLES).

Wire shape (JSON): `episode_id`, `detected_at_ns`, `asset_id`, `component`,
`fault_code`, `cleared_at_ns`, `cleared_by`, `resolution`, `sources` [..],
`baseline_id`, `mod_compliance` [..], `originator_nation`, `releasable_to`
[..] ([] = released to no other nation), `edge_id`, `region_id`.
Kafka key = asset_id.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from persistence import Write

from .base import now_utc, refuse_row

TABLE = "failure_episodes"
LABEL = "failure_episodes"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ns_to_dt(value: Any) -> datetime | None:
    """Integer ns since the epoch -> UTC datetime, microsecond precision
    (integer arithmetic, truncating: same record, same key). None if unset."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return _EPOCH + timedelta(microseconds=value // 1000)


def _nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    episode_id = decoded.get("episode_id")
    if not _nonempty_str(episode_id):
        refuse_row(LABEL, "missing_episode_id", f"episode_id={episode_id!r}")
        return None
    detected_at = _ns_to_dt(decoded.get("detected_at_ns"))
    if detected_at is None:
        refuse_row(LABEL, "missing_detected_at",
                   f"episode_id={episode_id!r}")
        return None
    cleared_at = _ns_to_dt(decoded.get("cleared_at_ns"))
    if cleared_at is None:
        refuse_row(LABEL, "missing_cleared_at", f"episode_id={episode_id!r}")
        return None
    if not _nonempty_str(decoded.get("asset_id")):
        refuse_row(LABEL, "missing_asset_id", f"episode_id={episode_id!r}")
        return None
    if not _nonempty_str(decoded.get("originator_nation")):
        refuse_row(LABEL, "unlabelled", f"episode_id={episode_id!r}")
        return None
    releasable = decoded.get("releasable_to")
    if releasable is None:
        releasable = []
    if not isinstance(releasable, list):
        refuse_row(LABEL, "bad_releasable_to", f"releasable_to={releasable!r}")
        return None
    row: dict[str, Any] = {
        "episode_id": episode_id,
        "detected_at": detected_at,
        "asset_id": decoded["asset_id"],
        "component": decoded.get("component") or "",
        "fault_code": decoded.get("fault_code") or "",
        "cleared_at": cleared_at,
        "cleared_by": decoded.get("cleared_by") or "",
        "resolution": decoded.get("resolution") or "",
        "sources": decoded.get("sources") or [],
        "baseline_id": decoded.get("baseline_id") or "",
        "mod_compliance": decoded.get("mod_compliance") or [],
        "originator_nation": decoded["originator_nation"],
        "releasable_to": [str(r) for r in releasable],
        "edge_id": decoded.get("edge_id") or "",
        "region_id": decoded.get("region_id") or None,
        "updated_at": now_utc(),
    }
    return Write(table=TABLE, mode="upsert",
                 key_columns=["episode_id", "detected_at"], row=row,
                 jsonb_columns={"sources", "mod_compliance"})
