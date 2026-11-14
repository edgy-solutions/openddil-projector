"""Handler: failure-episodes -> failure_episodes (ADR-0051).

mode "custom": `async handle(key, decoded, pool) -> None`. FIRST WRITE WINS,
keyed by (episode_id, detected_at): a closed episode is history. The same key
with the same body is a `duplicate`; the same key with a different body is a
counted, logged `conflict` and the stored row never changes. The body hash is
`body_sha256(decoded)`, over the wire record. Never pruned
(config.NEVER_PRUNED_TABLES).

Wire shape (JSON): `episode_id`, `detected_at_ns`, `asset_id`, `component`,
`fault_code`, `cleared_at_ns`, `cleared_by`, `resolution`, `sources` [..],
`baseline_id`, `mod_compliance` [..], `originator_nation`, `releasable_to`
[..] ([] = released to no other nation), `edge_id`, `region_id`.
Kafka key = asset_id.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from metrics import FAILURE_EPISODE_WRITES
from persistence import PostgresPool

from .base import _REFUSE_WARN_WINDOW_S, now_utc, refuse_row

log = logging.getLogger("projector.handlers.failure_episodes")
_conflict_last_warn: dict[str, float] = {}

TABLE = "failure_episodes"
LABEL = "failure_episodes"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def body_sha256(decoded: Any) -> str:
    """sha256 hex of the canonical JSON of the wire record."""
    return hashlib.sha256(json.dumps(
        decoded, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")).hexdigest()


def _ns_to_dt(value: Any) -> datetime | None:
    """Integer ns since the epoch -> UTC datetime, microsecond precision
    (integer arithmetic, truncating: same record, same key). None if unset."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return _EPOCH + timedelta(microseconds=value // 1000)


def _nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _note_conflict(episode_id: str, stored: str, new: str) -> None:
    now = time.monotonic()
    last = _conflict_last_warn.get(episode_id)
    if last is None or (now - last) >= _REFUSE_WARN_WINDOW_S:
        log.warning("CONFLICT: failure episode %s already stored with a "
                    "different body (stored %.12s.. new %.12s..); stored row "
                    "untouched", episode_id, stored, new)
        _conflict_last_warn[episode_id] = now


async def handle(key: str, decoded: dict[str, Any], pool: PostgresPool) -> None:
    episode_id = decoded.get("episode_id")
    if not _nonempty_str(episode_id):
        refuse_row(LABEL, "missing_episode_id", f"episode_id={episode_id!r}")
        return
    detected_at = _ns_to_dt(decoded.get("detected_at_ns"))
    if detected_at is None:
        refuse_row(LABEL, "missing_detected_at",
                   f"episode_id={episode_id!r}")
        return
    cleared_at = _ns_to_dt(decoded.get("cleared_at_ns"))
    if cleared_at is None:
        refuse_row(LABEL, "missing_cleared_at", f"episode_id={episode_id!r}")
        return
    if not _nonempty_str(decoded.get("asset_id")):
        refuse_row(LABEL, "missing_asset_id", f"episode_id={episode_id!r}")
        return
    if not _nonempty_str(decoded.get("originator_nation")):
        refuse_row(LABEL, "unlabelled", f"episode_id={episode_id!r}")
        return
    releasable = decoded.get("releasable_to")
    if releasable is None:
        refuse_row(LABEL, "missing_releasable_to", f"episode_id={episode_id!r}")
        return
    if not isinstance(releasable, list):
        refuse_row(LABEL, "bad_releasable_to", f"releasable_to={releasable!r}")
        return
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
        "body_sha256": body_sha256(decoded),
        "updated_at": now_utc(),
    }
    outcome = await pool.insert_failure_episode(row)
    FAILURE_EPISODE_WRITES.labels(outcome=outcome).inc()
    if outcome == "conflict":
        _note_conflict(episode_id, getattr(outcome, "stored_sha256", ""),
                       row["body_sha256"])
