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

A record this handler refuses (any reason) or cannot write (write_error) is
kept in failure_episodes_withheld with stage `projector`, never dropped.
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
from persistence.postgres import PostgresWriteError
from vendor.openddil_labels.lk1 import compile_lk1

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


async def _label_policy_id(pool: PostgresPool) -> str | None:
    """Asked once per process and cached on the pool. A policy change is a
    DESIGN-2026-11-06 section 1.4 backfill, not a re-read here."""
    if not getattr(pool, "_label_policy_loaded", False):
        pool._label_policy_id = await pool.fetch_label_policy_id()
        pool._label_policy_loaded = True
    return pool._label_policy_id


async def withhold(pool: PostgresPool, decoded: Any, stage: str, reason: str, *,
                   asset_id: Any = "", episode_id: Any = "",
                   detected_at: datetime | None = None,
                   edge_id: Any = "", region_id: Any = None) -> None:
    """Keep a record that could not be projected, with its reason, in
    failure_episodes_withheld, and count it. Never drops."""
    await pool.upsert_failure_episode_withheld({
        "body_sha256": body_sha256(decoded),
        "stage": stage,
        "reason": reason,
        "asset_id": asset_id if isinstance(asset_id, str) else "",
        "episode_id": episode_id if isinstance(episode_id, str) else "",
        "detected_at": detected_at,
        "body": decoded,
        "edge_id": edge_id if isinstance(edge_id, str) else "",
        "region_id": region_id if _nonempty_str(region_id) else None,
    })
    FAILURE_EPISODE_WRITES.labels(outcome="withheld").inc()


async def _refuse(pool: PostgresPool, decoded: dict[str, Any], reason: str,
                  detail: str) -> None:
    refuse_row(LABEL, reason, detail)
    await withhold(
        pool, decoded, "projector", reason,
        asset_id=decoded.get("asset_id"), episode_id=decoded.get("episode_id"),
        detected_at=_ns_to_dt(decoded.get("detected_at_ns")),
        edge_id=decoded.get("edge_id"), region_id=decoded.get("region_id"))


async def handle(key: str, decoded: dict[str, Any], pool: PostgresPool) -> None:
    episode_id = decoded.get("episode_id")
    ep = f"episode_id={episode_id!r}"
    if not _nonempty_str(episode_id):
        await _refuse(pool, decoded, "missing_episode_id", ep)
        return
    detected_at = _ns_to_dt(decoded.get("detected_at_ns"))
    if detected_at is None:
        await _refuse(pool, decoded, "missing_detected_at", ep)
        return
    cleared_at = _ns_to_dt(decoded.get("cleared_at_ns"))
    if cleared_at is None:
        await _refuse(pool, decoded, "missing_cleared_at", ep)
        return
    if not _nonempty_str(decoded.get("asset_id")):
        await _refuse(pool, decoded, "missing_asset_id", ep)
        return
    if not _nonempty_str(decoded.get("originator_nation")):
        await _refuse(pool, decoded, "unlabelled", ep)
        return
    releasable = decoded.get("releasable_to")
    if releasable is None:
        await _refuse(pool, decoded, "missing_releasable_to", ep)
        return
    if not isinstance(releasable, list):
        await _refuse(pool, decoded, "bad_releasable_to",
                      f"releasable_to={releasable!r}")
        return
    # lk1 key and body, compiled at write from the pinned openddil_labels.
    # None, None when there is no label_policy row. An unlabelled close was
    # refused above, so the originator is non-empty here.
    # A policy that will not compile, or a label that will not compile under
    # one, is withheld with its reason: never dropped, and never written with
    # NULL label columns (NULL means "no label_policy row").
    policy_id = await _label_policy_id(pool)
    try:
        compiled = compile_lk1(policy_id, decoded["originator_nation"],
                               releasable)
    except ValueError:
        await _refuse(pool, decoded, "label_policy_invalid",
                      f"{ep} policy_id={str(policy_id)[:64]!r}")
        return
    if compiled is None and isinstance(policy_id, str) and policy_id:
        await _refuse(pool, decoded, "label_invalid",
                      f"{ep} policy_id={policy_id[:64]!r}")
        return
    label_key, label = compiled if compiled is not None else (None, None)
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
        "label_key": label_key,
        "label": label,
        "edge_id": decoded.get("edge_id") or "",
        "region_id": decoded.get("region_id") or None,
        "body_sha256": body_sha256(decoded),
        "updated_at": now_utc(),
    }
    try:
        outcome = await pool.insert_failure_episode(row)
    except PostgresWriteError as exc:
        log.error("failure episode %s not written: %s", episode_id, exc)
        await withhold(pool, decoded, "projector", "write_error",
                       asset_id=decoded["asset_id"], episode_id=episode_id,
                       detected_at=detected_at, edge_id=row["edge_id"],
                       region_id=row["region_id"])
        return
    FAILURE_EPISODE_WRITES.labels(outcome=outcome).inc()
    if outcome == "conflict":
        _note_conflict(episode_id, getattr(outcome, "stored_sha256", ""),
                       row["body_sha256"])
