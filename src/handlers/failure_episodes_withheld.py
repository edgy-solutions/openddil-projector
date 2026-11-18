"""Handler: failure-episodes-withheld -> failure_episodes_withheld (ADR-0051).

mode "custom": `async handle(key, decoded, pool) -> None`. cm-service keeps a
failure-episode record it refused to emit (or backfill) on this topic with its
reason, instead of dropping it. Wire shape: `stage`, `reason`, `asset_id`,
`discrepancy_id`, `detected_at_ns`, `labels` {originator_nation,
releasable_to}, `tier` {edge_id, region_id}, `episode` {..the record..}.

One row per (body_sha256, stage, reason); a replay bumps times_seen. The body
is the whole decoded record. A record with no stage or no reason is still
kept, as stage `projector` / reason `malformed_withheld`. The table is never
pruned, operator-only, and not published to Electric.
"""
from __future__ import annotations

from typing import Any

from persistence import PostgresPool

from .failure_episodes import _nonempty_str, _ns_to_dt, withhold


async def handle(key: str, decoded: dict[str, Any], pool: PostgresPool) -> None:
    stage, reason = decoded.get("stage"), decoded.get("reason")
    if not (_nonempty_str(stage) and _nonempty_str(reason)):
        stage, reason = "projector", "malformed_withheld"
    tier = decoded.get("tier")
    if not isinstance(tier, dict):
        tier = {}
    await withhold(
        pool, decoded, stage, reason,
        asset_id=decoded.get("asset_id"),
        episode_id=decoded.get("discrepancy_id"),
        detected_at=_ns_to_dt(decoded.get("detected_at_ns")),
        edge_id=tier.get("edge_id"), region_id=tier.get("region_id"))
