"""Handler: region-top-factors -> region_top_factors.

Decodes openddil.regional.v1.RegionTopFactors. Compacted topic, keyed by
region_id -> UPSERT. ADR-0023 Phase 6b §B.

The `factors` JSONB column receives the wire-message's repeated
FactorCount field as-is — a sorted-DESC-by-count array of
{factor_id, count, severity_breakdown} entries. The UI consumes the
JSON directly to render the stacked-bar per factor; the projector does
no per-entry normalization.

Empty `factors` arrays do NOT land — faust-regional's aggregator skips
emission entirely under cold start (no constraining factors seen yet),
and on a steady-state region with zero factors above OK the array would
be empty. Either way, no row written until there's something to show.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import now_utc, parse_timestamp

TABLE = "region_top_factors"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    region_id = decoded.get("region_id") or key
    if not region_id:
        return None

    factors = decoded.get("factors") or []

    row = {
        "region_id":   region_id,
        "factors":     factors,
        "observed_at": parse_timestamp(decoded.get("observed_at")),
        "updated_at":  now_utc(),
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["region_id"],
        row=row,
        jsonb_columns={"factors"},
    )
