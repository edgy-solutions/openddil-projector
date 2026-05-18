"""Handler: region-fleet-summary -> region_fleet_summary.

Decodes openddil.regional.v1.RegionFleetSummary. Compacted topic, keyed
by region_id -> UPSERT. ADR-0023 Phase 6b §B.

The wire message carries the severity buckets directly (nominal,
degraded, critical, non_operational + asset_count). faust-regional's
aggregator has already done the work to derive each asset's bucket
(WORST-of logistics-severity + cm-state-derived-severity) and to count
them; this handler just persists the row.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import now_utc, parse_timestamp

TABLE = "region_fleet_summary"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    region_id = decoded.get("region_id") or key
    if not region_id:
        return None

    row = {
        "region_id": region_id,
        "nominal":         int(decoded.get("nominal") or 0),
        "degraded":        int(decoded.get("degraded") or 0),
        "critical":        int(decoded.get("critical") or 0),
        "non_operational": int(decoded.get("non_operational") or 0),
        "asset_count":     int(decoded.get("asset_count") or 0),
        "observed_at":     parse_timestamp(decoded.get("observed_at")),
        "updated_at":      now_utc(),
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["region_id"],
        row=row,
    )
