"""Handler: region-wear-trends -> region_wear_trends.

Decodes openddil.regional.v1.RegionWearTrends. Compacted topic, keyed by
region_id -> UPSERT. ADR-0023 Phase 6b §B.

The `components` JSONB column receives the wire-message's repeated
ComponentWearTrend field as-is — entries are keyed by (component_id,
unit) per the mixed-unit handling rule (Q3): the SAME component_id can
appear MULTIPLE times in the array with different units. The aggregator
refuses to mean across units; the UI must render each (component_id,
unit) row distinctly.

ASYMMETRIC COVERAGE per §B greenlight: faust-regional's aggregator
sources from derived-sustainment ONLY in §B. asset-telemetry-windows is
wired in the fan-in envelope but does NOT drive emissions (DEBUG no-op
in aggregator dispatcher) until follow-up #11's sustainment-data test
fixtures land. Rows in this table are derived-only for §B.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import now_utc, parse_timestamp

TABLE = "region_wear_trends"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    region_id = decoded.get("region_id") or key
    if not region_id:
        return None

    components = decoded.get("components") or []

    row = {
        "region_id":   region_id,
        "components":  components,
        "observed_at": parse_timestamp(decoded.get("observed_at")),
        "updated_at":  now_utc(),
    }

    return Write(
        table=TABLE,
        mode="upsert",
        key_columns=["region_id"],
        row=row,
        jsonb_columns={"components"},
    )
