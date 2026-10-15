"""Handler: asset-declarations -> asset_correlation (ADR-0050 section 4).

Same topic and record as asset_declaration, its own consumer group. Upsert
keyed by serial: the serial -> asset_id lookup the ingress consults. A record
with no serial (null / "" / "none") yields no correlation row.

Not pruned: the table has no asset_ttl_hours entry.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .asset_declaration import none_if_unset, validate_declaration
from .base import now_utc

TABLE = "asset_correlation"
LABEL = "asset_correlation"


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    serial = none_if_unset(decoded.get("serial"))
    if serial is None:
        return None
    checked = validate_declaration(decoded, LABEL)
    if checked is None:
        return None
    declared_at, releasable = checked
    row: dict[str, Any] = {
        "serial": serial,
        "asset_id": decoded["asset_id"],
        "edge_id": decoded["edge_id"],
        "region_id": none_if_unset(decoded.get("region_id")),
        "originator_nation": decoded["originator_nation"],
        "releasable_to": releasable,
        "declared_by": decoded["declared_by"],
        "declared_at": declared_at,
        "source": decoded["source"],
        "updated_at": now_utc(),
    }
    return Write(table=TABLE, mode="upsert",
                 key_columns=["serial"], row=row)
