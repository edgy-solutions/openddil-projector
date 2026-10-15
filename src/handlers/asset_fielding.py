"""Handler: asset-fielding -> asset_fielding (ADR-0050 section 4).

Pure upsert keyed by serial: one expected-arrival row per serial.

Wire shape (JSON): `serial`, `platform_variant`, `unit`, `owning_nation`,
`releasable_to` [..], `fielded_to` (edge id), `region_id` (or null),
`due_by` and `recorded_at` (RFC3339). `owning_nation` is stored in the
`originator_nation` column, the name the PEP predicate reads.

Not pruned: the table has no asset_ttl_hours entry.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .asset_declaration import none_if_unset
from .base import now_utc, parse_timestamp, refuse_row

TABLE = "asset_fielding"
LABEL = "asset_fielding"
_REQUIRED = ("serial", "platform_variant", "unit", "owning_nation",
             "fielded_to")


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    for col in _REQUIRED:
        v = decoded.get(col)
        if not (isinstance(v, str) and v.strip()):
            refuse_row(LABEL, "missing_field", f"{col}={v!r}")
            return None
    serial = none_if_unset(decoded["serial"])
    if serial is None:
        refuse_row(LABEL, "missing_field", f"serial={decoded['serial']!r}")
        return None
    due_by = parse_timestamp(decoded.get("due_by"))
    recorded_at = parse_timestamp(decoded.get("recorded_at"))
    if due_by is None or recorded_at is None:
        refuse_row(LABEL, "bad_timestamp",
                   f"due_by={decoded.get('due_by')!r} "
                   f"recorded_at={decoded.get('recorded_at')!r}")
        return None
    releasable = decoded.get("releasable_to") or []
    if not isinstance(releasable, list):
        refuse_row(LABEL, "bad_releasable_to", f"releasable_to={releasable!r}")
        return None
    row: dict[str, Any] = {
        "serial": serial,
        "platform_variant": decoded["platform_variant"],
        "unit": decoded["unit"],
        "originator_nation": decoded["owning_nation"],
        "releasable_to": [str(r) for r in releasable],
        "fielded_to": decoded["fielded_to"],
        "region_id": none_if_unset(decoded.get("region_id")),
        "due_by": due_by,
        "recorded_at": recorded_at,
        "updated_at": now_utc(),
    }
    return Write(table=TABLE, mode="upsert",
                 key_columns=["serial"], row=row)
