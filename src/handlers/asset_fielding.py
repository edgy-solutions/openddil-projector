"""Handler: asset-fielding -> asset_fielding (ADR-0050 section 4).

Pure upsert keyed by serial: one expected-arrival row per serial.

Wire shape (JSON): the Contract B fielding record as built by the logistics
system of record's adapter (and the logistics-sim stand-in) -- `serial`,
`platform_variant`, `unit`, `site` (the edge the item is fielded to),
`due_by_ns` and `observed_at_ns` (integer ns since the epoch), `provenance:
{originator_nation, releasable_to}`, `extraction`, and optional `extras`.
There is no `asset_id` or `sustainment_id`. `site` is stored in `fielded_to`,
`provenance.originator_nation` in `originator_nation` (the name the PEP
predicate reads), `due_by_ns` in `due_by` and `observed_at_ns` in
`recorded_at`. `extraction` and `extras` are on the wire but not stored.
`region_id` is not on the wire and stays None.

The old flat shape (`owning_nation`, `fielded_to`, RFC3339 `due_by`) is no
longer read: a record in it is refused as `missing_field`.

Not pruned: the table has no asset_ttl_hours entry.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .asset_declaration import none_if_unset
from .base import now_utc, parse_ns_timestamp, refuse_row, releasability_from

TABLE = "asset_fielding"
LABEL = "asset_fielding"
_REQUIRED = ("serial", "platform_variant", "unit", "site")


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
    prov = decoded.get("provenance")
    if not isinstance(prov, dict):
        prov = {}
    nation = prov.get("originator_nation")
    if not (isinstance(nation, str) and nation.strip()):
        refuse_row(LABEL, "missing_field",
                   f"provenance.originator_nation={nation!r}")
        return None
    releasable = prov.get("releasable_to")
    if releasable is not None and not isinstance(releasable, list):
        refuse_row(LABEL, "bad_releasable_to", f"releasable_to={releasable!r}")
        return None
    due_by = parse_ns_timestamp(decoded.get("due_by_ns"))
    recorded_at = parse_ns_timestamp(decoded.get("observed_at_ns"))
    if due_by is None or recorded_at is None:
        refuse_row(LABEL, "bad_timestamp",
                   f"due_by_ns={decoded.get('due_by_ns')!r} "
                   f"observed_at_ns={decoded.get('observed_at_ns')!r}")
        return None
    row: dict[str, Any] = {
        "serial": serial,
        "platform_variant": decoded["platform_variant"],
        "unit": decoded["unit"],
        "fielded_to": decoded["site"],
        "region_id": None,
        "due_by": due_by,
        "recorded_at": recorded_at,
        "updated_at": now_utc(),
    }
    row.update(releasability_from(prov))
    row["releasable_to"] = [str(r) for r in row["releasable_to"]]
    return Write(table=TABLE, mode="upsert",
                 key_columns=["serial"], row=row)
