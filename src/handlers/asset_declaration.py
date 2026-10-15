"""Handler: asset-declarations -> asset_declaration (ADR-0050 section 2).

Pure handler, `handle(key, decoded) -> Write | None`. Upsert keyed by
asset_id: one declaration row per asset, the latest record wins.

Wire shape (JSON): `asset_id`, `serial` (or null), `unit`,
`originator_nation`, `releasable_to` [..], `baseline_id` (or null),
`declared_by`, `declared_at` (RFC3339), `source` ("declare" |
"expect-confirm"), `edge_id`, `region_id` (or null), `owning_tier`.

`serial` and `baseline_id` of null / "" / "none" are stored as NULL.
Not pruned: the table has no asset_ttl_hours entry.
"""
from __future__ import annotations

from typing import Any

from persistence import Write

from .base import now_utc, parse_timestamp, refuse_row

TABLE = "asset_declaration"
LABEL = "asset_declaration"
SOURCES = ("declare", "expect-confirm")
_REQUIRED = ("asset_id", "unit", "originator_nation", "declared_by",
             "edge_id", "owning_tier")


def none_if_unset(value: Any) -> str | None:
    """null / "" / "none" (any case, padded) -> None; otherwise the string."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text.lower() == "none":
        return None
    return text


def _nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_declaration(decoded: dict[str, Any], label: str):
    """Shared by the declaration and correlation handlers. Returns
    (declared_at, releasable_to) or None after refusing the record."""
    for col in _REQUIRED:
        if not _nonempty_str(decoded.get(col)):
            refuse_row(label, "missing_field", f"{col}={decoded.get(col)!r}")
            return None
    if decoded.get("source") not in SOURCES:
        refuse_row(label, "bad_source", f"source={decoded.get('source')!r}")
        return None
    declared_at = parse_timestamp(decoded.get("declared_at"))
    if declared_at is None:
        refuse_row(label, "bad_declared_at",
                   f"declared_at={decoded.get('declared_at')!r}")
        return None
    releasable = decoded.get("releasable_to") or []
    if not isinstance(releasable, list):
        refuse_row(label, "bad_releasable_to", f"releasable_to={releasable!r}")
        return None
    return declared_at, [str(r) for r in releasable]


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    checked = validate_declaration(decoded, LABEL)
    if checked is None:
        return None
    declared_at, releasable = checked
    row: dict[str, Any] = {
        "asset_id": decoded["asset_id"],
        "serial": none_if_unset(decoded.get("serial")),
        "unit": decoded["unit"],
        "originator_nation": decoded["originator_nation"],
        "releasable_to": releasable,
        "baseline_id": none_if_unset(decoded.get("baseline_id")),
        "declared_by": decoded["declared_by"],
        "declared_at": declared_at,
        "source": decoded["source"],
        "edge_id": decoded["edge_id"],
        "region_id": none_if_unset(decoded.get("region_id")),
        "owning_tier": decoded["owning_tier"],
        "updated_at": now_utc(),
    }
    return Write(table=TABLE, mode="upsert",
                 key_columns=["asset_id"], row=row)
