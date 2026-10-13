"""Handler: parts-availability -> parts_availability.

Pure handler, `handle(key, decoded) -> Write | None`, like tactical_events.
Upsert keyed by (site, part_ref): one row per site per part, refreshed by each
sweep of the spare-parts stand-in.

Wire shape: JSON as built by openddil-logistics-sim
`src/logistics_sim/publisher.py` `publish_parts_availability` -- `site`,
`part_ref`, `item`, `on_hand`, `observed_at_ns`, `provenance:
{originator_nation, releasable_to}`, `extraction: {cursor, extracted_at_ns}`,
`source`, `nearest_site_with_stock`, and (only when present) `nearest_on_hand`
and `lead_time_days`.

`item` and `extraction` are on the wire but not stored: the table has no
column for either.

The upsert overwrites every non-key column with EXCLUDED, so a later sweep
that no longer declares a figure must clear the old one. The figure columns
are therefore written EXPLICITLY as None when the key is absent: unknown is not
the last-known value.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from persistence import Write

from .base import now_utc, refuse_row, releasability_from

TABLE = "parts_availability"

# Written as None when absent from the record (see module docstring).
_FIGURE_COLUMNS = ("on_hand", "lead_time_days", "nearest_site_with_stock",
                   "nearest_on_hand", "source")


def _nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def handle(key: str, decoded: dict[str, Any]) -> Write | None:
    site = decoded.get("site")
    part_ref = decoded.get("part_ref")
    if not _nonempty_str(site) or not _nonempty_str(part_ref):
        refuse_row("parts_availability", "missing_key",
                   f"site={site!r} part_ref={part_ref!r}")
        return None

    observed_ns = decoded.get("observed_at_ns")
    if isinstance(observed_ns, bool) or not isinstance(observed_ns, int):
        refuse_row("parts_availability", "no_observed_at",
                   f"{site}/{part_ref} observed_at_ns={observed_ns!r}")
        return None
    secs, rem_ns = divmod(observed_ns, 1_000_000_000)
    observed_at = datetime.fromtimestamp(secs, tz=timezone.utc).replace(
        microsecond=rem_ns // 1000)

    row: dict[str, Any] = {"site": site, "part_ref": part_ref}
    # Deliberately NOT releasability_from's omit-when-absent: an absent figure
    # is written as None so the upsert clears a stale one.
    for col in _FIGURE_COLUMNS:
        row[col] = decoded.get(col)
    row["observed_at"] = observed_at
    row["updated_at"] = now_utc()
    row.update(releasability_from(decoded.get("provenance")))

    return Write(table=TABLE, mode="upsert",
                 key_columns=["site", "part_ref"], row=row)
