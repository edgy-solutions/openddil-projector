"""Handler interface + shared field-mapping helpers.

A handler is a pure function: it takes the Kafka message key and the decoded
message dict, and returns a `Write` (or None to skip). No I/O, no Kafka, no
Postgres — that makes every handler unit-testable in isolation.

The helpers here cover the recurring proto-JSON quirks:
  - Timestamps serialise as RFC3339 strings; Postgres timestamptz wants
    datetime objects.
  - Durations serialise as "3.5s" strings; we store whole seconds.
  - Enums serialise as their string names (use_integers_for_enums=False),
    which is exactly what the text columns want — stored as-is.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from persistence import Write

# A handler maps (key, decoded) -> Write or None.
Handler = Callable[[str, dict[str, Any]], "Write | None"]


def now_utc() -> datetime:
    """Current UTC time — handlers stamp `updated_at`/`ingested_at` with this
    so the UPSERT refreshes it on every write (a DB default only fires on
    INSERT, not on ON CONFLICT DO UPDATE)."""
    return datetime.now(timezone.utc)


def parse_timestamp(value: Any) -> datetime | None:
    """RFC3339 string (proto JSON Timestamp) -> aware datetime, or None.

    Tolerates already-parsed datetimes and the trailing 'Z'.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_ns_timestamp(value: Any) -> datetime | None:
    """Integer nanoseconds-since-epoch -> aware datetime, or None.

    cm-service serialises proto Timestamps as `*_ns` integers in its JSON
    payloads (not RFC3339 strings). 0 is treated as 'unset' -> None.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        ns = int(value)
    except (TypeError, ValueError):
        return None
    if ns <= 0:
        return None
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)


def duration_to_seconds(value: Any) -> int | None:
    """Proto JSON Duration ("3.5s", "120s") -> whole seconds, or None."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        return None
    text = value[:-1] if value.endswith("s") else value
    try:
        return int(float(text))
    except ValueError:
        return None


def first_present(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Return d[k] for the first key present and truthy-or-zero, else default.

    Proto JSON omits unset scalar fields entirely, so a missing key is normal.
    """
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default
