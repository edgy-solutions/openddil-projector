"""CloudEvents JSON decoding for the `tactical-events` topic.

tactical-events carries JSON CloudEvents (structured content mode), not
protobuf — see faust-edge / cm-service which emit them. We parse the
envelope into a flat dict the `tactical_events` handler maps onto columns.

Schema-drift tolerance: a message that is not valid JSON, or is missing the
CloudEvents required attributes (`id`, `source`, `type`), raises
`DecodeError` and is logged-and-skipped.
"""
from __future__ import annotations

import json
from typing import Any

from .proto import DecodeError

# CloudEvents 1.0 spec-required context attributes.
_REQUIRED = ("id", "source", "type")


def decode_cloudevent(raw: bytes) -> dict[str, Any]:
    """Parse a JSON CloudEvent into a dict.

    Returns the full envelope as a dict. The handler picks out the columns
    it needs (`id`, `source`, `type`, `subject`, `time`, `data`, plus a
    best-effort `severity` extracted from `data` if present).
    """
    if raw is None:
        raise DecodeError("message value is None (tombstone?)")
    try:
        envelope = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DecodeError(f"not valid JSON: {exc}") from exc

    if not isinstance(envelope, dict):
        raise DecodeError(
            f"CloudEvent envelope is {type(envelope).__name__}, expected object"
        )

    missing = [attr for attr in _REQUIRED if not envelope.get(attr)]
    if missing:
        raise DecodeError(
            f"CloudEvent missing required attribute(s): {', '.join(missing)}"
        )

    return envelope
