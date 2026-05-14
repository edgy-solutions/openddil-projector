"""Plain-JSON decoding.

Used for topics whose producer emits `json.dumps(...)` rather than protobuf
or a CloudEvents envelope. `asset-cm-state` is the case discovered in Phase
4a: cm-service serialises AsMaintainedConfiguration as JSON (asset_cm.py),
with integer enum values and `*_ns` integer timestamps — the handler owns
the mapping back to readable column values.

Schema-drift tolerance: non-JSON or a non-object payload raises DecodeError,
which the consumer logs-and-skips.
"""
from __future__ import annotations

import json
from typing import Any

from .proto import DecodeError


def decode_json(raw: bytes) -> dict[str, Any]:
    """Parse a raw JSON object into a dict."""
    if raw is None:
        raise DecodeError("message value is None (tombstone?)")
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DecodeError(f"not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise DecodeError(
            f"JSON payload is {type(obj).__name__}, expected object"
        )
    return obj
