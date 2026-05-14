"""Wire-format decoders.

Three kinds, selected per topic via the `decode_as` config field:
  - protobuf  — `ProtoDecoder`, resolves a fully-qualified message name to
    its generated class and parses bytes into a dict.
  - cloudevents.json — `decode_cloudevent`, parses a JSON CloudEvent.
  - json — `decode_json`, parses a plain `json.dumps(...)` payload. Used for
    asset-cm-state, which cm-service emits as JSON (not protobuf).

All are schema-drift tolerant: an unrecognized field or a parse failure is
surfaced as a `DecodeError` the caller logs-and-skips, never a crash.
"""
from .proto import DecodeError, ProtoDecoder
from .cloudevents import decode_cloudevent
from .json_raw import decode_json

__all__ = ["DecodeError", "ProtoDecoder", "decode_cloudevent", "decode_json"]
