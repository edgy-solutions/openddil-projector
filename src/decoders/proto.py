"""Protobuf decoding for the projector.

`ProtoDecoder` resolves a fully-qualified message name (e.g.
`openddil.configuration.v1.AsMaintainedConfiguration`) to its generated
Python class and parses raw Kafka message bytes into a plain dict via
`MessageToDict`.

Schema-drift tolerance: protobuf's wire format already ignores unknown
fields on parse, so a producer that adds a field never breaks us. What we
guard here is the harder failure — bytes that are not valid protobuf at all
(wrong topic routing, a JSON message on a proto topic, truncation). Those
raise `DecodeError`, which the consumer logs-and-skips.
"""
from __future__ import annotations

import importlib
from typing import Any

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError as _PbDecodeError


class DecodeError(Exception):
    """Raised when a message cannot be decoded. Caller logs-and-skips."""


# Map a fully-qualified proto message name to (module_path, class_name).
# The module path follows the openddil-contracts gen/python layout:
#   openddil.configuration.v1.AsMaintainedConfiguration
#     -> openddil/configuration/v1/as_maintained_pb2.py : AsMaintainedConfiguration
# The _pb2 filename is not derivable from the message name (it is the .proto
# file stem, not the message), so the mapping is explicit.
_MESSAGE_MODULES: dict[str, tuple[str, str]] = {
    "openddil.configuration.v1.AsMaintainedConfiguration": (
        "openddil.configuration.v1.as_maintained_pb2",
        "AsMaintainedConfiguration",
    ),
    "openddil.logistics.v1.AssetLogisticsStatusUpdate": (
        "openddil.logistics.v1.logistics_status_pb2",
        "AssetLogisticsStatusUpdate",
    ),
    "openddil.telemetry.v1.EntityTelemetryEvent": (
        "openddil.telemetry.v1.telemetry_pb2",
        "EntityTelemetryEvent",
    ),
    "openddil.logistics.v1.WindowedTelemetry": (
        "openddil.logistics.v1.windowed_telemetry_pb2",
        "WindowedTelemetry",
    ),
    # ADR-0023 Phase 6b §B — regional rollups produced by faust-regional.
    "openddil.regional.v1.RegionFleetSummary": (
        "openddil.regional.v1.region_fleet_summary_pb2",
        "RegionFleetSummary",
    ),
    "openddil.regional.v1.RegionTopFactors": (
        "openddil.regional.v1.region_top_factors_pb2",
        "RegionTopFactors",
    ),
    "openddil.regional.v1.RegionWearTrends": (
        "openddil.regional.v1.region_wear_trends_pb2",
        "RegionWearTrends",
    ),
}


class ProtoDecoder:
    """Decodes one proto message type. Construct once per topic mapping."""

    def __init__(self, message_name: str) -> None:
        if message_name not in _MESSAGE_MODULES:
            raise DecodeError(
                f"no proto module registered for '{message_name}' — "
                f"add it to _MESSAGE_MODULES in decoders/proto.py"
            )
        module_path, class_name = _MESSAGE_MODULES[message_name]
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:  # pragma: no cover - import-time wiring
            raise DecodeError(
                f"cannot import '{module_path}' — is openddil-contracts "
                f"gen/python on PYTHONPATH? ({exc})"
            ) from exc
        self.message_name = message_name
        self._message_cls = getattr(module, class_name)

    def decode(self, raw: bytes) -> dict[str, Any]:
        """Parse raw bytes into a dict. Raises DecodeError on bad bytes."""
        if raw is None:
            raise DecodeError("message value is None (tombstone?)")
        msg = self._message_cls()
        try:
            msg.ParseFromString(raw)
        except _PbDecodeError as exc:
            raise DecodeError(f"not valid {self.message_name}: {exc}") from exc
        # preserving_proto_field_name keeps snake_case keys so handlers map
        # cleanly onto snake_case Postgres columns. including_default_value_
        # fields is intentionally OFF — absent fields stay absent so the
        # handler can distinguish "unset" from "zero".
        return MessageToDict(
            msg,
            preserving_proto_field_name=True,
            use_integers_for_enums=False,
        )
