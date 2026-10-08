"""Wire codec for the link heartbeat (openddil.link.v1.LinkHeartbeat).

Protobuf, like the other contract messages this projector consumes: the
generated classes arrive on PYTHONPATH from openddil-contracts. Traffic is
carried here as the short name ("ACTIVE" / "IDLE" / "UNSPECIFIED") and
mapped to the LINK_TRAFFIC_* enum only at the wire boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from google.protobuf.message import DecodeError as _PbDecodeError
from openddil.link.v1 import link_heartbeat_pb2 as pb

TRAFFIC_NAMES = ("UNSPECIFIED", "ACTIVE", "IDLE")


class MalformedHeartbeat(Exception):
    """The payload is not a usable LinkHeartbeat. Callers count and skip."""


@dataclass(frozen=True)
class Heartbeat:
    link_id: str
    emitted_at: datetime
    traffic: str
    bridge_lag: int


def encode_heartbeat(link_id: str, emitted_at: datetime, traffic: str, bridge_lag: int) -> bytes:
    msg = pb.LinkHeartbeat(
        link_id=link_id,
        traffic=pb.LinkTraffic.Value("LINK_TRAFFIC_" + traffic),
        bridge_lag=int(bridge_lag),
    )
    msg.emitted_at.FromDatetime(emitted_at.astimezone(timezone.utc))
    return msg.SerializeToString()


def decode_heartbeat(raw: bytes) -> Heartbeat:
    msg = pb.LinkHeartbeat()
    try:
        msg.ParseFromString(raw)
    except _PbDecodeError as exc:
        raise MalformedHeartbeat(f"not a LinkHeartbeat: {exc}") from exc
    if not msg.link_id:
        raise MalformedHeartbeat("empty link_id")
    name = pb.LinkTraffic.Name(msg.traffic) if msg.traffic in pb.LinkTraffic.values() else ""
    traffic = name.removeprefix("LINK_TRAFFIC_") or "UNSPECIFIED"
    return Heartbeat(
        link_id=msg.link_id,
        emitted_at=msg.emitted_at.ToDatetime(tzinfo=timezone.utc),
        traffic=traffic,
        bridge_lag=msg.bridge_lag,
    )
