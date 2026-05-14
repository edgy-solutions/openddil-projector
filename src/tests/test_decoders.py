"""Unit tests for decoders.

CloudEvents decoding is pure JSON and always tested. Proto decoding needs
the openddil-contracts gen/python stubs on PYTHONPATH; those tests skip
cleanly if the stubs aren't importable (e.g. a bare checkout with no
contracts sibling).
"""
from __future__ import annotations

import json

import pytest

from decoders import DecodeError, decode_cloudevent, decode_json
from decoders.proto import ProtoDecoder


# -- CloudEvents --------------------------------------------------------------

def test_cloudevent_decodes_minimal_valid():
    raw = json.dumps({
        "id": "ce-1", "source": "openddil-cm-service",
        "type": "openddil.configuration.discrepancy.detected",
        "subject": "USA-ARMY-1HBCT-M1A2-4773",
        "time": "2026-05-14T03:00:00Z",
        "data": {"severity": "CRITICAL"},
    }).encode("utf-8")
    out = decode_cloudevent(raw)
    assert out["id"] == "ce-1"
    assert out["data"]["severity"] == "CRITICAL"


def test_cloudevent_rejects_non_json():
    with pytest.raises(DecodeError, match="not valid JSON"):
        decode_cloudevent(b"\x00\x01 not json")


def test_cloudevent_rejects_missing_required_attrs():
    raw = json.dumps({"id": "ce-1", "source": "cm"}).encode("utf-8")  # no type
    with pytest.raises(DecodeError, match="missing required attribute"):
        decode_cloudevent(raw)


def test_cloudevent_rejects_non_object():
    with pytest.raises(DecodeError, match="expected object"):
        decode_cloudevent(b'["not", "an", "object"]')


def test_cloudevent_rejects_none():
    with pytest.raises(DecodeError, match="None"):
        decode_cloudevent(None)


# -- plain JSON ---------------------------------------------------------------

def test_decode_json_parses_object():
    raw = json.dumps({"asset_id": "dis:1:1:4773", "overall_status": 4}).encode()
    out = decode_json(raw)
    assert out["asset_id"] == "dis:1:1:4773"
    assert out["overall_status"] == 4


def test_decode_json_rejects_non_json():
    with pytest.raises(DecodeError, match="not valid JSON"):
        decode_json(b"\x08\x01\x12 protobuf-looking bytes")


def test_decode_json_rejects_non_object():
    with pytest.raises(DecodeError, match="expected object"):
        decode_json(b"[1, 2, 3]")


def test_decode_json_rejects_none():
    with pytest.raises(DecodeError, match="None"):
        decode_json(None)


# -- Proto --------------------------------------------------------------------

def test_proto_decoder_unknown_message_name_raises():
    with pytest.raises(DecodeError, match="no proto module registered"):
        ProtoDecoder("openddil.bogus.v1.NotAThing")


def _contracts_available() -> bool:
    try:
        ProtoDecoder("openddil.configuration.v1.AsMaintainedConfiguration")
        return True
    except DecodeError:
        return False


@pytest.mark.skipif(
    not _contracts_available(),
    reason="openddil-contracts gen/python not on PYTHONPATH",
)
def test_proto_decoder_rejects_garbage_bytes():
    dec = ProtoDecoder("openddil.configuration.v1.AsMaintainedConfiguration")
    # Random bytes that are not valid protobuf for this message.
    with pytest.raises(DecodeError):
        dec.decode(b"\xff\xfe\xfd\xfc this is not protobuf at all \x00\x01")


@pytest.mark.skipif(
    not _contracts_available(),
    reason="openddil-contracts gen/python not on PYTHONPATH",
)
def test_proto_decoder_roundtrips_a_real_message():
    from openddil.configuration.v1 import as_maintained_pb2 as pb

    msg = pb.AsMaintainedConfiguration(
        asset_id="USA-ARMY-1HBCT-M1A2-4773",
        baseline_id="M1A2-SEPv3-Baseline-2024.2",
    )
    dec = ProtoDecoder("openddil.configuration.v1.AsMaintainedConfiguration")
    out = dec.decode(msg.SerializeToString())
    assert out["asset_id"] == "USA-ARMY-1HBCT-M1A2-4773"
    assert out["baseline_id"] == "M1A2-SEPv3-Baseline-2024.2"
