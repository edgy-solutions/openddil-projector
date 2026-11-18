"""main._persist for a record-labelled band row the tier cannot label.

The pool answers with a refusal reason; the worker counts it under
openddil_band_rows_refused_total{type, reason} and does not count a UPSERT.
"""
from __future__ import annotations

import asyncio

import pytest

import main
from config import Mapping
from handlers import get_handler
from metrics import BAND_ROWS_REFUSED, UPSERTS


class _Msg:
    def __init__(self, value):
        self._value = value

    def value(self):
        return self._value

    def key(self):
        return b"asset-a"


class _Pool:
    def __init__(self, answer):
        self.answer = answer
        self.writes = []

    async def execute(self, write):
        self.writes.append(write)
        return self.answer


def _worker(pool):
    worker = main.ConsumerWorker.__new__(main.ConsumerWorker)
    worker.mapping = Mapping(
        topic="tactical-events", handler="tactical_events",
        table="tactical_events", consumer_group="g",
        decode_as="cloudevents.json", mode="append")
    worker._pool = pool
    worker._handler = get_handler("tactical_events")
    worker._decode = lambda raw: raw
    return worker


def _event(event_type):
    return {"id": f"ce-{event_type}", "source": "/openddil/faust-edge",
            "type": event_type, "subject": "asset-a",
            "time": "2026-11-07T03:00:00Z", "data": {}}


def _value(counter, **labels):
    return counter.labels(**labels)._value.get()


@pytest.mark.parametrize("event_type", ["openddil.element.band",
                                        "openddil.element.bandcount"])
@pytest.mark.parametrize("reason", ["no_label_record", "label_record_unlabelled"])
def test_refusal_is_counted_with_type_and_reason_and_is_not_an_upsert(event_type, reason):
    pool = _Pool(reason)
    refused_before = _value(BAND_ROWS_REFUSED, type=event_type, reason=reason)
    upserts_before = _value(UPSERTS, table="tactical_events")

    asyncio.run(_worker(pool)._persist(_Msg(_event(event_type))))

    assert pool.writes[0].labels_from_record == "asset-a"
    assert _value(BAND_ROWS_REFUSED, type=event_type, reason=reason) == refused_before + 1
    assert _value(UPSERTS, table="tactical_events") == upserts_before


def test_an_inserted_band_row_is_an_upsert_and_not_a_refusal():
    pool = _Pool("inserted")
    upserts_before = _value(UPSERTS, table="tactical_events")
    refused_before = _value(BAND_ROWS_REFUSED, type="openddil.element.band",
                            reason="no_label_record")

    asyncio.run(_worker(pool)._persist(_Msg(_event("openddil.element.band"))))

    assert _value(UPSERTS, table="tactical_events") == upserts_before + 1
    assert _value(BAND_ROWS_REFUSED, type="openddil.element.band",
                  reason="no_label_record") == refused_before
