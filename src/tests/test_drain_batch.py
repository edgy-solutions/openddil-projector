"""_drain_batch must keep every message it polls, including on a full batch."""
from __future__ import annotations

from collections import deque

import pytest

import main


class FakeConsumer:
    def __init__(self, n: int) -> None:
        self.q = deque(range(n))
        self.polls = 0

    def poll(self, timeout):
        self.polls += 1
        return self.q.popleft() if self.q else None


class Holder:
    def __init__(self, consumer) -> None:
        self._consumer = consumer


def _new_drain(consumer):
    h = Holder(consumer)
    return lambda: main.ConsumerWorker._drain_batch(h)


def _old_drain(consumer):
    batch = []
    msg = consumer.poll(main.BATCH_MAX_WAIT_MS / 1000.0)
    while msg is not None and len(batch) < main.BATCH_MAX_MESSAGES:
        batch.append(msg)
        msg = consumer.poll(0)
    return batch


def _run(drain, consumer):
    out, drains = [], 0
    while True:
        b = drain()
        drains += 1
        if not b:
            return out, drains, []
        out.append(b)


@pytest.mark.parametrize("n", [0, 1, 499, 500, 501, 1001, 1500])
def test_every_message_kept_in_order(monkeypatch, n):
    monkeypatch.setattr(main, "BATCH_MAX_MESSAGES", 500)
    c = FakeConsumer(n)
    batches, drains, _ = _run(_new_drain(c), c)
    assert [m for b in batches for m in b] == list(range(n))
    assert all(len(b) <= 500 for b in batches)
    assert c.polls <= n + drains


def test_old_body_loses_501st_new_loses_none(monkeypatch):
    monkeypatch.setattr(main, "BATCH_MAX_MESSAGES", 500)
    n = 1001
    old = FakeConsumer(n)
    got, _, _ = _run(lambda: _old_drain(old), old)
    seen = {m for b in got for m in b}
    lost_old = set(range(n)) - seen
    assert lost_old and 500 in lost_old  # index 500 is the 501st message
    new = FakeConsumer(n)
    got, _, _ = _run(_new_drain(new), new)
    assert set(range(n)) - {m for b in got for m in b} == set()
    print("old loss on 1001:", len(lost_old))
