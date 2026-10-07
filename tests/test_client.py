"""Client-side: persistent buffer, retry/backoff, restart recovery."""
import asyncio

import pytest

from crash_pipeline.client.buffer import PersistentEventBuffer
from crash_pipeline.client.uploader import BackoffPolicy, Uploader
from crash_pipeline.events import Event, EventKind


def make_event(kind=EventKind.TELEMETRY, msg="m"):
    return Event(kind=kind, tenant_id="t", message=msg)


class TestBuffer:
    def test_unacked_events_survive_restart(self, tmp_path):
        path = str(tmp_path / "buf.db")
        buf = PersistentEventBuffer(path)
        e1, e2 = make_event(msg="a"), make_event(msg="b")
        buf.enqueue(e1)
        buf.enqueue(e2)
        buf.acknowledge([e1.event_id])
        buf.close()

        reopened = PersistentEventBuffer(path)
        remaining = reopened.peek_batch(10)
        assert [e.event_id for e in remaining] == [e2.event_id]

    def test_acked_events_never_resent(self, tmp_path):
        buf = PersistentEventBuffer(str(tmp_path / "buf.db"))
        e = make_event()
        buf.enqueue(e)
        assert buf.acknowledge([e.event_id]) == 1
        assert buf.peek_batch(10) == []
        assert buf.pending_count() == 0

    def test_crash_never_evicted_before_telemetry(self, tmp_path):
        buf = PersistentEventBuffer(str(tmp_path / "buf.db"), max_events=2)
        crash = make_event(EventKind.CRASH, "boom")
        buf.enqueue(crash)
        buf.enqueue(make_event(msg="t1"))
        buf.enqueue(make_event(msg="t2"))  # evicts t1, keeps crash
        msgs = [e.message for e in buf.peek_batch(10)]
        assert "boom" in msgs and "t1" not in msgs
        assert buf.evicted_count() == 1

    def test_full_of_crashes_rejects_telemetry(self, tmp_path):
        buf = PersistentEventBuffer(str(tmp_path / "buf.db"), max_events=1)
        buf.enqueue(make_event(EventKind.CRASH, "boom"))
        assert buf.enqueue(make_event(msg="t1")) is False
        assert buf.pending_count() == 1


class FlakyTransport:
    """Fails `fail_times` times, then acks everything."""

    def __init__(self, fail_times=0):
        self.fail_times = fail_times
        self.calls = 0
        self.received = []

    async def __call__(self, payloads):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ConnectionError("network down")
        self.received.extend(payloads)
        return [p["event_id"] for p in payloads]


class TestUploader:
    def test_retry_with_backoff_then_recover(self, tmp_path):
        buf = PersistentEventBuffer(str(tmp_path / "b.db"))
        for i in range(3):
            buf.enqueue(make_event(msg=f"m{i}"))
        transport = FlakyTransport(fail_times=2)
        delays = []

        async def fake_sleep(s):
            delays.append(s)

        up = Uploader(buf, transport,
                      BackoffPolicy(initial_delay=0.1, multiplier=2.0),
                      sleep=fake_sleep)
        n = asyncio.run(up.run_until_empty())
        assert n == 3
        assert delays == [0.1, 0.2]  # exponential backoff applied
        assert buf.pending_count() == 0
        assert len(transport.received) == 3

    def test_events_not_lost_when_upload_fails_midway(self, tmp_path):
        buf = PersistentEventBuffer(str(tmp_path / "b.db"))
        e = make_event()
        buf.enqueue(e)
        up = Uploader(buf, FlakyTransport(fail_times=100),
                      BackoffPolicy(initial_delay=0, max_retries=3),
                      sleep=lambda s: asyncio.sleep(0))
        with pytest.raises(ConnectionError):
            asyncio.run(up.run_until_empty())
        assert buf.pending_count() == 1  # still buffered for next attempt

    def test_partial_ack_only_removes_acked(self, tmp_path):
        buf = PersistentEventBuffer(str(tmp_path / "b.db"))
        events = [make_event(msg=f"m{i}") for i in range(3)]
        for e in events:
            buf.enqueue(e)

        async def partial(payloads):
            return [payloads[0]["event_id"]]  # ack only the first

        up = Uploader(buf, partial, sleep=lambda s: asyncio.sleep(0))
        asyncio.run(up.run_once())
        remaining = [e.event_id for e in buf.peek_batch(10)]
        assert remaining == [events[1].event_id, events[2].event_id]
