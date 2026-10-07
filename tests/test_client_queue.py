"""客户端队列：持久化、确认语义、优先级淘汰、重启恢复。"""
from crash_pipeline.client.queue import PersistentEventQueue
from crash_pipeline.models import Event, Severity

from tests.conftest import make_events


def test_unacked_events_survive_restart(tmp_db):
    q = PersistentEventQueue(tmp_db)
    evs = make_events(3)
    for e in evs:
        q.enqueue(e)
    q.ack([evs[0].event_id])
    q.close()

    q2 = PersistentEventQueue(tmp_db)  # 模拟进程重启
    remaining = q2.peek_batch(10)
    assert len(remaining) == 2
    assert evs[0].event_id not in {e.event_id for e in remaining}


def test_acked_events_never_resent(tmp_db):
    q = PersistentEventQueue(tmp_db)
    evs = make_events(2)
    for e in evs:
        q.enqueue(e)
    q.ack([e.event_id for e in evs])
    q.close()
    q2 = PersistentEventQueue(tmp_db)
    assert len(q2) == 0


def test_fatal_never_evicted_before_telemetry(tmp_db):
    q = PersistentEventQueue(tmp_db, max_events=5)
    fatal = Event.new("t1", Severity.FATAL, "crash")
    q.enqueue(fatal)
    for e in make_events(10, severity=Severity.INFO):  # 写满并溢出
        q.enqueue(e)
    batch = q.peek_batch(10)
    assert len(batch) == 5
    assert batch[0].event_id == fatal.event_id  # fatal 保留且优先上传
    assert all(not e.is_fatal for e in batch[1:])


def test_queue_full_of_fatal_rejects_new_telemetry(tmp_db):
    q = PersistentEventQueue(tmp_db, max_events=3)
    for _ in range(3):
        assert q.enqueue(Event.new("t1", Severity.FATAL, "c"))
    assert not q.enqueue(Event.new("t1", Severity.INFO, "x"))
    assert len(q) == 3


def test_enqueue_idempotent(tmp_db):
    q = PersistentEventQueue(tmp_db)
    e = make_events(1)[0]
    q.enqueue(e)
    q.enqueue(e)
    assert len(q) == 1
