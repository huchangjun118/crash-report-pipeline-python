"""上传器：退避重试、网络恢复续传、乱序不影响结果。"""
import random

from crash_pipeline.client.queue import PersistentEventQueue
from crash_pipeline.client.uploader import BackoffConfig, Uploader
from crash_pipeline.models import UploadResponse

from tests.conftest import make_events, make_send


def test_retry_with_configurable_backoff(tmp_db):
    q = PersistentEventQueue(tmp_db)
    for e in make_events(2):
        q.enqueue(e)
    calls = {"n": 0}
    sleeps = []

    def flaky(batch):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("down")
        return UploadResponse(accepted=[e.event_id for e in batch])

    up = Uploader(q, flaky, BackoffConfig(initial_delay=0.1, multiplier=3.0,
                                          max_delay=1.0, max_attempts=5),
                  sleep=sleeps.append)
    assert up.flush_once()
    assert calls["n"] == 3
    import pytest
    assert sleeps == pytest.approx([0.1, 0.3])  # 指数退避序列
    assert len(q) == 0


def test_network_recovery_resumes_upload(tmp_db):
    q = PersistentEventQueue(tmp_db)
    evs = make_events(3)
    for e in evs:
        q.enqueue(e)

    down = Uploader(q, lambda b: (_ for _ in ()).throw(ConnectionError()),
                    BackoffConfig(initial_delay=0.01, max_attempts=2),
                    sleep=lambda s: None)
    assert not down.flush_once()
    assert len(q) == 3  # 未确认，事件保留

    recovered = Uploader(q, lambda b: UploadResponse(accepted=[e.event_id for e in b]),
                         sleep=lambda s: None)
    assert recovered.flush_once()
    assert len(q) == 0


def test_out_of_order_batches_converge(tmp_db, client):
    """事件乱序到达服务端，聚合结果与顺序无关。"""
    evs = make_events(10)
    send = make_send(client)

    q = PersistentEventQueue(tmp_db)
    for e in evs:
        q.enqueue(e)
    Uploader(q, send, batch_size=100, sleep=lambda s: None).flush_all()

    shuffled = list(evs)
    random.Random(42).shuffle(shuffled)
    q2 = PersistentEventQueue(tmp_db + ".2")
    for e in shuffled:
        q2.enqueue(e)
    Uploader(q2, send, batch_size=3, sleep=lambda s: None).flush_all()

    issues = client.get("/v1/issues", params={"tenant_id": "t1"}).json()
    assert len(issues) == 1
    assert issues[0]["count"] == 10  # 乱序重投不增加计数
