"""端到端：客户端崩溃重启 + 服务端重启，事件不丢不重。"""
from crash_pipeline.client.queue import PersistentEventQueue
from crash_pipeline.client.uploader import BackoffConfig, Uploader
from crash_pipeline.models import Severity, UploadResponse
from crash_pipeline.server.app import create_app
from fastapi.testclient import TestClient

from tests.conftest import make_events, make_send


def test_client_crash_mid_upload_resumes_after_restart(tmp_path):
    server_db = str(tmp_path / "server.db")
    queue_db = str(tmp_path / "queue.db")
    app = create_app(server_db)

    evs = make_events(6)
    # 第一次运行：上传 1 批（2 条）后"进程崩溃"（queue 未关闭直接丢弃对象）
    with TestClient(app) as c:
        q = PersistentEventQueue(queue_db)
        for e in evs:
            q.enqueue(e)
        up = Uploader(q, make_send(c), batch_size=2, sleep=lambda s: None)
        assert up.flush_once()
        assert len(q) == 4
        del q  # 模拟崩溃：未 ack 的 4 条仍在磁盘上

    # 重启：从未确认位置继续，已确认的 2 条不重传
    with TestClient(create_app(server_db)) as c:
        sent_batches = []

        def spy_send(batch):
            sent_batches.append([e.event_id for e in batch])
            return make_send(c)(batch)

        q2 = PersistentEventQueue(queue_db)
        up2 = Uploader(q2, spy_send, batch_size=2, sleep=lambda s: None)
        assert up2.flush_all()
        sent_ids = {i for b in sent_batches for i in b}
        first_two = {e.event_id for e in evs[:2]}
        assert not (sent_ids & first_two)  # 已确认的不再重传
        assert len(q2) == 0

    # 服务端重启后数据仍在，重投全部识别为重复
    app2 = create_app(server_db)
    with TestClient(app2) as c2:
        issues = c2.get("/v1/issues", params={"tenant_id": "t1"}).json()
        assert issues[0]["count"] == 6
        r = c2.post("/v1/events",
                    json={"events": [e.to_dict() for e in evs]}).json()
        assert len(r["duplicates"]) == 6 and not r["accepted"]
        issues = c2.get("/v1/issues", params={"tenant_id": "t1"}).json()
        assert issues[0]["count"] == 6  # 不重


def test_flaky_network_then_full_delivery(tmp_path):
    """网络抖动 -> 退避重试 -> 恢复后全部送达且只入库一次。"""
    app = create_app(str(tmp_path / "server.db"))
    with TestClient(app) as c:
        good_send = make_send(c)
        state = {"fails_left": 4}

        def flaky(batch):
            if state["fails_left"] > 0:
                state["fails_left"] -= 1
                raise ConnectionError("unstable")
            return good_send(batch)

        q = PersistentEventQueue(str(tmp_path / "q.db"))
        evs = make_events(5)
        for e in evs:
            q.enqueue(e)
        up = Uploader(q, flaky, BackoffConfig(initial_delay=0.01, max_attempts=3),
                      sleep=lambda s: None)
        assert up.flush_all()
        issues = c.get("/v1/issues", params={"tenant_id": "t1"}).json()
        assert issues[0]["count"] == 5
