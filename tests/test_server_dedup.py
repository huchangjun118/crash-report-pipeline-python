"""服务端：重复投递去重、乱序归并、并发收敛、重启恢复。"""
import random
import threading

from crash_pipeline.models import Event, Severity
from crash_pipeline.server.app import create_app
from fastapi.testclient import TestClient

from tests.conftest import make_events


def _post(client, evs):
    r = client.post("/v1/events", json={"events": [e.to_dict() for e in evs]})
    r.raise_for_status()
    return r.json()


def test_duplicate_delivery_not_double_counted(client):
    evs = make_events(5)
    r1 = _post(client, evs)
    assert len(r1["accepted"]) == 5
    r2 = _post(client, evs)  # 整批重投
    assert len(r2["duplicates"]) == 5
    assert not r2["accepted"]
    issues = client.get("/v1/issues", params={"tenant_id": "t1"}).json()
    assert issues[0]["count"] == 5  # 计数不虚高


def test_out_of_order_arrival_same_grouping(client):
    evs = make_events(8)
    shuffled = list(evs)
    random.Random(7).shuffle(shuffled)
    _post(client, shuffled[:4])
    _post(client, shuffled[4:])
    issues = client.get("/v1/issues", params={"tenant_id": "t1"}).json()
    assert len(issues) == 1 and issues[0]["count"] == 8


def test_concurrent_ingest_same_event_converges(tmp_path):
    """多个并发请求同时处理同一事件：只入库一次。"""
    app = create_app(str(tmp_path / "server.db"))
    ev = make_events(1)[0]
    payload = {"events": [ev.to_dict()]}
    results = []
    barrier = threading.Barrier(8)

    with TestClient(app) as client:
        def worker():
            barrier.wait()
            for _ in range(5):
                results.append(client.post("/v1/events", json=payload).json())

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        accepted = sum(len(r["accepted"]) for r in results)
        duplicates = sum(len(r["duplicates"]) for r in results)
        assert accepted == 1           # 全局只入库一次
        assert duplicates == 39
        issues = client.get("/v1/issues", params={"tenant_id": "t1"}).json()
        assert issues[0]["count"] == 1


def test_server_restart_resumes_without_loss_or_dup(tmp_path):
    db = str(tmp_path / "server.db")
    evs = make_events(4)
    app1 = create_app(db)
    with TestClient(app1) as c1:
        r = _post(c1, evs[:2])
        assert len(r["accepted"]) == 2

    app2 = create_app(db)  # 重启
    with TestClient(app2) as c2:
        r = _post(c2, evs)  # 前两条是重投，后两条是新的
        assert len(r["duplicates"]) == 2
        assert len(r["accepted"]) == 2
        issues = c2.get("/v1/issues", params={"tenant_id": "t1"}).json()
        assert issues[0]["count"] == 4  # 不丢不重
