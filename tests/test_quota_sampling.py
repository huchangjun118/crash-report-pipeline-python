"""配额与采样：拒绝有原因可查、fatal 全量保留、采样确定性。"""
from crash_pipeline.models import Severity
from crash_pipeline.server.app import create_app
from crash_pipeline.server.quota import QuotaManager, TenantPolicy
from fastapi.testclient import TestClient

from tests.conftest import make_events


def _app_with_policy(tmp_path, policy):
    quota = QuotaManager()
    quota.set_policy("t1", policy)
    return create_app(str(tmp_path / "server.db"), quota=quota)


def test_quota_rejection_is_queryable(tmp_path):
    app = _app_with_policy(tmp_path, TenantPolicy(sample_rate=1.0, max_events=3))
    with TestClient(app) as c:
        evs = make_events(5)
        r = c.post("/v1/events", json={"events": [e.to_dict() for e in evs]}).json()
        assert len(r["accepted"]) == 3
        assert len(r["rejected"]) == 2
        assert all(x["reason"] == "quota_exceeded" for x in r["rejected"])
        # 拒绝原因落库可查询，不静默吞掉
        rej = c.get("/v1/rejections", params={"tenant_id": "t1"}).json()
        assert len(rej) == 2
        assert {x["reason"] for x in rej} == {"quota_exceeded"}


def test_fatal_never_sampled_and_survives_quota(tmp_path):
    app = _app_with_policy(tmp_path, TenantPolicy(sample_rate=0.0, max_events=0,
                                                  over_quota="degrade"))
    with TestClient(app) as c:
        fatal = make_events(2, severity=Severity.FATAL)
        tele = make_events(3, severity=Severity.INFO)
        r = c.post("/v1/events",
                   json={"events": [e.to_dict() for e in fatal + tele]}).json()
        assert len(r["accepted"]) == 2  # fatal 全量保留
        assert len(r["rejected"]) == 3


def test_sampling_deterministic_per_event(tmp_path):
    """同一事件重投，采样结论一致（不会因重试时而被拒时而被收）。"""
    app = _app_with_policy(tmp_path, TenantPolicy(sample_rate=0.5, max_events=10**9))
    with TestClient(app) as c:
        evs = make_events(50, severity=Severity.INFO)
        body = {"events": [e.to_dict() for e in evs]}
        r1 = c.post("/v1/events", json=body).json()
        rejected1 = {x["event_id"] for x in r1["rejected"]}
        # 重投：之前被采掉的仍被采掉，之前被收的是重复
        r2 = c.post("/v1/events", json=body).json()
        rejected2 = {x["event_id"] for x in r2["rejected"]}
        assert rejected1 == rejected2
        assert len(r2["duplicates"]) == len(r1["accepted"])
        kept = len(r1["accepted"])
        assert 10 < kept < 40  # 大约一半


def test_rejected_events_not_reuploaded_forever(tmp_path):
    """客户端收到明确拒绝后确认出队，不形成毒消息循环。"""
    from crash_pipeline.client.queue import PersistentEventQueue
    from crash_pipeline.client.uploader import Uploader
    from tests.conftest import make_send

    app = _app_with_policy(tmp_path, TenantPolicy(sample_rate=0.0, max_events=0))
    with TestClient(app) as c:
        q = PersistentEventQueue(str(tmp_path / "q.db"))
        for e in make_events(3, severity=Severity.INFO):
            q.enqueue(e)
        up = Uploader(q, make_send(c), sleep=lambda s: None)
        assert up.flush_all()
        assert len(q) == 0
