"""过载降级：fatal 通道优先、普通遥测丢弃计数、内存有界。"""
from crash_pipeline.models import Severity
from crash_pipeline.server.app import create_app
from crash_pipeline.server.backpressure import BackpressureController
from fastapi.testclient import TestClient

from tests.conftest import make_events


def test_overload_drops_telemetry_but_keeps_fatal(tmp_path):
    bp = BackpressureController(max_pending=4, fatal_reserved=2)
    # 预占满普通遥测通道（pending=2，非 fatal 上限即 max-reserved=2）
    bp.admit(make_events(1, severity=Severity.INFO)[0])
    bp.admit(make_events(1, severity=Severity.INFO)[0])
    app = create_app(str(tmp_path / "server.db"), backpressure=bp)
    with TestClient(app) as c:
        tele = make_events(5, severity=Severity.INFO)
        fatal = make_events(3, severity=Severity.FATAL)
        body = {"events": [e.to_dict() for e in tele + fatal]}
        r = c.post("/v1/events", json=body).json()
        # 普通遥测全部被背压丢弃，fatal 全部放行
        assert len(r["accepted"]) == 3
        assert len(r["rejected"]) == 5
        assert all(x["reason"] == "overloaded" for x in r["rejected"])
        # 丢弃量可查询
        stats = c.get("/v1/stats").json()
        assert stats["dropped"] == 5
        assert stats["dropped_detail"] == {"t1/info": 5}
        rej = c.get("/v1/rejections", params={"tenant_id": "t1"}).json()
        assert len(rej) == 5 and {x["reason"] for x in rej} == {"overloaded"}


def test_pending_is_bounded(tmp_path):
    """在途事件数有硬上限，降级过程内存不无界增长。"""
    bp = BackpressureController(max_pending=10)
    for e in make_events(1000, severity=Severity.INFO):
        bp.admit(e)
    assert bp.pending <= 10
    assert bp.dropped == 1000 - (10 - bp._reserved)
