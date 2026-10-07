"""FastAPI 服务端：接收批量事件，完成背压 -> 配额/采样 -> 指纹 -> 去重入库。

处理顺序（每条事件）：
1. 背压：过载时普通遥测直接丢弃（计数可查），fatal 优先放行；
2. 配额/采样：拒绝的记录原因落库，可查询；
3. 指纹 + 去重入库：同一事件只入库一次，重复投递返回 duplicates。
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from crash_pipeline.models import Event, RejectedItem, RejectReason, UploadResponse
from crash_pipeline.server.backpressure import BackpressureController
from crash_pipeline.server.fingerprint import FingerprintConfig, compute_fingerprint
from crash_pipeline.server.quota import QuotaManager, TenantPolicy
from crash_pipeline.server.store import EventStore


class IngestRequest(BaseModel):
    events: list[dict]


def create_app(db_path: str = ":memory:",
               fingerprint_config: FingerprintConfig | None = None,
               quota: QuotaManager | None = None,
               backpressure: BackpressureController | None = None) -> FastAPI:
    store = EventStore(db_path)
    fp_config = fingerprint_config or FingerprintConfig()
    quota = quota or QuotaManager()
    bp = backpressure or BackpressureController()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        store.close()

    app = FastAPI(title="crash-report-pipeline", lifespan=lifespan)
    app.state.store = store
    app.state.quota = quota
    app.state.backpressure = bp

    @app.post("/v1/events", response_model=UploadResponse)
    def ingest(req: IngestRequest) -> UploadResponse:
        resp = UploadResponse()
        admitted: list[Event] = []
        usage_cache: dict[str, int] = {}  # 批内累计用量，配额判定即时生效

        def usage_of(tenant_id: str) -> int:
            if tenant_id not in usage_cache:
                usage_cache[tenant_id] = store.usage(tenant_id)
            return usage_cache[tenant_id]

        for raw in req.events:
            try:
                ev = Event.from_dict(raw)
            except Exception as exc:
                resp.rejected.append(RejectedItem(
                    event_id=str(raw.get("event_id", "")),
                    reason=RejectReason.INVALID.value, detail=str(exc)))
                continue
            if not bp.admit(ev):
                store.record_rejection(ev.event_id, ev.tenant_id,
                                       RejectReason.OVERLOADED.value,
                                       "backpressure: pending full")
                resp.rejected.append(RejectedItem(
                    event_id=ev.event_id, reason=RejectReason.OVERLOADED.value))
                continue
            try:
                ok, reason = quota.admit(ev, usage_of(ev.tenant_id))
                if not ok:
                    store.record_rejection(ev.event_id, ev.tenant_id, reason)
                    resp.rejected.append(RejectedItem(event_id=ev.event_id, reason=reason))
                    continue
                admitted.append(ev)
                usage_cache[ev.tenant_id] = usage_of(ev.tenant_id) + 1
            finally:
                bp.release()
        if admitted:
            fps = {ev.event_id: compute_fingerprint(ev, fp_config) for ev in admitted}
            result = store.ingest(admitted, fps)
            resp.accepted = result["accepted"]
            resp.duplicates = result["duplicates"]
        return resp

    @app.get("/v1/issues")
    def issues(tenant_id: str) -> list[dict]:
        return store.issues(tenant_id)

    @app.get("/v1/rejections")
    def rejections(tenant_id: str) -> list[dict]:
        return store.rejections(tenant_id)

    @app.get("/v1/stats")
    def stats() -> dict:
        return {"pending": bp.pending, "dropped": bp.dropped,
                "dropped_detail": bp.dropped_detail(),
                "committed_batches": store.committed_batches()}

    @app.put("/v1/policies/{tenant_id}")
    def set_policy(tenant_id: str, policy: TenantPolicy) -> dict:
        quota.set_policy(tenant_id, policy)
        return {"ok": True}

    return app
