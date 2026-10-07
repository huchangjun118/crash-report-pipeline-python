"""FastAPI ingest service tying together pipeline, quota, dedup and storage."""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel

from crash_pipeline.events import Event
from crash_pipeline.server.backpressure import IngestPipeline, SubmitResult
from crash_pipeline.server.fingerprint import FingerprintConfig, compute_fingerprint
from crash_pipeline.server.quota import QuotaManager, RejectReason
from crash_pipeline.server.storage import EventStore


class IngestRequest(BaseModel):
    events: list[dict[str, Any]]


def create_app(
    store: EventStore,
    quota: QuotaManager | None = None,
    pipeline: IngestPipeline | None = None,
    fingerprint_config: FingerprintConfig | None = None,
) -> FastAPI:
    quota = quota or QuotaManager()
    pipeline = pipeline or IngestPipeline()
    fp_config = fingerprint_config or FingerprintConfig()
    app = FastAPI(title="crash-report-pipeline")

    @app.post("/ingest")
    def ingest(req: IngestRequest) -> dict[str, Any]:
        outcomes: list[dict[str, Any]] = []
        for raw in req.events:
            event = Event.from_dict(raw)
            admission = quota.admit(event)
            if not admission.accepted:
                store.record_drop(event, admission.reason)
                outcomes.append({"event_id": event.event_id,
                                 "status": "rejected",
                                 "reason": admission.reason.value})
                continue
            result, evicted = pipeline.submit(event)
            for dropped in evicted:
                store.record_drop(dropped, RejectReason.OVERLOADED)
                outcomes.append({"event_id": dropped.event_id,
                                 "status": "rejected",
                                 "reason": RejectReason.OVERLOADED.value})
            if result is SubmitResult.DROPPED_OVERLOAD:
                store.record_drop(event, RejectReason.OVERLOADED)
                outcomes.append({"event_id": event.event_id,
                                 "status": "rejected",
                                 "reason": RejectReason.OVERLOADED.value})
        # drain the bounded queue into durable storage (crashes first)
        for event in pipeline.drain():
            fp = compute_fingerprint(event, fp_config)
            outcome = store.record_event(event, fp)
            outcomes.append({"event_id": outcome.event_id,
                             "status": outcome.status,
                             "issue_id": outcome.issue_id})
        # every event gets a terminal answer: acked ids are safe to delete
        # on the client (stored, duplicate and rejected are all final)
        return {
            "acked": [o["event_id"] for o in outcomes],
            "outcomes": outcomes,
        }

    @app.get("/issues")
    def list_issues(tenant_id: str | None = None) -> list[dict[str, Any]]:
        return [vars(i) for i in store.list_issues(tenant_id)]

    @app.get("/issues/{issue_id}")
    def get_issue(issue_id: str) -> dict[str, Any]:
        issue = store.get_issue(issue_id)
        if issue is None:
            return {"error": "not found"}
        return vars(issue)

    @app.get("/drops")
    def drops(tenant_id: str | None = None) -> dict[str, int]:
        return store.drop_stats(tenant_id)

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        s = pipeline.stats()
        return {"accepted": s.accepted, "dropped": s.dropped,
                "processed": s.processed, "queued": pipeline.queued(),
                "stored_events": store.event_count()}

    return app
