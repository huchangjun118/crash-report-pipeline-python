"""End-to-end: client buffer -> uploader -> HTTP ingest -> store."""
import asyncio

import pytest
from fastapi.testclient import TestClient

from crash_pipeline.client.buffer import PersistentEventBuffer
from crash_pipeline.client.uploader import BackoffPolicy, Uploader
from crash_pipeline.events import Event, EventKind, Severity
from crash_pipeline.server.app import create_app
from crash_pipeline.server.backpressure import IngestPipeline
from crash_pipeline.server.quota import QuotaManager, TenantPolicy
from crash_pipeline.server.storage import EventStore


def telemetry(msg, tenant="t"):
    return Event(kind=EventKind.TELEMETRY, tenant_id=tenant, message=msg)


def crash(msg="boom", tenant="t"):
    return Event(kind=EventKind.CRASH, tenant_id=tenant, message=msg,
                 severity=Severity.FATAL, stacktrace=["f1", "f2"])


@pytest.fixture
def env(tmp_path):
    store = EventStore(str(tmp_path / "server.db"))
    app = create_app(store)
    return store, TestClient(app)


class HttpTransport:
    """Uploads via the FastAPI app; can be toggled offline."""

    def __init__(self, client):
        self.client = client
        self.online = True

    async def __call__(self, payloads):
        if not self.online:
            raise ConnectionError("offline")
        r = self.client.post("/ingest", json={"events": payloads})
        r.raise_for_status()
        return r.json()["acked"]


class TestEndToEnd:
    def test_network_outage_then_recovery(self, tmp_path, env):
        store, client = env
        buf = PersistentEventBuffer(str(tmp_path / "client.db"))
        events = [telemetry(f"m{i}") for i in range(5)]
        for e in events:
            buf.enqueue(e)
        transport = HttpTransport(client)
        up = Uploader(buf, transport, BackoffPolicy(initial_delay=0),
                      sleep=lambda s: asyncio.sleep(0))

        transport.online = False
        with pytest.raises(ConnectionError):
            asyncio.run(up.run_once())
        assert buf.pending_count() == 5  # nothing lost while offline

        transport.online = True
        n = asyncio.run(up.run_until_empty())
        assert n == 5
        assert store.event_count() == 5
        assert buf.pending_count() == 0

    def test_client_restart_mid_upload(self, tmp_path, env):
        store, client = env
        path = str(tmp_path / "client.db")
        buf = PersistentEventBuffer(path)
        events = [telemetry(f"m{i}") for i in range(4)]
        for e in events:
            buf.enqueue(e)
        transport = HttpTransport(client)
        up = Uploader(buf, transport, batch_size=2,
                      sleep=lambda s: asyncio.sleep(0))
        asyncio.run(up.run_once())  # uploads 2 of 4, then "crash"
        buf.close()

        # restart: only unacked events are sent, no duplicates
        buf2 = PersistentEventBuffer(path)
        up2 = Uploader(buf2, transport, sleep=lambda s: asyncio.sleep(0))
        asyncio.run(up2.run_until_empty())
        assert store.event_count() == 4
        assert buf2.pending_count() == 0

    def test_duplicate_delivery_over_http(self, env):
        store, client = env
        e = crash()
        for _ in range(3):
            r = client.post("/ingest", json={"events": [e.to_dict()]})
            assert e.event_id in r.json()["acked"]
        assert store.event_count() == 1
        issues = client.get("/issues").json()
        assert issues[0]["event_count"] == 1

    def test_out_of_order_batches_group_together(self, env):
        store, client = env
        cfg_events = [crash(msg=f"segfault user {i}") for i in range(6)]
        # deliver in scrambled order across batches
        for e in [cfg_events[4], cfg_events[1], cfg_events[5],
                  cfg_events[0], cfg_events[3], cfg_events[2]]:
            client.post("/ingest", json={"events": [e.to_dict()]})
        issues = client.get("/issues", params={"tenant_id": "t"}).json()
        assert len(issues) == 1
        assert issues[0]["event_count"] == 6

    def test_overload_drops_telemetry_keeps_crashes(self, tmp_path):
        store = EventStore(str(tmp_path / "s.db"))
        pipeline = IngestPipeline(capacity=2)
        app = create_app(store, pipeline=pipeline)
        client = TestClient(app)
        # flood one request: 5 telemetry + 1 crash, capacity 2
        events = [telemetry(f"t{i}").to_dict() for i in range(5)]
        events.append(crash().to_dict())
        r = client.post("/ingest", json={"events": events})
        outcomes = r.json()["outcomes"]
        rejected = [o for o in outcomes if o["status"] == "rejected"]
        assert all(o["reason"] == "overloaded" for o in rejected)
        # crash survived; drops are queryable with a reason
        kinds = [store and o for o in outcomes]
        drops = client.get("/drops").json()
        assert drops.get("overloaded", 0) == len(rejected)
        stored = store.event_count()
        assert stored + len(rejected) == 6
        # memory stayed bounded
        assert pipeline.queued() <= 2

    def test_quota_rejection_visible_via_api(self, tmp_path):
        store = EventStore(str(tmp_path / "s.db"))
        quota = QuotaManager()
        quota.set_policy("t", TenantPolicy(sample_rate=1.0,
                                           max_events_per_window=2,
                                           window_seconds=60))
        client = TestClient(create_app(store, quota=quota))
        events = [telemetry(f"m{i}").to_dict() for i in range(4)]
        r = client.post("/ingest", json={"events": events})
        outcomes = r.json()["outcomes"]
        assert sum(o["status"] == "rejected" for o in outcomes) == 2
        assert all(o.get("reason") == "quota_exceeded"
                   for o in outcomes if o["status"] == "rejected")
        assert client.get("/drops").json() == {"quota_exceeded": 2}
        assert store.event_count() == 2
