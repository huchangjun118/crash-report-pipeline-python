import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from crash_pipeline.models import Event, Severity, UploadResponse
from crash_pipeline.server.app import create_app


@pytest.fixture()
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "server.db"))
    with TestClient(app) as c:
        yield c


def make_send(test_client):
    """把 TestClient 包装成 Uploader 的 SendFunc。"""
    def send(batch):
        r = test_client.post("/v1/events",
                             json={"events": [e.to_dict() for e in batch]})
        r.raise_for_status()
        return UploadResponse(**r.json())
    return send


def make_events(n, tenant="t1", severity=Severity.ERROR, **kw):
    kw.setdefault("exception_type", "NPE")
    kw.setdefault("stack_frames", ("a.py:1", "b.py:2"))
    return [Event.new(tenant, severity, f"boom {i}", **kw) for i in range(n)]
