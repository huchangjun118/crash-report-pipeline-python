"""把 Uploader 接到真实 HTTP 服务端的发送函数。"""
from __future__ import annotations

import httpx

from crash_pipeline.models import Event, UploadResponse


def make_http_sender(base_url: str, timeout: float = 10.0,
                     client: httpx.Client | None = None):
    """返回一个 SendFunc：POST {base_url}/v1/events。

    网络/5xx 错误抛异常（由 Uploader 退避重试）；
    4xx 视为请求本身有问题，同样抛出让上层决定。
    """
    http = client or httpx.Client(base_url=base_url, timeout=timeout)

    def send(batch: list[Event]) -> UploadResponse:
        resp = http.post("/v1/events",
                         json={"events": [e.to_dict() for e in batch]})
        resp.raise_for_status()
        return UploadResponse(**resp.json())

    return send
