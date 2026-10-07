"""客户端上传器：可配置退避重试，网络恢复后自动继续。

- 发送失败（网络异常）按 BackoffConfig 退避重试；
- 超过 max_attempts 本轮放弃，事件留在队列里，下一轮 flush 自动续传；
- 服务端确认（accepted/duplicates）或明确拒绝（rejected）的事件都会从
  队列中确认删除：前者不重复上传，后者避免毒消息死循环。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

from crash_pipeline.client.queue import PersistentEventQueue
from crash_pipeline.models import Event, UploadResponse


@dataclass
class BackoffConfig:
    initial_delay: float = 0.5   # 首次重试等待秒数
    max_delay: float = 30.0      # 退避上限
    multiplier: float = 2.0      # 指数倍率
    max_attempts: int = 8        # 单批最大尝试次数


SendFunc = Callable[[list[Event]], UploadResponse]


class Uploader:
    def __init__(self, queue: PersistentEventQueue, send: SendFunc,
                 backoff: BackoffConfig | None = None, batch_size: int = 50,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self._queue = queue
        self._send = send
        self._backoff = backoff or BackoffConfig()
        self._batch_size = batch_size
        self._sleep = sleep

    def flush_once(self) -> bool:
        """尝试上传一批。返回 False 表示网络不可用（事件保留，下轮续传）。"""
        batch = self._queue.peek_batch(self._batch_size)
        if not batch:
            return True
        resp = self._send_with_retry(batch)
        if resp is None:
            return False  # 网络仍不可用，事件保留在队列中
        # 已确认 + 被明确拒绝的都从队列移除
        done = resp.acked_ids() + [r.event_id for r in resp.rejected]
        self._queue.ack(done)
        return True

    def flush_all(self, max_rounds: int = 100) -> bool:
        """循环上传直到队列清空或轮次用尽。

        某一轮失败（网络不可用）不立即放弃：事件保留在队列中，
        下一轮重新入退避重试，网络恢复后自动续传。
        返回 False 表示轮次用尽仍有未确认事件。
        """
        for _ in range(max_rounds):
            self.flush_once()
            if len(self._queue) == 0:
                return True
        return False

    def _send_with_retry(self, batch: list[Event]) -> UploadResponse | None:
        cfg = self._backoff
        delay = cfg.initial_delay
        for attempt in range(cfg.max_attempts):
            try:
                return self._send(batch)
            except Exception:
                if attempt == cfg.max_attempts - 1:
                    return None
                self._sleep(delay)
                delay = min(delay * cfg.multiplier, cfg.max_delay)
        return None
