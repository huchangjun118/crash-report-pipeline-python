"""背压控制：有界在途缓冲，过载时优先保证 fatal 通道。

- max_pending 限制同时在处理的事件数，内存有界，不会无界增长；
- 容量满时：fatal 仍可进入（为 fatal 预留通道），普通遥测直接丢弃；
- 每一次丢弃都按 (tenant, severity) 计数，可查询，不静默。
"""
from __future__ import annotations

import threading
from collections import Counter

from crash_pipeline.models import Event


class BackpressureController:
    def __init__(self, max_pending: int = 1_000, fatal_reserved: int | None = None) -> None:
        self._max = max_pending
        # 为 fatal 预留的容量，默认 10%
        self._reserved = fatal_reserved if fatal_reserved is not None else max(1, max_pending // 10)
        self._pending = 0
        self._dropped: Counter[tuple[str, str]] = Counter()
        self._lock = threading.Lock()

    def admit(self, event: Event) -> bool:
        """返回 False 表示因过载被丢弃（已计数）。"""
        with self._lock:
            limit = self._max if event.is_fatal else self._max - self._reserved
            if self._pending >= limit:
                if not event.is_fatal:
                    self._dropped[(event.tenant_id, event.severity)] += 1
                    return False
                # fatal 连预留通道都满了：仍放行（fatal 不丢），允许短暂超限
            self._pending += 1
            return True

    def release(self, n: int = 1) -> None:
        with self._lock:
            self._pending = max(0, self._pending - n)

    @property
    def pending(self) -> int:
        with self._lock:
            return self._pending

    @property
    def dropped(self) -> int:
        with self._lock:
            return sum(self._dropped.values())

    def dropped_by_tenant(self, tenant_id: str) -> int:
        with self._lock:
            return sum(v for (t, _), v in self._dropped.items() if t == tenant_id)

    def dropped_detail(self) -> dict[str, int]:
        with self._lock:
            return {f"{t}/{s}": n for (t, s), n in self._dropped.items()}
