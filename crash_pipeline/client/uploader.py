"""Async uploader: drains the persistent buffer with configurable backoff.

Only server-acknowledged events are removed from the buffer, so a restart
mid-upload never loses events and acknowledged ones are never re-sent.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from crash_pipeline.client.buffer import PersistentEventBuffer


@dataclass
class BackoffPolicy:
    initial_delay: float = 0.5
    max_delay: float = 30.0
    multiplier: float = 2.0
    max_retries: int = 0  # 0 = retry forever

    def delay_for(self, attempt: int) -> float:
        """Delay before retry number `attempt` (0-based)."""
        return min(self.initial_delay * (self.multiplier ** attempt),
                   self.max_delay)


# Transport: (events) -> acked event ids; raises on transient failure.
Transport = Callable[[list[dict[str, Any]]], Awaitable[list[str]]]


class Uploader:
    def __init__(
        self,
        buffer: PersistentEventBuffer,
        transport: Transport,
        backoff: BackoffPolicy | None = None,
        batch_size: int = 50,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._buffer = buffer
        self._transport = transport
        self._backoff = backoff or BackoffPolicy()
        self._batch_size = batch_size
        self._sleep = sleep

    async def run_once(self) -> int:
        """Try to upload one batch. Returns number of acknowledged events."""
        batch = self._buffer.peek_batch(self._batch_size)
        if not batch:
            return 0
        acked = await self._transport([e.to_dict() for e in batch])
        return self._buffer.acknowledge(list(acked))

    async def run_until_empty(self, max_rounds: int = 100) -> int:
        """Upload until the buffer is empty, backing off on failures.

        Network recovery is automatic: each failure just increases the
        delay (up to `max_delay`) and the next attempt uses the persisted
        buffer, so nothing is lost while the network is down.
        """
        total = 0
        attempt = 0
        for _ in range(max_rounds):
            if self._buffer.pending_count() == 0:
                break
            try:
                total += await self.run_once()
                attempt = 0
            except Exception:
                policy = self._backoff
                if policy.max_retries and attempt >= policy.max_retries:
                    raise
                await self._sleep(policy.delay_for(attempt))
                attempt += 1
        return total
