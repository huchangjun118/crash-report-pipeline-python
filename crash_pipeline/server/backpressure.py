"""Bounded ingest pipeline with priority-aware backpressure.

Memory is strictly bounded by `capacity`. Under overload ordinary telemetry
is dropped first (and counted); crash events are only dropped when the queue
contains nothing but crashes — the fatal lane is starved last.
"""
from __future__ import annotations

import enum
import threading
from collections import deque
from dataclasses import dataclass, field

from crash_pipeline.events import Event, EventKind, Severity


class SubmitResult(enum.Enum):
    ACCEPTED = "accepted"
    DROPPED_OVERLOAD = "dropped_overload"


@dataclass
class PipelineStats:
    accepted: int = 0
    dropped: int = 0
    processed: int = 0


def _is_fatal(event: Event) -> bool:
    return (event.kind is EventKind.CRASH
            or event.severity is Severity.FATAL)


class IngestPipeline:
    """Bounded queue; `drain` yields crashes first, then FIFO."""

    def __init__(self, capacity: int = 1000) -> None:
        self._capacity = capacity
        self._queue: deque[Event] = deque()
        self._lock = threading.Lock()
        self._stats = PipelineStats()

    def submit(self, event: Event) -> tuple[SubmitResult, list[Event]]:
        """Non-blocking submit; drops telemetry (never crashes) when full.

        Returns the result plus any events evicted to make room, so the
        caller can record them as drops (nothing is dropped silently).
        """
        with self._lock:
            if len(self._queue) < self._capacity:
                self._queue.append(event)
                self._stats.accepted += 1
                return SubmitResult.ACCEPTED, []
            if _is_fatal(event):
                # make room by discarding the oldest non-fatal event
                for i, queued in enumerate(self._queue):
                    if not _is_fatal(queued):
                        del self._queue[i]
                        self._stats.dropped += 1
                        self._queue.append(event)
                        self._stats.accepted += 1
                        return SubmitResult.ACCEPTED, [queued]
            self._stats.dropped += 1
            return SubmitResult.DROPPED_OVERLOAD, []

    def drain(self, limit: int | None = None) -> list[Event]:
        """Pop queued events, crashes first, bounded by `limit`."""
        with self._lock:
            n = len(self._queue) if limit is None else min(limit, len(self._queue))
            out: list[Event] = []
            # fatal events first, preserving their relative order
            remaining: deque[Event] = deque()
            for event in self._queue:
                if len(out) < n and _is_fatal(event):
                    out.append(event)
                else:
                    remaining.append(event)
            while len(out) < n and remaining:
                out.append(remaining.popleft())
            self._queue = remaining
            self._stats.processed += len(out)
            return out

    def stats(self) -> PipelineStats:
        with self._lock:
            return PipelineStats(self._stats.accepted, self._stats.dropped,
                                 self._stats.processed)

    def queued(self) -> int:
        with self._lock:
            return len(self._queue)
