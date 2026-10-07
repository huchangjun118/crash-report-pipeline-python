"""Per-tenant sampling and quota enforcement.

Sampling is deterministic on event_id, so a retried/redelivered event always
gets the same admit/reject decision and can never be counted twice.
Fatal events bypass sampling entirely (always kept).
"""
from __future__ import annotations

import enum
import hashlib
import threading
import time
from dataclasses import dataclass, field

from crash_pipeline.events import Event, EventKind, Severity


class RejectReason(str, enum.Enum):
    SAMPLED_OUT = "sampled_out"
    QUOTA_EXCEEDED = "quota_exceeded"
    OVERLOADED = "overloaded"


@dataclass
class TenantPolicy:
    sample_rate: float = 1.0        # applies to TELEMETRY only; fatal kept 100%
    max_events_per_window: int = 1000
    window_seconds: float = 60.0


@dataclass
class AdmissionResult:
    accepted: bool
    reason: RejectReason | None = None


def _sampled_in(event_id: str, rate: float) -> bool:
    h = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
    bucket = int(h[:8], 16) / 0xFFFFFFFF
    return bucket < rate


class QuotaManager:
    """Thread-safe admission control: deterministic sampling + rate quota."""

    def __init__(self, default_policy: TenantPolicy | None = None) -> None:
        self._default = default_policy or TenantPolicy()
        self._policies: dict[str, TenantPolicy] = {}
        # tenant_id -> (window_start, count)
        self._windows: dict[str, tuple[float, int]] = {}
        self._lock = threading.Lock()

    def set_policy(self, tenant_id: str, policy: TenantPolicy) -> None:
        with self._lock:
            self._policies[tenant_id] = policy

    def admit(self, event: Event, now: float | None = None) -> AdmissionResult:
        now = time.time() if now is None else now
        with self._lock:
            policy = self._policies.get(event.tenant_id, self._default)
            fatal = (event.kind is EventKind.CRASH
                     or event.severity is Severity.FATAL)
            if not fatal and event.kind is EventKind.TELEMETRY:
                if not _sampled_in(event.event_id, policy.sample_rate):
                    return AdmissionResult(False, RejectReason.SAMPLED_OUT)
            start, count = self._windows.get(event.tenant_id, (now, 0))
            if now - start >= policy.window_seconds:
                start, count = now, 0
            if count >= policy.max_events_per_window and not fatal:
                self._windows[event.tenant_id] = (start, count)
                return AdmissionResult(False, RejectReason.QUOTA_EXCEEDED)
            self._windows[event.tenant_id] = (start, count + 1)
            return AdmissionResult(True)
