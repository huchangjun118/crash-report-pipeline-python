"""Event model shared by client and server."""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any


class EventKind(str, enum.Enum):
    CRASH = "crash"          # fatal crash, never sampled, never evicted first
    ERROR = "error"          # non-fatal error
    TELEMETRY = "telemetry"  # ordinary telemetry, sampleable, evictable


class Severity(str, enum.Enum):
    FATAL = "fatal"
    ERROR = "error"
    INFO = "info"


@dataclass
class Event:
    kind: EventKind
    tenant_id: str
    message: str
    severity: Severity = Severity.ERROR
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: float = field(default_factory=time.time)
    stacktrace: list[str] = field(default_factory=list)
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["kind"] = self.kind.value
        d["severity"] = self.severity.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Event":
        d = dict(d)
        d["kind"] = EventKind(d["kind"])
        d["severity"] = Severity(d["severity"])
        return cls(**d)
