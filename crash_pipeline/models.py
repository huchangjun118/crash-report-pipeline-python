"""共享数据模型：客户端与服务端共用的事件定义。"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class Severity(str, Enum):
    FATAL = "fatal"        # 崩溃/致命错误：永不采样、永不因过载丢弃
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"          # 普通遥测


class RejectReason(str, Enum):
    QUOTA_EXCEEDED = "quota_exceeded"
    SAMPLED_OUT = "sampled_out"
    OVERLOADED = "overloaded"
    INVALID = "invalid"


@dataclass
class Event:
    """一条待上报事件。event_id 由客户端生成，全局唯一，是端到端去重的依据。"""
    event_id: str
    tenant_id: str
    severity: str
    message: str
    timestamp: float = field(default_factory=time.time)
    exception_type: str = ""
    stack_frames: tuple[str, ...] = ()
    tags: dict[str, str] = field(default_factory=dict)
    payload: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def new(tenant_id: str, severity: Severity | str, message: str, **kw: Any) -> "Event":
        sev = severity.value if isinstance(severity, Severity) else str(severity)
        return Event(event_id=uuid.uuid4().hex, tenant_id=tenant_id,
                     severity=sev, message=message, **kw)

    @property
    def is_fatal(self) -> bool:
        return self.severity == Severity.FATAL.value

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["stack_frames"] = list(self.stack_frames)
        return d

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "Event":
        d = dict(d)
        d["stack_frames"] = tuple(d.get("stack_frames") or ())
        return Event(**d)


@dataclass
class RejectedItem:
    event_id: str
    reason: str
    detail: str = ""


@dataclass
class UploadResponse:
    """服务端对一批事件的逐条处理结果。"""
    accepted: list[str] = field(default_factory=list)    # 已入库的 event_id
    duplicates: list[str] = field(default_factory=list)  # 已存在、被去重的 event_id
    rejected: list[RejectedItem] = field(default_factory=list)

    def __post_init__(self) -> None:
        # 兼容从 JSON 反序列化得到的 dict
        self.rejected = [
            r if isinstance(r, RejectedItem) else RejectedItem(**r)
            for r in self.rejected
        ]

    def acked_ids(self) -> list[str]:
        """客户端可安全确认（不再重传）的事件 id。"""
        return self.accepted + self.duplicates
