"""可配置的事件指纹：把相同崩溃归并为同一问题。

- 指纹是 (事件内容, 配置) 的纯函数，与事件到达顺序无关；
- fields 可配置，支持：
    exception_type / message / severity / tenant_id /
    stack_top（前 stack_depth 帧）/ stack_full / tag:<name>
- 归一化：message 中的数字折叠为 '#'，堆栈帧去掉行号，
  避免同类崩溃因变量值或行号漂移被拆成多个问题。
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from crash_pipeline.models import Event

_NUM_RE = re.compile(r"\d+")
_LINE_RE = re.compile(r":\d+(?::\d+)?$")


@dataclass
class FingerprintConfig:
    fields: list[str] = field(default_factory=lambda: ["exception_type", "stack_top"])
    stack_depth: int = 3


def _norm_message(msg: str) -> str:
    return _NUM_RE.sub("#", msg.strip())


def _norm_frame(frame: str) -> str:
    return _LINE_RE.sub("", frame.strip())


def _field_value(event: Event, name: str, config: FingerprintConfig) -> str:
    if name == "exception_type":
        return event.exception_type.strip().lower()
    if name == "message":
        return _norm_message(event.message)
    if name == "severity":
        return event.severity
    if name == "tenant_id":
        return event.tenant_id
    if name == "stack_top":
        frames = [_norm_frame(f) for f in event.stack_frames[: config.stack_depth]]
        return "\n".join(frames)
    if name == "stack_full":
        return "\n".join(_norm_frame(f) for f in event.stack_frames)
    if name.startswith("tag:"):
        return event.tags.get(name[4:], "")
    raise ValueError(f"unknown fingerprint field: {name!r}")


def compute_fingerprint(event: Event, config: FingerprintConfig | None = None) -> str:
    config = config or FingerprintConfig()
    parts = [f"{name}={_field_value(event, name, config)}" for name in config.fields]
    material = "\x1f".join(parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()
