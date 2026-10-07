"""Configurable crash fingerprinting.

The fingerprint is a pure function of (event, config): the same event under
the same config always maps to the same fingerprint regardless of arrival
order, so grouping results are deterministic.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from crash_pipeline.events import Event

_HEX_ADDR = re.compile(r"0x[0-9a-fA-F]+")
_NUMBERS = re.compile(r"\d+")


@dataclass
class FingerprintConfig:
    """Which parts of an event participate in the fingerprint."""
    use_kind: bool = True
    use_message: bool = True
    # number of top stack frames included; 0 = ignore the stack
    stack_frames: int = 3
    # attribute keys included in the fingerprint (sorted for determinism)
    attribute_keys: tuple[str, ...] = field(default_factory=tuple)
    # normalize digits in messages (e.g. "user 42" == "user 7")
    normalize_numbers: bool = True


def _normalize(text: str, config: FingerprintConfig) -> str:
    text = _HEX_ADDR.sub("0x?", text)
    if config.normalize_numbers:
        text = _NUMBERS.sub("?", text)
    return text


def compute_fingerprint(event: Event, config: FingerprintConfig) -> str:
    parts: list[str] = []
    if config.use_kind:
        parts.append(f"kind={event.kind.value}")
    if config.use_message:
        parts.append(f"msg={_normalize(event.message, config)}")
    if config.stack_frames > 0:
        frames = [_normalize(f, config) for f in event.stacktrace]
        parts.append(f"stack={'|'.join(frames[: config.stack_frames])}")
    for key in sorted(config.attribute_keys):
        if key in event.attributes:
            parts.append(f"attr:{key}={event.attributes[key]}")
    material = "\n".join(parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
