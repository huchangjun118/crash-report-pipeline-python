"""采样与配额。

- 采样只作用于普通遥测（非 fatal）；fatal 永远全量保留；
- 采样决策基于 event_id 的哈希：同一事件重投多次结论一致，
  不会因重试导致计数虚高或前后矛盾；
- 超出配额按策略处理：reject（全部拒绝）或 degrade（只保留 fatal）；
- 所有拒绝都带原因，由调用方落库供查询，不静默吞掉。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from crash_pipeline.models import Event, RejectReason


@dataclass
class TenantPolicy:
    sample_rate: float = 1.0      # 仅作用于普通遥测；fatal 永远全量保留
    max_events: int = 100_000     # 租户配额（已接受事件总数）
    over_quota: str = "reject"    # reject | degrade（degrade: 超配后只保留 fatal）


class QuotaManager:
    def __init__(self, default_policy: TenantPolicy | None = None) -> None:
        self._default = default_policy or TenantPolicy()
        self._policies: dict[str, TenantPolicy] = {}

    def set_policy(self, tenant_id: str, policy: TenantPolicy) -> None:
        self._policies[tenant_id] = policy

    def policy_for(self, tenant_id: str) -> TenantPolicy:
        return self._policies.get(tenant_id, self._default)

    def admit(self, event: Event, current_usage: int) -> tuple[bool, str]:
        """返回 (是否放行, 拒绝原因)。原因为空串表示放行。"""
        if event.is_fatal:
            return True, ""  # 致命错误全量保留，不采样、不受配额限制
        policy = self.policy_for(event.tenant_id)
        if current_usage >= policy.max_events:
            # reject 与 degrade 对非 fatal 都是拒绝，区别在 fatal（上面已放行）
            return False, RejectReason.QUOTA_EXCEEDED.value
        if not self._sampled_in(event.event_id, policy.sample_rate):
            return False, RejectReason.SAMPLED_OUT.value
        return True, ""

    @staticmethod
    def _sampled_in(event_id: str, rate: float) -> bool:
        if rate >= 1.0:
            return True
        if rate <= 0.0:
            return False
        h = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        bucket = int(h[:8], 16) / 0xFFFFFFFF
        return bucket < rate
