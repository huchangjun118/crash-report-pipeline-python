"""Server-side: dedup, fingerprint grouping, quota, concurrency, restart."""
import threading

from crash_pipeline.events import Event, EventKind, Severity
from crash_pipeline.server.fingerprint import (
    FingerprintConfig, compute_fingerprint)
from crash_pipeline.server.quota import QuotaManager, RejectReason, TenantPolicy
from crash_pipeline.server.storage import EventStore, make_issue_id


def crash(msg="boom", stack=("f1", "f2"), tenant="t"):
    return Event(kind=EventKind.CRASH, tenant_id=tenant, message=msg,
                 severity=Severity.FATAL, stacktrace=list(stack))


class TestDedup:
    def test_redelivery_stored_once(self, tmp_path):
        store = EventStore(str(tmp_path / "s.db"))
        e = crash()
        fp = compute_fingerprint(e, FingerprintConfig())
        assert store.record_event(e, fp).status == "stored"
        assert store.record_event(e, fp).status == "duplicate"
        assert store.event_count() == 1
        issue = store.get_issue(make_issue_id("t", fp))
        assert issue.event_count == 1  # no inflated counts

    def test_concurrent_dedup_no_race(self, tmp_path):
        store = EventStore(str(tmp_path / "s.db"))
        e = crash()
        fp = compute_fingerprint(e, FingerprintConfig())
        results = []

        def worker():
            results.append(store.record_event(e, fp).status)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert results.count("stored") == 1
        assert results.count("duplicate") == 19
        assert store.event_count() == 1


class TestGrouping:
    def test_order_independent_grouping(self, tmp_path):
        cfg = FingerprintConfig()
        # same crash, differing only in volatile details, shuffled order
        events = [
            crash(msg=f"segfault at 0x{i:x} for user {i}",
                  stack=("a", "b", "c"))
            for i in range(10)
        ]
        fps = {compute_fingerprint(e, cfg) for e in events}
        assert len(fps) == 1  # all map to one fingerprint

        store = EventStore(str(tmp_path / "s.db"))
        for e in reversed(events):  # arrival order must not matter
            store.record_event(e, compute_fingerprint(e, cfg))
        issues = store.list_issues("t")
        assert len(issues) == 1
        assert issues[0].event_count == 10

    def test_config_changes_grouping(self):
        e1 = crash(stack=("a", "b"))
        e2 = crash(stack=("x", "y"))
        with_stack = FingerprintConfig(stack_frames=2)
        no_stack = FingerprintConfig(stack_frames=0)
        assert compute_fingerprint(e1, with_stack) != compute_fingerprint(
            e2, with_stack)
        assert compute_fingerprint(e1, no_stack) == compute_fingerprint(
            e2, no_stack)


class TestQuota:
    def test_quota_rejection_is_queryable(self, tmp_path):
        store = EventStore(str(tmp_path / "s.db"))
        qm = QuotaManager()
        qm.set_policy("t", TenantPolicy(sample_rate=1.0,
                                        max_events_per_window=2,
                                        window_seconds=60))
        tel = [Event(kind=EventKind.TELEMETRY, tenant_id="t", message=f"m{i}")
               for i in range(5)]
        rejected = 0
        for e in tel:
            r = qm.admit(e, now=1000.0)
            if not r.accepted:
                store.record_drop(e, r.reason)
                rejected += 1
        assert rejected == 3
        assert store.drop_stats("t") == {"quota_exceeded": 3}

    def test_fatal_never_sampled_or_quota_rejected(self):
        qm = QuotaManager()
        qm.set_policy("t", TenantPolicy(sample_rate=0.0,
                                        max_events_per_window=0))
        e = crash()
        assert qm.admit(e).accepted

    def test_sampling_deterministic_per_event(self):
        qm = QuotaManager()
        qm.set_policy("t", TenantPolicy(sample_rate=0.5,
                                        max_events_per_window=10_000))
        e = Event(kind=EventKind.TELEMETRY, tenant_id="t", message="m")
        first = qm.admit(e, now=0.0)
        # a redelivery of the same event must get the same sampling verdict
        if not first.accepted:
            again = qm.admit(e, now=0.0)
            assert not again.accepted
            assert again.reason == RejectReason.SAMPLED_OUT


class TestRestart:
    def test_store_survives_restart(self, tmp_path):
        path = str(tmp_path / "s.db")
        store = EventStore(path)
        e = crash()
        fp = compute_fingerprint(e, FingerprintConfig())
        out = store.record_event(e, fp)
        store.record_drop(e, RejectReason.OVERLOADED)
        store.close()

        reopened = EventStore(path)
        assert reopened.event_count() == 1
        assert reopened.get_issue(out.issue_id).event_count == 1
        assert reopened.drop_stats("t") == {"overloaded": 1}
        # and dedup state survives too: no double-store after restart
        assert reopened.record_event(e, fp).status == "duplicate"
        assert reopened.event_count() == 1
