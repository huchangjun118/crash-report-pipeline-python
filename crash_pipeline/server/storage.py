"""Durable server-side storage (SQLite).

- dedup: each event_id is inserted at most once; INSERT OR IGNORE is atomic,
  so concurrent ingesters cannot double-store the same event;
- issues: events grouped by fingerprint with occurrence counts, updated in
  the same transaction as the event insert — no half-written records;
- drops: rejected/dropped events recorded with a queryable reason;
- restarts resume from the last committed state (WAL + transactions).
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass

from crash_pipeline.events import Event
from crash_pipeline.server.quota import RejectReason

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    ingested_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS issues (
    issue_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    title TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    first_seen REAL NOT NULL,
    last_seen REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS drops (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    message TEXT NOT NULL,
    dropped_at REAL NOT NULL
);
"""


@dataclass
class IngestOutcome:
    event_id: str
    status: str  # "stored" | "duplicate" | "rejected"
    reason: str | None = None
    issue_id: str | None = None


@dataclass
class Issue:
    issue_id: str
    tenant_id: str
    fingerprint: str
    title: str
    event_count: int
    first_seen: float
    last_seen: float


def make_issue_id(tenant_id: str, fingerprint: str) -> str:
    return f"{tenant_id}:{fingerprint[:24]}"


class EventStore:
    def __init__(self, path: str) -> None:
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)
        self._db.commit()

    def record_event(self, event: Event, fingerprint: str) -> IngestOutcome:
        """Idempotently store an event and bump its issue counters."""
        issue_id = make_issue_id(event.tenant_id, fingerprint)
        now = time.time()
        with self._lock, self._db:
            cur = self._db.execute(
                "INSERT OR IGNORE INTO events"
                " (event_id, tenant_id, kind, issue_id, payload, ingested_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (event.event_id, event.tenant_id, event.kind.value, issue_id,
                 json.dumps(event.to_dict()), now),
            )
            if cur.rowcount == 0:
                row = self._db.execute(
                    "SELECT issue_id FROM events WHERE event_id = ?",
                    (event.event_id,),
                ).fetchone()
                return IngestOutcome(event.event_id, "duplicate",
                                     issue_id=row[0])
            self._db.execute(
                "INSERT INTO issues"
                " (issue_id, tenant_id, fingerprint, title, event_count,"
                "  first_seen, last_seen)"
                " VALUES (?, ?, ?, ?, 1, ?, ?)"
                " ON CONFLICT(issue_id) DO UPDATE SET"
                "   event_count = event_count + 1,"
                "   last_seen = MAX(last_seen, excluded.last_seen),"
                "   first_seen = MIN(first_seen, excluded.first_seen)",
                (issue_id, event.tenant_id, fingerprint, event.message[:200],
                 event.timestamp, event.timestamp),
            )
            return IngestOutcome(event.event_id, "stored", issue_id=issue_id)

    def record_drop(self, event: Event, reason: RejectReason) -> None:
        """Record a rejected/dropped event so it stays queryable."""
        with self._lock, self._db:
            self._db.execute(
                "INSERT INTO drops (event_id, tenant_id, reason, message,"
                " dropped_at) VALUES (?, ?, ?, ?, ?)",
                (event.event_id, event.tenant_id, reason.value,
                 event.message[:200], time.time()),
            )

    def get_issue(self, issue_id: str) -> Issue | None:
        with self._lock:
            row = self._db.execute(
                "SELECT issue_id, tenant_id, fingerprint, title, event_count,"
                " first_seen, last_seen FROM issues WHERE issue_id = ?",
                (issue_id,),
            ).fetchone()
        return Issue(*row) if row else None

    def list_issues(self, tenant_id: str | None = None) -> list[Issue]:
        sql = ("SELECT issue_id, tenant_id, fingerprint, title, event_count,"
               " first_seen, last_seen FROM issues")
        args: tuple = ()
        if tenant_id is not None:
            sql += " WHERE tenant_id = ?"
            args = (tenant_id,)
        sql += " ORDER BY last_seen DESC"
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        return [Issue(*r) for r in rows]

    def drop_stats(self, tenant_id: str | None = None) -> dict[str, int]:
        """reason -> count of dropped events."""
        sql = "SELECT reason, COUNT(*) FROM drops"
        args: tuple = ()
        if tenant_id is not None:
            sql += " WHERE tenant_id = ?"
            args = (tenant_id,)
        sql += " GROUP BY reason"
        with self._lock:
            return dict(self._db.execute(sql, args).fetchall())

    def event_count(self) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def close(self) -> None:
        with self._lock:
            self._db.close()
