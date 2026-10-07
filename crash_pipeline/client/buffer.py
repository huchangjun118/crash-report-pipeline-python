"""Persistent client-side event buffer (SQLite-backed).

Guarantees:
- events survive process restarts until acknowledged by the server;
- acknowledged events are never re-uploaded;
- when full, lower-priority events are evicted first; crash events are
  evicted last (never before ordinary telemetry).
"""
from __future__ import annotations

import json
import sqlite3
import threading

from crash_pipeline.events import Event, EventKind

# Higher priority = evicted later.
_PRIORITY = {
    EventKind.TELEMETRY: 0,
    EventKind.ERROR: 1,
    EventKind.CRASH: 2,
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    priority INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class PersistentEventBuffer:
    """Durable FIFO queue with priority-aware eviction."""

    def __init__(self, path: str, max_events: int = 10_000) -> None:
        self._max = max_events
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)
        self._db.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('evicted', '0')"
        )
        self._db.commit()

    def enqueue(self, event: Event) -> bool:
        """Persist an event; evict lower-priority events if full.

        Returns False only if the buffer is full of crash events and the
        incoming event could not be kept.
        """
        payload = json.dumps(event.to_dict())
        prio = _PRIORITY[event.kind]
        with self._lock, self._db:
            cur = self._db.execute(
                "INSERT OR IGNORE INTO events(event_id, priority, payload)"
                " VALUES (?, ?, ?)",
                (event.event_id, prio, payload),
            )
            if cur.rowcount == 0:
                return True  # already buffered; treat as kept
            new_seq = cur.lastrowid
            overflow = self._count() - self._max
            if overflow <= 0:
                return True
            # 1) evict non-crash events, lowest priority / oldest first;
            #    the newcomer itself is considered only as a last resort
            rows = self._db.execute(
                "SELECT seq FROM events WHERE priority < 2 AND seq != ?"
                " ORDER BY priority ASC, seq ASC LIMIT ?",
                (new_seq, overflow),
            ).fetchall()
            seqs = [r[0] for r in rows]
            self._delete_seqs(seqs)
            self._bump_evicted(len(seqs))
            overflow -= len(seqs)
            if overflow <= 0:
                return True
            # 2) only crashes remain: keep crashes, drop the newcomer if it
            #    is not a crash; otherwise evict the oldest crash(es)
            if prio < 2:
                self._db.execute(
                    "DELETE FROM events WHERE event_id = ?", (event.event_id,)
                )
                return False
            rows = self._db.execute(
                "SELECT seq, event_id FROM events ORDER BY seq ASC LIMIT ?",
                (overflow,),
            ).fetchall()
            self._delete_seqs([r[0] for r in rows])
            self._bump_evicted(len(rows))
            return all(r[1] != event.event_id for r in rows)

    def peek_batch(self, limit: int) -> list[Event]:
        """Return up to `limit` pending events in FIFO order (no removal)."""
        with self._lock:
            rows = self._db.execute(
                "SELECT payload FROM events ORDER BY seq ASC LIMIT ?", (limit,)
            ).fetchall()
        return [Event.from_dict(json.loads(r[0])) for r in rows]

    def acknowledge(self, event_ids: list[str]) -> int:
        """Permanently remove acknowledged events. Returns removed count."""
        if not event_ids:
            return 0
        with self._lock, self._db:
            cur = self._db.executemany(
                "DELETE FROM events WHERE event_id = ?",
                [(eid,) for eid in event_ids],
            )
            return cur.rowcount

    def pending_count(self) -> int:
        with self._lock:
            return self._count()

    def evicted_count(self) -> int:
        """Events evicted due to capacity (persisted across restarts)."""
        with self._lock:
            row = self._db.execute(
                "SELECT value FROM meta WHERE key = 'evicted'"
            ).fetchone()
        return int(row[0])

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # -- internals (caller holds the lock) --

    def _count(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def _delete_seqs(self, seqs: list[int]) -> None:
        if seqs:
            self._db.executemany(
                "DELETE FROM events WHERE seq = ?", [(s,) for s in seqs]
            )

    def _bump_evicted(self, n: int) -> None:
        if n:
            self._db.execute(
                "UPDATE meta SET value = CAST(value AS INTEGER) + ?"
                " WHERE key = 'evicted'",
                (n,),
            )
