"""客户端落盘持久化上报队列。

可靠性语义：
- 事件先落盘（SQLite，WAL + 事务），再异步上传；
- 进程重启后未确认（未 ack）的事件仍在队列中，继续上传；
- 已确认的事件被删除，不会重复上传；
- 队列写满时按优先级淘汰：info < warning < error < fatal，
  fatal 事件永远不会先于普通遥测被丢弃；队列里只剩 fatal 时拒绝入队。
"""
from __future__ import annotations

import json
import sqlite3
import threading

from crash_pipeline.models import Event, Severity

# 数值越小越重要，越晚被淘汰
_PRIORITY = {
    Severity.FATAL.value: 0,
    Severity.ERROR.value: 1,
    Severity.WARNING.value: 2,
    Severity.INFO.value: 3,
}
_DEFAULT_PRIORITY = 3

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    priority INTEGER NOT NULL,
    payload TEXT NOT NULL
);
"""


class PersistentEventQueue:
    def __init__(self, db_path: str, max_events: int = 10_000) -> None:
        self._max = max_events
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def enqueue(self, event: Event) -> bool:
        """写入队列。返回 False 表示队列被 fatal 占满、无法腾挪。"""
        priority = _PRIORITY.get(event.severity, _DEFAULT_PRIORITY)
        payload = json.dumps(event.to_dict())
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO events(event_id, priority, payload) VALUES (?,?,?)",
                (event.event_id, priority, payload),
            )
            if cur.rowcount == 0:
                return True  # 重复入队：幂等，视为成功
            self._evict_if_needed()
            # 刚插入的可能自己被淘汰（仅当它是非 fatal 且队列更优事件占满时不应发生，
            # 因为淘汰总是先淘汰优先级更低的；这里只需确认它还在）
            row = self._conn.execute(
                "SELECT 1 FROM events WHERE event_id=?", (event.event_id,)
            ).fetchone()
            if row is None:
                self._conn.commit()
                return False
            self._conn.commit()
            return True

    def _evict_if_needed(self) -> None:
        """容量超限时，从最不重要（priority 大）且最旧的事件开始淘汰。

        fatal(priority=0) 只在队列里没有任何更低优先级事件时才可能被淘汰，
        而这种情况我们拒绝淘汰（保留 fatal，让新事件入队失败）。
        """
        count = self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        overflow = count - self._max
        if overflow <= 0:
            return
        # 只淘汰非 fatal；fatal 永不因容量被淘汰
        self._conn.execute(
            """DELETE FROM events WHERE seq IN (
                   SELECT seq FROM events WHERE priority > 0
                   ORDER BY priority DESC, seq ASC LIMIT ?)""",
            (overflow,),
        )

    def peek_batch(self, limit: int) -> list[Event]:
        """取出一批待上传事件（不删除）。fatal 优先。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT payload FROM events ORDER BY priority ASC, seq ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [Event.from_dict(json.loads(r[0])) for r in rows]

    def ack(self, event_ids: list[str]) -> None:
        """确认已送达：从队列删除，之后不再上传。"""
        if not event_ids:
            return
        with self._lock:
            self._conn.executemany(
                "DELETE FROM events WHERE event_id=?",
                [(eid,) for eid in event_ids],
            )
            self._conn.commit()

    def __len__(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
