"""服务端存储：去重、聚合、查询。

- SQLite 持久化，WAL；每个批次一个事务（BEGIN IMMEDIATE），
  事件行与聚合同事务提交：要么都落盘，要么都不落盘，不留半写记录；
- events.event_id 主键 + INSERT OR IGNORE：同一事件无论重投多少次、
  多少并发请求同时处理，只入库一次，结果收敛；
- 重复投递不增加聚合计数，计数不虚高；
- 重启后从已提交状态继续，未提交的事务由 SQLite 自动回滚。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from crash_pipeline.models import Event

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    severity TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    payload TEXT NOT NULL,
    received_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS issues (
    tenant_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    count INTEGER NOT NULL,
    first_seen REAL NOT NULL,
    last_seen REAL NOT NULL,
    sample_message TEXT NOT NULL,
    severity TEXT NOT NULL,
    PRIMARY KEY (tenant_id, fingerprint)
);
CREATE TABLE IF NOT EXISTS rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class EventStore:
    def __init__(self, db_path: str) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def ingest(self, events: list[Event], fingerprints: dict[str, str]) -> dict[str, list[str]]:
        """批量入库，单事务。返回 {"accepted": [...], "duplicates": [...]}。幂等。"""
        accepted: list[str] = []
        duplicates: list[str] = []
        now = time.time()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for ev in events:
                    fp = fingerprints[ev.event_id]
                    cur = self._conn.execute(
                        "INSERT OR IGNORE INTO events"
                        "(event_id, tenant_id, severity, fingerprint, payload, received_at)"
                        " VALUES (?,?,?,?,?,?)",
                        (ev.event_id, ev.tenant_id, ev.severity, fp,
                         json.dumps(ev.to_dict()), now),
                    )
                    if cur.rowcount == 0:
                        duplicates.append(ev.event_id)  # 已存在：去重，不计数
                        continue
                    accepted.append(ev.event_id)
                    self._conn.execute(
                        """INSERT INTO issues
                               (tenant_id, fingerprint, count, first_seen, last_seen,
                                sample_message, severity)
                           VALUES (?,?,?,?,?,?,?)
                           ON CONFLICT(tenant_id, fingerprint) DO UPDATE SET
                               count = count + 1,
                               last_seen = excluded.last_seen""",
                        (ev.tenant_id, fp, 1, now, now, ev.message, ev.severity),
                    )
                # 记录已提交位置（批次序号单调递增）
                self._conn.execute(
                    """INSERT INTO meta(key, value) VALUES ('committed_batches', '1')
                       ON CONFLICT(key) DO UPDATE SET value = CAST(value AS INTEGER) + 1"""
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return {"accepted": accepted, "duplicates": duplicates}

    def record_rejection(self, event_id: str, tenant_id: str, reason: str,
                         detail: str = "") -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO rejections(event_id, tenant_id, reason, detail, ts)"
                " VALUES (?,?,?,?,?)",
                (event_id, tenant_id, reason, detail, time.time()),
            )
            self._conn.commit()

    def usage(self, tenant_id: str) -> int:
        """租户已接受事件总数（配额依据）。"""
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) FROM events WHERE tenant_id=?", (tenant_id,)
            ).fetchone()[0]

    def issues(self, tenant_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT fingerprint, count, first_seen, last_seen, sample_message, severity"
                " FROM issues WHERE tenant_id=? ORDER BY count DESC",
                (tenant_id,),
            ).fetchall()
        return [
            {"fingerprint": r[0], "count": r[1], "first_seen": r[2],
             "last_seen": r[3], "sample_message": r[4], "severity": r[5]}
            for r in rows
        ]

    def rejections(self, tenant_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, reason, detail, ts FROM rejections"
                " WHERE tenant_id=? ORDER BY id",
                (tenant_id,),
            ).fetchall()
        return [{"event_id": r[0], "reason": r[1], "detail": r[2], "ts": r[3]}
                for r in rows]

    def committed_batches(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='committed_batches'").fetchone()
        return int(row[0]) if row else 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()
