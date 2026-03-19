"""SQLite persistence for Sentinel incidents, events, and actions."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger("sentinel.db")

# Store DB in a data directory, not alongside code. Fall back to module dir.
_DATA_DIR = Path.home() / ".sentinel"
_DATA_DIR.mkdir(exist_ok=True)
_DB_PATH = _DATA_DIR / "sentinel.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL    NOT NULL,
    severity    TEXT    NOT NULL,
    component   TEXT    NOT NULL,
    signature   TEXT    NOT NULL,
    summary     TEXT    NOT NULL,
    actions     TEXT    DEFAULT '[]',
    resolved_at REAL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL    NOT NULL,
    event_type  TEXT    NOT NULL,
    component   TEXT,
    detail      TEXT    DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS actions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL    NOT NULL,
    action_type TEXT    NOT NULL,
    target      TEXT    NOT NULL,
    result      TEXT    NOT NULL,
    detail      TEXT    DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_incidents_ts ON incidents(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_actions_ts ON actions(timestamp);
CREATE INDEX IF NOT EXISTS idx_incidents_component ON incidents(component);
"""


class SentinelDB:
    """Thin wrapper around sqlite3 for Sentinel persistence."""

    def __init__(self, path: Optional[Path] = None):
        self._path = str(path or _DB_PATH)
        self._conn: Optional[sqlite3.Connection] = None

    def open(self) -> None:
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self.open()
        assert self._conn is not None
        return self._conn

    # -- incidents -----------------------------------------------------------

    def log_incident(
        self,
        severity: str,
        component: str,
        signature: str,
        summary: str,
        actions: Optional[list[str]] = None,
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO incidents (timestamp, severity, component, signature, summary, actions) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (time.time(), severity, component, signature, summary, json.dumps(actions or [])),
        )
        self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def resolve_incident(self, incident_id: int) -> None:
        self.conn.execute(
            "UPDATE incidents SET resolved_at = ? WHERE id = ?",
            (time.time(), incident_id),
        )
        self.conn.commit()

    def get_recent_incidents(self, limit: int = 50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM incidents ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_unresolved_incidents(self, component: Optional[str] = None) -> list[dict]:
        if component:
            rows = self.conn.execute(
                "SELECT * FROM incidents WHERE resolved_at IS NULL AND component = ? "
                "ORDER BY timestamp DESC",
                (component,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM incidents WHERE resolved_at IS NULL ORDER BY timestamp DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # -- events --------------------------------------------------------------

    def log_event(
        self,
        event_type: str,
        component: Optional[str] = None,
        detail: Optional[dict] = None,
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO events (timestamp, event_type, component, detail) VALUES (?, ?, ?, ?)",
            (time.time(), event_type, component, json.dumps(detail or {})),
        )
        self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def get_recent_events(self, limit: int = 100) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM events ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # -- actions -------------------------------------------------------------

    def log_action(
        self,
        action_type: str,
        target: str,
        result: str,
        detail: Optional[dict] = None,
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO actions (timestamp, action_type, target, result, detail) VALUES (?, ?, ?, ?, ?)",
            (time.time(), action_type, target, result, json.dumps(detail or {})),
        )
        self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def get_actions_since(self, since: float, target: Optional[str] = None) -> list[dict]:
        if target:
            rows = self.conn.execute(
                "SELECT * FROM actions WHERE timestamp >= ? AND target = ? ORDER BY timestamp DESC",
                (since, target),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM actions WHERE timestamp >= ? ORDER BY timestamp DESC",
                (since,),
            ).fetchall()
        return [dict(r) for r in rows]

    def count_actions_since(self, since: float, target: str, action_type: str = "restart") -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM actions WHERE timestamp >= ? AND target = ? AND action_type = ?",
            (since, target, action_type),
        ).fetchone()
        return row["cnt"] if row else 0
