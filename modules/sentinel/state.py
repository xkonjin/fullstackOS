"""Thread-safe shared state for Sentinel.

Bridges the monitor (async), web UI (async), and menu bar (main thread).
All cross-thread access goes through the Lock.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class SharedState:
    """Thread-safe state shared between monitor, web UI, and menu bar."""

    _lock: threading.Lock = field(default_factory=threading.Lock)
    services: Dict[str, dict] = field(default_factory=dict)
    tokens: List[dict] = field(default_factory=list)
    incidents: List[dict] = field(default_factory=list)
    auto_heal_paused: bool = False
    auto_heal_pause_until: Optional[datetime] = None
    overall_status: str = "healthy"  # healthy, degraded, critical
    sse_queues: List[Any] = field(default_factory=list)  # asyncio.Queue list
    last_check: Optional[datetime] = None

    def update_service(self, name: str, status_dict: dict) -> None:
        with self._lock:
            self.services[name] = {
                **status_dict,
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
            self._recalculate_overall()

    def update_tokens(self, token_list: list[dict]) -> None:
        with self._lock:
            self.tokens = token_list

    def add_incident(self, incident: dict) -> None:
        with self._lock:
            self.incidents.insert(0, {
                **incident,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            # Keep last 100 incidents
            self.incidents = self.incidents[:100]

    def set_auto_heal_paused(self, paused: bool, until: Optional[datetime] = None) -> None:
        with self._lock:
            self.auto_heal_paused = paused
            self.auto_heal_pause_until = until

    def is_auto_heal_active(self) -> bool:
        with self._lock:
            if not self.auto_heal_paused:
                return True
            if self.auto_heal_pause_until and datetime.now(timezone.utc) >= self.auto_heal_pause_until:
                self.auto_heal_paused = False
                self.auto_heal_pause_until = None
                return True
            return False

    def get_snapshot(self) -> dict:
        with self._lock:
            return {
                "services": dict(self.services),
                "tokens": list(self.tokens),
                "incidents": list(self.incidents[:20]),
                "overall": self.overall_status,
                "auto_heal_paused": self.auto_heal_paused,
                "auto_heal_pause_until": (
                    self.auto_heal_pause_until.isoformat()
                    if self.auto_heal_pause_until
                    else None
                ),
                "last_check": self.last_check.isoformat() if self.last_check else None,
            }

    def push_sse(self, event_type: str, data: Any) -> None:
        """Push event to all SSE subscriber queues."""
        with self._lock:
            dead: list[Any] = []
            for q in self.sse_queues:
                try:
                    q.put_nowait({"event": event_type, "data": data})
                except Exception:
                    dead.append(q)
            for q in dead:
                self.sse_queues.remove(q)

    def register_sse_queue(self, q: Any) -> None:
        with self._lock:
            self.sse_queues.append(q)

    def unregister_sse_queue(self, q: Any) -> None:
        with self._lock:
            if q in self.sse_queues:
                self.sse_queues.remove(q)

    def _recalculate_overall(self) -> None:
        """Must be called while holding _lock."""
        statuses = [s.get("status") for s in self.services.values()]
        if any(s in ("frozen", "down") for s in statuses):
            self.overall_status = "critical"
        elif any(s == "degraded" for s in statuses):
            self.overall_status = "degraded"
        else:
            self.overall_status = "healthy"
        self.last_check = datetime.now(timezone.utc)
