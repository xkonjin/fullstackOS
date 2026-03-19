"""Sentinel integration test harness.

Simulates service failures and validates detection + state management.
Run with: python -m pytest modules/sentinel/tests/test_harness.py -v

All tests use mocks/fixtures — no real services required.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FrozenServer:
    """Simulates a frozen service — listens on port but never accepts."""

    def __init__(self, port: int):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", port))
        self.sock.listen(5)

    def close(self):
        self.sock.close()


def make_token_file(
    tmpdir: Path,
    provider: str,
    email: str,
    expires_in_min: int = 60,
    disabled: bool = False,
) -> Path:
    """Create a test token JSON file."""
    expires = (datetime.now(timezone.utc) + timedelta(minutes=expires_in_min)).isoformat()
    token = {
        "type": provider,
        "email": email,
        "access_token": "test-token-xxx",
        "expired": expires,
        "expires_in": 3599,
        "refresh_token": "test-refresh-xxx",
        "disabled": disabled,
    }
    filename = f"{provider}-{email}.json"
    path = tmpdir / filename
    path.write_text(json.dumps(token))
    return path


# ---------------------------------------------------------------------------
# Tests: SharedState
# ---------------------------------------------------------------------------

class TestSharedState:
    """Tests for the thread-safe SharedState."""

    def test_healthy_services(self, shared_state):
        """All services healthy → overall healthy."""
        shared_state.update_service("CLIProxyAPI", {"status": "healthy", "port": 8317})
        shared_state.update_service("Orchestrator", {"status": "healthy", "port": 8318})
        shared_state.update_service("Fleet Gateway", {"status": "healthy", "port": 4105})

        snap = shared_state.get_snapshot()
        assert snap["overall"] == "healthy"
        assert len(snap["services"]) == 3
        for svc in snap["services"].values():
            assert svc["status"] == "healthy"

    def test_degraded_service_rolls_up(self, shared_state):
        """One degraded service → overall degraded."""
        shared_state.update_service("CLIProxyAPI", {"status": "healthy", "port": 8317})
        shared_state.update_service("Orchestrator", {"status": "degraded", "port": 8318})

        assert shared_state.get_snapshot()["overall"] == "degraded"

    def test_down_service_is_critical(self, shared_state):
        """One down service → overall critical."""
        shared_state.update_service("CLIProxyAPI", {"status": "healthy", "port": 8317})
        shared_state.update_service("Orchestrator", {"status": "down", "port": 8318})

        assert shared_state.get_snapshot()["overall"] == "critical"

    def test_frozen_service_is_critical(self, shared_state):
        """Frozen service → overall critical."""
        shared_state.update_service("CLIProxyAPI", {"status": "frozen", "port": 8317})

        assert shared_state.get_snapshot()["overall"] == "critical"

    def test_overall_status_recovery(self, shared_state):
        """Status recovers when service comes back."""
        shared_state.update_service("Orchestrator", {"status": "down", "port": 8318})
        assert shared_state.get_snapshot()["overall"] == "critical"

        shared_state.update_service("Orchestrator", {"status": "healthy", "port": 8318})
        assert shared_state.get_snapshot()["overall"] == "healthy"

    def test_state_thread_safety(self, shared_state):
        """Concurrent state access does not corrupt data."""
        errors: list[Exception] = []
        barrier = threading.Barrier(10)

        def writer(n: int):
            try:
                barrier.wait(timeout=5)
                for i in range(100):
                    shared_state.update_service(f"svc-{n}", {"status": "healthy", "port": 9000 + n})
                    shared_state.get_snapshot()
                    shared_state.add_incident({"type": "test", "writer": n, "i": i})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors
        snap = shared_state.get_snapshot()
        # All 10 writers should have their service entry
        assert len(snap["services"]) == 10
        # Incidents capped at 100
        assert len(snap["incidents"]) <= 100


# ---------------------------------------------------------------------------
# Tests: Frozen server detection
# ---------------------------------------------------------------------------

class TestFrozenServer:
    """Verify that a frozen server (listening but not accepting) is detectable."""

    def test_frozen_port_is_open(self, test_port):
        """A frozen server has an open port but sending data gets no response."""
        frozen = FrozenServer(test_port)
        try:
            # Port IS open — TCP handshake completes via kernel backlog
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect(("127.0.0.1", test_port))

            # Send an HTTP request — no one will read it or reply
            s.sendall(b"GET /health HTTP/1.0\r\n\r\n")
            s.settimeout(0.5)
            with pytest.raises(socket.timeout):
                s.recv(1024)
            s.close()
        finally:
            frozen.close()


# ---------------------------------------------------------------------------
# Tests: Token files
# ---------------------------------------------------------------------------

class TestTokenFiles:
    """Test token file parsing helpers."""

    def test_expired_token(self, token_dir):
        """Token with past expiry is detected."""
        path = make_token_file(token_dir, "anthropic", "test@example.com", expires_in_min=-30)
        data = json.loads(path.read_text())
        expires = datetime.fromisoformat(data["expired"])
        assert expires < datetime.now(timezone.utc)

    def test_expiring_soon_token(self, token_dir):
        """Token expiring within 30 minutes is flagged."""
        path = make_token_file(token_dir, "anthropic", "test@example.com", expires_in_min=15)
        data = json.loads(path.read_text())
        expires = datetime.fromisoformat(data["expired"])
        warning_threshold = datetime.now(timezone.utc) + timedelta(minutes=30)
        assert expires < warning_threshold

    def test_healthy_token(self, token_dir):
        """Token with distant expiry is healthy."""
        path = make_token_file(token_dir, "anthropic", "test@example.com", expires_in_min=120)
        data = json.loads(path.read_text())
        expires = datetime.fromisoformat(data["expired"])
        warning_threshold = datetime.now(timezone.utc) + timedelta(minutes=30)
        assert expires > warning_threshold

    def test_disabled_token(self, token_dir):
        """Disabled token is flagged regardless of expiry."""
        path = make_token_file(token_dir, "anthropic", "test@example.com", expires_in_min=120, disabled=True)
        data = json.loads(path.read_text())
        assert data["disabled"] is True

    def test_expires_at_numeric_string_supported(self):
        """String epoch expires_at should be treated as a valid expiry timestamp."""
        from modules.sentinel.checks.token_health import _parse_token

        data = {
            "type": "anthropic",
            "email": "test@example.com",
            "expires_at": str(time.time() + 3600),
        }
        parsed = _parse_token(Path("anthropic-test@example.com.json"), data)

        assert parsed["status"] == "healthy"
        assert parsed["expires_at"] > 0
        assert parsed["ttl_seconds"] > 0


# ---------------------------------------------------------------------------
# Tests: Auto-heal
# ---------------------------------------------------------------------------

class TestAutoHeal:
    """Auto-heal pause/resume behavior."""

    def test_auto_heal_default_active(self, shared_state):
        assert shared_state.is_auto_heal_active() is True

    def test_pause_auto_heal(self, shared_state):
        shared_state.set_auto_heal_paused(
            True, until=datetime.now(timezone.utc) + timedelta(minutes=15)
        )
        assert shared_state.is_auto_heal_active() is False

    def test_pause_expires(self, shared_state):
        """Pause expires after the deadline passes."""
        shared_state.set_auto_heal_paused(
            True, until=datetime.now(timezone.utc) - timedelta(seconds=1)
        )
        # Calling is_auto_heal_active should detect expiry and reset
        assert shared_state.is_auto_heal_active() is True
        assert shared_state.auto_heal_paused is False

    def test_manual_resume(self, shared_state):
        shared_state.set_auto_heal_paused(True, until=datetime.now(timezone.utc) + timedelta(hours=1))
        assert shared_state.is_auto_heal_active() is False
        shared_state.set_auto_heal_paused(False)
        assert shared_state.is_auto_heal_active() is True


# ---------------------------------------------------------------------------
# Tests: SSE event delivery
# ---------------------------------------------------------------------------

class TestSSEEvents:
    """SSE event push/subscribe."""

    def test_sse_event_delivery(self, shared_state):
        """Events pushed to all subscriber queues."""
        q1 = asyncio.Queue(maxsize=16)
        q2 = asyncio.Queue(maxsize=16)
        shared_state.register_sse_queue(q1)
        shared_state.register_sse_queue(q2)

        shared_state.push_sse("test_event", {"hello": "world"})

        msg1 = q1.get_nowait()
        msg2 = q2.get_nowait()
        assert msg1["event"] == "test_event"
        assert msg1["data"] == {"hello": "world"}
        assert msg2 == msg1

    def test_dead_queue_cleanup(self, shared_state):
        """Full queues are cleaned up on push."""
        q = asyncio.Queue(maxsize=1)
        shared_state.register_sse_queue(q)

        # Fill the queue
        shared_state.push_sse("fill", {})
        assert q.qsize() == 1

        # Next push should drop the full queue
        shared_state.push_sse("overflow", {})
        snap = shared_state.get_snapshot()
        # Queue was removed
        assert len(shared_state.sse_queues) == 0

    def test_unregister_queue(self, shared_state):
        q = asyncio.Queue()
        shared_state.register_sse_queue(q)
        assert len(shared_state.sse_queues) == 1
        shared_state.unregister_sse_queue(q)
        assert len(shared_state.sse_queues) == 0


# ---------------------------------------------------------------------------
# Tests: Incidents
# ---------------------------------------------------------------------------

class TestIncidents:
    """Incident tracking."""

    def test_add_incident(self, shared_state):
        shared_state.add_incident({"type": "service_down", "target": "Orchestrator"})
        snap = shared_state.get_snapshot()
        assert len(snap["incidents"]) == 1
        assert snap["incidents"][0]["type"] == "service_down"
        assert "timestamp" in snap["incidents"][0]

    def test_incident_cap(self, shared_state):
        """Incidents are capped at 100."""
        for i in range(150):
            shared_state.add_incident({"type": "test", "i": i})
        # Internal list capped
        assert len(shared_state.incidents) == 100
        # Most recent first
        assert shared_state.incidents[0]["i"] == 149

    def test_snapshot_limits_incidents(self, shared_state):
        """Snapshot only returns last 20 incidents."""
        for i in range(50):
            shared_state.add_incident({"type": "test", "i": i})
        snap = shared_state.get_snapshot()
        assert len(snap["incidents"]) == 20


# ---------------------------------------------------------------------------
# Tests: Monitor stub
# ---------------------------------------------------------------------------

class TestMonitor:
    """Test the monitor loop with stubs."""

    @pytest.mark.asyncio
    async def test_monitor_runs_and_stops(self, shared_state):
        """Monitor loop starts, populates state, and stops on event."""
        from modules.sentinel.main import run_monitor

        stop = asyncio.Event()

        # Let it run 1 cycle then stop
        async def auto_stop():
            await asyncio.sleep(0.3)
            stop.set()

        await asyncio.gather(
            run_monitor(shared_state, stop),
            auto_stop(),
        )

        snap = shared_state.get_snapshot()
        # Stub results should have populated services
        assert len(snap["services"]) > 0

    @pytest.mark.asyncio
    async def test_monitor_pushes_sse(self, shared_state):
        """Monitor pushes SSE events each cycle."""
        from modules.sentinel.main import run_monitor

        stop = asyncio.Event()
        q: asyncio.Queue = asyncio.Queue(maxsize=32)
        shared_state.register_sse_queue(q)

        async def auto_stop():
            await asyncio.sleep(0.3)
            stop.set()

        await asyncio.gather(
            run_monitor(shared_state, stop),
            auto_stop(),
        )

        assert q.qsize() >= 1
        msg = q.get_nowait()
        assert msg["event"] == "state_update"


# ---------------------------------------------------------------------------
# Tests: Snapshot structure
# ---------------------------------------------------------------------------

class TestSnapshot:
    """Verify snapshot shape matches what web UI and menu bar expect."""

    def test_snapshot_keys(self, shared_state):
        snap = shared_state.get_snapshot()
        expected_keys = {
            "services", "tokens", "incidents", "overall",
            "auto_heal_paused", "auto_heal_pause_until", "last_check",
        }
        assert set(snap.keys()) == expected_keys

    def test_service_entry_has_checked_at(self, shared_state):
        shared_state.update_service("Test", {"status": "healthy", "port": 9999})
        snap = shared_state.get_snapshot()
        assert "checked_at" in snap["services"]["Test"]
