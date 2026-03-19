"""Tests for nanoclaw self-healing daemon.

Run: cd ~/Dev/fullstackOS && python3 -m pytest tests/test_nanoclaw.py -v
"""

import json
import os
import sys
import time
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Add nanoclaw to path
sys.path.insert(0, str(Path(__file__).parent.parent / "services" / "nanoclaw"))

# Override state/log dirs BEFORE importing nanoclaw to prevent test contamination
import tempfile
_test_dir = Path(tempfile.mkdtemp(prefix="nanoclaw-test-"))
os.environ["NANOCLAW_STATE_DIR"] = str(_test_dir)

import nanoclaw

# Redirect nanoclaw logging to test dir
nanoclaw.STATE_DIR = _test_dir
nanoclaw.STATE_FILE = _test_dir / "state.json"
nanoclaw.LOG_DIR = _test_dir / "logs"
nanoclaw.LOG_DIR.mkdir(exist_ok=True)

from nanoclaw import (
    Nanoclaw, load_state, save_state, _default_state, _now_iso,
    probe_services, probe_providers, restart_service, clean_logs,
    check_disk_space, send_alert, http_get, http_post, tcp_check,
    SERVICES, STATE_FILE, STATE_DIR, MAX_RESTARTS_PER_HOUR,
)
from refreshers import (
    TokenRefresher, _refresh_claude, _refresh_codex, _refresh_antigravity,
    _refresh_gemini, _refresh_kimi, _parse_iso, _write_token, _read_token,
)
from reasoning import KimiReasoner


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_state_dir(tmp_path):
    """Override nanoclaw state dir to temp."""
    state_file = tmp_path / "state.json"
    with patch.object(nanoclaw, "STATE_FILE", state_file), \
         patch.object(nanoclaw, "STATE_DIR", tmp_path):
        yield tmp_path


@pytest.fixture
def tmp_auth_dir(tmp_path):
    """Create a temp auth dir with sample token files."""
    auth_dir = tmp_path / "auth"
    auth_dir.mkdir()

    # Claude token expiring in 30 minutes (should refresh)
    soon = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
    (auth_dir / "claude-test@example.com.json").write_text(json.dumps({
        "type": "claude",
        "email": "test@example.com",
        "access_token": "at_test",
        "refresh_token": "rt_test",
        "expired": soon,
    }))

    # Claude token expiring in 6 hours (should NOT refresh)
    later = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
    (auth_dir / "claude-safe@example.com.json").write_text(json.dumps({
        "type": "claude",
        "email": "safe@example.com",
        "access_token": "at_safe",
        "refresh_token": "rt_safe",
        "expired": later,
    }))

    # Antigravity token expired (should refresh)
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    (auth_dir / "antigravity-test@example.com.json").write_text(json.dumps({
        "type": "antigravity",
        "email": "test@example.com",
        "access_token": "ya29.expired",
        "refresh_token": "1//03refreshme",
        "expired": past,
        "project_id": "test-project",
    }))

    # Disabled token (should be skipped)
    (auth_dir / "claude-disabled@example.com.json").write_text(json.dumps({
        "type": "claude",
        "email": "disabled@example.com",
        "access_token": "at_disabled",
        "refresh_token": "rt_disabled",
        "expired": past,
        "disabled": True,
    }))

    # Gemini token with nested structure
    (auth_dir / "gemini-test@example.com.json").write_text(json.dumps({
        "type": "gemini",
        "email": "test@example.com",
        "token": {
            "access_token": "ya29.gemini",
            "refresh_token": "1//03gemini",
            "client_id": "test-client-id",
            "client_secret": "test-secret",
            "expiry": soon,
        },
    }))

    yield auth_dir


# ---------------------------------------------------------------------------
# State management tests
# ---------------------------------------------------------------------------

class TestState:
    def test_default_state(self):
        state = _default_state()
        assert "services" in state
        assert "tokens" in state
        assert "kimi_usage" in state

    def test_save_load_roundtrip(self, tmp_state_dir):
        state = _default_state()
        state["services"]["test"] = {"status": "healthy"}
        nanoclaw.STATE_FILE = tmp_state_dir / "state.json"
        save_state(state)
        loaded = load_state()
        assert loaded["services"]["test"]["status"] == "healthy"

    def test_load_missing_file(self, tmp_state_dir):
        nanoclaw.STATE_FILE = tmp_state_dir / "nonexistent.json"
        state = load_state()
        assert state == _default_state()

    def test_load_corrupt_file(self, tmp_state_dir):
        bad_file = tmp_state_dir / "state.json"
        bad_file.write_text("not json{{{")
        nanoclaw.STATE_FILE = bad_file
        state = load_state()
        assert state == _default_state()


# ---------------------------------------------------------------------------
# Health probe tests
# ---------------------------------------------------------------------------

class TestHealthProbes:
    def test_probe_healthy_services(self):
        state = _default_state()
        with patch.object(nanoclaw, "http_get", return_value=(200, '{"status":"ok","accounts":{"healthy":20,"total":22}}')), \
             patch.object(nanoclaw, "tcp_check", return_value=True):
            failures = asyncio.run(probe_services(state))
            assert len(failures) == 0
            assert state["services"]["cliproxyapi"]["status"] == "healthy"
            assert state["services"]["orchestrator"]["status"] == "healthy"

    def test_probe_down_service(self):
        state = _default_state()
        def mock_get(url, **kwargs):
            if "8317" in url:
                return 0, "Connection refused"
            return 200, '{"status":"ok","accounts":{"healthy":20,"total":22}}'
        with patch.object(nanoclaw, "http_get", side_effect=mock_get), \
             patch.object(nanoclaw, "tcp_check", return_value=True):
            failures = asyncio.run(probe_services(state))
            assert any(f["service"] == "cliproxyapi" for f in failures)
            assert state["services"]["cliproxyapi"]["status"] == "down"

    def test_probe_low_healthy_accounts(self):
        state = _default_state()
        with patch.object(nanoclaw, "http_get", return_value=(200, '{"status":"ok","accounts":{"healthy":3,"total":22}}')), \
             patch.object(nanoclaw, "tcp_check", return_value=True):
            failures = asyncio.run(probe_services(state))
            warnings = [f for f in failures if f.get("severity") == "warning"]
            assert len(warnings) == 1
            assert "Low healthy accounts" in warnings[0]["error"]

    def test_probe_fleet_tcp_failure(self):
        state = _default_state()
        with patch.object(nanoclaw, "http_get", return_value=(200, '{"status":"ok","accounts":{"healthy":20,"total":22}}')), \
             patch.object(nanoclaw, "tcp_check", return_value=False):
            failures = asyncio.run(probe_services(state))
            assert any(f["service"] == "fleet-gateway" for f in failures)


class TestProviderProbes:
    def test_all_providers_healthy(self):
        state = _default_state()
        with patch.object(nanoclaw, "http_post", return_value=(200, json.dumps({
            "choices": [{"finish_reason": "stop", "message": {"content": "ok"}}],
        }))):
            failures = asyncio.run(probe_providers(state))
            assert len(failures) == 0

    def test_provider_auth_failure(self):
        state = _default_state()
        def mock_post(url, body, **kwargs):
            model = body.get("model", "") if isinstance(body, dict) else ""
            if "gemini" in model:
                return 500, '{"error": {"message": "auth_unavailable"}}'
            return 200, json.dumps({"choices": [{"finish_reason": "stop"}]})
        with patch.object(nanoclaw, "http_post", side_effect=mock_post):
            failures = asyncio.run(probe_providers(state))
            assert any(f["provider"] == "gemini" for f in failures)


# ---------------------------------------------------------------------------
# Token refresher tests
# ---------------------------------------------------------------------------

class TestTokenRefresher:
    def test_discovers_token_files(self, tmp_auth_dir):
        refresher = TokenRefresher(tmp_auth_dir, dry_run=True)
        successes, failures = refresher.check_and_refresh_all()
        # Should find claude-test (needs refresh), claude-safe (not due),
        # antigravity-test (needs refresh), gemini-test (needs refresh)
        # Should NOT find claude-disabled (disabled=True)
        all_emails = [s[1] for s in successes] + [f[1] for f in failures]
        assert "disabled@example.com" not in all_emails

    def test_skips_not_due(self, tmp_auth_dir):
        refresher = TokenRefresher(tmp_auth_dir, dry_run=True)
        successes, failures = refresher.check_and_refresh_all()
        safe_results = [s for s in successes if s[1] == "safe@example.com"]
        # safe@ has 6h left, should not be refreshed
        assert len(safe_results) == 0  # Filtered out (Not yet due)

    def test_claude_refresh_success(self, tmp_auth_dir):
        token_file = tmp_auth_dir / "claude-test@example.com.json"
        data = json.loads(token_file.read_text())

        with patch("refreshers._http_post_json", return_value=(200, {
            "access_token": "new_at",
            "refresh_token": "new_rt",
            "expires_in": 7200,
        })):
            ok, msg = _refresh_claude(token_file, data, dry_run=False)
            assert ok
            assert "Refreshed" in msg

            # Verify file was updated
            updated = json.loads(token_file.read_text())
            assert updated["access_token"] == "new_at"
            assert updated["refresh_token"] == "new_rt"

    def test_claude_refresh_failure(self, tmp_auth_dir):
        token_file = tmp_auth_dir / "claude-test@example.com.json"
        data = json.loads(token_file.read_text())

        with patch("refreshers._http_post_json", return_value=(401, {"error": "invalid_grant"})):
            ok, msg = _refresh_claude(token_file, data, dry_run=False)
            assert not ok
            assert "401" in msg

    def test_antigravity_refresh_writes_file(self, tmp_auth_dir):
        """Key test: antigravity refresh must write to file (unlike Go in-memory only)."""
        token_file = tmp_auth_dir / "antigravity-test@example.com.json"
        data = json.loads(token_file.read_text())

        with patch("refreshers._http_post_form", return_value=(200, {
            "access_token": "ya29.new_token",
            "expires_in": 3600,
            "token_type": "Bearer",
        })):
            ok, msg = _refresh_antigravity(token_file, data, dry_run=False)
            assert ok

            # Verify file was written (this is the bug fix)
            updated = json.loads(token_file.read_text())
            assert updated["access_token"] == "ya29.new_token"
            assert "last_refresh" in updated

    def test_gemini_nested_token_refresh(self, tmp_auth_dir):
        token_file = tmp_auth_dir / "gemini-test@example.com.json"
        data = json.loads(token_file.read_text())

        with patch("refreshers._http_post_form", return_value=(200, {
            "access_token": "ya29.new_gemini",
            "expires_in": 3600,
            "scope": "https://www.googleapis.com/auth/generative-language",
        })):
            ok, msg = _refresh_gemini(token_file, data, dry_run=False)
            assert ok

            updated = json.loads(token_file.read_text())
            assert updated["token"]["access_token"] == "ya29.new_gemini"

    def test_no_refresh_token_fails_gracefully(self, tmp_auth_dir):
        token_file = tmp_auth_dir / "claude-test@example.com.json"
        data = {"type": "claude", "email": "test@example.com", "access_token": "at"}
        # No refresh_token
        ok, msg = _refresh_claude(token_file, data, dry_run=False)
        assert not ok
        assert "No refresh_token" in msg

    def test_force_refresh_provider(self, tmp_auth_dir):
        refresher = TokenRefresher(tmp_auth_dir, dry_run=True)
        # Should not raise
        refresher.refresh_provider("claude")
        refresher.refresh_provider("unknown_provider")


# ---------------------------------------------------------------------------
# Kimi reasoning tests
# ---------------------------------------------------------------------------

class TestKimiReasoner:
    def test_dry_run_returns_alert(self):
        reasoner = KimiReasoner("http://127.0.0.1:8317", dry_run=True)
        result = reasoner.diagnose("test", {"error": "something"})
        assert result["action"] == "alert_only"
        assert result["tokens_used"] == 0

    def test_successful_diagnosis(self):
        reasoner = KimiReasoner("http://127.0.0.1:8317", dry_run=False)
        mock_response = json.dumps({
            "choices": [{
                "finish_reason": "stop",
                "message": {
                    "content": json.dumps({
                        "action": "restart",
                        "target": "cliproxyapi",
                        "explanation": "Service is down, restart should fix it",
                    }),
                },
            }],
            "usage": {"total_tokens": 500},
        }).encode()

        mock_resp = MagicMock()
        mock_resp.read.return_value = mock_response
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = reasoner.diagnose("service_down", {"service": "cliproxyapi"})
            assert result["action"] == "restart"
            assert result["target"] == "cliproxyapi"
            assert result["tokens_used"] == 500

    def test_selects_diagnosis_model_for_complex(self):
        """Complex issues should use kimi-k2 (deep), not kimi-k2.5 (fast)."""
        reasoner = KimiReasoner("http://127.0.0.1:8317", dry_run=False)
        captured_body = {}

        def mock_urlopen(req, **kwargs):
            captured_body["data"] = json.loads(req.data)
            resp = MagicMock()
            resp.read.return_value = json.dumps({
                "choices": [{"finish_reason": "stop", "message": {"content": '{"action":"alert_only","explanation":"test"}'}}],
                "usage": {"total_tokens": 100},
            }).encode()
            resp.__enter__ = lambda s: s
            resp.__exit__ = MagicMock(return_value=False)
            return resp

        with patch("urllib.request.urlopen", side_effect=mock_urlopen):
            reasoner.diagnose("cascade", {"still_failing": True, "restart_attempted": True})
            assert captured_body["data"]["model"] == "kimi-k2"

    def test_handles_kimi_http_error(self):
        reasoner = KimiReasoner("http://127.0.0.1:8317", dry_run=False)
        import urllib.error
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            "http://test", 500, "Internal Server Error", {}, None
        )):
            result = reasoner.diagnose("test", {})
            assert result is None


# ---------------------------------------------------------------------------
# Healing action tests
# ---------------------------------------------------------------------------

class TestHealingActions:
    def test_restart_service_success(self):
        state = _default_state()
        state["services"]["cliproxyapi"] = {
            "status": "down", "restart_timestamps": [], "last_restart": None,
        }
        with patch("subprocess.run", return_value=MagicMock(returncode=0)):
            ok = restart_service(state, "cliproxyapi")
            assert ok
            assert len(state["services"]["cliproxyapi"]["restart_timestamps"]) == 1

    def test_restart_rate_limit(self):
        state = _default_state()
        now = time.time()
        state["services"]["cliproxyapi"] = {
            "status": "down",
            "restart_timestamps": [now - 100, now - 50, now - 10],
            "last_restart": None,
        }
        with patch.object(nanoclaw, "send_alert"):
            ok = restart_service(state, "cliproxyapi")
            assert not ok  # Hit rate limit

    def test_restart_unknown_service(self):
        state = _default_state()
        ok = restart_service(state, "nonexistent")
        assert not ok

    def test_clean_logs(self, tmp_path):
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        # Old file
        old = log_dir / "old.log"
        old.write_text("old log data")
        old_time = time.time() - (5 * 86400)
        import os
        os.utime(old, (old_time, old_time))
        # New file
        (log_dir / "new.log").write_text("new log data")

        with patch.object(nanoclaw, "AUTH_DIR", tmp_path):
            result = clean_logs(_default_state())
            assert result["deleted_files"] == 1
            assert not old.exists()
            assert (log_dir / "new.log").exists()

    def test_check_disk_space(self):
        free = check_disk_space()
        assert isinstance(free, float)
        assert free > 0


# ---------------------------------------------------------------------------
# Alert tests
# ---------------------------------------------------------------------------

class TestAlerts:
    def test_alert_cooldown(self):
        state = _default_state()
        with patch.object(nanoclaw, "TELEGRAM_BOT_TOKEN", ""):
            send_alert(state, "test_issue", "First alert")
            assert "test_issue" in state["alert_cooldowns"]

            # Second alert within cooldown should be suppressed
            first_time = state["alert_cooldowns"]["test_issue"]
            send_alert(state, "test_issue", "Second alert")
            # Time should not have changed (suppressed)
            assert state["alert_cooldowns"]["test_issue"] == first_time

    def test_different_issue_types_independent(self):
        state = _default_state()
        with patch.object(nanoclaw, "TELEGRAM_BOT_TOKEN", ""):
            send_alert(state, "issue_a", "Alert A")
            send_alert(state, "issue_b", "Alert B")
            assert "issue_a" in state["alert_cooldowns"]
            assert "issue_b" in state["alert_cooldowns"]


# ---------------------------------------------------------------------------
# Integration test (dry run)
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_single_cycle_dry_run(self, tmp_state_dir):
        """Full cycle in dry-run mode should complete without errors."""
        daemon = Nanoclaw(dry_run=True)
        with patch.object(nanoclaw, "http_get", return_value=(200, '{"status":"ok","accounts":{"healthy":20,"total":22}}')), \
             patch.object(nanoclaw, "http_post", return_value=(200, json.dumps({"choices": [{"finish_reason": "stop"}]}))), \
             patch.object(nanoclaw, "tcp_check", return_value=True):
            asyncio.run(_run_once_test(daemon))

    def test_daemon_stop_signal(self):
        daemon = Nanoclaw(dry_run=True)
        assert daemon.running
        daemon.stop()
        assert not daemon.running


async def _run_once_test(daemon):
    await daemon._health_cycle()
    await daemon._token_cycle()
    await daemon._provider_cycle()
    await daemon._housekeep_cycle()


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_parse_iso_none(self):
        assert _parse_iso(None) is None
        assert _parse_iso("") is None
        assert _parse_iso("not-a-date") is None

    def test_parse_iso_with_z(self):
        dt = _parse_iso("2026-03-17T12:00:00Z")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_write_token_atomic(self, tmp_path):
        f = tmp_path / "test.json"
        _write_token(f, {"key": "value"})
        assert json.loads(f.read_text()) == {"key": "value"}
        # No .tmp leftover
        assert not (tmp_path / "test.tmp").exists()

    def test_read_token_missing(self, tmp_path):
        assert _read_token(tmp_path / "missing.json") is None

    def test_read_token_corrupt(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("{{invalid")
        assert _read_token(f) is None

    def test_kimi_daily_cap_respected(self, tmp_state_dir):
        daemon = Nanoclaw(dry_run=False)
        daemon.state["kimi_usage"] = {
            "tokens_today": 50001, "calls_today": 100, "date": nanoclaw._today(),
        }
        with patch.object(nanoclaw, "send_alert"):
            asyncio.run(daemon._escalate_to_kimi("test", {"error": "test"}))
            # Should NOT have called Kimi (cap reached)

    def test_housekeep_resets_daily_counter(self, tmp_state_dir):
        daemon = Nanoclaw(dry_run=True)
        daemon.state["kimi_usage"] = {
            "tokens_today": 1000, "calls_today": 5, "date": "2020-01-01",
        }
        with patch.object(nanoclaw, "clean_logs", return_value={"deleted_files": 0, "freed_bytes": 0}), \
             patch.object(nanoclaw, "check_disk_space", return_value=100.0):
            asyncio.run(daemon._housekeep_cycle())
            assert daemon.state["kimi_usage"]["tokens_today"] == 0

    def test_parse_iso_naive_gets_utc(self):
        """Bug 14: naive datetime must get UTC tzinfo to prevent TypeError."""
        dt = _parse_iso("2026-04-01T12:00:00")
        assert dt is not None
        assert dt.tzinfo is not None
        # Should be comparable with aware datetime without TypeError
        from datetime import datetime, timezone
        diff = datetime.now(timezone.utc) - dt
        assert isinstance(diff, timedelta)

    def test_write_token_no_tmp_leftover(self, tmp_path):
        """Bug 3: unique tmp path prevents collisions."""
        f = tmp_path / "token.json"
        _write_token(f, {"a": 1})
        _write_token(f, {"a": 2})
        # No .tmp files left behind
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert len(tmp_files) == 0
        assert json.loads(f.read_text()) == {"a": 2}

    def test_save_state_caps_incidents(self, tmp_state_dir):
        """Bug 11: incidents array must be capped at 100."""
        state = _default_state()
        state["incidents"] = [{"id": i} for i in range(200)]
        nanoclaw.STATE_FILE = tmp_state_dir / "state.json"
        save_state(state)
        loaded = load_state()
        assert len(loaded["incidents"]) == 100
        # Should keep the LAST 100
        assert loaded["incidents"][0]["id"] == 100

    def test_save_state_unique_tmp(self, tmp_state_dir):
        """Bug 3: save_state uses PID-unique tmp path."""
        state = _default_state()
        nanoclaw.STATE_FILE = tmp_state_dir / "state.json"
        save_state(state)
        # No leftover tmp files
        tmp_files = list(tmp_state_dir.glob("*.tmp"))
        assert len(tmp_files) == 0
