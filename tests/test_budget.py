"""Unit tests for coordinator budget functions.

Tests cover:
- _estimate_cost: known models, unknown models, zero tokens
- _pre_dispatch_budget_check: under cap, at cap, over cap, no cap set
- _check_budget_alerts: thresholds at 69%, 71%, 81%, 91%, 100%
- _get_or_create_budget: creates new, returns existing
- _record_cost: updates spent_usd, fires alerts
"""

import sqlite3
import time
from pathlib import Path

import pytest

from coordinator.ai_coordinator import (
    _check_budget_alerts,
    _estimate_cost,
    _get_or_create_budget,
    _pre_dispatch_budget_check,
    _record_cost,
)


@pytest.fixture
def db_conn():
    """Create in-memory SQLite database with schema."""
    Path(":memory:")
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Initialize schema manually (simpler than calling _init_db which requires file path)
    conn.executescript("""
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS budget_state (
            id INTEGER PRIMARY KEY,
            month TEXT NOT NULL,
            cap_usd REAL DEFAULT 0.0,
            spent_usd REAL DEFAULT 0.0,
            alert_70_sent INTEGER DEFAULT 0,
            alert_80_sent INTEGER DEFAULT 0,
            alert_90_sent INTEGER DEFAULT 0,
            alert_100_sent INTEGER DEFAULT 0,
            last_updated INTEGER DEFAULT 0,
            UNIQUE(month)
        );

        CREATE TABLE IF NOT EXISTS cost_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            timestamp INTEGER,
            provider TEXT,
            model TEXT,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            estimated_cost_usd REAL DEFAULT 0.0,
            tier TEXT,
            via TEXT
        );

        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            severity TEXT DEFAULT 'info',
            task_id TEXT,
            run_id TEXT,
            created_at INTEGER NOT NULL,
            delivered_at INTEGER,
            channel TEXT,
            delivery_status TEXT DEFAULT 'pending'
        );

        CREATE TABLE IF NOT EXISTS notification_channels (
            id INTEGER PRIMARY KEY,
            channel_type TEXT NOT NULL UNIQUE,
            enabled INTEGER DEFAULT 0,
            config_json TEXT DEFAULT '{}',
            last_delivery INTEGER DEFAULT 0
        );
    """)
    conn.commit()

    yield conn
    conn.close()


@pytest.fixture
def sample_config():
    """Sample coordinator config with cost estimation data."""
    return {
        "cost_estimation": {
            "claude-opus-4-6": {"input_per_m": 15.0, "output_per_m": 75.0},
            "claude-sonnet-4-5": {"input_per_m": 3.0, "output_per_m": 15.0},
            "claude-haiku-4-5": {"input_per_m": 0.8, "output_per_m": 4.0},
            "gpt-4o": {"input_per_m": 5.0, "output_per_m": 15.0},
        },
        "budget": {
            "monthly_cap_usd": 50.0
        }
    }


class TestEstimateCost:
    """Tests for _estimate_cost function."""

    def test_known_model_exact_match(self, sample_config):
        """Test cost estimation for exact model name match."""
        cost = _estimate_cost(sample_config, "claude-opus-4-6", 1_000_000, 1_000_000)
        (1_000_000 / 1_000_000) * 15.0 + (1_000_000 / 1_000_000) * 75.0
        assert cost == 90.0

    def test_known_model_partial_match(self, sample_config):
        """Test cost estimation with partial model name match (substring)."""
        # Model name contains "sonnet" which should match "claude-sonnet-4-5"
        cost = _estimate_cost(sample_config, "anthropic/claude-sonnet-4-5-20250929", 500_000, 200_000)
        (500_000 / 1_000_000) * 3.0 + (200_000 / 1_000_000) * 15.0
        assert cost == round(1.5 + 3.0, 6)

    def test_unknown_model_returns_zero(self, sample_config):
        """Test that unknown models return 0.0 cost."""
        cost = _estimate_cost(sample_config, "unknown-model-xyz", 1_000_000, 1_000_000)
        assert cost == 0.0

    def test_zero_tokens(self, sample_config):
        """Test cost estimation with zero tokens."""
        cost = _estimate_cost(sample_config, "claude-haiku-4-5", 0, 0)
        assert cost == 0.0

    def test_only_input_tokens(self, sample_config):
        """Test cost with only input tokens."""
        cost = _estimate_cost(sample_config, "gpt-4o", 2_000_000, 0)
        (2_000_000 / 1_000_000) * 5.0
        assert cost == 10.0

    def test_only_output_tokens(self, sample_config):
        """Test cost with only output tokens."""
        cost = _estimate_cost(sample_config, "gpt-4o", 0, 1_000_000)
        (1_000_000 / 1_000_000) * 15.0
        assert cost == 15.0


class TestGetOrCreateBudget:
    """Tests for _get_or_create_budget function."""

    def test_creates_new_budget(self, db_conn):
        """Test creation of new budget row for a month."""
        budget = _get_or_create_budget(db_conn, "2026-02", default_cap=100.0)

        assert budget["month"] == "2026-02"
        assert budget["cap_usd"] == 100.0
        assert budget["spent_usd"] == 0.0
        assert budget["alert_70_sent"] == 0
        assert budget["alert_80_sent"] == 0
        assert budget["alert_90_sent"] == 0
        assert budget["alert_100_sent"] == 0

    def test_returns_existing_budget(self, db_conn):
        """Test that existing budget is returned instead of creating duplicate."""
        # Create initial budget
        _get_or_create_budget(db_conn, "2026-02", default_cap=50.0)

        # Modify it
        db_conn.execute("UPDATE budget_state SET spent_usd = 25.0 WHERE month = ?", ("2026-02",))
        db_conn.commit()

        # Request again - should return existing with modifications
        budget2 = _get_or_create_budget(db_conn, "2026-02", default_cap=100.0)

        assert budget2["month"] == "2026-02"
        assert budget2["cap_usd"] == 50.0  # Original cap, not new default
        assert budget2["spent_usd"] == 25.0  # Modified value preserved


class TestCheckBudgetAlerts:
    """Tests for _check_budget_alerts function."""

    def test_no_alerts_below_70_percent(self, db_conn):
        """Test no alerts fired when below 70% threshold."""
        budget = _get_or_create_budget(db_conn, "2026-02", default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 69.0 WHERE month = ?", ("2026-02",))
        db_conn.commit()
        budget = _get_or_create_budget(db_conn, "2026-02")

        alerts = _check_budget_alerts(db_conn, budget)
        assert len(alerts) == 0

    def test_alert_70_percent(self, db_conn):
        """Test 70% alert fires at threshold."""
        budget = _get_or_create_budget(db_conn, "2026-02", default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 71.0 WHERE month = ?", ("2026-02",))
        db_conn.commit()
        budget = _get_or_create_budget(db_conn, "2026-02")

        alerts = _check_budget_alerts(db_conn, budget)
        assert len(alerts) == 1
        assert "budget_alert_70pct" in alerts[0]
        assert "71.00 / 100.00 USD" in alerts[0]

        # Check flag is set
        budget_after = _get_or_create_budget(db_conn, "2026-02")
        assert budget_after["alert_70_sent"] == 1

    def test_alert_80_percent(self, db_conn):
        """Test 80% alert fires at threshold."""
        budget = _get_or_create_budget(db_conn, "2026-02", default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 81.0 WHERE month = ?", ("2026-02",))
        db_conn.commit()
        budget = _get_or_create_budget(db_conn, "2026-02")

        alerts = _check_budget_alerts(db_conn, budget)
        # Should fire both 70% and 80% if neither sent yet
        assert len(alerts) == 2
        assert any("budget_alert_70pct" in a for a in alerts)
        assert any("budget_alert_80pct" in a for a in alerts)

    def test_alert_90_percent(self, db_conn):
        """Test 90% alert fires at threshold."""
        budget = _get_or_create_budget(db_conn, "2026-02", default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 91.0 WHERE month = ?", ("2026-02",))
        db_conn.commit()
        budget = _get_or_create_budget(db_conn, "2026-02")

        alerts = _check_budget_alerts(db_conn, budget)
        assert len(alerts) == 3
        assert any("budget_alert_70pct" in a for a in alerts)
        assert any("budget_alert_80pct" in a for a in alerts)
        assert any("budget_alert_90pct" in a for a in alerts)

    def test_alert_100_percent(self, db_conn):
        """Test 100% alert fires when cap reached."""
        budget = _get_or_create_budget(db_conn, "2026-02", default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 100.0 WHERE month = ?", ("2026-02",))
        db_conn.commit()
        budget = _get_or_create_budget(db_conn, "2026-02")

        alerts = _check_budget_alerts(db_conn, budget)
        assert len(alerts) == 4
        assert any("budget_alert_100pct" in a for a in alerts)

    def test_no_duplicate_alerts(self, db_conn):
        """Test that alerts are not re-sent once flags are set."""
        budget = _get_or_create_budget(db_conn, "2026-02", default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 71.0 WHERE month = ?", ("2026-02",))
        db_conn.commit()
        budget = _get_or_create_budget(db_conn, "2026-02")

        # First call fires alert
        alerts1 = _check_budget_alerts(db_conn, budget)
        assert len(alerts1) == 1

        # Second call with same budget should not fire again
        budget = _get_or_create_budget(db_conn, "2026-02")
        alerts2 = _check_budget_alerts(db_conn, budget)
        assert len(alerts2) == 0

    def test_no_alerts_when_cap_zero(self, db_conn):
        """Test no alerts when cap is 0 (unlimited budget)."""
        budget = _get_or_create_budget(db_conn, "2026-02", default_cap=0.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 1000.0 WHERE month = ?", ("2026-02",))
        db_conn.commit()
        budget = _get_or_create_budget(db_conn, "2026-02")

        alerts = _check_budget_alerts(db_conn, budget)
        assert len(alerts) == 0


class TestPreDispatchBudgetCheck:
    """Tests for _pre_dispatch_budget_check function."""

    def test_allows_when_under_cap(self, db_conn, sample_config):
        """Test execution allowed when under budget cap."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 50.0 WHERE month = ?", (time.strftime("%Y-%m"),))
        db_conn.commit()

        allowed, reason = _pre_dispatch_budget_check(db_conn, sample_config, 10.0)
        assert allowed is True
        assert reason == ""

    def test_blocks_when_over_cap(self, db_conn, sample_config):
        """Test execution blocked when would exceed budget cap."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 95.0 WHERE month = ?", (time.strftime("%Y-%m"),))
        db_conn.commit()

        allowed, reason = _pre_dispatch_budget_check(db_conn, sample_config, 10.0)
        assert allowed is False
        assert "budget_exceeded" in reason
        assert "95.00 + 10.00 > 100.00" in reason

    def test_blocks_when_exactly_at_cap(self, db_conn, sample_config):
        """Test execution blocked when exactly at cap."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 100.0 WHERE month = ?", (time.strftime("%Y-%m"),))
        db_conn.commit()

        allowed, reason = _pre_dispatch_budget_check(db_conn, sample_config, 0.01)
        assert allowed is False
        assert "budget_exceeded" in reason

    def test_allows_when_no_cap_set(self, db_conn, sample_config):
        """Test execution allowed when cap is 0 (unlimited)."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=0.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 1000.0 WHERE month = ?", (time.strftime("%Y-%m"),))
        db_conn.commit()

        allowed, reason = _pre_dispatch_budget_check(db_conn, sample_config, 500.0)
        assert allowed is True
        assert reason == ""

    def test_warning_at_90_percent(self, db_conn, sample_config):
        """Test warning message when approaching 90% threshold."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 85.0 WHERE month = ?", (time.strftime("%Y-%m"),))
        db_conn.commit()

        allowed, reason = _pre_dispatch_budget_check(db_conn, sample_config, 7.0)
        assert allowed is True
        assert "budget_warning" in reason
        assert "92.0%" in reason


class TestRecordCost:
    """Tests for _record_cost function."""

    def test_records_cost_log_entry(self, db_conn):
        """Test that cost is recorded in cost_log table."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=100.0)

        _record_cost(db_conn, "run_123", "anthropic", "claude-opus-4-6", 1000, 500, 1.25, "smart", "auto")

        # Check cost_log entry
        row = db_conn.execute("SELECT * FROM cost_log WHERE run_id = ?", ("run_123",)).fetchone()
        assert row is not None
        assert row["provider"] == "anthropic"
        assert row["model"] == "claude-opus-4-6"
        assert row["input_tokens"] == 1000
        assert row["output_tokens"] == 500
        assert row["estimated_cost_usd"] == 1.25
        assert row["tier"] == "smart"
        assert row["via"] == "auto"

    def test_updates_budget_spent(self, db_conn):
        """Test that spent_usd is updated in budget_state."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=100.0)

        _record_cost(db_conn, "run_123", "anthropic", "claude-opus-4-6", 1000, 500, 1.25, "smart", "auto")

        budget_after = _get_or_create_budget(db_conn, time.strftime("%Y-%m"))
        assert budget_after["spent_usd"] == 1.25

    def test_fires_alerts_at_threshold(self, db_conn):
        """Test that alerts are fired when threshold crossed."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=100.0)
        db_conn.execute("UPDATE budget_state SET spent_usd = 65.0 WHERE month = ?", (time.strftime("%Y-%m"),))
        db_conn.commit()

        # This should push spent to 71.0, crossing 70% threshold
        _record_cost(db_conn, "run_123", "anthropic", "claude-opus-4-6", 1000, 500, 6.0, "smart", "auto")

        budget_after = _get_or_create_budget(db_conn, time.strftime("%Y-%m"))
        assert budget_after["spent_usd"] == 71.0
        assert budget_after["alert_70_sent"] == 1

        # Check notification was created
        notification = db_conn.execute(
            "SELECT * FROM notifications WHERE event_type = ? AND run_id = ?",
            ("budget_alert", "run_123")
        ).fetchone()
        assert notification is not None
        assert "budget_alert_70pct" in notification["message"]

    def test_accumulates_costs(self, db_conn):
        """Test that multiple cost records accumulate correctly."""
        _get_or_create_budget(db_conn, time.strftime("%Y-%m"), default_cap=100.0)

        _record_cost(db_conn, "run_1", "anthropic", "claude-opus-4-6", 1000, 500, 1.25, "smart", "auto")
        _record_cost(db_conn, "run_2", "openai", "gpt-4o", 2000, 1000, 2.50, "fast", "manual")
        _record_cost(db_conn, "run_3", "anthropic", "claude-haiku-4-5", 500, 250, 0.50, "quick", "auto")

        budget_after = _get_or_create_budget(db_conn, time.strftime("%Y-%m"))
        assert budget_after["spent_usd"] == 4.25

        # Check all cost_log entries
        rows = db_conn.execute("SELECT * FROM cost_log ORDER BY id").fetchall()
        assert len(rows) == 3
