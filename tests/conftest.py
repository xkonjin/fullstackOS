"""Shared pytest fixtures for coordinator tests."""
import copy
import os
import sqlite3
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT_CONFTEST = str(Path(__file__).resolve().parent.parent)
_FLEET_PIPELINE = str(Path(__file__).resolve().parent.parent / "fleet" / "pipeline")
for _p in (_PROJECT_ROOT_CONFTEST, _FLEET_PIPELINE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Ensure the fullstackOS root and fleet/ are on sys.path so `coordinator`, `pipeline`, etc. are importable.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
_FLEET_ROOT = str(Path(__file__).resolve().parent.parent / "fleet")
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
if _FLEET_ROOT not in sys.path:
    sys.path.insert(0, _FLEET_ROOT)


@pytest.fixture(autouse=True)
def _disable_sentry():
    """Disable Sentry for all tests."""
    old = os.environ.get("SENTRY_DSN")
    os.environ["SENTRY_DSN"] = ""
    yield
    if old is not None:
        os.environ["SENTRY_DSN"] = old
    else:
        os.environ.pop("SENTRY_DSN", None)


_SAMPLE_CFG = {
        "theorist": {
            "enabled": False,
            "notes_dir": "docs/theorist/notes"
        },
        "routing": {
            "premium_keywords": ["swarm", "orchestrate", "multi-agent"],
            "coding_keywords": ["implement", "refactor", "src/", ".py", ".ts"],
            "simple_keywords": ["what", "how", "why", "explain"],
            "simple_max_chars": 240,
            "premium_min_chars": 1200,
            "tier_models": {
                "simple": "claude-haiku-4-5",
                "coding": "smart-coding",
                "premium": "smart-opus-4.6"
            },
            "via_defaults": {
                "simple": "claude",
                "coding": "codex",
                "premium": "droid"
            },
            "via_model_overrides": {
                "codex": {
                    "default": "claude-sonnet-4-5",
                    "premium": "claude-opus-4-6"
                },
                "gemini": {
                    "default": "gemini-2.0-flash-exp"
                },
                "droid": {
                    "default": "claude-opus-4-6"
                }
            }
        },
        "budget": {
            "monthly_cap_usd": 50.0
        },
        "model_pricing": {
            "claude-opus-4-6": {"input_per_m": 15.0, "output_per_m": 75.0},
            "claude-sonnet-4-5": {"input_per_m": 3.0, "output_per_m": 15.0},
            "claude-haiku-4-5": {"input_per_m": 0.8, "output_per_m": 4.0},
            "gpt-4o": {"input_per_m": 5.0, "output_per_m": 15.0},
        }
    }


@pytest.fixture
def sample_cfg():
    """Sample coordinator config — returns deep copy per test."""
    return copy.deepcopy(_SAMPLE_CFG)


@pytest.fixture
def db_conn():
    """Create in-memory SQLite database with schema."""
    from git_ops import init_pipeline_tables

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript("PRAGMA journal_mode=WAL;")

    # Pipeline tables via canonical init function
    init_pipeline_tables(conn)

    # Coordinator-only tables not managed by init_pipeline_tables
    conn.executescript("""
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

        CREATE TABLE IF NOT EXISTS notification_channels (
            id INTEGER PRIMARY KEY,
            channel_type TEXT NOT NULL UNIQUE,
            enabled INTEGER DEFAULT 0,
            config_json TEXT DEFAULT '{}',
            last_delivery INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS task_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL UNIQUE,
            objective TEXT NOT NULL,
            repo TEXT,
            via TEXT DEFAULT 'auto',
            model TEXT DEFAULT 'auto',
            tier TEXT,
            priority INTEGER DEFAULT 5,
            status TEXT DEFAULT 'pending',
            submitted_at INTEGER NOT NULL,
            started_at INTEGER,
            completed_at INTEGER,
            run_id TEXT,
            exit_code INTEGER,
            output_path TEXT,
            error TEXT,
            notes TEXT,
            heartbeat_ts INTEGER,
            claimed_by TEXT
        );

        CREATE TABLE IF NOT EXISTS repo_learning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT NOT NULL,
            fix TEXT NOT NULL,
            confidence REAL DEFAULT 0.5,
            occurrences INTEGER DEFAULT 1,
            last_seen INTEGER,
            created_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS global_learning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            rule TEXT NOT NULL,
            rationale TEXT,
            tags TEXT,
            source TEXT,
            confidence REAL NOT NULL DEFAULT 0.7
        );

        CREATE TABLE IF NOT EXISTS pending_approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            telegram_msg_id INTEGER,
            requested_at INTEGER NOT NULL,
            timeout_at INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            response TEXT,
            responded_at INTEGER,
            reminder_count INTEGER DEFAULT 0,
            last_reminder_at INTEGER,
            UNIQUE(pipeline_id, stage_name)
        );

        CREATE INDEX IF NOT EXISTS idx_pending_approvals_status ON pending_approvals(status);

        CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            telegram_msg_id INTEGER NOT NULL,
            message_type TEXT NOT NULL,
            sent_at INTEGER NOT NULL,
            acknowledged_at INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_message_log_pipeline ON telegram_message_log(pipeline_id);
        CREATE INDEX IF NOT EXISTS idx_message_log_ack ON telegram_message_log(acknowledged_at);

        CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            cycle INTEGER NOT NULL DEFAULT 1,
            commit_hash TEXT,
            diff_stat TEXT,
            snapshot_at INTEGER NOT NULL,
            FOREIGN KEY (pipeline_id) REFERENCES pipeline_runs(pipeline_id)
        );

        CREATE INDEX IF NOT EXISTS idx_checkpoint_pipeline ON pipeline_checkpoints(pipeline_id, snapshot_at DESC);

        CREATE TABLE IF NOT EXISTS run_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            run_id TEXT,
            repo TEXT,
            objective TEXT,
            via TEXT,
            tier TEXT,
            model TEXT,
            status TEXT,
            checkpoint_gate INTEGER NOT NULL DEFAULT 0,
            output_path TEXT,
            notes TEXT,
            cost_usd REAL DEFAULT 0.0,
            duration_seconds INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            timestamp INTEGER,
            objective_hash TEXT,
            objective_length INTEGER,
            tier TEXT,
            tier_reason TEXT,
            via TEXT,
            via_reason TEXT,
            model TEXT,
            model_reason TEXT,
            budget_check REAL,
            alternatives_json TEXT
        );

        CREATE TABLE IF NOT EXISTS rate_limit_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_type TEXT,
            profile_name TEXT,
            remaining_requests INTEGER,
            limit_requests INTEGER,
            reset_ts INTEGER,
            last_429_ts INTEGER,
            consecutive_429s INTEGER DEFAULT 0,
            cooldown_until INTEGER,
            last_updated INTEGER
        );

        CREATE TABLE IF NOT EXISTS reflection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            run_id TEXT,
            pipeline_id TEXT,
            repo TEXT,
            category TEXT,
            insight TEXT,
            action_taken TEXT,
            issue_url TEXT,
            issue_number INTEGER,
            pattern_hash TEXT,
            confidence REAL
        );
    """)
    conn.commit()

    yield conn
    conn.close()
