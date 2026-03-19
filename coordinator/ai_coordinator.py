#!/usr/bin/env python3
"""ai-coordinator: unified local AI coordinator with gated autonomy and persistent learning.

Capabilities:
- Unified preflight checks for dependencies, endpoints, and production env requirements
- Auto model/tool routing for Claude/Codex/Droid harness paths
- Global + per-repo learning memory (SQLite)
- Rules drift checks (global + repo)
- Skills discovery from local skill libraries
- Checkpoint gates before autonomous runs
"""

from __future__ import annotations

import warnings

warnings.filterwarnings(
    "ignore", message="Core Pydantic V1 functionality", category=UserWarning
)

import logging
import os
import stat

log = logging.getLogger("aifleet.coordinator")

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None  # tests may run in environments without sentry_sdk installed
else:
    sentry_sdk.init(
        dsn=os.environ.get("SENTRY_DSN", ""),
        traces_sample_rate=0.2,
        environment=os.environ.get("ENVIRONMENT", "production"),
    )

import argparse
import hashlib
import json
import math
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import textwrap
import time
from pathlib import Path
from typing import Any

import urllib3

# Module-level connection pool for HTTP requests to local services.
# Reuses TCP connections across calls to _http_json(), _gateway_health(),
# etc. — avoids per-request TCP+TLS handshake overhead.
_http_pool = urllib3.PoolManager(
    num_pools=4,
    maxsize=10,
    retries=False,
    timeout=urllib3.Timeout(connect=5.0, read=10.0),
)

# Add repo roots to sys.path so that `from pipeline import ...` works
# whether run from inside Docker or via the CLI
_REPO_ROOT = Path(__file__).resolve().parent.parent
_FLEET_ROOT = _REPO_ROOT / "fleet"
for _p in (_FLEET_ROOT, _REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

REPO_DEFAULT_CONFIG = Path(__file__).with_name("coordinator.default.json")
REPO_DEFAULT_RULES = Path(__file__).with_name("global_rules.md")
USER_CONFIG_PATH = Path.home() / ".ai-fleet" / "coordinator.json"

ACTIVE_RUNS: dict[str, dict] = {}  # {run_id: {"pid": int, "started_at": int}}


def _expand(path_str: str) -> Path:
    return Path(path_str).expanduser().resolve()


def _now_ts() -> int:
    return int(time.time())


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _truncate(text: str, limit: int = 240) -> str:
    text = text.strip().replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _redact_sensitive_text(text: str) -> str:
    if not text:
        return text

    redacted = text

    # Generic bearer/token patterns
    redacted = re.sub(r"\b(sk-[A-Za-z0-9._-]{16,})\b", "sk-***REDACTED***", redacted)
    redacted = re.sub(r"\b(ghp_[A-Za-z0-9]{20,})\b", "ghp_***REDACTED***", redacted)
    redacted = re.sub(
        r"\b(pplx-[A-Za-z0-9._-]{16,})\b", "pplx-***REDACTED***", redacted
    )

    # KEY=VALUE style env lines
    redacted = re.sub(
        r"(?im)\b([A-Z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD)[A-Z0-9_]*)\s*=\s*([^\s\"']+)",
        r"\1=***REDACTED***",
        redacted,
    )
    return redacted


def _compact_output_for_display(text: str, limit: int = 4000) -> str:
    if not text:
        return text

    noisy_patterns = [
        r"codex_core::rollout::list: state db missing rollout path",
        r"codex_core::state_db: state db record_discrepancy",
    ]

    kept: list[str] = []
    removed = 0
    for line in text.splitlines():
        if any(re.search(pat, line) for pat in noisy_patterns):
            removed += 1
            continue
        kept.append(line)

    out = "\n".join(kept).strip()
    if removed:
        marker = f"[filtered {removed} noisy lines; see run log for full output]"
        out = f"{out}\n{marker}".strip()

    if len(out) > limit:
        out = out[: limit - 16] + "\n...[truncated]"
    return out


def _ensure_str(val: Any) -> str:
    """Coerce bytes/None to str. Codex can return bytes on timeout."""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val) if val else ""


def _looks_like_rate_limit(result: dict[str, Any]) -> bool:
    """Detect 429/rate-limit errors in CLI output."""
    text = (
        _ensure_str(result.get("stdout")) + "\n" + _ensure_str(result.get("stderr"))
    ).lower()
    rate_limit_patterns = [
        r"429",
        r"rate.?limit",
        r"too many requests",
        r"quota.?exceeded",
        r"overloaded",
        r"over.?capacity",
        r"throttl",
        r"usage.?cap",
    ]
    return any(re.search(pat, text) for pat in rate_limit_patterns)


def _looks_like_runtime_failure(via: str, result: dict[str, Any]) -> bool:
    if not result.get("ok"):
        return True

    text = (
        _ensure_str(result.get("stdout")) + "\n" + _ensure_str(result.get("stderr"))
    ).lower()
    if via == "droid":
        failure_patterns = [
            r'"type":"throttling_error"',
            r"all models exhausted",
            r"orchestration produced no output",
            r"upstream_unreachable",
        ]
        return any(re.search(pat, text) for pat in failure_patterns)

    if via == "gemini":
        failure_patterns = [
            r"error when talking to gemini api",
            r"an unexpected critical error occurred",
            r"gemini-client-error",
            r"code_assist/server\.js",
            r"syntaxerror: expected property name or '\}' in json",
        ]
        return any(re.search(pat, text) for pat in failure_patterns)

    return False


def _read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
        log.warning("Failed to read JSON from %s: %s — using fallback", path, exc)
        return fallback


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _merge_str_lists(default_items: Any, override_items: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for items in (default_items, override_items):
        if not isinstance(items, list):
            continue
        for item in items:
            value = str(item)
            if value in seen:
                continue
            seen.add(value)
            merged.append(value)
    return merged


def _merge_named_dict_list(
    default_items: Any, override_items: Any, name_key: str = "name"
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    override_index: dict[str, dict[str, Any]] = {}

    if isinstance(override_items, list):
        for item in override_items:
            if not isinstance(item, dict):
                continue
            name = str(item.get(name_key, "")).strip()
            if name:
                override_index[name] = item

    if isinstance(default_items, list):
        for item in default_items:
            if not isinstance(item, dict):
                continue
            name = str(item.get(name_key, "")).strip()
            merged_item = dict(item)
            if name and name in override_index:
                merged_item = _deep_merge(merged_item, override_index[name])
            merged.append(merged_item)
            if name:
                seen_names.add(name)

    if isinstance(override_items, list):
        for item in override_items:
            if not isinstance(item, dict):
                continue
            name = str(item.get(name_key, "")).strip()
            if name and name in seen_names:
                continue
            merged.append(item)
            if name:
                seen_names.add(name)

    return merged


def load_config(validate: bool = True) -> dict[str, Any]:
    default_cfg = _read_json(REPO_DEFAULT_CONFIG, {})
    user_cfg = _read_json(USER_CONFIG_PATH, {})
    merged = _deep_merge(default_cfg, user_cfg)

    # For evolving coordinator versions, preserve user overrides while backfilling
    # required safety checks from defaults.
    d_pf = (default_cfg.get("preflight") or {}) if isinstance(default_cfg, dict) else {}
    u_pf = (user_cfg.get("preflight") or {}) if isinstance(user_cfg, dict) else {}
    m_pf = merged.setdefault("preflight", {})
    if isinstance(d_pf, dict) and isinstance(u_pf, dict) and isinstance(m_pf, dict):
        m_pf["required_commands"] = _merge_str_lists(
            d_pf.get("required_commands", []),
            u_pf.get("required_commands", []),
        )
        m_pf["required_connections_before_autonomous"] = _merge_str_lists(
            d_pf.get("required_connections_before_autonomous", []),
            u_pf.get("required_connections_before_autonomous", []),
        )
        m_pf["required_endpoints"] = _merge_named_dict_list(
            d_pf.get("required_endpoints", []),
            u_pf.get("required_endpoints", []),
            name_key="name",
        )

        d_by_via = d_pf.get("required_commands_by_via", {})
        u_by_via = u_pf.get("required_commands_by_via", {})
        out_by_via: dict[str, list[str]] = {}
        for key in set((d_by_via or {}).keys()) | set((u_by_via or {}).keys()):
            out_by_via[str(key)] = _merge_str_lists(
                (d_by_via or {}).get(key, []),
                (u_by_via or {}).get(key, []),
            )
        if out_by_via:
            m_pf["required_commands_by_via"] = out_by_via

    # Validate config if requested and running interactively
    if validate and sys.stderr.isatty():
        schema_path = (
            REPO_DEFAULT_CONFIG.parent.parent / "config" / "coordinator.schema.json"
        )
        errors = _validate_config(merged, str(schema_path))
        for err in errors:
            if _is_known_validation_noise(err):
                continue
            if err["severity"] == "error":
                print(
                    f"[ERROR] config validation: {err['path']}: {err['message']}",
                    file=sys.stderr,
                )
            elif err["severity"] == "warning":
                print(f"[WARN] config validation: {err['message']}", file=sys.stderr)

    return merged


def _validate_config(config: dict[str, Any], schema_path: str) -> list[dict[str, Any]]:
    """Validate config against JSON Schema. Returns list of {path, message, severity} errors."""
    try:
        import jsonschema
    except ImportError:
        return [
            {
                "path": "",
                "message": "jsonschema not installed — skipping validation (pip install jsonschema)",
                "severity": "warning",
            }
        ]

    schema_file = Path(schema_path)
    if not schema_file.exists():
        return [
            {
                "path": "",
                "message": f"Schema file not found: {schema_path}",
                "severity": "warning",
            }
        ]

    try:
        schema = json.loads(schema_file.read_text())
    except Exception as exc:
        return [
            {
                "path": "",
                "message": f"Failed to load schema: {exc}",
                "severity": "warning",
            }
        ]

    validator = jsonschema.Draft7Validator(schema)
    errors = []
    for error in sorted(validator.iter_errors(config), key=lambda e: list(e.path)):
        errors.append(
            {
                "path": ".".join(str(p) for p in error.path) or "(root)",
                "message": error.message,
                "severity": "error",
            }
        )
    return errors


def _is_known_validation_noise(err: dict[str, Any]) -> bool:
    """Suppress noisy schema drift warnings during normal execution paths."""
    path = str(err.get("path", ""))
    message = str(err.get("message", ""))
    if "Additional properties are not allowed" not in message:
        return False
    noisy_paths = {
        "(root)",
        "autonomy",
        "droid",
        "routing.via_model_overrides",
    }
    if path in noisy_paths:
        return True

    noisy_props = ("autonomy", "droid", "via_model_overrides")
    if any(f"'{prop}'" in message for prop in noisy_props):
        return True

    return path.startswith("routing") and "via_model_overrides" in message


def ensure_state(cfg: dict[str, Any], force: bool = False) -> dict[str, Path]:
    paths_cfg = cfg.get("paths", {})
    state_dir = _expand(str(paths_cfg.get("state_dir", "~/.ai-fleet/coordinator")))
    db_path = _expand(str(paths_cfg.get("db_path", str(state_dir / "memory.db"))))
    rules_path = _expand(
        str(paths_cfg.get("global_rules_path", "~/.ai-fleet/global-rules.md"))
    )
    checkpoint_dir = _expand(
        str(paths_cfg.get("checkpoint_dir", str(state_dir / "checkpoints")))
    )
    snapshot_dir = _expand(
        str(paths_cfg.get("snapshot_dir", str(state_dir / "snapshots")))
    )
    runs_dir = _expand(str(paths_cfg.get("runs_dir", str(state_dir / "runs"))))

    for path in (
        state_dir,
        checkpoint_dir,
        snapshot_dir,
        runs_dir,
        db_path.parent,
        USER_CONFIG_PATH.parent,
        rules_path.parent,
    ):
        path.mkdir(parents=True, exist_ok=True)

    if force or not USER_CONFIG_PATH.exists():
        USER_CONFIG_PATH.write_text(json.dumps(cfg, indent=2) + "\n")

    if (force or not rules_path.exists()) and REPO_DEFAULT_RULES.exists():
        rules_path.write_text(REPO_DEFAULT_RULES.read_text())

    _init_db(db_path)
    return {
        "state_dir": state_dir,
        "db_path": db_path,
        "rules_path": rules_path,
        "checkpoint_dir": checkpoint_dir,
        "snapshot_dir": snapshot_dir,
        "runs_dir": runs_dir,
    }


def _init_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS global_learning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            rule TEXT NOT NULL,
            rationale TEXT,
            tags TEXT,
            source TEXT,
            confidence REAL NOT NULL DEFAULT 0.7
        );

        CREATE TABLE IF NOT EXISTS repo_learning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            repo TEXT NOT NULL,
            pattern TEXT NOT NULL,
            fix TEXT NOT NULL,
            tags TEXT,
            source TEXT,
            confidence REAL NOT NULL DEFAULT 0.7
        );

        CREATE TABLE IF NOT EXISTS run_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            repo TEXT,
            objective TEXT,
            via TEXT,
            tier TEXT,
            model TEXT,
            status TEXT,
            checkpoint_gate INTEGER NOT NULL DEFAULT 0,
            output_path TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS rule_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            repo TEXT,
            global_hash TEXT,
            repo_hash TEXT,
            drift INTEGER NOT NULL DEFAULT 0,
            notes TEXT
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

        CREATE INDEX IF NOT EXISTS idx_cost_log_timestamp ON cost_log(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_cost_log_via ON cost_log(via);

        CREATE TABLE IF NOT EXISTS auth_health (
            profile_type TEXT,
            profile_name TEXT,
            last_check INTEGER,
            healthy INTEGER DEFAULT 1,
            expiry_ts INTEGER,
            error TEXT,
            PRIMARY KEY (profile_type, profile_name)
        );

        CREATE INDEX IF NOT EXISTS idx_auth_health_last_check ON auth_health(last_check DESC);

        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY,
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
            budget_check TEXT,
            alternatives_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_decisions_run_id ON decisions(run_id);

        CREATE TABLE IF NOT EXISTS test_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            repo TEXT NOT NULL,
            test_file TEXT NOT NULL,
            test_name TEXT NOT NULL,
            error_type TEXT,
            error_message TEXT,
            short_tb TEXT,
            occurrence_count INTEGER DEFAULT 1,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            status TEXT DEFAULT 'open',
            fix_hint TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_repo_learning_repo_ts ON repo_learning(repo, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_run_history_repo_ts ON run_history(repo, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_rule_checks_repo_ts ON rule_checks(repo, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_decisions_timestamp ON decisions(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_test_failures_repo ON test_failures(repo, status);
        CREATE INDEX IF NOT EXISTS idx_test_failures_name ON test_failures(test_name, repo);
        """
    )
    conn.commit()

    # Learning decay columns (idempotent)
    for table in ["global_learning", "repo_learning"]:
        for col, default in [("last_used_ts", "0"), ("use_count", "0")]:
            try:
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {col} INTEGER DEFAULT {default}"
                )
            except sqlite3.OperationalError:
                pass  # column already exists

    # Run history extensions (idempotent)
    for col, coltype, default in [
        ("run_id", "TEXT", "NULL"),
        ("duration_seconds", "REAL", "NULL"),
        ("exit_code", "INTEGER", "NULL"),
        ("input_tokens", "INTEGER", "NULL"),
        ("output_tokens", "INTEGER", "NULL"),
        ("cost_usd", "REAL", "NULL"),
    ]:
        try:
            conn.execute(
                f"ALTER TABLE run_history ADD COLUMN {col} {coltype} DEFAULT {default}"
            )
        except sqlite3.OperationalError:
            pass  # column already exists

    # Index for fast run_id lookups
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_history_run_id ON run_history(run_id)"
        )
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_history_ts ON run_history(ts DESC)"
        )
    except sqlite3.OperationalError:
        pass

    conn.commit()

    # Budget state table (idempotent)
    conn.execute("""
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
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_budget_state_month ON budget_state(month)"
    )
    conn.commit()

    # Rate limit state table (idempotent)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rate_limit_state (
            id INTEGER PRIMARY KEY,
            profile_type TEXT NOT NULL,
            profile_name TEXT NOT NULL,
            remaining_requests INTEGER DEFAULT -1,
            limit_requests INTEGER DEFAULT -1,
            reset_ts INTEGER DEFAULT 0,
            last_429_ts INTEGER DEFAULT 0,
            consecutive_429s INTEGER DEFAULT 0,
            cooldown_until INTEGER DEFAULT 0,
            last_updated INTEGER DEFAULT 0,
            UNIQUE(profile_type, profile_name)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rate_limit_profile ON rate_limit_state(profile_type, profile_name)"
    )
    conn.commit()

    # Codex profile usage tracking (idempotent)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS codex_profile_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_name TEXT NOT NULL,
            ts INTEGER NOT NULL,
            event TEXT NOT NULL,
            requests_delta INTEGER DEFAULT 0,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            success INTEGER DEFAULT 1,
            error_code INTEGER,
            error_msg TEXT,
            weekly_period TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_codex_usage_profile_ts ON codex_profile_usage(profile_name, ts DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_codex_usage_period ON codex_profile_usage(weekly_period, profile_name)"
    )
    conn.commit()

    # Profile scheduler state (idempotent)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS codex_profile_schedule (
            profile_name TEXT PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            plan TEXT,
            allow_overage INTEGER DEFAULT 0,
            safety_margin_pct INTEGER DEFAULT 10,
            weekly_resets_at TEXT,
            last_reset_ts INTEGER DEFAULT 0,
            last_exhausted_ts INTEGER DEFAULT 0,
            total_requests_this_period INTEGER DEFAULT 0,
            total_429s_this_period INTEGER DEFAULT 0,
            estimated_weekly_budget INTEGER DEFAULT 0,
            burn_rate_per_hour REAL DEFAULT 0.0,
            predicted_exhaustion_ts INTEGER DEFAULT 0,
            auto_disabled_reason TEXT
        )
    """)
    conn.commit()

    # Claude profile usage tracking (idempotent)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS claude_profile_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_name TEXT NOT NULL,
            ts INTEGER NOT NULL,
            event TEXT NOT NULL,
            model TEXT,
            requests_delta INTEGER DEFAULT 0,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            success INTEGER DEFAULT 1,
            error_code INTEGER,
            error_msg TEXT,
            weekly_period TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_claude_usage_profile_ts ON claude_profile_usage(profile_name, ts DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_claude_usage_period ON claude_profile_usage(weekly_period, profile_name)"
    )
    conn.commit()

    # Claude profile schedule state (idempotent)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS claude_profile_schedule (
            profile_name TEXT PRIMARY KEY,
            email TEXT,
            context TEXT,
            plan TEXT,
            channel TEXT DEFAULT 'claude',
            enabled INTEGER DEFAULT 1,
            has_extra_usage INTEGER DEFAULT 0,
            extra_usage_limit_gbp REAL DEFAULT 0,
            extra_usage_spent_gbp REAL DEFAULT 0,
            session_pct INTEGER DEFAULT 0,
            weekly_all_pct INTEGER DEFAULT 0,
            weekly_sonnet_pct INTEGER DEFAULT 0,
            weekly_resets_at TEXT,
            session_resets_at TEXT,
            last_sync_ts INTEGER DEFAULT 0,
            total_requests_this_period INTEGER DEFAULT 0,
            total_429s_this_period INTEGER DEFAULT 0,
            estimated_weekly_budget INTEGER DEFAULT 0,
            estimated_session_budget INTEGER DEFAULT 0,
            burn_rate_per_hour REAL DEFAULT 0.0,
            auto_disabled_reason TEXT
        )
    """)
    conn.commit()

    # Task queue table (idempotent)
    conn.execute("""
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
            outcome_details TEXT,
            notes TEXT,
            heartbeat_ts INTEGER,
            claimed_by TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_task_queue_status ON task_queue(status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_task_queue_task_id ON task_queue(task_id)"
    )
    conn.commit()

    # Add heartbeat/detail columns to existing task_queue (idempotent migration)
    for col, coltype in [
        ("heartbeat_ts", "INTEGER"),
        ("claimed_by", "TEXT"),
        ("outcome_details", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE task_queue ADD COLUMN {col} {coltype}")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists

    # Notification tables (idempotent)
    conn.execute("""
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
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_notifications_created ON notifications(created_at DESC)"
    )
    conn.commit()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS notification_channels (
            id INTEGER PRIMARY KEY,
            channel_type TEXT NOT NULL UNIQUE,
            enabled INTEGER DEFAULT 0,
            config_json TEXT DEFAULT '{}',
            last_delivery INTEGER DEFAULT 0
        )
    """)
    conn.commit()

    # Reflection log table (idempotent)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reflection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            run_id TEXT,
            pipeline_id TEXT,
            repo TEXT,
            category TEXT NOT NULL,
            insight TEXT NOT NULL,
            action_taken TEXT,
            issue_url TEXT,
            issue_number INTEGER,
            pattern_hash TEXT,
            confidence REAL DEFAULT 0.7
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_reflection_ts ON reflection_log(ts DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_reflection_hash ON reflection_log(pattern_hash)"
    )
    conn.commit()

    # Pipeline tables (idempotent) - v2
    try:
        from pipeline.git_ops import init_pipeline_tables

        init_pipeline_tables(conn)
    except ImportError:
        pass  # pipeline module not available yet

    conn.close()


def _open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=5.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    # Enforce restrictive permissions — DB may contain cached credentials
    try:
        if db_path.exists():
            os.chmod(str(db_path), stat.S_IRUSR | stat.S_IWUSR)  # 0o600
            # Also lock down WAL and SHM files if they exist
            for suffix in ("-wal", "-shm"):
                wal_path = Path(str(db_path) + suffix)
                if wal_path.exists():
                    os.chmod(str(wal_path), stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass  # Best-effort; may fail on some filesystems
    return conn


def _run_cmd(
    cmd: list[str], cwd: Path | None = None, timeout: int = 300
) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout or "",
            "stderr": proc.stderr or "",
        }
    except subprocess.TimeoutExpired as exc:
        # exc.stdout/stderr can be bytes even with text=True on timeout
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        return {
            "ok": False,
            "returncode": 124,
            "stdout": stdout,
            "stderr": stderr,
        }


def _http_json(
    url: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
    timeout: int = 8,
) -> dict[str, Any]:
    """HTTP JSON helper using module-level connection pool for TCP reuse."""
    req_body = None
    hdrs = dict(headers or {})
    if payload is not None:
        req_body = json.dumps(payload).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")

    try:
        resp = _http_pool.request(
            method,
            url,
            body=req_body,
            headers=hdrs,
            timeout=urllib3.Timeout(connect=5.0, read=float(timeout)),
        )
        raw = resp.data.decode("utf-8", errors="replace")
        try:
            obj = json.loads(raw)
        except Exception:
            obj = {"raw": raw}
        if 200 <= resp.status < 300:
            return {"ok": True, "status": resp.status, "json": obj, "raw": raw}
        return {
            "ok": False,
            "status": resp.status,
            "json": obj,
            "raw": raw,
            "error": f"HTTP {resp.status}",
        }
    except urllib3.exceptions.HTTPError as err:
        return {"ok": False, "status": 0, "json": {}, "raw": "", "error": str(err)}


def _gateway_key() -> str:
    env_key = os.getenv("AI_FLEET_GATEWAY_KEY", "").strip()
    if env_key:
        return env_key

    gateway_cfg = _read_json(Path.home() / ".ai-fleet" / "gateway.json", {})
    keys = (gateway_cfg.get("server") or {}).get("api_keys") or []
    if keys:
        return str(keys[0])
    return ""


def _gateway_config_path() -> Path:
    return Path.home() / ".ai-fleet" / "gateway.json"


def _load_gateway_config() -> dict[str, Any]:
    cfg = _read_json(_gateway_config_path(), {})
    return cfg if isinstance(cfg, dict) else {}


def _save_gateway_config(cfg: dict[str, Any]) -> None:
    path = _gateway_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg, indent=2) + "\n")


def _gateway_headers() -> dict[str, str]:
    key = _gateway_key()
    if key:
        return {"Authorization": f"Bearer {key}"}
    return {}


def _detect_repo_verifier_steps(
    repo_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Derive a pragmatic verifier contract from common project scripts."""
    hard: list[dict[str, Any]] = []
    soft: list[dict[str, Any]] = []

    package_json = repo_path / "package.json"
    if package_json.exists():
        try:
            pkg = json.loads(package_json.read_text())
            scripts = pkg.get("scripts", {}) if isinstance(pkg, dict) else {}
            if isinstance(scripts, dict):
                if "test" in scripts:
                    hard.append(
                        {
                            "id": "test",
                            "label": "Tests",
                            "run": "npm test -- --runInBand",
                            "required": True,
                            "parser": "exit_code",
                        }
                    )
                if "build" in scripts:
                    hard.append(
                        {
                            "id": "build",
                            "label": "Build",
                            "run": "npm run build",
                            "required": True,
                            "parser": "exit_code",
                        }
                    )
                if "lint" in scripts:
                    soft.append(
                        {
                            "id": "lint",
                            "label": "Lint",
                            "run": "npm run lint",
                            "required": False,
                            "parser": "exit_code",
                            "weight": 1,
                        }
                    )
        except Exception:
            pass

    pyproject = repo_path / "pyproject.toml"
    if pyproject.exists():
        hard.append(
            {
                "id": "pytest",
                "label": "Pytest",
                "run": "python -m pytest -x -q",
                "required": True,
                "parser": "exit_code",
            }
        )

    cargo_toml = repo_path / "Cargo.toml"
    if cargo_toml.exists():
        hard.append(
            {
                "id": "cargo-test",
                "label": "Cargo test",
                "run": "cargo test",
                "required": True,
                "parser": "exit_code",
            }
        )

    go_mod = repo_path / "go.mod"
    if go_mod.exists():
        hard.append(
            {
                "id": "go-test",
                "label": "Go test",
                "run": "go test ./...",
                "required": True,
                "parser": "exit_code",
            }
        )

    # Fallback gate: at minimum ensure repository state is valid.
    if not hard:
        hard.append(
            {
                "id": "git-status-clean-parse",
                "label": "Git status parse",
                "run": "git status --short >/dev/null",
                "required": True,
                "parser": "exit_code",
            }
        )

    return hard, soft


def _should_auto_use_experiment_loop(
    objective: str, cfg: dict[str, Any], pipeline_cfg: dict[str, Any] | None = None
) -> bool:
    """Heuristic gate for when fleetmax should branch into experiment-loop."""
    exp_cfg = _resolve_experiment_loop_cfg(cfg, pipeline_cfg)

    if exp_cfg.get("enabled") is False:
        return False
    if exp_cfg.get("force") is True:
        return True

    low = objective.lower()
    trigger_keywords = exp_cfg.get(
        "trigger_keywords",
        [
            "intermittent",
            "flaky",
            "regression",
            "hard to reproduce",
            "best implementation",
            "compare approaches",
            "tradeoff",
            "until tests pass",
            "iterate",
            "multiple variants",
        ],
    )
    if any(str(keyword).lower() in low for keyword in trigger_keywords):
        return True

    min_len = int(exp_cfg.get("min_objective_chars", 260))
    if len(objective) >= min_len:
        return True

    from fleet.pipeline.classifier import classify_task
    return classify_task(objective).complexity >= 4


def _start_experiment_loop(
    objective: str,
    repo_path: Path,
    cfg: dict[str, Any],
    pipeline_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create an experiment-loop run via orchestrator and return launch metadata."""
    hard, soft = _detect_repo_verifier_steps(repo_path)
    exp_cfg = _resolve_experiment_loop_cfg(cfg, pipeline_cfg)
    launch_timeout_seconds = max(5, int(exp_cfg.get("launch_timeout_seconds", 60)))
    launch_retries = max(0, int(exp_cfg.get("launch_retries", 2)))

    payload = {
        "objective": objective,
        "cwd": str(repo_path),
        "strategy": "fanout-narrow",
        "initial_variants": int(exp_cfg.get("initial_variants", 4)),
        "survivors_per_round": int(exp_cfg.get("survivors_per_round", 2)),
        "children_per_survivor": int(exp_cfg.get("children_per_survivor", 2)),
        "max_rounds": int(exp_cfg.get("max_rounds", 3)),
        "max_concurrency": int(exp_cfg.get("max_concurrency", 4)),
        "loop_timeout_ms": int(exp_cfg.get("loop_timeout_ms", 2 * 60 * 60 * 1000)),
        "fail_fast": bool(exp_cfg.get("fail_fast", False)),
        "verifier": {
            "hard_gates": hard,
            "soft_checks": soft,
            "minimum_soft_score": float(exp_cfg.get("minimum_soft_score", 0.75)),
            "artifacts": ["log", "diff"],
        },
        "dispatch_policy": {
            "implement_cli": str(exp_cfg.get("implement_cli", "codex")),
            "verify_cli": str(exp_cfg.get("verify_cli", "claude")),
            "review_cli": str(exp_cfg.get("review_cli", "claude")),
        },
        "metadata": {
            "source": "ai-fleet fleetmax auto",
            "auto_invoked": True,
        },
    }

    headers = {"Authorization": f"Bearer {os.getenv('ORCHESTRATOR_API_KEY', 'your-proxy-key')}"}
    launch: dict[str, Any] = {}
    for attempt in range(launch_retries + 1):
        launch = _http_json(
            "http://127.0.0.1:8318/v1/fleet/experiment-loops",
            method="POST",
            headers=headers,
            payload=payload,
            timeout=launch_timeout_seconds,
        )
        if launch.get("ok"):
            break
        error_text = str(launch.get("error") or launch.get("raw") or "").lower()
        if attempt >= launch_retries:
            break
        if not any(
            token in error_text
            for token in (
                "timed out",
                "timeout",
                "connection",
                "refused",
                "temporarily unavailable",
            )
        ):
            break
        backoff_seconds = min(2**attempt, 8)
        print(
            f"[WARN] experiment-loop launch attempt {attempt + 1} failed; retrying in {backoff_seconds}s",
            file=sys.stderr,
        )
        time.sleep(backoff_seconds)

    return {
        "launch": launch,
        "payload": payload,
    }


def _resolve_experiment_loop_cfg(
    cfg: dict[str, Any],
    pipeline_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged = dict(cfg.get("fleetmax", {}) or {})
    if isinstance(pipeline_cfg, dict):
        fm = pipeline_cfg.get("fleetmax")
        if isinstance(fm, dict):
            merged.update(fm)
    exp_cfg = merged.get("experiment_loop", {})
    if not isinstance(exp_cfg, dict):
        return {}
    return exp_cfg


def _poll_experiment_loop(
    loop_id: str,
    timeout_seconds: int = 7200,
    request_timeout_seconds: int = 20,
) -> dict[str, Any]:
    """Poll experiment-loop status until terminal or timeout."""
    headers = {"Authorization": f"Bearer {os.getenv('ORCHESTRATOR_API_KEY', 'your-proxy-key')}"}
    started = time.time()
    last: dict[str, Any] | None = None
    last_progress_key = ""
    last_error_key = ""
    printed_artifact_root = ""
    consecutive_transport_failures = 0
    while (time.time() - started) < timeout_seconds:
        resp = _http_json(
            f"http://127.0.0.1:8318/v1/fleet/experiment-loops/{loop_id}",
            method="GET",
            headers=headers,
            timeout=max(5, request_timeout_seconds),
        )
        last = resp
        if not resp.get("ok"):
            consecutive_transport_failures += 1
            err_key = f"{resp.get('status')}|{resp.get('error')}"
            if err_key != last_error_key:
                print(
                    f"experiment_loop_poll_error loop_id={loop_id} status={resp.get('status')} error={resp.get('error')}",
                    file=sys.stderr,
                )
                last_error_key = err_key
            backoff_seconds = min(
                3 * (2 ** max(0, consecutive_transport_failures - 1)), 30
            )
            time.sleep(backoff_seconds)
            continue
        consecutive_transport_failures = 0
        body = resp.get("json") or {}
        status = str(body.get("status") or "").lower()

        rounds = body.get("rounds") if isinstance(body.get("rounds"), list) else []
        active_round: dict[str, Any] | None = None
        if rounds:
            for round_entry in rounds:
                if (
                    isinstance(round_entry, dict)
                    and str(round_entry.get("status", "")).lower() == "running"
                ):
                    active_round = round_entry
                    break
            if active_round is None:
                maybe_last = rounds[-1]
                if isinstance(maybe_last, dict):
                    active_round = maybe_last

        round_number = active_round.get("round") if active_round else None
        round_status = (
            str(active_round.get("status", "unknown")) if active_round else "unknown"
        )

        candidates = (
            body.get("candidates") if isinstance(body.get("candidates"), dict) else {}
        )
        counts = {"pending": 0, "running": 0, "passed": 0, "failed": 0, "unknown": 0}
        for candidate in candidates.values():
            if not isinstance(candidate, dict):
                counts["unknown"] += 1
                continue
            c_status = str(candidate.get("status", "unknown")).lower()
            if c_status in counts:
                counts[c_status] += 1
            else:
                counts["unknown"] += 1

        winner = str(body.get("winner_candidate_id") or "-")
        total_rounds = len(rounds)
        round_label = "-" if round_number is None else str(round_number)
        artifact_root = str(body.get("artifact_root") or "")
        progress_key = (
            f"{status}|{round_label}|{round_status}|"
            f"{counts['pending']}|{counts['running']}|{counts['passed']}|{counts['failed']}|{counts['unknown']}|{winner}"
        )
        if progress_key != last_progress_key:
            print(
                "experiment_loop_progress "
                f"loop_id={loop_id} "
                f"status={status or 'unknown'} "
                f"round={round_label}/{total_rounds} "
                f"round_status={round_status} "
                f"candidates=pending:{counts['pending']},running:{counts['running']},passed:{counts['passed']},failed:{counts['failed']},unknown:{counts['unknown']} "
                f"winner={winner}"
            )
            if artifact_root and artifact_root != printed_artifact_root:
                print(f"experiment_loop_artifacts={artifact_root}")
                printed_artifact_root = artifact_root
            last_progress_key = progress_key

        if status in {"completed", "failed", "cancelled"}:
            return {
                "ok": status == "completed",
                "status": status,
                "response": resp,
            }
        time.sleep(5)
    return {
        "ok": False,
        "status": "timeout",
        "response": last or {},
    }


def _gateway_health(cfg: dict[str, Any]) -> dict[str, Any]:
    base = str(
        cfg.get("gateway", {}).get("base_url", "http://127.0.0.1:4105/v1")
    ).rstrip("/")
    return _http_json(base + "/health", headers=_gateway_headers(), timeout=8)


def _gateway_route(cfg: dict[str, Any]) -> dict[str, Any]:
    base = str(
        cfg.get("gateway", {}).get("base_url", "http://127.0.0.1:4105/v1")
    ).rstrip("/")
    return _http_json(base + "/route", headers=_gateway_headers(), timeout=8)


GATEWAY_MAX_RETRIES = int(os.getenv("GATEWAY_MAX_RETRIES", "3"))
GATEWAY_RETRY_BASE_DELAY = float(os.getenv("GATEWAY_RETRY_BASE_DELAY", "1.0"))
GATEWAY_RETRY_MAX_DELAY = float(os.getenv("GATEWAY_RETRY_MAX_DELAY", "10.0"))
GATEWAY_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


def _run_gateway_chat(
    model: str, prompt: str, cfg: dict[str, Any], timeout: int = 120
) -> dict[str, Any]:
    """Send a chat completion request to the gateway HTTP API with retry.

    Returns the same shape as _run_cmd: {ok, returncode, stdout, stderr}.
    The gateway routes smart-* model names through tier_chains with full
    fallback across Kimi, MiniMax, Deepseek, Gemini, OpenRouter, etc.

    Retries up to GATEWAY_MAX_RETRIES times on transient failures (429, 5xx,
    connection errors) with exponential backoff.
    """
    base = str(
        cfg.get("gateway", {}).get("base_url", "http://127.0.0.1:4105/v1")
    ).rstrip("/")
    url = base + "/chat/completions"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 16384,
        "temperature": 0.2,
    }

    last_resp: dict[str, Any] = {}
    for attempt in range(GATEWAY_MAX_RETRIES):
        if attempt > 0:
            delay = min(
                GATEWAY_RETRY_BASE_DELAY * (2 ** (attempt - 1)), GATEWAY_RETRY_MAX_DELAY
            )
            log.debug(
                "gateway retry %d/%d in %.1fs for model=%s",
                attempt + 1,
                GATEWAY_MAX_RETRIES,
                delay,
                model,
            )
            time.sleep(delay)

        resp = _http_json(
            url,
            method="POST",
            headers=_gateway_headers(),
            payload=payload,
            timeout=timeout,
        )
        last_resp = resp

        if resp.get("ok"):
            body = resp.get("json", {})
            choices = body.get("choices", [])
            content = (
                choices[0].get("message", {}).get("content", "") if choices else ""
            )
            return {"ok": True, "returncode": 0, "stdout": content, "stderr": ""}

        status = resp.get("status", 0)
        # Only retry on transient failures; 4xx (except 429) are permanent
        if status not in GATEWAY_RETRYABLE_STATUSES and status != 0:
            break

    status = last_resp.get("status", 0)
    err_msg = last_resp.get("raw", "") or last_resp.get("error", "")
    is_429 = status == 429
    return {
        "ok": False,
        "returncode": 1,
        "stdout": "",
        "stderr": f"gateway_error status={status}: {err_msg[:500]}",
        "_is_429": is_429,
    }


def _mask_secret(value: str, keep: int = 4) -> str:
    raw = str(value or "")
    if not raw:
        return ""
    if len(raw) <= keep:
        return "*" * len(raw)
    return ("*" * max(0, len(raw) - keep)) + raw[-keep:]


def _fmt_ts(ts: int | None) -> str:
    if not ts:
        return "n/a"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))


def _provider_specs() -> dict[str, dict[str, str]]:
    return {
        "openrouter": {
            "env": "OPENROUTER_API_KEY",
            "base_url": "https://openrouter.ai/api/v1",
            "path": "/chat/completions",
        },
        "kimi": {
            "env": "KIMI_API_KEY",
            "base_url": "https://api.kimi.com/coding/v1",
            "path": "/chat/completions",
        },
        "minimax": {
            "env": "MINIMAX_API_KEY",
            "base_url": "https://api.minimax.io/v1",
            "path": "/chat/completions",
        },
    }


def _find_external_provider(
    cfg: dict[str, Any], provider: str
) -> dict[str, Any] | None:
    providers = cfg.get("external_providers", [])
    if not isinstance(providers, list):
        return None
    for p in providers:
        if not isinstance(p, dict):
            continue
        if str(p.get("name", "")).strip().lower() == provider.lower():
            return p
    return None


def _ensure_external_provider(cfg: dict[str, Any], provider: str) -> dict[str, Any]:
    providers = cfg.setdefault("external_providers", [])
    if not isinstance(providers, list):
        providers = []
        cfg["external_providers"] = providers

    existing = _find_external_provider(cfg, provider)
    if existing is not None:
        return existing

    spec = _provider_specs().get(provider, {})
    entry = {
        "name": provider,
        "enabled": False,
        "base_url": spec.get("base_url", ""),
        "chat_completions_path": spec.get("path", "/chat/completions"),
        "api_key_env": spec.get("env", ""),
        "timeout_seconds": 90,
        "headers": {},
    }
    providers.append(entry)
    return entry


def _repo_root(path: str | Path | None) -> Path:
    base = Path(path).expanduser().resolve() if path else Path.cwd().resolve()
    probe = _run_cmd(
        ["git", "-C", str(base), "rev-parse", "--show-toplevel"], timeout=5
    )
    if probe["ok"]:
        out = (probe["stdout"] or "").strip()
        if out:
            return Path(out).resolve()
    return base


def _load_env_files(repo: Path, env_files: list[str]) -> tuple[set[str], list[str]]:
    keys: set[str] = set()
    missing: list[str] = []

    for rel in env_files:
        p = (repo / rel).resolve()
        if not p.exists():
            missing.append(rel)
            continue
        for line in p.read_text(errors="ignore").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "=" not in s:
                continue
            k = s.split("=", 1)[0].strip()
            if k:
                keys.add(k)

    # Process env vars count as available too
    for key in os.environ.keys():
        keys.add(key)

    return keys, missing


def _objective_is_production(
    objective: str, cfg: dict[str, Any], explicit: bool
) -> bool:
    if explicit:
        return True
    low = objective.lower()
    tokens = set(re.findall(r"[a-z0-9_]+", low))
    for kw in cfg.get("preflight", {}).get("production_keywords", []):
        k = str(kw).strip().lower()
        if not k:
            continue
        if " " in k:
            if k in low:
                return True
            continue
        if k in tokens:
            return True
    return False


def run_preflight(
    cfg: dict[str, Any],
    repo: Path,
    objective: str,
    production: bool,
    require_swarm: bool = False,
    planned_via: str = "auto",
    autonomous: bool = False,
) -> dict[str, Any]:
    pf = cfg.get("preflight", {})
    results: list[dict[str, Any]] = []

    required_cmds = [str(x) for x in pf.get("required_commands", [])]
    by_via = pf.get("required_commands_by_via", {}) or {}
    if planned_via in by_via and isinstance(by_via.get(planned_via), list):
        required_cmds.extend([str(x) for x in by_via.get(planned_via, [])])
    if require_swarm:
        required_cmds.append("droid")

    seen_cmds: set[str] = set()
    for cmd in required_cmds:
        if cmd in seen_cmds:
            continue
        seen_cmds.add(cmd)
        exists = shutil.which(cmd) is not None
        results.append(
            {
                "type": "command",
                "name": cmd,
                "ok": exists,
                "required": True,
                "detail": "found" if exists else "missing",
            }
        )

    gateway_key = _gateway_key()
    for endpoint in pf.get("required_endpoints", []):
        name = str(endpoint.get("name", "endpoint"))
        url = str(endpoint.get("url", "")).strip()
        required = bool(endpoint.get("required", True))
        allow_statuses = {int(x) for x in endpoint.get("allow_statuses", [200])}
        headers: dict[str, str] = {}
        if "4105" in url and gateway_key:
            headers["Authorization"] = f"Bearer {gateway_key}"

        probe = _http_json(url, headers=headers, timeout=6)
        status = int(probe.get("status", 0) or 0)
        ok = bool(probe.get("ok")) or (status in allow_statuses)
        detail = f"status={probe.get('status', 0)}"
        if not ok and probe.get("error"):
            detail += f" err={probe['error']}"

        results.append(
            {
                "type": "endpoint",
                "name": name,
                "ok": ok,
                "required": required,
                "detail": detail,
                "url": url,
            }
        )

    if autonomous:
        required_connections = [
            str(x) for x in pf.get("required_connections_before_autonomous", [])
        ]
        endpoint_map = {
            (row.get("name") or ""): row
            for row in results
            if row.get("type") == "endpoint"
        }
        for conn_name in required_connections:
            row = endpoint_map.get(conn_name)
            ok = bool(row and row.get("ok"))
            detail = "ready" if ok else "not_ready"
            results.append(
                {
                    "type": "connection",
                    "name": conn_name,
                    "ok": ok,
                    "required": True,
                    "detail": detail,
                }
            )

    low_objective = objective.lower()
    for env_hint in pf.get("provider_env_warnings", []):
        env_name = str(env_hint.get("env", "")).strip()
        if not env_name:
            continue
        kws = [str(k).lower() for k in env_hint.get("when_keywords", [])]
        if kws and not any(k in low_objective for k in kws):
            continue
        has_env = bool(os.getenv(env_name, "").strip())
        results.append(
            {
                "type": "provider_env",
                "name": env_name,
                "ok": has_env,
                "required": False,
                "detail": "present" if has_env else "missing",
            }
        )

    needs_prod = _objective_is_production(objective, cfg, production)
    if needs_prod:
        env_files = [str(x) for x in pf.get("production_required_env_files", [])]
        env_vars = [str(x) for x in pf.get("production_required_env_vars", [])]
        available_keys, missing_files = _load_env_files(repo, env_files)

        for rel in env_files:
            ok = rel not in missing_files
            results.append(
                {
                    "type": "env_file",
                    "name": rel,
                    "ok": ok,
                    "required": True,
                    "detail": "present" if ok else "missing",
                }
            )

        for var in env_vars:
            ok = var in available_keys
            results.append(
                {
                    "type": "env_var",
                    "name": var,
                    "ok": ok,
                    "required": True,
                    "detail": "present" if ok else "missing",
                }
            )

    hard_failures = [r for r in results if r["required"] and not r["ok"]]
    return {
        "ok": len(hard_failures) == 0,
        "results": results,
        "hard_failures": hard_failures,
        "production_checks": needs_prod,
    }


def _objective_tokens(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-zA-Z0-9_-]{3,}", text.lower())}


def _classify_tier(objective: str, cfg: dict[str, Any]) -> str:
    routing = cfg.get("routing", {})
    text = objective or ""
    low = text.lower()
    if len(text) >= int(routing.get("premium_min_chars", 1200)):
        return "premium"

    premium_kw = [str(x).lower() for x in routing.get("premium_keywords", [])]
    coding_kw = [str(x).lower() for x in routing.get("coding_keywords", [])]
    simple_kw = [str(x).lower() for x in routing.get("simple_keywords", [])]

    if any(k in low for k in premium_kw):
        return "premium"
    if any(k in low for k in coding_kw):
        return "coding"
    if len(text) <= int(routing.get("simple_max_chars", 240)) or any(
        k in low for k in simple_kw
    ):
        return "simple"
    return "coding"


def _normalize_repo_path(repo: Path | str) -> str:
    """Normalize repo path: resolve symlinks, strip .worktrees/* suffixes so worktree
    dispatches map back to the parent repo for learning lookup."""
    p = str(repo)
    # Strip worktree suffixes like /.worktrees/fleetmax-12345
    wt_marker = "/.worktrees/"
    if wt_marker in p:
        p = p[: p.index(wt_marker)]
    # Resolve to absolute path
    try:
        p = str(Path(p).resolve())
    except Exception:
        pass
    return p


def _check_learned_via(
    objective: str, cfg: dict[str, Any], repo: Path | str | None = None
) -> str:
    """Check repo_learning table for routing patterns matching objective. Scoped to repo if provided."""
    if not objective or not objective.strip():
        return ""
    db_path = _expand(
        str(
            (cfg.get("paths", {}) or {}).get(
                "db_path", "~/.ai-fleet/coordinator/memory.db"
            )
        )
    )
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        query = """
            SELECT pattern, fix, confidence, ts
            FROM repo_learning
            WHERE confidence >= 0.5
              AND tags LIKE '%routing%'
        """
        params: list[Any] = []
        if repo:
            norm = _normalize_repo_path(repo)
            query += " AND (repo = ? OR repo LIKE ?)"
            params.extend([norm, f"{norm}%"])
        query += " ORDER BY confidence DESC, ts DESC LIMIT 20"
        patterns = conn.execute(query, params).fetchall()

        obj_lower = (objective or "").lower()
        for p in patterns:
            pattern_lower = str(p["pattern"]).lower()
            fix_str = str(p["fix"])
            # Apply confidence decay
            effective_conf = _decay_confidence(
                float(p["confidence"]), int(p["ts"] or 0)
            )
            if effective_conf < 0.3:
                continue

            if pattern_lower in obj_lower or obj_lower in pattern_lower:
                learned_via = _extract_via_from_fix(fix_str)
                if learned_via:
                    conn.close()
                    return learned_via

        conn.close()
    except Exception as exc:
        print(f"warn: learning query failed: {exc}", file=sys.stderr)

    return ""


def _extract_via_from_fix(fix: str) -> str:
    """Extract via provider from fix string. Returns empty string if not found."""
    fix_lower = fix.lower()

    # Check for explicit via= or via:
    for prefix in ["via=", "via:"]:
        if prefix in fix_lower:
            idx = fix_lower.index(prefix) + len(prefix)
            rest = fix[idx:].strip()
            # Take first word
            via_word = rest.split()[0] if rest.split() else ""
            via_word = via_word.rstrip(",.;:)")
            if via_word in ["codex", "gemini", "droid"]:
                return via_word

    # Check for standalone provider names
    if "codex" in fix_lower:
        return "codex"
    if "gemini" in fix_lower:
        return "gemini"
    if "droid" in fix_lower:
        return "droid"

    return ""


def _select_via_and_model(
    objective: str,
    via: str,
    model: str,
    cfg: dict[str, Any],
    repo: Path | str | None = None,
) -> tuple[str, str, str]:
    tier = _classify_tier(objective, cfg)
    routing = cfg.get("routing", {})

    selected_model = (
        model
        if model and model != "auto"
        else str((routing.get("tier_models") or {}).get(tier, "smart-coding"))
    )

    if via and via != "auto":
        return tier, via, selected_model

    # Check repo_learning for patterns before using defaults (scoped to repo)
    learned_via = _check_learned_via(objective, cfg, repo=repo)
    if learned_via:
        return tier, learned_via, selected_model

    low = objective.lower()
    swarm_words = ["swarm", "orchestrate", "multi-agent", "parallel workstreams"]
    if any(w in low for w in swarm_words):
        selected_via = "droid"
    else:
        selected_via = str((routing.get("via_defaults") or {}).get(tier, "codex"))

    return tier, selected_via, selected_model


def _resolve_runtime_model(
    via: str,
    tier: str,
    model_lane: str,
    cfg: dict[str, Any],
    explicit_model: bool = False,
    swarm: bool = False,
) -> str:
    if explicit_model and model_lane and model_lane != "auto":
        explicit = model_lane.strip()
        low = explicit.lower()

        # Normalize known Gemini aliases that are rejected by some runtimes.
        gemini_aliases = {
            "gemini-3.1-pro": "gemini-3-pro-preview",
            "gemini-3-pro": "gemini-3-pro-preview",
        }
        if via == "gemini" and low in gemini_aliases:
            return gemini_aliases[low]

        # Provider CLIs do not accept gateway lane aliases (smart-*, *-auto).
        # If an alias is passed explicitly, fall back to via/tier override model.
        alias_like = low.startswith("smart-") or low.endswith("-auto")
        if alias_like and via in {"codex", "gemini", "droid"}:
            via_overrides = (
                cfg.get("routing", {}).get("via_model_overrides", {}) or {}
            ).get(via, {}) or {}
            model = via_overrides.get(tier) or via_overrides.get("default")
            if isinstance(model, str) and model.strip():
                return model.strip()
        return explicit

    via_overrides = (cfg.get("routing", {}).get("via_model_overrides", {}) or {}).get(
        via, {}
    ) or {}
    if via_overrides:
        model = via_overrides.get(tier) or via_overrides.get("default")
        if isinstance(model, str) and model.strip():
            return model.strip()

    if via == "droid":
        return _select_droid_model(cfg, swarm=swarm or tier == "premium")

    return model_lane


def _classify_tier_with_reason(objective: str, cfg: dict[str, Any]) -> tuple[str, str]:
    """Classify tier with explanation of reasoning."""
    routing = cfg.get("routing", {})
    obj_lower = objective.lower()
    obj_len = len(objective)

    # Check routing_overrides first (highest priority)
    for override in routing.get("routing_overrides", []):
        pattern = override.get("pattern", "")
        if not pattern:
            continue
        try:
            if re.search(pattern, objective, re.IGNORECASE):
                tier = override.get("tier", "coding")
                return tier, f"routing_override matched pattern='{pattern}'"
        except re.error as e:
            print(
                f"[WARN] Invalid routing_override pattern '{pattern}': {e}",
                file=sys.stderr,
            )

    # Check keywords
    simple_kw = [str(x).lower() for x in routing.get("simple_keywords", [])]
    coding_kw = [str(x).lower() for x in routing.get("coding_keywords", [])]
    premium_kw = [str(x).lower() for x in routing.get("premium_keywords", [])]

    matched_simple = [k for k in simple_kw if k in obj_lower]
    matched_coding = [k for k in coding_kw if k in obj_lower]
    matched_premium = [k for k in premium_kw if k in obj_lower]

    # Length thresholds
    simple_max = int(routing.get("simple_max_chars", 240))
    premium_min = int(routing.get("premium_min_chars", 1200))

    # Premium keywords take precedence
    if matched_premium:
        return "premium", f"premium keywords matched: {matched_premium}"

    # Length-based classification
    if obj_len >= premium_min:
        return "premium", f"objective length {obj_len} >= {premium_min}"

    if matched_simple and obj_len <= simple_max:
        return (
            "simple",
            f"simple keywords matched: {matched_simple}, length {obj_len} <= {simple_max}",
        )

    if matched_coding:
        return "coding", f"coding keywords matched: {matched_coding}"

    if matched_simple:
        return "simple", f"simple keywords matched: {matched_simple}"

    if obj_len <= simple_max:
        return (
            "simple",
            f"objective length {obj_len} <= {simple_max} (default to simple)",
        )

    return "coding", f"default tier: no specific keywords, length {obj_len}"


def _query_routing_learnings(
    conn: sqlite3.Connection | None, tier: str, repo: Path | None = None
) -> dict[str, Any] | None:
    """Check repo_learning for routing patterns relevant to this tier. Scoped to repo if provided.

    Uses fuzzy matching to handle both old format (space-separated) and new format (colon-separated).
    Also checks failure patterns to avoid known-bad routes.
    """
    if conn is None:
        return None
    try:
        # Fuzzy match: look for any routing pattern mentioning this tier
        success_patterns = [
            f"%routing%success%tier={tier}%",
            f"%routing:success:tier={tier}%",
        ]
        failure_patterns = [
            f"%routing%fail%tier={tier}%",
            f"%routing:failed:tier={tier}%",
        ]

        norm = _normalize_repo_path(repo) if repo else None
        all_rows = []
        for pat in success_patterns + failure_patterns:
            if norm:
                rows = conn.execute(
                    "SELECT pattern, fix, confidence, ts FROM repo_learning WHERE pattern LIKE ? AND tags LIKE '%routing%' AND (repo = ? OR repo LIKE ?) ORDER BY confidence DESC, ts DESC LIMIT 5",
                    (pat, norm, f"{norm}%"),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT pattern, fix, confidence, ts FROM repo_learning WHERE pattern LIKE ? AND tags LIKE '%routing%' ORDER BY confidence DESC, ts DESC LIMIT 5",
                    (pat,),
                ).fetchall()
            all_rows.extend(rows)

        if not all_rows:
            return None

        # Find the highest-confidence non-decayed success, and collect vias to avoid
        best = None
        avoid_vias: set[str] = set()
        for r in all_rows:
            effective_conf = _decay_confidence(float(r[2] or 0), int(r[3] or 0))
            pattern_str = r[0] or ""
            via_match = re.search(r"via=(\w+)", pattern_str)
            if not via_match:
                continue
            via_name = via_match.group(1)

            if "fail" in pattern_str and effective_conf > 0.3:
                avoid_vias.add(via_name)
            elif "success" in pattern_str and effective_conf > 0.3:
                if best is None or effective_conf > best["confidence"]:
                    best = {
                        "via": via_name,
                        "confidence": effective_conf,
                        "avoid_vias": list(avoid_vias),
                    }

        if best:
            best["avoid_vias"] = list(avoid_vias)
        return best
    except Exception as exc:
        print(f"warn: learning query failed: {exc}", file=sys.stderr)
        return None


def _pick_via_with_reason(
    tier: str,
    cfg: dict[str, Any],
    explicit_via: str = "auto",
    objective: str = "",
    conn: sqlite3.Connection | None = None,
    repo: Path | None = None,
) -> tuple[str, str, list[dict[str, Any]]]:
    """Pick provider with explanation. Returns (via, reason, alternatives)."""
    if explicit_via != "auto":
        return explicit_via, f"explicit --via={explicit_via}", []

    routing = cfg.get("routing", {})

    # Check for swarm keywords
    low = objective.lower() if objective else ""
    swarm_words = ["swarm", "orchestrate", "multi-agent", "parallel workstreams"]
    if any(w in low for w in swarm_words):
        return (
            "droid",
            f"swarm keywords detected: {[w for w in swarm_words if w in low]}",
            [],
        )

    # Check learned routing preferences (scoped to repo)
    learned = _query_routing_learnings(conn, tier, repo=repo)
    if learned and learned["confidence"] > 0.5:
        return (
            learned["via"],
            f"learned from past success (confidence={learned['confidence']:.2f})",
            [],
        )

    via_defaults = routing.get(
        "via_defaults", {"simple": "claude", "coding": "codex", "premium": "droid"}
    )
    via = via_defaults.get(tier, "claude")
    reason = f"default for tier={tier} from via_defaults config"

    # Build alternatives — always include gateway as last-resort fallback
    alternatives = []
    for alt_tier_name, alt_provider in via_defaults.items():
        if alt_provider != via:
            alternatives.append(
                {"via": alt_provider, "reason": f"default for tier={alt_tier_name}"}
            )
    if via != "gateway":
        alternatives.append(
            {
                "via": "gateway",
                "reason": "gateway fallback (Kimi/MiniMax/Deepseek/OpenRouter)",
            }
        )

    return via, reason, alternatives[:4]


def _pick_model_with_reason(
    tier: str, via: str, cfg: dict[str, Any], explicit_model: str = "auto"
) -> tuple[str, str]:
    """Pick model with explanation. Returns (model, reason)."""
    if explicit_model != "auto":
        return explicit_model, f"explicit --model={explicit_model}"

    routing = cfg.get("routing", {})

    # Check via_model_overrides first
    overrides = routing.get("via_model_overrides", {})
    via_override = overrides.get(via, {})
    if tier in via_override:
        model = via_override[tier]
        return model, f"via_model_overrides.{via}.{tier}"

    # Fall back to tier_models
    tier_models = routing.get("tier_models", {})
    if tier in tier_models:
        model = tier_models[tier]
        return model, f"tier_models.{tier}"

    print(
        f"[WARN] No model configured for tier={tier}, via={via}. Using smart-auto fallback.",
        file=sys.stderr,
    )
    return "smart-auto", "fallback to smart-auto (no specific model configured)"


def _skill_roots(repo: Path, cfg: dict[str, Any]) -> list[Path]:
    roots = []
    for raw in cfg.get("skills", {}).get("roots", []):
        path = str(raw).replace("{repo}", str(repo))
        roots.append(_expand(path))
    return roots


def _extract_keywords_from_json(path: Path) -> set[str]:
    obj = _read_json(path, {})
    out: set[str] = set()
    if isinstance(obj, dict):
        for key in ("keywords", "tags"):
            vals = obj.get(key, [])
            if isinstance(vals, list):
                for v in vals:
                    if isinstance(v, str):
                        out.update(_objective_tokens(v))
        for key in ("name", "description", "instructions"):
            v = obj.get(key)
            if isinstance(v, str):
                out.update(_objective_tokens(v))
    return out


def _parse_skill_frontmatter(path: Path) -> dict[str, Any]:
    """Parse YAML frontmatter from SKILL.md. Returns dict with summary, keywords, description.
    Handles multi-line values (continuation lines starting with whitespace)."""
    try:
        text = path.read_text(errors="ignore")[:4000]
    except Exception:
        return {}
    if not text.startswith("---"):
        return {}
    end = text.find("---", 3)
    if end < 0:
        return {}
    fm_text = text[3:end].strip()
    result: dict[str, Any] = {}
    # Join continuation lines (indented lines) with their parent key
    lines = fm_text.splitlines()
    merged: list[str] = []
    for line in lines:
        if line and not line[0].isspace() and ":" in line:
            merged.append(line)
        elif merged:
            merged[-1] += " " + line.strip()
    for line in merged:
        key, _, val = line.partition(":")
        key = key.strip().strip('"').strip("'")
        val = val.strip().strip('"').strip("'")
        if key == "keywords":
            # Parse [a, b, c] format (inline or multi-line joined)
            val = val.strip("[]")
            result["keywords"] = {
                w.strip().strip('"').strip("'").lower()
                for w in val.split(",")
                if w.strip()
            }
        elif key == "summary":
            result["summary"] = val
        elif key == "description":
            result["description"] = val
    return result


def _read_skill_content(path: Path, max_chars: int = 800) -> str:
    """Read skill content, stripping YAML frontmatter. Returns first max_chars."""
    try:
        text = path.read_text(errors="ignore")
    except Exception:
        return ""
    if text.startswith("---"):
        end = text.find("---", 3)
        if end >= 0:
            text = text[end + 3 :].strip()
    return text[:max_chars]


def discover_skills(
    repo: Path,
    objective: str,
    cfg: dict[str, Any],
    limit: int | None = None,
    categories: list[str] | None = None,
) -> list[dict[str, Any]]:
    obj_tokens = _objective_tokens(objective)
    if not obj_tokens:
        return []

    max_results = int(limit or cfg.get("skills", {}).get("max_results", 8))
    max_scan = int(cfg.get("skills", {}).get("max_scan_files", 5000))

    candidates: dict[str, dict[str, Any]] = {}
    scanned = 0

    for root in _skill_roots(repo, cfg):
        if not root.exists() or not root.is_dir():
            continue

        for pattern in ("SKILL.md", "skill.json", "prompt.md"):
            for path in root.rglob(pattern):
                scanned += 1
                if scanned > max_scan:
                    break
                if not path.is_file():
                    continue

                key = str(path.parent)
                entry = candidates.setdefault(
                    key,
                    {
                        "name": path.parent.name,
                        "path": str(path.parent),
                        "keywords": set(_objective_tokens(path.parent.name)),
                        "score": 0,
                        "summary": "",
                        "skill_file": str(path),
                    },
                )

                if path.name == "skill.json":
                    entry["keywords"].update(_extract_keywords_from_json(path))
                elif path.name in ("SKILL.md", "prompt.md"):
                    # Parse frontmatter for structured keywords/summary
                    fm = _parse_skill_frontmatter(path)
                    if fm.get("keywords"):
                        entry["keywords"].update(fm["keywords"])
                    if fm.get("summary"):
                        entry["summary"] = fm["summary"]
                    elif fm.get("description"):
                        entry["summary"] = fm["description"][:120]
                    # Also add description tokens for matching
                    if fm.get("description"):
                        entry["keywords"].update(_objective_tokens(fm["description"]))
                    # Fallback: tokenize full content for matching
                    txt = path.read_text(errors="ignore")[:3000]
                    entry["keywords"].update(_objective_tokens(txt))

            if scanned > max_scan:
                break
        if scanned > max_scan:
            break

    scored: list[dict[str, Any]] = []
    for entry in candidates.values():
        keywords: set[str] = entry.get("keywords", set())
        # Score by keyword overlap
        overlap = len(obj_tokens & keywords)
        score = overlap

        # Boost if frontmatter keywords match (these are curated, higher signal)
        fm = (
            _parse_skill_frontmatter(Path(entry["skill_file"]))
            if entry.get("skill_file")
            else {}
        )
        fm_keywords = fm.get("keywords", set())
        if fm_keywords:
            fm_overlap = len(obj_tokens & fm_keywords)
            score += fm_overlap * 2  # frontmatter keywords count double

        # lightweight fuzzy boosts
        low_name = entry["name"].lower()
        if "swarm" in low_name and any(
            t in obj_tokens for t in {"swarm", "agent", "orchestrate"}
        ):
            score += 2
        if "memory" in low_name and any(
            t in obj_tokens for t in {"memory", "context", "learn"}
        ):
            score += 2
        if "research" in low_name and any(
            t in obj_tokens for t in {"research", "search", "analyze"}
        ):
            score += 2

        if score > 0:
            scored.append(
                {
                    "name": entry["name"],
                    "path": entry["path"],
                    "score": score,
                    "summary": entry.get("summary", ""),
                    "skill_file": entry.get("skill_file", ""),
                }
            )

    # Category filtering: boost or filter by skill categories
    if categories:
        from pipeline.stages import (
            categorize_skill,
        )  # lazy import to avoid circular deps at module load

        filtered = []
        for s in scored:
            skill_cats = categorize_skill(s["name"], s["path"])
            if any(c in skill_cats for c in categories):
                s["score"] += 2  # boost matching category
                s["categories"] = skill_cats
                filtered.append(s)
            else:
                # still include non-matching but don't boost
                s["categories"] = skill_cats
                filtered.append(s)
        scored = filtered

    scored.sort(key=lambda x: (-x["score"], x["name"]))
    return scored[:max_results]


def _derive_subtask_queries(objective: str, max_queries: int = 5) -> list[str]:
    raw_parts = re.split(
        r"[;,]|\band\b|\bthen\b|\bwith\b|\bplus\b", objective, flags=re.IGNORECASE
    )
    out: list[str] = []
    seen: set[str] = set()

    for part in raw_parts:
        cleaned = re.sub(r"\s+", " ", part).strip()
        if len(cleaned) < 12:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
        if len(out) >= max_queries:
            return out

    low = objective.lower()
    hints = {
        "security": "security validation and hardening",
        "test": "test strategy and regression coverage",
        "deploy": "deployment checks and rollout safety",
        "migration": "migration sequencing and rollback",
        "orchestr": "orchestration control and checkpointing",
        "memory": "memory persistence and drift prevention",
    }
    for needle, query in hints.items():
        if needle in low and query.lower() not in seen:
            seen.add(query.lower())
            out.append(query)
            if len(out) >= max_queries:
                break

    return out


def discover_skills_for_subtasks(
    repo: Path, objective: str, cfg: dict[str, Any], per_query_limit: int = 3
) -> list[dict[str, Any]]:
    queries = _derive_subtask_queries(objective)
    bundles: list[dict[str, Any]] = []
    for q in queries:
        bundles.append(
            {
                "query": q,
                "skills": discover_skills(repo, q, cfg, limit=per_query_limit),
            }
        )
    return bundles


def _global_rules_text(cfg: dict[str, Any]) -> str:
    rules_path = _expand(
        str(
            cfg.get("paths", {}).get("global_rules_path", "~/.ai-fleet/global-rules.md")
        )
    )
    if not rules_path.exists():
        return ""
    return rules_path.read_text(errors="ignore")


def _repo_rules_text(repo: Path) -> tuple[str, str]:
    candidates = [
        repo / ".ai-coordinator" / "rules.md",
        repo / "CLAUDE.md",
        repo / ".claude" / "CLAUDE.md",
    ]
    for path in candidates:
        if path.exists():
            return path.read_text(errors="ignore"), str(path)
    return "", ""


def ensure_rules_check(
    conn: sqlite3.Connection, repo: Path, cfg: dict[str, Any], force: bool = False
) -> dict[str, Any]:
    global_text = _global_rules_text(cfg)
    repo_text, repo_rules_path = _repo_rules_text(repo)

    global_hash = _sha256_text(global_text) if global_text else ""
    repo_hash = _sha256_text(repo_text) if repo_text else ""

    row = conn.execute(
        "SELECT ts, global_hash, repo_hash FROM rule_checks WHERE repo=? ORDER BY ts DESC LIMIT 1",
        (str(repo),),
    ).fetchone()

    recheck_minutes = int(cfg.get("autonomy", {}).get("rules_recheck_minutes", 90))
    due = force
    if row is None:
        due = True
    else:
        due = due or ((_now_ts() - int(row["ts"])) >= recheck_minutes * 60)

    drift = False
    if row is not None:
        drift = (row["global_hash"] != global_hash) or (row["repo_hash"] != repo_hash)

    if due or drift:
        conn.execute(
            "INSERT INTO rule_checks(ts, repo, global_hash, repo_hash, drift, notes) VALUES(?,?,?,?,?,?)",
            (
                _now_ts(),
                str(repo),
                global_hash,
                repo_hash,
                1 if drift else 0,
                f"repo_rules={repo_rules_path or 'none'}",
            ),
        )
        conn.commit()

    return {
        "checked": due or drift,
        "drift": drift,
        "repo_rules_path": repo_rules_path,
        "global_hash": global_hash,
        "repo_hash": repo_hash,
    }


def _load_recent_repo_learning(
    conn: sqlite3.Connection, repo: Path, limit: int
) -> list[sqlite3.Row]:
    norm = _normalize_repo_path(repo)
    return conn.execute(
        "SELECT ts, pattern, fix, tags, source, confidence FROM repo_learning WHERE (repo=? OR repo LIKE ?) ORDER BY ts DESC LIMIT ?",
        (norm, f"{norm}%", limit),
    ).fetchall()


def _load_recent_global_learning(
    conn: sqlite3.Connection, limit: int
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT ts, rule, rationale, tags, source, confidence FROM global_learning ORDER BY ts DESC LIMIT ?",
        (limit,),
    ).fetchall()


def _prompt_context(
    repo: Path,
    objective: str,
    tier: str,
    model: str,
    rules_info: dict[str, Any],
    skills: list[dict[str, Any]],
    subtask_skills: list[dict[str, Any]],
    repo_learning: list[sqlite3.Row],
    global_learning: list[sqlite3.Row],
) -> str:
    lines = []
    lines.append("Coordinator Context:")
    lines.append(f"- Repository: {repo}")
    lines.append(f"- Tier: {tier}")
    lines.append(f"- Preferred model lane: {model}")
    lines.append(
        f"- Rules drift detected: {'yes' if rules_info.get('drift') else 'no'}"
    )

    if rules_info.get("repo_rules_path"):
        lines.append(f"- Repo rules source: {rules_info['repo_rules_path']}")

    if skills:
        # Tiered skill injection based on relevance score:
        # T2 (score >= 5): name + summary + actionable content (800 chars) — max 2
        # T1 (score >= 3): name + summary (1-liner)
        # T0 (score < 3): name + path only
        t2_budget = 2  # max skills getting full content injection
        t2_injected = 0
        lines.append("- Relevant skills discovered:")
        for skill in skills:
            score = skill.get("score", 0)
            summary = skill.get("summary", "")
            if score >= 5 and t2_injected < t2_budget:
                # T2: inject actionable content
                skill_file = skill.get("skill_file", "")
                content = (
                    _read_skill_content(Path(skill_file), max_chars=800)
                    if skill_file
                    else ""
                )
                if summary:
                    lines.append(f"  * {skill['name']} — {summary}")
                else:
                    lines.append(f"  * {skill['name']} ({skill['path']})")
                if content:
                    # Indent content under the skill for clarity
                    for cline in content.splitlines()[:15]:
                        lines.append(f"    {cline}")
                t2_injected += 1
            elif score >= 3 and summary:
                # T1: name + summary
                lines.append(f"  * {skill['name']} — {summary}")
            else:
                # T0: name + path only
                lines.append(f"  * {skill['name']} ({skill['path']})")

    if subtask_skills:
        lines.append("- Subtask skill research:")
        for bundle in subtask_skills:
            query = bundle.get("query", "")
            lines.append(f"  * query={query}")
            for skill in bundle.get("skills", []):
                lines.append(f"    - {skill['name']} ({skill['path']})")

    if repo_learning:
        lines.append("- Recent repo learnings:")
        for row in repo_learning:
            lines.append(
                f"  * pattern={_truncate(str(row['pattern']), 100)} | fix={_truncate(str(row['fix']), 100)}"
            )

    if global_learning:
        lines.append("- Recent global learnings:")
        for row in global_learning:
            lines.append(
                f"  * rule={_truncate(str(row['rule']), 100)} | rationale={_truncate(str(row['rationale'] or ''), 100)}"
            )

    lines.append("Execution constraints:")
    lines.append("- Follow global and repo rules.")
    lines.append("- Keep output actionable and deterministic.")
    lines.append("- If blocked by missing dependency/env, report blocker explicitly.")
    lines.append("")
    lines.append("SCOPE GUARDRAILS:")
    lines.append(
        "- ONLY work on files and features explicitly listed in the objective below"
    )
    lines.append(
        "- Do NOT refactor, clean up, or improve code outside the objective scope"
    )
    lines.append(
        "- Do NOT switch git branches unless the objective explicitly requires it"
    )
    lines.append(
        "- Do NOT create documentation files unless the objective explicitly requests it"
    )
    lines.append(
        "- If you discover additional work needed, note it in your output but do NOT implement it"
    )
    lines.append("- Stay on the assigned branch for the entire session")
    lines.append("")
    lines.append("Task Objective:")
    lines.append(objective)

    return "\n".join(lines)


def _write_checkpoint(checkpoint_dir: Path, payload: dict[str, Any]) -> Path:
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    path = checkpoint_dir / f"checkpoint_{ts}_{os.getpid()}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n")
    return path


def _record_run(
    conn: sqlite3.Connection,
    repo: Path,
    objective: str,
    via: str,
    tier: str,
    model: str,
    status: str,
    checkpoint_gate: bool,
    output_path: str = "",
    notes: str = "",
    duration_seconds: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO run_history(ts, repo, objective, via, tier, model, status, checkpoint_gate, output_path, notes, duration_seconds)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            _now_ts(),
            str(repo),
            objective,
            via,
            tier,
            model,
            status,
            1 if checkpoint_gate else 0,
            output_path,
            notes,
            duration_seconds,
        ),
    )
    conn.commit()


def _record_decision(
    conn: sqlite3.Connection,
    run_id: str,
    objective: str,
    tier: str,
    tier_reason: str,
    via: str,
    via_reason: str,
    model: str,
    model_reason: str,
    budget_check: str = "",
    alternatives: list[dict[str, Any]] | None = None,
) -> None:
    """Record a routing decision to the decisions table."""
    obj_hash = hashlib.sha256(objective.encode()).hexdigest()[:16]
    conn.execute(
        """INSERT INTO decisions(run_id, timestamp, objective_hash, objective_length, tier, tier_reason, via, via_reason, model, model_reason, budget_check, alternatives_json)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            run_id,
            _now_ts(),
            obj_hash,
            len(objective),
            tier,
            tier_reason,
            via,
            via_reason,
            model,
            model_reason,
            budget_check,
            json.dumps(alternatives or []),
        ),
    )
    conn.commit()


def _record_repo_learning(
    conn: sqlite3.Connection,
    repo: Path,
    pattern: str,
    fix: str,
    tags: str,
    source: str,
    confidence: float,
) -> None:
    norm = _normalize_repo_path(repo)
    conn.execute(
        "INSERT INTO repo_learning(ts, repo, pattern, fix, tags, source, confidence) VALUES(?,?,?,?,?,?,?)",
        (_now_ts(), norm, pattern, fix, tags, source, confidence),
    )
    conn.commit()


def _record_global_learning(
    conn: sqlite3.Connection,
    rule: str,
    rationale: str,
    tags: str,
    source: str,
    confidence: float,
) -> None:
    conn.execute(
        "INSERT INTO global_learning(ts, rule, rationale, tags, source, confidence) VALUES(?,?,?,?,?,?)",
        (_now_ts(), rule, rationale, tags, source, confidence),
    )
    conn.commit()


def _decay_confidence(confidence: float, ts: int, tau_days: float = 30.0) -> float:
    """Apply exponential decay to learning confidence. tau_days is half-life (30 days default)."""
    age_seconds = time.time() - ts
    age_days = age_seconds / 86400
    return confidence * math.exp(-age_days / tau_days)


ALLOWED_LEARNING_TABLES = frozenset({"global_learning", "repo_learning"})


def _reinforce_learning(
    conn: sqlite3.Connection, table: str, learning_id: int, boost: float = 0.05
) -> None:
    """Boost confidence on successful use. Clamp at 1.0."""
    if table not in ALLOWED_LEARNING_TABLES:
        raise ValueError(
            f"Invalid learning table: {table!r} (allowed: {ALLOWED_LEARNING_TABLES})"
        )
    conn.execute(
        f"UPDATE {table} SET confidence = MIN(1.0, confidence + ?), last_used_ts = ?, use_count = use_count + 1 WHERE id = ?",
        (boost, int(time.time()), learning_id),
    )
    conn.commit()


def _estimate_cost(
    cfg: dict, model: str, input_tokens: int, output_tokens: int
) -> float:
    """Estimate cost in USD based on config pricing table."""
    pricing = cfg.get("cost_estimation", {})
    rates = pricing.get(model)
    if not rates:
        for key, val in pricing.items():
            if key in model or model in key:
                rates = val
                break
    if not rates:
        return 0.0
    input_cost = (input_tokens / 1_000_000) * rates.get("input_per_m", 0)
    output_cost = (output_tokens / 1_000_000) * rates.get("output_per_m", 0)
    return round(input_cost + output_cost, 6)


def _current_month() -> str:
    """Return current month as YYYY-MM string."""
    return time.strftime("%Y-%m")


def _get_or_create_budget(conn, month: str, default_cap: float = 50.0) -> dict:
    """Get or create budget row for month, return as dict."""
    row = conn.execute(
        "SELECT * FROM budget_state WHERE month = ?", (month,)
    ).fetchone()
    if row:
        return dict(row)
    conn.execute(
        "INSERT INTO budget_state(month, cap_usd, spent_usd, last_updated) VALUES(?,?,?,?)",
        (month, default_cap, 0.0, _now_ts()),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM budget_state WHERE month = ?", (month,)
    ).fetchone()
    return dict(row)


def _update_budget_spent(conn, month: str, amount: float) -> dict:
    """Atomically add to spent_usd, return updated budget."""
    conn.execute(
        "UPDATE budget_state SET spent_usd = spent_usd + ?, last_updated = ? WHERE month = ?",
        (amount, _now_ts(), month),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM budget_state WHERE month = ?", (month,)
    ).fetchone()
    return dict(row)


def _check_budget_alerts(conn, budget: dict) -> list[str]:
    """Check alert thresholds (70/80/90/100%), return list of alert messages for any thresholds newly crossed."""
    cap = budget["cap_usd"]
    spent = budget["spent_usd"]
    month = budget["month"]
    alerts = []
    if cap <= 0:
        return alerts
    pct = (spent / cap) * 100
    thresholds = [
        (70, "alert_70_sent"),
        (80, "alert_80_sent"),
        (90, "alert_90_sent"),
        (100, "alert_100_sent"),
    ]
    for threshold, flag in thresholds:
        if pct >= threshold and not budget[flag]:
            alerts.append(f"budget_alert_{threshold}pct: {spent:.2f} / {cap:.2f} USD")
            conn.execute(
                f"UPDATE budget_state SET {flag} = 1, last_updated = ? WHERE month = ?",
                (_now_ts(), month),
            )
            conn.commit()
    return alerts


def _pre_dispatch_budget_check(
    conn, cfg: dict, estimated_cost: float
) -> tuple[bool, str]:
    """Returns (allowed, reason). If budget cap > 0 and spent + estimated > cap, return (False, reason)."""
    month = _current_month()
    budget = _get_or_create_budget(
        conn, month, default_cap=cfg.get("budget", {}).get("monthly_cap_usd", 50.0)
    )
    cap = budget["cap_usd"]
    spent = budget["spent_usd"]
    if cap <= 0:
        return (True, "")
    if spent + estimated_cost > cap:
        return (
            False,
            f"budget_exceeded: {spent:.2f} + {estimated_cost:.2f} > {cap:.2f} USD cap for {month}",
        )
    pct = ((spent + estimated_cost) / cap) * 100
    if pct >= 90:
        return (True, f"budget_warning: {pct:.1f}% of cap will be used")
    return (True, "")


def _generate_task_id() -> str:
    """Generate task_id in format tsk_XXXXXXXX."""
    return "tsk_" + os.urandom(4).hex()


def _submit_task(
    conn,
    objective: str,
    repo: str = ".",
    via: str = "auto",
    model: str = "auto",
    priority: int = 5,
    notes: str = "",
) -> str:
    """Submit a task to the queue, return task_id."""
    task_id = _generate_task_id()
    conn.execute(
        "INSERT INTO task_queue(task_id, objective, repo, via, model, priority, status, submitted_at, notes) VALUES(?,?,?,?,?,?,?,?,?)",
        (task_id, objective, repo, via, model, priority, "pending", _now_ts(), notes),
    )
    conn.commit()
    return task_id


def _claim_next_task(conn) -> dict | None:
    """Atomically claim the highest priority pending task. Two-query pattern for SQLite <3.35."""
    conn.row_factory = sqlite3.Row
    while True:
        row = conn.execute("""
            SELECT task_id FROM task_queue
            WHERE status = 'pending'
            ORDER BY priority ASC, submitted_at ASC, id ASC
            LIMIT 1
        """).fetchone()
        if not row:
            return None
        task_id = row["task_id"]
        now = _now_ts()
        update_cur = conn.execute(
            """
            UPDATE task_queue SET status = 'running', started_at = ?
            WHERE task_id = ? AND status = 'pending'
        """,
            (now, task_id),
        )
        if update_cur.rowcount == 0:
            # Lost race to another worker. Retry claim loop.
            continue
        conn.commit()
        result = conn.execute(
            "SELECT * FROM task_queue WHERE task_id = ? AND status = 'running'",
            (task_id,),
        ).fetchone()
        return dict(result) if result else None


def _claim_next_task_with_budget(conn) -> dict | None:
    """Atomically claim next pending task, checking budget allows execution.

    Uses two-query pattern for SQLite <3.35 compatibility (no RETURNING).
    The WHERE status='pending' guard on UPDATE prevents double-claim races.
    """
    conn.row_factory = sqlite3.Row
    month = time.strftime("%Y-%m")

    while True:
        # Find best candidate with budget check + conflict detection (skip repos with running tasks)
        row = conn.execute(
            """
            SELECT tq.task_id FROM task_queue tq
            LEFT JOIN budget_state bs ON bs.month = ?
            WHERE tq.status = 'pending'
              AND (bs.cap_usd IS NULL OR bs.cap_usd <= 0 OR bs.spent_usd < bs.cap_usd)
              AND (tq.repo IS NULL OR tq.repo = '' OR NOT EXISTS (
                SELECT 1 FROM task_queue tq2
                WHERE tq2.status = 'running' AND tq2.repo = tq.repo
                  AND tq2.repo IS NOT NULL AND tq2.repo != ''
              ))
            ORDER BY tq.priority ASC, tq.submitted_at ASC, tq.id ASC
            LIMIT 1
        """,
            (month,),
        ).fetchone()

        if not row:
            return None

        task_id = row["task_id"]
        now = _now_ts()
        worker_id = f"pid-{os.getpid()}-{threading.get_ident()}"

        # Claim it — WHERE status='pending' prevents race with other workers.
        update_cur = conn.execute(
            """
            UPDATE task_queue
            SET status = 'running', started_at = ?, heartbeat_ts = ?, claimed_by = ?
            WHERE task_id = ? AND status = 'pending'
        """,
            (now, now, worker_id, task_id),
        )

        if update_cur.rowcount == 0:
            # Another worker claimed it between SELECT and UPDATE — retry.
            continue

        conn.commit()
        result = conn.execute(
            "SELECT * FROM task_queue WHERE task_id = ? AND status = 'running' AND claimed_by = ?",
            (task_id, worker_id),
        ).fetchone()
        return dict(result) if result else None


def _requeue_stale_tasks(conn, timeout_seconds: int = 14400) -> int:
    """Requeue tasks stuck in 'running' with no recent heartbeat. Returns count requeued."""
    cutoff = _now_ts() - timeout_seconds
    update_cur = conn.execute(
        """
        UPDATE task_queue SET status = 'pending', started_at = NULL, heartbeat_ts = NULL, claimed_by = NULL
        WHERE status = 'running' AND (heartbeat_ts < ? OR heartbeat_ts IS NULL) AND started_at < ?
    """,
        (cutoff, cutoff),
    )
    requeued = update_cur.rowcount
    if requeued:
        conn.commit()
    return requeued


def _preflight_git_check(task: dict, conn) -> tuple[bool, str]:
    """Pre-flight check: verify task's repo/objective is not stale.

    Returns (should_execute, skip_reason). If should_execute is False,
    the task should be skipped.
    """
    repo_path = task.get("repo", "").strip()
    if not repo_path or repo_path == ".":
        return True, ""

    repo = Path(repo_path).expanduser().resolve()
    if not repo.is_dir():
        return False, f"repo not found: {repo_path}"

    try:
        # Check recent commits that may have already addressed this objective
        log_result = subprocess.run(
            ["git", "log", "--oneline", "-5", "--since=1 hour ago"],
            cwd=str(repo),
            capture_output=True,
            text=True,
            timeout=30,
        )
        recent_commits = log_result.stdout.strip()

        objective = task.get("objective", "").lower()
        obj_words = {w for w in objective.split() if len(w) > 4}

        if recent_commits and obj_words:
            commit_text = recent_commits.lower()
            matched = sum(
                1 for w in obj_words if re.search(rf"\b{re.escape(w)}\b", commit_text)
            )
            if matched > len(obj_words) * 0.4:
                return (
                    False,
                    f"objective likely addressed by recent commits: {recent_commits.splitlines()[0]}",
                )
    except Exception as e:
        print(
            f"warn: _preflight_git_check failed for task {task.get('task_id')}: {e}",
            file=sys.stderr,
        )

    return True, ""


def _check_task_overlap(task: dict, conn) -> tuple[bool, str]:
    """Session-aware dedup: check if recently completed tasks overlap >50% with this one.

    Returns (has_overlap, overlap_reason).
    """
    repo = task.get("repo", "").strip()
    if not repo or repo == ".":
        return False, ""

    try:
        conn.row_factory = sqlite3.Row
        recent = conn.execute(
            """
            SELECT task_id, objective FROM task_queue
            WHERE status = 'completed' AND repo = ? AND completed_at > ?
            ORDER BY completed_at DESC LIMIT 5
        """,
            (repo, int(time.time()) - 3600),
        ).fetchall()

        if not recent:
            return False, ""

        obj_words = {w.lower() for w in task.get("objective", "").split() if len(w) > 3}
        if not obj_words:
            # Fallback: exact match for short objectives
            obj_text = task.get("objective", "").strip().lower()
            if not obj_text:
                return False, ""
            for row in recent:
                if (row["objective"] or "").strip().lower() == obj_text:
                    return True, f"exact match with completed task {row['task_id']}"
            return False, ""

        for row in recent:
            completed_words = {
                w.lower() for w in (row["objective"] or "").split() if len(w) > 3
            }
            overlap = obj_words & completed_words
            if len(overlap) > len(obj_words) * 0.5:
                return True, f"overlaps with completed task {row['task_id']}"
    except Exception as e:
        print(
            f"warn: _check_task_overlap failed for task {task.get('task_id')}: {e}",
            file=sys.stderr,
        )

    return False, ""


def _auto_push(repo_path: str, task_id: str, conn) -> bool:
    """Push current branch to origin after successful task completion."""
    try:
        repo = Path(repo_path).expanduser().resolve()
        if not repo.is_dir():
            return False

        result = subprocess.run(
            ["git", "push", "-u", "origin", "HEAD"],
            cwd=str(repo),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            _notify_event(
                conn, "auto_push", f"Task {task_id}: pushed to origin", "info", task_id
            )
            return True
        else:
            _notify_event(
                conn,
                "auto_push_failed",
                f"Task {task_id}: push failed: {result.stderr.strip()[:200]}",
                "warning",
                task_id,
            )
            return False
    except Exception as e:
        _notify_event(
            conn,
            "auto_push_failed",
            f"Task {task_id}: push error: {e}",
            "warning",
            task_id,
        )
        return False


def _check_token_usage(output: str, task_id: str, cfg: dict, conn) -> None:
    """Parse subprocess output for token counts and alert if thresholds exceeded."""
    token_patterns = [
        r"total[_\s]tokens[:\s]+(\d[\d,]+)",
        r"tokens[_\s]used[:\s]+(\d[\d,]+)",
        r"(\d[\d,]+)\s+tokens?\s+(?:used|consumed|total)",
    ]
    max_tokens = 0
    for pattern in token_patterns:
        for m in re.findall(pattern, output, re.IGNORECASE):
            try:
                count = int(m.replace(",", ""))
                max_tokens = max(max_tokens, count)
            except ValueError:
                pass

    if max_tokens == 0:
        return

    queue_cfg = cfg.get("queue", {})
    warning = queue_cfg.get("token_warning", 1_000_000)
    critical = queue_cfg.get("token_critical", 2_000_000)

    if max_tokens >= critical:
        _notify_event(
            conn,
            "token_budget_critical",
            f"Task {task_id}: {max_tokens:,} tokens (critical >{critical:,})",
            "critical",
            task_id,
        )
    elif max_tokens >= warning:
        _notify_event(
            conn,
            "token_budget_warning",
            f"Task {task_id}: {max_tokens:,} tokens (warning >{warning:,})",
            "warning",
            task_id,
        )


def _complete_task(
    conn,
    task_id: str,
    exit_code: int,
    run_id: str = "",
    output_path: str = "",
    error: str = "",
    outcome_details: str = "",
) -> None:
    """Mark task as completed or failed."""
    status = "completed" if exit_code == 0 else "failed"
    conn.execute(
        "UPDATE task_queue SET status = ?, completed_at = ?, run_id = ?, exit_code = ?, output_path = ?, error = ?, outcome_details = ? WHERE task_id = ?",
        (
            status,
            _now_ts(),
            run_id,
            exit_code,
            output_path,
            error,
            outcome_details,
            task_id,
        ),
    )
    conn.commit()


def _cancel_task(conn, task_id: str) -> bool:
    """Cancel a pending task, return True if was pending."""
    row = conn.execute(
        "SELECT status FROM task_queue WHERE task_id = ?", (task_id,)
    ).fetchone()
    if not row or row[0] != "pending":
        return False
    conn.execute(
        "UPDATE task_queue SET status = 'cancelled', completed_at = ? WHERE task_id = ?",
        (_now_ts(), task_id),
    )
    conn.commit()
    return True


def _create_notification(
    conn,
    event_type: str,
    message: str,
    severity: str = "info",
    task_id: str = "",
    run_id: str = "",
) -> int:
    """Create notification, return id."""
    cursor = conn.execute(
        "INSERT INTO notifications(event_type, message, severity, task_id, run_id, created_at) VALUES(?,?,?,?,?,?)",
        (event_type, message, severity, task_id, run_id, _now_ts()),
    )
    conn.commit()
    return cursor.lastrowid


def _deliver_notification(
    conn, notification_id: int, channel_configs: list[dict]
) -> bool:
    """Attempt delivery to enabled channels, return True if any succeeded."""
    row = conn.execute(
        "SELECT event_type, message, severity FROM notifications WHERE id = ?",
        (notification_id,),
    ).fetchone()
    if not row:
        return False

    event_type, message, severity = row
    delivered = False
    channels_used = []

    for ch in channel_configs:
        if ch["channel_type"] == "terminal":
            print(
                f"\a[{severity.upper()}] {event_type}: {message}",
                file=sys.stderr,
                flush=True,
            )
            delivered = True
            channels_used.append("terminal")

        elif ch["channel_type"] == "telegram":
            bot_token = ch["config"].get("bot_token") or os.getenv("TELEGRAM_BOT_TOKEN")
            chat_id = ch["config"].get("chat_id") or os.getenv("TELEGRAM_CHAT_ID")

            if bot_token and chat_id:
                try:
                    from pipeline.telegram import send_telegram

                    severity_icon = {
                        "info": "ℹ️",
                        "warning": "⚠️",
                        "error": "❌",
                        "critical": "🚨",
                    }.get(severity, "📢")
                    telegram_msg = f"{severity_icon} *{event_type}*\n\n{message}"
                    success, error = send_telegram(bot_token, chat_id, telegram_msg)
                    if success:
                        delivered = True
                        channels_used.append("telegram")
                except Exception as exc:
                    print(f"warn: telegram notification failed: {exc}", file=sys.stderr)

    if delivered:
        conn.execute(
            "UPDATE notifications SET delivered_at = ?, delivery_status = 'delivered', channel = ? WHERE id = ?",
            (_now_ts(), ",".join(channels_used), notification_id),
        )
        conn.commit()

    return delivered


def _notify_event(
    conn,
    event_type: str,
    message: str,
    severity: str = "info",
    task_id: str = "",
    run_id: str = "",
) -> None:
    """Create and deliver notification with deduplication."""
    cutoff = _now_ts() - 300
    dup = conn.execute(
        "SELECT id FROM notifications WHERE event_type = ? AND message = ? AND created_at > ? LIMIT 1",
        (event_type, message, cutoff),
    ).fetchone()
    if dup:
        return

    notif_id = _create_notification(
        conn, event_type, message, severity, task_id, run_id
    )
    channels = _get_enabled_channels(conn)
    _deliver_notification(conn, notif_id, channels)


def _get_enabled_channels(conn) -> list[dict]:
    """Return enabled notification channels with their configs."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM notification_channels WHERE enabled = 1"
    ).fetchall()
    result = []
    for r in rows:
        cfg = {}
        try:
            cfg = json.loads(r["config_json"] or "{}")
        except Exception:
            pass
        result.append({"channel_type": r["channel_type"], "config": cfg})
    return result


def _extract_tokens(via: str, raw_output: str) -> tuple[int, int]:
    """Extract input/output token counts from provider response."""
    input_tokens = 0
    output_tokens = 0
    m = re.search(r'"(?:input_tokens|prompt_tokens)":\s*(\d+)', raw_output)
    if m:
        input_tokens = int(m.group(1))
    m = re.search(r'"(?:output_tokens|completion_tokens)":\s*(\d+)', raw_output)
    if m:
        output_tokens = int(m.group(1))
    if output_tokens == 0 and raw_output:
        # Don't guess tokens from raw output length — it's wildly inaccurate.
        # Return 0 and let cost estimation handle unknown tokens gracefully.
        pass
    return input_tokens, output_tokens


def _record_cost(
    conn,
    run_id: str,
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: float,
    tier: str,
    via: str,
) -> None:
    conn.execute(
        "INSERT INTO cost_log(run_id, timestamp, provider, model, input_tokens, output_tokens, estimated_cost_usd, tier, via) VALUES(?,?,?,?,?,?,?,?,?)",
        (
            run_id,
            int(time.time()),
            provider,
            model,
            input_tokens,
            output_tokens,
            cost_usd,
            tier,
            via,
        ),
    )
    # Update budget state and check alerts
    month = time.strftime("%Y-%m")
    conn.execute(
        """
        UPDATE budget_state SET spent_usd = spent_usd + ?, last_updated = ?
        WHERE month = ?
    """,
        (cost_usd, _now_ts(), month),
    )
    conn.commit()

    budget = _get_or_create_budget(conn, month)
    alerts = _check_budget_alerts(conn, budget)
    for alert_msg in alerts:
        _notify_event(
            conn, "budget_alert", alert_msg, severity="warning", run_id=run_id
        )


def _parse_rate_limit_headers(headers: dict) -> dict | None:
    """Parse X-RateLimit-Remaining, X-RateLimit-Limit, X-RateLimit-Reset from response headers."""
    result = {}
    for key, value in headers.items():
        key_lower = key.lower()
        if key_lower == "x-ratelimit-remaining":
            result["remaining"] = int(value)
        elif key_lower == "x-ratelimit-limit":
            result["limit"] = int(value)
        elif key_lower == "x-ratelimit-reset":
            result["reset_ts"] = int(value)
    if not result:
        return None
    return result


def _update_rate_limit_state(
    conn, profile_type: str, profile_name: str, headers: dict
) -> None:
    """Upsert rate_limit_state from parsed headers."""
    parsed = _parse_rate_limit_headers(headers)
    if not parsed:
        return
    conn.execute(
        """
        INSERT INTO rate_limit_state(profile_type, profile_name, remaining_requests, limit_requests, reset_ts, last_updated)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(profile_type, profile_name) DO UPDATE SET
            remaining_requests = excluded.remaining_requests,
            limit_requests = excluded.limit_requests,
            reset_ts = excluded.reset_ts,
            last_updated = excluded.last_updated,
            consecutive_429s = 0,
            cooldown_until = 0
    """,
        (
            profile_type,
            profile_name,
            parsed.get("remaining", -1),
            parsed.get("limit", -1),
            parsed.get("reset_ts", 0),
            _now_ts(),
        ),
    )
    conn.commit()


def _record_429(conn, profile_type: str, profile_name: str) -> int:
    """Increment consecutive_429s, set cooldown_until with exponential backoff (base 30s, max 600s), return cooldown seconds."""
    row = conn.execute(
        "SELECT consecutive_429s FROM rate_limit_state WHERE profile_type = ? AND profile_name = ?",
        (profile_type, profile_name),
    ).fetchone()
    consecutive = (row["consecutive_429s"] + 1) if row else 1
    backoff_seconds = min(30 * (2 ** (consecutive - 1)), 600)
    cooldown_until = _now_ts() + backoff_seconds
    conn.execute(
        """
        INSERT INTO rate_limit_state(profile_type, profile_name, last_429_ts, consecutive_429s, cooldown_until, last_updated)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(profile_type, profile_name) DO UPDATE SET
            last_429_ts = excluded.last_429_ts,
            consecutive_429s = excluded.consecutive_429s,
            cooldown_until = excluded.cooldown_until,
            last_updated = excluded.last_updated
    """,
        (profile_type, profile_name, _now_ts(), consecutive, cooldown_until, _now_ts()),
    )
    conn.commit()
    return backoff_seconds


def _select_best_profile(conn, profile_type: str, profiles: list[str]) -> str | None:
    """Select profile with most remaining requests that isn't in cooldown. If all in cooldown, return the one whose cooldown expires soonest."""
    now = _now_ts()
    available = []
    for profile_name in profiles:
        row = conn.execute(
            "SELECT remaining_requests, cooldown_until FROM rate_limit_state WHERE profile_type = ? AND profile_name = ?",
            (profile_type, profile_name),
        ).fetchone()
        if not row:
            available.append((profile_name, -1, 0))
        else:
            available.append(
                (profile_name, row["remaining_requests"], row["cooldown_until"])
            )
    not_in_cooldown = [
        (name, remaining) for name, remaining, cooldown in available if cooldown <= now
    ]
    if not_in_cooldown:
        not_in_cooldown.sort(key=lambda x: x[1], reverse=True)
        return not_in_cooldown[0][0]
    available.sort(key=lambda x: x[2])
    return available[0][0] if available else None


def _is_profile_in_cooldown(conn, profile_type: str, profile_name: str) -> bool:
    """Check if cooldown_until > now."""
    row = conn.execute(
        "SELECT cooldown_until FROM rate_limit_state WHERE profile_type = ? AND profile_name = ?",
        (profile_type, profile_name),
    ).fetchone()
    if not row:
        return False
    return row["cooldown_until"] > _now_ts()


def _write_repo_snapshot(
    conn: sqlite3.Connection, paths: dict[str, Path], repo: Path, rules: dict[str, Any]
) -> Path:
    repo_rows = _load_recent_repo_learning(conn, repo, 20)
    global_rows = _load_recent_global_learning(conn, 20)
    snapshot = {
        "ts": _now_ts(),
        "repo": str(repo),
        "rules": rules,
        "repo_learning": [dict(r) for r in repo_rows],
        "global_learning": [dict(r) for r in global_rows],
        "recent_runs": [
            dict(r)
            for r in conn.execute(
                "SELECT ts, objective, via, tier, model, status FROM run_history WHERE repo=? ORDER BY ts DESC LIMIT 20",
                (str(repo),),
            ).fetchall()
        ],
    }
    snap_name = re.sub(r"[^a-zA-Z0-9._-]", "_", str(repo)) + ".json"
    snap_path = paths["snapshot_dir"] / snap_name
    snap_path.write_text(json.dumps(snapshot, indent=2) + "\n")
    return snap_path


def _maybe_auto_snapshot(
    conn: sqlite3.Connection,
    cfg: dict[str, Any],
    paths: dict[str, Path],
    repo: Path,
    rules: dict[str, Any],
) -> str:
    every = int(cfg.get("learning", {}).get("snapshot_every_runs", 5) or 0)
    if every <= 0:
        return ""
    count = conn.execute(
        "SELECT COUNT(*) FROM run_history WHERE repo=?", (str(repo),)
    ).fetchone()[0]
    if count % every != 0:
        return ""
    return str(_write_repo_snapshot(conn, paths, repo, rules))


def _select_droid_model(cfg: dict[str, Any], swarm: bool) -> str:
    models = cfg.get("droid", {}).get("models", {})
    return str(models.get("swarm" if swarm else "direct", "smart-codex"))


def _print_preflight(preflight: dict[str, Any]) -> None:
    print("Preflight")
    print("---------")
    for row in preflight["results"]:
        marker = "PASS" if row["ok"] else ("FAIL" if row["required"] else "WARN")
        print(f"[{marker}] {row['type']}:{row['name']} {row['detail']}")
    print(f"preflight_ok={preflight['ok']}")
    if preflight["production_checks"]:
        print("production_checks=enabled")


def _save_run_output(
    runs_dir: Path, via: str, objective: str, result: dict[str, Any]
) -> Path:
    runs_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = runs_dir / f"run_{ts}_{via}.log"
    body = []
    body.append(f"timestamp={_now_ts()}")
    body.append(f"via={via}")
    body.append(f"objective={objective}")
    body.append(f"returncode={result['returncode']}")
    body.append("\n[stdout]\n")
    body.append(_redact_sensitive_text(_ensure_str(result.get("stdout", ""))))
    body.append("\n[stderr]\n")
    body.append(_redact_sensitive_text(_ensure_str(result.get("stderr", ""))))
    out_path.write_text("\n".join(body))
    return out_path


def _render_skills(skills: list[dict[str, Any]]) -> None:
    if not skills:
        print("No skill matches found.")
        return
    print("skill_matches")
    print("------------")
    for s in skills:
        print(f"{s['score']:>2}  {s['name']}  {s['path']}")


def _build_observability_snapshot(
    cfg: dict[str, Any],
    conn: sqlite3.Connection,
    repo: Path | None,
    hours: int,
    limit: int,
) -> dict[str, Any]:
    since_ts = _now_ts() - max(1, int(hours)) * 3600
    where = "ts >= ?"
    params: list[Any] = [since_ts]
    repo_str = ""
    if repo is not None:
        repo_str = str(repo)
        where += " AND repo = ?"
        params.append(repo_str)

    total_runs = int(
        conn.execute(
            f"SELECT COUNT(*) FROM run_history WHERE {where}", tuple(params)
        ).fetchone()[0]
    )
    status_rows = conn.execute(
        f"SELECT status, COUNT(*) AS count FROM run_history WHERE {where} GROUP BY status ORDER BY count DESC",
        tuple(params),
    ).fetchall()
    via_rows = conn.execute(
        f"SELECT via, COUNT(*) AS count FROM run_history WHERE {where} GROUP BY via ORDER BY count DESC",
        tuple(params),
    ).fetchall()
    fallback_runs = int(
        conn.execute(
            f"SELECT COUNT(*) FROM run_history WHERE {where} AND (status LIKE '%fallback%' OR notes LIKE '%fallback=%')",
            tuple(params),
        ).fetchone()[0]
    )
    failed_runs = int(
        conn.execute(
            f"SELECT COUNT(*) FROM run_history WHERE {where} AND status IN ('failed','blocked_preflight')",
            tuple(params),
        ).fetchone()[0]
    )

    recent_failures = [
        dict(r)
        for r in conn.execute(
            f"""
            SELECT ts, repo, objective, via, tier, model, status, notes, output_path
            FROM run_history
            WHERE {where} AND status IN ('failed','blocked_preflight')
            ORDER BY ts DESC
            LIMIT ?
            """,
            tuple(params + [max(1, int(limit))]),
        ).fetchall()
    ]

    health = _gateway_health(cfg)
    route = _gateway_route(cfg)
    health_obj = health.get("json", {}) if health.get("ok") else {}

    return {
        "window_hours": int(hours),
        "since_ts": since_ts,
        "since_local": _fmt_ts(since_ts),
        "repo_filter": repo_str,
        "runs": {
            "total": total_runs,
            "failed": failed_runs,
            "fallback": fallback_runs,
            "failure_rate": round((failed_runs / total_runs), 4) if total_runs else 0.0,
            "fallback_rate": round((fallback_runs / total_runs), 4)
            if total_runs
            else 0.0,
            "status_counts": {str(r["status"]): int(r["count"]) for r in status_rows},
            "via_counts": {str(r["via"]): int(r["count"]) for r in via_rows},
            "recent_failures": recent_failures,
        },
        "gateway": {
            "health_ok": bool(health.get("ok")),
            "route_ok": bool(route.get("ok")),
            "status_code": int(health.get("status", 0) or 0),
            "route": route.get("json", {}) if route.get("ok") else {},
            "claude_usage": ((health_obj.get("claude") or {}).get("usage") or {}),
            "codex": health_obj.get("codex", {}),
            "gemini": health_obj.get("gemini", {}),
            "external_providers": health_obj.get("external_providers", []),
        },
    }


def cmd_init(args: argparse.Namespace) -> int:
    if args.force:
        cfg = _read_json(REPO_DEFAULT_CONFIG, {})
    else:
        cfg = load_config()
    paths = ensure_state(cfg, force=args.force)
    print(f"config={USER_CONFIG_PATH}")
    print(f"db={paths['db_path']}")
    print(f"global_rules={paths['rules_path']}")
    print(f"state_dir={paths['state_dir']}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    health = _gateway_health(cfg)
    route = _gateway_route(cfg)
    health_obj = health.get("json", {}) if health.get("ok") else {}

    repo_count = conn.execute("SELECT COUNT(*) FROM repo_learning").fetchone()[0]
    global_count = conn.execute("SELECT COUNT(*) FROM global_learning").fetchone()[0]
    run_count = conn.execute("SELECT COUNT(*) FROM run_history").fetchone()[0]
    last_run = conn.execute(
        "SELECT ts, repo, status, via, tier, model FROM run_history ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    checkpoints = len(list(paths["checkpoint_dir"].glob("checkpoint_*.json")))
    since_24h = _now_ts() - 86400
    failed_24h = int(
        conn.execute(
            "SELECT COUNT(*) FROM run_history WHERE ts >= ? AND status IN ('failed','blocked_preflight')",
            (since_24h,),
        ).fetchone()[0]
    )
    fallback_24h = int(
        conn.execute(
            "SELECT COUNT(*) FROM run_history WHERE ts >= ? AND (status LIKE '%fallback%' OR notes LIKE '%fallback=%')",
            (since_24h,),
        ).fetchone()[0]
    )

    print("AI Coordinator Status")
    print("---------------------")
    print(f"gateway_ok={health.get('ok', False)}")
    print(f"route_ok={route.get('ok', False)}")

    if health.get("ok"):
        print(f"gateway_status={health_obj.get('status', 'unknown')}")
        print(
            f"recommended_codex={((health_obj.get('codex') or {}).get('recommended_profile', ''))}"
        )
        print(
            f"recommended_gemini={((health_obj.get('gemini') or {}).get('recommended_profile', ''))}"
        )

    if route.get("ok"):
        r = route.get("json", {})
        print(f"route_codex_profile={r.get('codex_profile', '')}")
        print(f"route_gemini_profile={r.get('gemini_profile', '')}")
        print(f"route_claude_source={r.get('claude_source', '')}")

    print(f"repo_learning_rows={repo_count}")
    print(f"global_learning_rows={global_count}")
    print(f"run_history_rows={run_count}")
    print(f"checkpoint_files={checkpoints}")
    print(f"failed_runs_24h={failed_24h}")
    print(f"fallback_runs_24h={fallback_24h}")

    if last_run:
        print(
            "last_run="
            f"ts={last_run['ts']} repo={last_run['repo']} status={last_run['status']} "
            f"via={last_run['via']} tier={last_run['tier']} model={last_run['model']}"
        )

    usage = (health_obj.get("claude") or {}).get("usage") or {}
    sources = usage.get("sources") or []
    if sources:
        print("\nclaude_sources")
        print("--------------")
        for src in sources:
            if not isinstance(src, dict):
                continue
            print(
                f"name={src.get('name', '')} status={src.get('status', '')} "
                f"email={src.get('email', '') or 'none'} account_type={src.get('account_type', '') or 'none'} "
                f"standby={src.get('standby', False)} group={src.get('group', '') or 'none'} "
                f"ok_rate={src.get('ok_rate', '')} fail_ratio={src.get('fail_ratio', '')} score={src.get('score', '')}"
            )

    providers = health_obj.get("external_providers") or []
    if providers:
        print("\nexternal_providers")
        print("------------------")
        for p in providers:
            if not isinstance(p, dict):
                continue
            print(
                f"name={p.get('name', '')} enabled={p.get('enabled', False)} key_present={p.get('key_present', False)} "
                f"healthy={p.get('healthy', False)} failures={p.get('failures', 0)} requests={p.get('requests', 0)}"
            )

    conn.close()
    return 0


# --- Doctor & Setup ---


def _check_binary(name: str) -> bool:
    """Check if a binary is available on PATH."""
    return shutil.which(name) is not None


def _check_port(port: int, timeout: float = 2.0) -> bool:
    """Check if a service is listening on a port."""
    import socket

    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except (ConnectionRefusedError, OSError, TimeoutError):
        return False


def _doctor_checks() -> list[dict[str, Any]]:
    """Run all doctor checks and return results."""
    checks: list[dict[str, Any]] = []
    home = Path.home()
    fleet_dir = home / ".ai-fleet"
    auth_dir = home / ".cli-proxy-api"

    def add(category: str, name: str, ok: bool, fix: str = "", detail: str = ""):
        checks.append(
            {"category": category, "name": name, "ok": ok, "fix": fix, "detail": detail}
        )

    # --- Prerequisites ---
    py_ok = _check_binary("python3")
    add("prereqs", "python3", py_ok, "brew install python3")
    if py_ok:
        import sys

        py_ver = f"{sys.version_info.major}.{sys.version_info.minor}"
        add(
            "prereqs",
            f"python >= 3.12 (have {py_ver})",
            sys.version_info >= (3, 12),
            "brew install python@3.12",
        )

    for tool, install in [
        ("gh", "brew install gh"),
        ("curl", "brew install curl"),
        ("jq", "brew install jq"),
        ("sqlite3", "built-in"),
    ]:
        add("prereqs", tool, _check_binary(tool), install)

    # CLI tools (optional but important)
    for tool, install in [
        ("claude", "npm install -g @anthropic-ai/claude-code"),
        ("codex", "npm install -g @openai/codex"),
    ]:
        add("cli_tools", tool, _check_binary(tool), install)

    # --- Config Files ---
    add("config", "~/.ai-fleet/ directory", fleet_dir.exists(), "mkdir -p ~/.ai-fleet")
    add(
        "config",
        "coordinator.json",
        (fleet_dir / "coordinator.json").exists(),
        "cp coordinator/coordinator.default.json ~/.ai-fleet/coordinator.json",
    )
    add(
        "config",
        "gateway.json",
        (fleet_dir / "gateway.json").exists(),
        "ai-fleet-gateway (auto-creates on first run)",
    )

    # --- Services ---
    add(
        "services",
        "CLIProxyAPI (:8317)",
        _check_port(8317),
        "brew services start cliproxyapi",
    )
    add(
        "services",
        "Gateway (:4105)",
        _check_port(4105),
        "launchctl load ~/Library/LaunchAgents/com.ai.fleet.gateway.plist",
    )

    # --- Auth Profiles ---
    if auth_dir.exists():
        claude_auths = list(auth_dir.glob("claude-*.json"))
        ag_auths = list(auth_dir.glob("antigravity-*.json"))
        codex_auths = list(auth_dir.glob("codex-*.json"))
        gemini_auths = list(auth_dir.glob("*gen-lang-client*.json"))

        add(
            "auth",
            f"Claude accounts ({len(claude_auths)})",
            len(claude_auths) >= 1,
            "cliproxyapi -claude-login",
            detail=", ".join(f.stem.replace("claude-", "") for f in claude_auths),
        )
        add(
            "auth",
            f"Antigravity accounts ({len(ag_auths)})",
            len(ag_auths) >= 1,
            "cliproxyapi -antigravity-login",
            detail=", ".join(f.stem.replace("antigravity-", "") for f in ag_auths),
        )
        add(
            "auth",
            f"Codex accounts ({len(codex_auths)})",
            len(codex_auths) >= 1,
            "cliproxyapi -codex-login",
            detail=", ".join(f.stem for f in codex_auths),
        )
        add(
            "auth",
            f"Gemini accounts ({len(gemini_auths)})",
            len(gemini_auths) >= 1,
            "cliproxyapi -login",
            detail=", ".join(f.stem for f in gemini_auths),
        )
    else:
        add(
            "auth",
            "CLIProxyAPI auth dir",
            False,
            "cliproxyapi -claude-login (creates dir on first login)",
        )

    # --- CLI Wrappers ---
    local_bin = home / ".local" / "bin"
    for wrapper in ["ai-fleet", "ai-fleet-gateway", "ai-auth-manage"]:
        add("install", wrapper, (local_bin / wrapper).exists(), "./install.sh")

    # --- Database ---
    db_path = fleet_dir / "coordinator" / "memory.db"
    db_alt = fleet_dir / "coordinator.db"
    add(
        "state",
        "coordinator database",
        db_path.exists() or db_alt.exists(),
        "python3 -m coordinator.ai_coordinator init",
    )

    # --- Connectivity (parallel probes via connection pool) ---
    def _check_cliproxy_health() -> list[dict[str, Any]]:
        """Check CLIProxyAPI health + models using connection pool."""
        results: list[dict[str, Any]] = []
        if not _check_port(8317):
            return results
        cliproxy_healthy = False
        resp = _http_json("http://127.0.0.1:8317/health", timeout=5)
        if resp.get("ok"):
            hdata = resp.get("json", {})
            cliproxy_healthy = hdata.get("status") == "ok"
            ver = hdata.get("version", "unknown")
            results.append(
                {
                    "category": "connectivity",
                    "name": f"CLIProxyAPI health (v{ver})",
                    "ok": cliproxy_healthy,
                    "fix": "CLIProxyAPI /health endpoint not responding",
                    "detail": "",
                }
            )
        else:
            # Try root endpoint for older builds
            resp2 = _http_json("http://127.0.0.1:8317/", timeout=5)
            if resp2.get("ok"):
                cliproxy_healthy = True
                results.append(
                    {
                        "category": "connectivity",
                        "name": "CLIProxyAPI health",
                        "ok": True,
                        "fix": "CLIProxyAPI not responding (no /health endpoint — update recommended)",
                        "detail": "",
                    }
                )
            else:
                results.append(
                    {
                        "category": "connectivity",
                        "name": "CLIProxyAPI health",
                        "ok": False,
                        "fix": "CLIProxyAPI not responding to HTTP requests",
                        "detail": "",
                    }
                )

        # Check models
        cliproxy_key = os.getenv("CLIPROXY_API_KEY", "").strip()
        if not cliproxy_key:
            try:
                import yaml as _yaml

                with open(os.path.expanduser("~/.claudemax/config.yaml")) as _f:
                    _orch_cfg = _yaml.safe_load(_f) or {}
                for _k in _orch_cfg.get("api_keys", []):
                    if isinstance(_k, str) and "*" not in _k:
                        cliproxy_key = _k
                        break
            except Exception:
                pass
        if not cliproxy_key:
            results.append(
                {
                    "category": "connectivity",
                    "name": "CLIProxyAPI models",
                    "ok": False,
                    "fix": "Set CLIPROXY_API_KEY or configure a non-wildcard API key in ~/.claudemax/config.yaml",
                    "detail": "missing_api_key",
                }
            )
            return results

        resp3 = _http_json(
            "http://127.0.0.1:8317/v1/models",
            headers={"Authorization": f"Bearer {cliproxy_key}"},
            timeout=5,
        )
        if resp3.get("ok"):
            model_count = len(resp3.get("json", {}).get("data", []))
            results.append(
                {
                    "category": "connectivity",
                    "name": f"CLIProxyAPI models ({model_count})",
                    "ok": model_count > 0,
                    "fix": "Check CLIProxyAPI auth — no models available",
                    "detail": "",
                }
            )
        elif not cliproxy_healthy:
            results.append(
                {
                    "category": "connectivity",
                    "name": "CLIProxyAPI models",
                    "ok": False,
                    "fix": "CLIProxyAPI not responding to model list",
                    "detail": "",
                }
            )
        else:
            results.append(
                {
                    "category": "connectivity",
                    "name": "CLIProxyAPI models",
                    "ok": False,
                    "fix": "CLIProxyAPI rejected model list request. Verify CLIPROXY_API_KEY is valid",
                    "detail": str(resp3.get("status", "auth_error")),
                }
            )
        return results

    def _check_gateway_health() -> list[dict[str, Any]]:
        """Check gateway health using connection pool."""
        results: list[dict[str, Any]] = []
        if not _check_port(4105):
            return results
        cfg = load_config()
        health = _gateway_health(cfg)
        gw_ok = health.get("ok", False)
        gw_status = (
            health.get("json", {}).get("status", "unknown") if gw_ok else "unreachable"
        )
        results.append(
            {
                "category": "connectivity",
                "name": f"Gateway health ({gw_status})",
                "ok": gw_ok and gw_status == "ok",
                "fix": "Check gateway logs: ~/.ai-fleet/logs/gateway.stderr.log",
                "detail": "",
            }
        )
        return results

    # Run connectivity checks in parallel
    with ThreadPoolExecutor(max_workers=2) as pool:
        cliproxy_future = pool.submit(_check_cliproxy_health)
        gateway_future = pool.submit(_check_gateway_health)
        for result_list in (cliproxy_future.result(), gateway_future.result()):
            checks.extend(result_list)

    return checks


def cmd_doctor(args: argparse.Namespace) -> int:
    """Run comprehensive health checks and show what's missing."""
    checks = _doctor_checks()

    categories = [
        "prereqs",
        "config",
        "services",
        "auth",
        "install",
        "state",
        "connectivity",
        "cli_tools",
    ]
    category_labels = {
        "prereqs": "Prerequisites",
        "config": "Configuration",
        "services": "Services",
        "auth": "Auth Profiles",
        "install": "CLI Wrappers",
        "state": "State/Database",
        "connectivity": "Connectivity",
        "cli_tools": "AI CLI Tools",
    }

    total_ok = sum(1 for c in checks if c["ok"])
    total = len(checks)
    all_good = total_ok == total

    for cat in categories:
        cat_checks = [c for c in checks if c["category"] == cat]
        if not cat_checks:
            continue
        label = category_labels.get(cat, cat)
        print(f"\n{label}")
        print("-" * len(label))
        for c in cat_checks:
            icon = "OK" if c["ok"] else "FAIL"
            line = f"  [{icon:>4}] {c['name']}"
            if c["detail"] and c["ok"]:
                line += f"  ({c['detail']})"
            print(line)
            if not c["ok"] and c["fix"]:
                print(f"         fix: {c['fix']}")

    print(
        f"\n{'ALL GOOD' if all_good else 'ISSUES FOUND'} ({total_ok}/{total} checks passed)"
    )

    if not all_good:
        print("\nTo fix automatically where possible:")
        print("  python3 -m coordinator.ai_coordinator setup")

    return 0 if all_good else 1


def cmd_setup(args: argparse.Namespace) -> int:
    """Interactive setup wizard — fixes what it can, guides what it can't."""
    home = Path.home()
    fleet_dir = home / ".ai-fleet"
    repo_root = Path(__file__).resolve().parent.parent

    print("AI Fleet Setup")
    print("=" * 50)

    # Step 1: Create directories
    print("\n[1/6] Directories")
    for d in [fleet_dir, fleet_dir / "logs"]:
        if not d.exists():
            d.mkdir(parents=True)
            print(f"  Created {d}")
        else:
            print(f"  OK {d}")

    # Step 2: Config files
    print("\n[2/6] Configuration")
    coord_cfg = fleet_dir / "coordinator.json"
    coord_default = repo_root / "coordinator" / "coordinator.default.json"
    if not coord_cfg.exists() and coord_default.exists():
        import shutil as _shutil

        _shutil.copy2(coord_default, coord_cfg)
        print(f"  Created {coord_cfg} from defaults")
    elif coord_cfg.exists():
        print(f"  OK {coord_cfg}")
    else:
        print(f"  SKIP coordinator.json (no default template found at {coord_default})")

    global_rules = fleet_dir / "global-rules.md"
    global_rules_src = repo_root / "coordinator" / "global_rules.md"
    if not global_rules.exists() and global_rules_src.exists():
        import shutil as _shutil

        _shutil.copy2(global_rules_src, global_rules)
        print(f"  Created {global_rules}")
    elif global_rules.exists():
        print(f"  OK {global_rules}")

    # Step 3: Initialize database
    print("\n[3/6] Database")
    cfg = load_config()
    paths = ensure_state(cfg)
    db_path = paths["db_path"]
    if Path(db_path).exists():
        print(f"  OK {db_path}")
    else:
        print(f"  Created {db_path}")

    # Step 4: Check services
    print("\n[4/6] Services")
    services = [
        ("CLIProxyAPI", 8317, "brew services start cliproxyapi"),
        (
            "Gateway",
            4105,
            "launchctl load ~/Library/LaunchAgents/com.ai.fleet.gateway.plist",
        ),
    ]
    for name, port, fix in services:
        if _check_port(port):
            print(f"  OK {name} (:{port})")
        else:
            print(f"  DOWN {name} (:{port})")
            print(f"       start: {fix}")

    # Step 5: Auth profiles
    print("\n[5/6] Auth Profiles")
    auth_dir = home / ".cli-proxy-api"
    if auth_dir.exists():
        claude_count = len(list(auth_dir.glob("claude-*.json")))
        ag_count = len(list(auth_dir.glob("antigravity-*.json")))
        codex_count = len(list(auth_dir.glob("codex-*.json")))
        print(f"  Claude:      {claude_count} accounts")
        print(f"  Antigravity: {ag_count} accounts")
        print(f"  Codex:       {codex_count} accounts")
        if claude_count == 0:
            print("  To add Claude:      cliproxyapi -claude-login")
        if ag_count == 0:
            print("  To add Antigravity: cliproxyapi -antigravity-login")
    else:
        print("  No auth directory found. Run:")
        print("    cliproxyapi -claude-login")

    # Step 6: Verify
    print("\n[6/6] Verification")
    checks = _doctor_checks()
    ok_count = sum(1 for c in checks if c["ok"])
    fail_count = len(checks) - ok_count

    if fail_count == 0:
        print("  ALL GOOD — ready to use!")
        print("\n  Try: ai-fleet exec 'Say hello' --via codex")
    else:
        print(f"  {ok_count} passed, {fail_count} remaining issues")
        print("  Run: python3 -m coordinator.ai_coordinator doctor")
        print("  to see what still needs attention.")

    return 0


def cmd_observe(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    repo: Path | None
    if str(args.repo).strip().lower() in {"", ".", "all", "*"}:
        repo = (
            _repo_root(
                args.repo if str(args.repo).strip() not in {"", "all", "*"} else "."
            )
            if str(args.repo).strip() == "."
            else None
        )
    else:
        repo = _repo_root(args.repo)

    snapshot = _build_observability_snapshot(
        cfg=cfg,
        conn=conn,
        repo=repo,
        hours=int(args.hours),
        limit=int(args.limit),
    )

    if args.json:
        print(json.dumps(snapshot, indent=2))
        conn.close()
        return 0

    runs = snapshot["runs"]
    gateway = snapshot["gateway"]

    print("Coordinator Observability")
    print("-------------------------")
    print(f"window_hours={snapshot['window_hours']}")
    print(f"since={snapshot['since_local']}")
    print(f"repo_filter={snapshot['repo_filter'] or 'all'}")
    print(
        "runs="
        f"total={runs['total']} failed={runs['failed']} fallback={runs['fallback']} "
        f"failure_rate={runs['failure_rate']} fallback_rate={runs['fallback_rate']}"
    )
    if runs.get("status_counts"):
        status_summary = " ".join(
            [f"{k}:{v}" for k, v in sorted((runs["status_counts"] or {}).items())]
        )
        print(f"status_counts={status_summary}")
    if runs.get("via_counts"):
        via_summary = " ".join(
            [f"{k}:{v}" for k, v in sorted((runs["via_counts"] or {}).items())]
        )
        print(f"via_counts={via_summary}")

    print(
        "gateway="
        f"health_ok={gateway.get('health_ok', False)} route_ok={gateway.get('route_ok', False)} "
        f"status_code={gateway.get('status_code', 0)}"
    )
    route_obj = gateway.get("route") or {}
    if route_obj:
        print(
            "route="
            f"codex={route_obj.get('codex_profile', '')} gemini={route_obj.get('gemini_profile', '')} "
            f"claude={route_obj.get('claude_source', '')}"
        )

    usage = gateway.get("claude_usage") or {}
    sources = usage.get("sources") or []
    if sources:
        print("\nclaude_sources")
        print("--------------")
        for src in sources:
            if not isinstance(src, dict):
                continue
            print(
                f"name={src.get('name', '')} status={src.get('status', '')} "
                f"email={src.get('email', '') or 'none'} account_type={src.get('account_type', '') or 'none'} "
                f"standby={src.get('standby', False)} group={src.get('group', '') or 'none'} "
                f"ok_rate={src.get('ok_rate', '')} fail_ratio={src.get('fail_ratio', '')} score={src.get('score', '')}"
            )

    providers = gateway.get("external_providers") or []
    if providers:
        print("\nexternal_providers")
        print("------------------")
        for p in providers:
            if not isinstance(p, dict):
                continue
            print(
                f"name={p.get('name', '')} enabled={p.get('enabled', False)} key_present={p.get('key_present', False)} "
                f"healthy={p.get('healthy', False)} failures={p.get('failures', 0)} requests={p.get('requests', 0)}"
            )

    recent_failures = runs.get("recent_failures") or []
    if recent_failures:
        print("\nrecent_failures")
        print("---------------")
        for row in recent_failures:
            print(
                f"ts={_fmt_ts(int(row.get('ts', 0) or 0))} status={row.get('status', '')} via={row.get('via', '')} "
                f"objective={_truncate(str(row.get('objective', '')), 100)} "
                f"log={row.get('output_path', '')}"
            )
            if row.get("notes"):
                print(f"notes={_truncate(str(row.get('notes', '')), 180)}")

    conn.close()
    return 0


def cmd_cost(args: argparse.Namespace) -> int:
    """Show cost tracking data with filters."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))
    conn.row_factory = sqlite3.Row

    conditions = []
    params = []
    now = int(time.time())

    if hasattr(args, "today") and args.today:
        start_of_day = now - (now % 86400)
        conditions.append("timestamp >= ?")
        params.append(start_of_day)
    elif hasattr(args, "week") and args.week:
        conditions.append("timestamp >= ?")
        params.append(now - 7 * 86400)
    elif hasattr(args, "month") and args.month:
        conditions.append("timestamp >= ?")
        params.append(now - 30 * 86400)
    elif hasattr(args, "since") and args.since:
        try:
            from datetime import datetime

            dt = datetime.strptime(args.since, "%Y-%m-%d")
            conditions.append("timestamp >= ?")
            params.append(int(dt.timestamp()))
        except ValueError:
            print(f"Invalid date format: {args.since} (expected YYYY-MM-DD)")
            return 1

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    group_col = "model"
    if hasattr(args, "by_provider") and args.by_provider:
        group_col = "provider"
    elif hasattr(args, "by_tier") and args.by_tier:
        group_col = "tier"
    elif hasattr(args, "by_model") and args.by_model:
        group_col = "model"

    query = f"""
        SELECT {group_col}, COUNT(*) as requests,
               SUM(input_tokens) as total_input,
               SUM(output_tokens) as total_output,
               SUM(estimated_cost_usd) as total_cost
        FROM cost_log
        {where}
        GROUP BY {group_col}
        ORDER BY total_cost DESC
    """

    rows = conn.execute(query, params).fetchall()
    conn.close()

    if hasattr(args, "json") and args.json:
        print(json.dumps([dict(r) for r in rows], indent=2))
        return 0

    if not rows:
        print("No cost data found for the specified period.")
        return 0

    print(
        f"{'Group':<30} {'Requests':>8} {'Input Tokens':>12} {'Output Tokens':>13} {'Cost (USD)':>10}"
    )
    print("-" * 75)
    total_cost = 0.0
    for r in rows:
        cost = r["total_cost"] or 0.0
        total_cost += cost
        print(
            f"{r[group_col] or 'unknown':<30} {r['requests']:>8} {r['total_input'] or 0:>12} {r['total_output'] or 0:>13} ${cost:>9.4f}"
        )
    print("-" * 75)
    print(f"{'TOTAL':<30} {'':>8} {'':>12} {'':>13} ${total_cost:>9.4f}")

    return 0


def cmd_preflight(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    _ = paths
    repo = _repo_root(args.repo)
    preflight = run_preflight(
        cfg,
        repo,
        args.objective or "",
        args.production,
        require_swarm=args.require_swarm,
        planned_via=args.via,
        autonomous=args.autonomous,
    )
    _print_preflight(preflight)
    return 0 if preflight["ok"] else 2


def cmd_skills(args: argparse.Namespace) -> int:
    cfg = load_config()
    ensure_state(cfg)
    repo = _repo_root(args.repo)
    matches = discover_skills(repo, args.objective, cfg, limit=args.limit)
    _render_skills(matches)
    if args.with_subtasks:
        bundles = discover_skills_for_subtasks(
            repo, args.objective, cfg, per_query_limit=max(1, min(args.limit, 4))
        )
        if bundles:
            print("\nsubtask_skill_matches")
            print("--------------------")
            for bundle in bundles:
                print(f"query: {bundle.get('query', '')}")
                for skill in bundle.get("skills", []):
                    print(f"  {skill['score']:>2}  {skill['name']}  {skill['path']}")
    return 0


def cmd_rules_check(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    repo = _repo_root(args.repo)
    info = ensure_rules_check(conn, repo, cfg, force=args.force)
    print(f"repo={repo}")
    print(f"checked={info['checked']}")
    print(f"drift={info['drift']}")
    print(f"repo_rules_path={info['repo_rules_path'] or 'none'}")
    conn.close()
    return 0


def cmd_learn_add(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    conf = float(
        args.confidence
        if args.confidence is not None
        else cfg.get("learning", {}).get("default_confidence", 0.7)
    )

    if args.scope == "global":
        _record_global_learning(
            conn,
            args.rule,
            args.rationale or "",
            args.tags or "",
            args.source or "manual",
            conf,
        )
        print("added=global_learning")
    else:
        repo = _repo_root(args.repo)
        _record_repo_learning(
            conn,
            repo,
            args.pattern,
            args.fix,
            args.tags or "",
            args.source or "manual",
            conf,
        )
        print(f"added=repo_learning repo={repo}")

    conn.close()
    return 0


def cmd_learn_list(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    if args.scope == "global":
        rows = _load_recent_global_learning(conn, args.limit)
        print("global_learning")
        print("---------------")
        for row in rows:
            print(
                f"ts={row['ts']} rule={_truncate(str(row['rule']), 120)} rationale={_truncate(str(row['rationale'] or ''), 120)} source={row['source']}"
            )
    else:
        repo = _repo_root(args.repo)
        rows = _load_recent_repo_learning(conn, repo, args.limit)
        print(f"repo_learning repo={repo}")
        print("---------------------------")
        for row in rows:
            print(
                f"ts={row['ts']} pattern={_truncate(str(row['pattern']), 120)} fix={_truncate(str(row['fix']), 120)} source={row['source']}"
            )

    conn.close()
    return 0


def cmd_learn_query(args: argparse.Namespace) -> int:
    """Query learnings with decay-adjusted confidence and filters."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))

    scope = args.scope
    table = "global_learning" if scope == "global" else "repo_learning"

    where_parts = ["1=1"]
    params = []

    if scope == "repo":
        repo = str(Path(args.repo).resolve())
        where_parts.append("repo = ?")
        params.append(repo)

    if args.tags:
        for tag in args.tags.split(","):
            where_parts.append("tags LIKE ?")
            params.append(f"%{tag.strip()}%")

    if args.source:
        where_parts.append("source = ?")
        params.append(args.source)

    where = " AND ".join(where_parts)
    limit = args.limit

    if scope == "global":
        rows = conn.execute(
            f"SELECT id, ts, rule, rationale, tags, source, confidence, last_used_ts, use_count FROM {table} WHERE {where} ORDER BY ts DESC LIMIT ?",
            params + [limit],
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT id, ts, pattern, fix, tags, source, confidence, last_used_ts, use_count FROM {table} WHERE {where} ORDER BY ts DESC LIMIT ?",
            params + [limit],
        ).fetchall()

    conn.close()

    tau = float(cfg.get("learning", {}).get("decay_tau_days", 14.0))
    min_conf = args.min_confidence

    results = []
    for r in rows:
        raw_conf = r[6] or 0.7
        effective = _decay_confidence(raw_conf, r[1], tau)
        if effective < min_conf:
            continue
        results.append(
            {
                "id": r[0],
                "ts": r[1],
                "content": r[2],  # rule or pattern
                "detail": r[3],  # rationale or fix
                "tags": r[4],
                "source": r[5],
                "raw_confidence": round(raw_conf, 3),
                "effective_confidence": round(effective, 3),
                "last_used_ts": r[7] or 0,
                "use_count": r[8] or 0,
            }
        )

    if getattr(args, "json", False):
        print(json.dumps(results, indent=2))
    else:
        for item in results:
            ts_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(item["ts"]))
            print(
                f"[{ts_str}] id={item['id']} conf={item['effective_confidence']} (raw={item['raw_confidence']}) uses={item['use_count']}"
            )
            print(f"  {item['content'][:120]}")
            if item.get("tags"):
                print(f"  tags={item['tags']}")
        if not results:
            print(f"No learnings found (min_confidence={min_conf})")

    return 0


def cmd_learn_compact(args: argparse.Namespace) -> int:
    """Remove stale learnings with decayed confidence below threshold."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))

    tau = float(cfg.get("learning", {}).get("decay_tau_days", 14.0))
    threshold = args.threshold
    dry_run = args.dry_run

    removed = 0
    for table in ["global_learning", "repo_learning"]:
        rows = conn.execute(
            f"SELECT id, ts, last_used_ts, use_count, confidence FROM {table}"
        ).fetchall()
        for row in rows:
            row_id, ts, last_used_ts, use_count, conf = row
            decay_from = last_used_ts if last_used_ts and last_used_ts > 0 else ts
            effective = _decay_confidence(conf or 0.7, decay_from, tau)
            # Boost for frequently used learnings
            if use_count and use_count > 5:
                effective = min(1.0, effective + 0.1)
            if effective < threshold:
                if dry_run:
                    print(
                        f"  would remove {table}.id={row_id} effective_conf={effective:.3f}"
                    )
                else:
                    conn.execute(f"DELETE FROM {table} WHERE id = ?", (row_id,))
                removed += 1

    if not dry_run:
        conn.commit()
    conn.close()

    action = "would remove" if dry_run else "removed"
    print(f"{action} {removed} stale learnings (threshold={threshold}, tau={tau}d)")
    return 0


def cmd_learn_stats(args: argparse.Namespace) -> int:
    """Show pattern statistics from learning tables."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))

    tau = float(cfg.get("learning", {}).get("decay_tau_days", 14.0))

    # Global learning stats
    global_rows = conn.execute(
        "SELECT ts, confidence, tags FROM global_learning"
    ).fetchall()
    global_total = len(global_rows)

    # Repo learning stats
    repo_rows = conn.execute(
        "SELECT ts, confidence, tags, repo FROM repo_learning"
    ).fetchall()
    repo_total = len(repo_rows)

    print("Learning Pattern Statistics")
    print(f"  Total global patterns: {global_total}")
    print(f"  Total repo patterns: {repo_total}")
    print(f"  Decay tau: {tau} days")
    print()

    # Top patterns by effective confidence (global)
    if global_rows:
        print("Top Global Patterns (by effective confidence):")
        scored = []
        for ts, conf, tags in global_rows:
            effective = _decay_confidence(conf or 0.7, ts, tau)
            scored.append((effective, ts, tags or ""))
        scored.sort(reverse=True)
        for i, (eff_conf, ts, tags) in enumerate(scored[:10], 1):
            age_days = (time.time() - ts) / 86400
            print(f"  {i}. conf={eff_conf:.3f} age={age_days:.1f}d tags={tags}")
        print()

    # Top patterns by effective confidence (repo)
    if repo_rows:
        print("Top Repo Patterns (by effective confidence):")
        scored = []
        for ts, conf, tags, repo in repo_rows:
            effective = _decay_confidence(conf or 0.7, ts, tau)
            scored.append((effective, ts, tags or "", repo))
        scored.sort(reverse=True)
        for i, (eff_conf, ts, tags, repo) in enumerate(scored[:10], 1):
            age_days = (time.time() - ts) / 86400
            print(
                f"  {i}. conf={eff_conf:.3f} age={age_days:.1f}d tags={tags} repo={Path(repo).name}"
            )
        print()

    # Tag frequency counts
    tag_counts: dict[str, int] = {}
    for _, _, tags in global_rows:
        if tags:
            for tag in tags.split(","):
                tag = tag.strip()
                if tag:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
    for _, _, tags, _ in repo_rows:
        if tags:
            for tag in tags.split(","):
                tag = tag.strip()
                if tag:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1

    if tag_counts:
        print("Tag Frequency:")
        sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
        for tag, count in sorted_tags[:15]:
            print(f"  {tag}: {count}")

    conn.close()
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    repo = _repo_root(args.repo)

    tier, via, model = _select_via_and_model(
        args.objective, args.via, args.model, cfg, repo=repo
    )
    runtime_model = _resolve_runtime_model(
        via,
        tier,
        model,
        cfg,
        explicit_model=bool(args.model and args.model != "auto"),
        swarm=False,
    )
    preflight = run_preflight(
        cfg,
        repo,
        args.objective,
        args.production,
        planned_via=via,
        autonomous=False,
    )
    skills = discover_skills(repo, args.objective, cfg)
    subtask_skills = discover_skills_for_subtasks(repo, args.objective, cfg)
    rules = ensure_rules_check(conn, repo, cfg, force=False)

    plan = {
        "repo": str(repo),
        "objective": args.objective,
        "tier": tier,
        "via": via,
        "model_lane": model,
        "runtime_model": runtime_model,
        "preflight_ok": preflight["ok"],
        "production_checks": preflight["production_checks"],
        "rules_drift": rules["drift"],
        "skills": skills,
        "subtask_skills": subtask_skills,
    }
    print(json.dumps(plan, indent=2))
    conn.close()
    return 0 if preflight["ok"] else 2


def _execution_cmd(
    via: str,
    runtime_model: str,
    prompt: str,
    cfg: dict[str, Any],
    tier: str = "coding",
    swarm: bool = False,
) -> list[str]:
    if via == "claude":
        cmd = ["claude", "--print"]
        if runtime_model:
            cmd += ["--model", runtime_model]
        cmd.append(prompt)
        return cmd
    if via == "codex":
        cmd = ["codex", "exec", "--full-auto", "--skip-git-repo-check", "--ephemeral"]
        if runtime_model:
            cmd += ["-m", runtime_model]
        effort_by_tier = (
            cfg.get("routing", {}).get("codex_reasoning_effort_by_tier", {}) or {}
        )
        effort = str(
            effort_by_tier.get(tier) or effort_by_tier.get("default") or ""
        ).strip()
        if effort:
            cmd += ["-c", f'model_reasoning_effort="{effort}"']
        cmd.append(prompt)
        return cmd
    if via == "droid":
        model = runtime_model or _select_droid_model(cfg, swarm=swarm)
        return ["droid", "exec", "--model", model, prompt]
    if via == "gemini":
        # Gemini lane runs via native Gemini CLI (not droid/factory).
        cmd = ["gemini"]
        model = (runtime_model or "").strip()

        # Known Gemini CLI stability issue on 3-pro-preview in this environment.
        # Pin to flash-lite for reliable non-interactive execution.
        if model.lower() in {"gemini-3-pro-preview", "gemini-3-pro", "gemini-3.1-pro"}:
            model = "gemini-2.5-flash-lite"

        if model and model not in {
            "auto",
            "smart-auto",
            "smart-workhorse",
            "smart-fast",
            "smart-opus-4.6",
        }:
            cmd += ["--model", model]
        cmd += ["-p", prompt]
        return cmd
    if via == "gateway":
        # Gateway is HTTP-based, not CLI. Return a sentinel command.
        # cmd_exec detects this and calls _run_gateway_chat() instead.
        return ["__gateway__", runtime_model or "smart-auto", prompt]
    raise ValueError(f"unsupported via '{via}'")


def cmd_exec(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    repo = _repo_root(args.repo)

    run_id = (
        time.strftime("%Y%m%d_%H%M%S")
        + "_"
        + hashlib.md5(str(time.time()).encode()).hexdigest()[:4]
    )

    checkpoint_payload: dict[str, Any] | None = None
    if args.checkpoint:
        checkpoint_path = Path(args.checkpoint).expanduser().resolve()
        if not checkpoint_path.exists():
            print(f"checkpoint_not_found={checkpoint_path}")
            conn.close()
            return 2
        checkpoint_payload = _read_json(checkpoint_path, {})
        checkpoint_repo = str(checkpoint_payload.get("repo", "")).strip()
        if checkpoint_repo and args.repo in (".", ""):
            repo = _repo_root(checkpoint_repo)

    objective = args.objective
    if checkpoint_payload and not objective:
        objective = str(checkpoint_payload.get("objective", ""))

    if not objective:
        print("missing_objective: pass --objective or --checkpoint")
        conn.close()
        return 2

    via_hint = args.via
    model_hint = args.model
    swarm_mode = bool(args.swarm)
    if checkpoint_payload:
        if via_hint == "auto":
            cp_via = str(checkpoint_payload.get("via", "")).strip().lower()
            if cp_via in {"claude", "codex", "droid", "gemini"}:
                via_hint = cp_via
        if model_hint == "auto":
            cp_model = str(checkpoint_payload.get("model_lane", "")).strip()
            if cp_model:
                model_hint = cp_model
        if not swarm_mode and bool(checkpoint_payload.get("swarm", False)):
            swarm_mode = True

    if getattr(args, "tier", None):
        tier, tier_reason = args.tier, "explicit_cli_override"
    else:
        tier, tier_reason = _classify_tier_with_reason(objective, cfg)
    via, via_reason, alternatives = _pick_via_with_reason(
        tier,
        cfg,
        explicit_via=via_hint,
        objective=objective,
        conn=conn,
        repo=repo,
    )
    model, model_reason = _pick_model_with_reason(
        tier,
        via,
        cfg,
        explicit_model=model_hint,
    )
    runtime_model = _resolve_runtime_model(
        via,
        tier,
        model,
        cfg,
        explicit_model=bool(model_hint and model_hint != "auto"),
        swarm=swarm_mode,
    )

    # Decision record will be updated with budget_check after budget check below
    _record_decision(
        conn,
        run_id,
        objective,
        tier,
        tier_reason,
        via,
        via_reason,
        model,
        model_reason,
        alternatives=alternatives,
    )

    preflight = run_preflight(
        cfg,
        repo,
        objective,
        args.production,
        require_swarm=swarm_mode,
        planned_via=via,
        autonomous=bool(args.autonomous),
    )
    _print_preflight(preflight)
    if not preflight["ok"]:
        _record_run(
            conn,
            repo,
            objective,
            "n/a",
            "n/a",
            "n/a",
            "blocked_preflight",
            False,
            notes="required preflight checks failed",
        )
        conn.close()
        return 2

    rules = ensure_rules_check(conn, repo, cfg, force=False)
    skills = discover_skills(repo, objective, cfg)
    subtask_skills = discover_skills_for_subtasks(repo, objective, cfg)

    max_learning = int(cfg.get("learning", {}).get("max_prompt_learnings", 8))
    repo_learning = _load_recent_repo_learning(conn, repo, max_learning)
    global_learning = _load_recent_global_learning(conn, max_learning)

    autonomy_cfg = cfg.get("autonomy", {})
    auto_approve_tiers = autonomy_cfg.get("auto_approve_tiers", [])
    full_auto = autonomy_cfg.get("full_auto", False)

    if full_auto:
        gate_required = False
        if autonomy_cfg.get("full_auto_notify", True):
            _notify_event(
                conn,
                "auto_approved",
                f"full_auto: {tier}/{via} — {_truncate(objective, 80)}",
                "info",
                run_id=run_id,
            )
    elif tier in auto_approve_tiers:
        gate_required = False
        if autonomy_cfg.get("full_auto_notify", True):
            _notify_event(
                conn,
                "auto_approved",
                f"tier_auto: {tier}/{via} — {_truncate(objective, 80)}",
                "info",
                run_id=run_id,
            )
    else:
        gate_required = (
            bool(autonomy_cfg.get("checkpoint_required", True)) and args.autonomous
        )

    if gate_required and not args.approve and checkpoint_payload is None:
        payload = {
            "ts": _now_ts(),
            "repo": str(repo),
            "objective": objective,
            "tier": tier,
            "via": via,
            "model_lane": model,
            "runtime_model": runtime_model,
            "swarm": swarm_mode,
            "skills": skills,
            "subtask_skills": subtask_skills,
            "preflight": preflight,
            "rules": rules,
        }
        cp = _write_checkpoint(paths["checkpoint_dir"], payload)
        _record_run(
            conn,
            repo,
            objective,
            via,
            tier,
            model,
            "checkpoint_required",
            True,
            notes=f"checkpoint={cp}",
        )
        print(f"checkpoint_created={cp}")
        print("re_run_with=--checkpoint <path> --approve")
        conn.close()
        return 10

    prompt = _prompt_context(
        repo,
        objective,
        tier,
        model,
        rules,
        skills,
        subtask_skills,
        repo_learning,
        global_learning,
    )

    try:
        cmd = _execution_cmd(
            via, runtime_model, prompt, cfg, tier=tier, swarm=swarm_mode
        )
    except ValueError as err:
        print(str(err))
        conn.close()
        return 2

    print("execution_plan")
    print("--------------")
    print(f"repo={repo}")
    print(f"tier={tier}")
    print(f"via={via}")
    print(f"model_lane={model}")
    print(f"runtime_model={runtime_model}")
    if skills:
        print(f"skills_selected={len(skills)}")
    if subtask_skills:
        total_subskills = sum(len(x.get("skills", [])) for x in subtask_skills)
        print(f"subtask_skill_matches={total_subskills}")

    # Budget check before execution
    estimated_input_tokens = max(1000, len(prompt) // 4)
    estimated_output_tokens = 2000
    estimated_cost = _estimate_cost(
        cfg, runtime_model, estimated_input_tokens, estimated_output_tokens
    )
    budget_allowed, budget_reason = _pre_dispatch_budget_check(
        conn, cfg, estimated_cost
    )
    # Update decision record with budget check result
    budget_status = "pass" if budget_allowed else "fail"
    conn.execute(
        "UPDATE decisions SET budget_check = ? WHERE run_id = ?",
        (
            f"{budget_status}: {budget_reason}" if budget_reason else budget_status,
            run_id,
        ),
    )
    conn.commit()
    if not budget_allowed:
        print(budget_reason)
        _notify_event(
            conn, "budget_exceeded", budget_reason, severity="error", run_id=run_id
        )
        _record_run(
            conn,
            repo,
            objective,
            via,
            tier,
            model,
            "budget_exceeded",
            False,
            notes=budget_reason,
        )
        conn.close()
        return 1
    if budget_reason:
        print(budget_reason)

    exec_started_at = time.time()
    if cmd[0] == "__gateway__":
        # Gateway via — HTTP request instead of subprocess
        gw_model, gw_prompt = cmd[1], cmd[2]
        result = _run_gateway_chat(gw_model, gw_prompt, cfg, timeout=args.timeout)
    else:
        result = _run_cmd(cmd, cwd=repo, timeout=args.timeout)

    # 429 rate-limit detection and automatic failover
    if _looks_like_rate_limit(result):
        profile_key = runtime_model or model
        cooldown_secs = _record_429(conn, via, profile_key)
        print(
            f"rate_limit_detected via={via} model={profile_key} cooldown={cooldown_secs}s"
        )
        _notify_event(
            conn,
            "rate_limit",
            f"429 on {via}:{profile_key}, cooldown {cooldown_secs}s",
            "warning",
            run_id=run_id,
        )
        # Extract partial tokens from failed response and record 429
        partial_out = (
            _ensure_str(result.get("stdout")) + "\n" + _ensure_str(result.get("stderr"))
        )
        partial_in, partial_out_tok = _extract_tokens(via, partial_out)
        if via == "codex":
            _record_codex_usage(
                conn,
                profile_key,
                False,
                input_tokens=partial_in,
                output_tokens=partial_out_tok,
                error_code=429,
                error_msg="rate_limit_detected",
            )
        elif via == "claude":
            _record_claude_usage(
                conn,
                profile_key,
                model=runtime_model or model,
                success=False,
                input_tokens=partial_in,
                output_tokens=partial_out_tok,
                error_code=429,
                error_msg="rate_limit_detected",
            )
        # Try alternative providers in order (includes gateway as last resort)
        for alt in alternatives:
            alt_via = alt["via"]
            alt_model, _ = _pick_model_with_reason(
                tier, alt_via, cfg, explicit_model="auto"
            )
            alt_runtime = _resolve_runtime_model(
                alt_via, tier, alt_model, cfg, explicit_model=False, swarm=swarm_mode
            )
            try:
                alt_cmd = _execution_cmd(
                    alt_via, alt_runtime, prompt, cfg, tier=tier, swarm=swarm_mode
                )
                print(f"429_failover retrying via={alt_via} model={alt_runtime}")
                if alt_cmd[0] == "__gateway__":
                    result = _run_gateway_chat(
                        alt_cmd[1], alt_cmd[2], cfg, timeout=args.timeout
                    )
                else:
                    result = _run_cmd(alt_cmd, cwd=repo, timeout=args.timeout)
                via = alt_via
                runtime_model = alt_runtime
                model = alt_model
                if not _looks_like_rate_limit(result):
                    break  # This provider worked, stop trying
                _record_429(conn, alt_via, alt_runtime or alt_model)
                print(f"429_failover_also_limited via={alt_via}")
            except ValueError:
                continue  # unsupported provider, try next

    if _looks_like_runtime_failure(via, result):
        result["ok"] = False
        if int(result.get("returncode", 0) or 0) == 0:
            result["returncode"] = 1
    fallback_meta = ""
    should_fallback = (not result["ok"]) and (via in {"droid", "gemini"})
    if should_fallback:
        if via == "gemini":
            fallback_via = "codex"
        else:
            fallback_via = (
                str(
                    (cfg.get("droid", {}) or {}).get("fallback_via_on_failure", "codex")
                )
                .strip()
                .lower()
            )
        if fallback_via in {"claude", "codex"}:
            failure_summary = _truncate(
                _redact_sensitive_text(
                    _compact_output_for_display(
                        _ensure_str(result.get("stderr"))
                        or _ensure_str(result.get("stdout"))
                        or "",
                        limit=1000,
                    )
                ),
                400,
            )
            fallback_prompt = textwrap.dedent(
                f"""
                Provider execution fallback.
                Original provider: {via}
                Original objective: {objective}
                Original model: {runtime_model}
                Failure summary:
                {failure_summary}

                Complete the objective directly and return the practical result.
                """
            ).strip()
            fb_model = _resolve_runtime_model(
                fallback_via,
                tier,
                model,
                cfg,
                explicit_model=False,
                swarm=False,
            )
            fb_cmd = _execution_cmd(
                fallback_via, fb_model, fallback_prompt, cfg, tier=tier, swarm=False
            )
            fb_timeout = int(
                (cfg.get("droid", {}) or {}).get("fallback_timeout_seconds", 300)
            )
            fb = _run_cmd(fb_cmd, cwd=repo, timeout=fb_timeout)

            fb_path = (
                paths["runs_dir"]
                / f"run_{time.strftime('%Y%m%d_%H%M%S')}_{via}_fallback_{fallback_via}.log"
            )
            fb_path.write_text(
                _ensure_str(fb.get("stdout"))
                + "\n"
                + _ensure_str(fb.get("stderr"))
                + "\n"
            )

            if fb.get("ok"):
                base = (
                    _ensure_str(result.get("stderr"))
                    or _ensure_str(result.get("stdout"))
                    or ""
                ).strip()
                fb_out = _ensure_str(fb.get("stdout")).strip()
                fail_excerpt = _truncate(
                    _compact_output_for_display(
                        _redact_sensitive_text(base), limit=400
                    ),
                    220,
                )
                result["ok"] = True
                result["returncode"] = 0
                result["stdout"] = fb_out
                meta_stderr = (
                    f"[fallback-from-droid] via={fallback_via}; reason={fail_excerpt}"
                )
                result["stderr"] = meta_stderr + (
                    "\n" + str(fb.get("stderr") or "") if fb.get("stderr") else ""
                )
                fallback_meta = f"fallback={fallback_via} log={fb_path}"
            else:
                result["stderr"] = (
                    _ensure_str(result.get("stderr"))
                    + f"\n\nfallback_failed={fallback_via} log={fb_path}\n"
                    + _ensure_str(fb.get("stderr"))
                )
                fallback_meta = f"fallback_failed={fallback_via} log={fb_path}"

    output_path = _save_run_output(paths["runs_dir"], via, objective, result)

    status = "success" if result["ok"] else "failed"
    if result["ok"] and fallback_meta:
        status = "completed_with_fallback"
    notes_source = (
        _ensure_str(result.get("stdout"))
        if result["ok"]
        else (_ensure_str(result.get("stderr")) or _ensure_str(result.get("stdout")))
    )
    notes = _truncate(
        _redact_sensitive_text(
            _compact_output_for_display(notes_source or "", limit=800)
        ),
        240,
    )
    exec_duration = round(time.time() - exec_started_at, 2)
    _record_run(
        conn,
        repo,
        objective,
        via,
        tier,
        runtime_model or model,
        status,
        gate_required,
        output_path=str(output_path),
        notes=f"lane={model}; {fallback_meta} {notes}".strip(),
        duration_seconds=exec_duration,
    )

    raw_output = (
        _ensure_str(result.get("stdout")) or _ensure_str(result.get("stderr")) or ""
    )
    input_tokens, output_tokens = _extract_tokens(via, raw_output)
    cost_usd = _estimate_cost(cfg, runtime_model or model, input_tokens, output_tokens)
    _m = (runtime_model or model).lower()
    _v = via.lower()
    if (
        _m.startswith("claude")
        or _m.startswith("smart-opus")
        or _m.startswith("smart-sonnet")
        or _m.startswith("smart-haiku")
    ):
        provider = "anthropic"
    elif (
        _v.startswith("codex")
        or _m.startswith("gpt")
        or _m.startswith("codex")
        or _m.startswith("o1")
        or _m.startswith("o3")
    ):
        provider = "openai"
    elif _m.startswith("gemini") or _m.startswith("palm"):
        provider = "google"
    else:
        provider = "custom"
    _record_cost(
        conn,
        run_id,
        provider,
        runtime_model or model,
        input_tokens,
        output_tokens,
        cost_usd,
        tier,
        via,
    )

    # Notify on exec completion
    if result["ok"]:
        _notify_event(
            conn,
            "exec_complete",
            f"Run {run_id} completed: {_truncate(objective, 80)}",
            "info",
            run_id=run_id,
        )
    else:
        _notify_event(
            conn,
            "exec_failed",
            f"Run {run_id} failed: {_truncate(objective, 80)}",
            "error",
            run_id=run_id,
        )

    default_confidence = float(cfg.get("learning", {}).get("default_confidence", 0.7))
    if (not result["ok"]) and bool(
        cfg.get("learning", {}).get("auto_record_failures", True)
    ):
        import json as _json

        fail_data = _json.dumps(
            {
                "via": via,
                "tier": tier,
                "model": runtime_model or model,
                "duration_s": exec_duration,
                "error": _truncate(notes, 160),
            }
        )
        _record_repo_learning(
            conn,
            repo,
            pattern=f"failure:{tier}:{_truncate(objective, 120)}",
            fix=f"session={fail_data}; log={output_path.name}",
            tags="auto,failure,structured",
            source="ai-coordinator",
            confidence=default_confidence,
        )
        # Routing negative signal
        _record_repo_learning(
            conn,
            repo,
            pattern=f"routing:failed tier={tier} via={via} model={runtime_model or model}",
            fix=f"dur={exec_duration}s; Avoid for similar patterns, try alternatives",
            tags="routing,failure,auto-learned",
            source="ai-coordinator-routing",
            confidence=0.8,
        )
    elif result["ok"] and bool(
        cfg.get("learning", {}).get("auto_record_successes", True)
    ):
        success_chars = int(cfg.get("learning", {}).get("success_snippet_chars", 180))
        summary = _truncate(
            _ensure_str(result.get("stdout"))
            or _ensure_str(result.get("stderr"))
            or "",
            success_chars,
        )
        # Structured success learning with actionable data
        import json as _json

        session_data = _json.dumps(
            {
                "via": via,
                "tier": tier,
                "model": runtime_model or model,
                "duration_s": exec_duration,
                "cost_usd": cost_usd,
                "tokens": {"input": input_tokens, "output": output_tokens},
            }
        )
        _record_repo_learning(
            conn,
            repo,
            pattern=f"success:{tier}:{_truncate(objective, 120)}",
            fix=f"session={session_data}; summary={_truncate(summary, 80)}",
            tags="auto,success,structured",
            source="ai-coordinator",
            confidence=default_confidence,
        )
        # Routing positive signal (keep for backward compat but with structured data)
        _record_repo_learning(
            conn,
            repo,
            pattern=f"routing:success tier={tier} via={via} model={runtime_model or model}",
            fix=f"dur={exec_duration}s cost=${cost_usd:.4f} tokens={input_tokens + output_tokens}",
            tags="routing,success,auto-learned",
            source="ai-coordinator-routing",
            confidence=0.6,
        )
        # Cross-project global learning with structured data
        _record_global_learning(
            conn,
            rule=f"{via}:{runtime_model or model} tier={tier} dur={exec_duration}s cost=${cost_usd:.4f}",
            rationale=f"objective={_truncate(objective, 100)}",
            tags="routing,cross-project,auto-learned",
            source="ai-coordinator-routing",
            confidence=0.5,
        )
    # Reinforce learnings that contributed to this run's routing decision
    try:
        _norm = _normalize_repo_path(repo)
        if result["ok"]:
            # Boost success patterns for this tier/via combination
            matching = conn.execute(
                "SELECT id FROM repo_learning WHERE (repo=? OR repo LIKE ?) AND tags LIKE '%routing%' AND pattern LIKE ? ORDER BY ts DESC LIMIT 5",
                (_norm, f"{_norm}%", f"%tier={tier}%via={via}%"),
            ).fetchall()
            for row in matching:
                _reinforce_learning(conn, "repo_learning", row[0], boost=0.10)
        else:
            # Record failure pattern so future routing avoids this via
            _record_repo_learning(
                conn,
                repo,
                pattern=f"routing:failed:tier={tier}:via={via}:model={runtime_model or model}",
                fix=f"Failed with status={status}; try alternative via",
                tags="routing,failed,auto-learned",
                source="ai-coordinator-reinforcement",
                confidence=0.7,
            )
    except Exception as exc:
        print(f"warn: learning reinforcement failed: {exc}", file=sys.stderr)

    # Post-run theorist sync hook for compounding ops knowledge
    if result["ok"] and bool(
        (cfg.get("theorist", {}) or {}).get("auto_sync_on_success", True)
    ):
        try:
            sync_timeout = int(
                (cfg.get("theorist", {}) or {}).get("sync_timeout_seconds", 180)
            )
            sync_proc = subprocess.run(
                ["make", "theorist-sync"],
                cwd=str(repo),
                capture_output=True,
                text=True,
                timeout=sync_timeout,
            )
            if sync_proc.returncode == 0:
                _notify_event(
                    conn,
                    "theorist_sync",
                    f"Run {run_id}: theorist sync completed",
                    "info",
                    run_id=run_id,
                )
            else:
                sync_err = _truncate(
                    _ensure_str(sync_proc.stderr) or _ensure_str(sync_proc.stdout), 220
                )
                _notify_event(
                    conn,
                    "theorist_sync_failed",
                    f"Run {run_id}: theorist sync failed: {sync_err}",
                    "warning",
                    run_id=run_id,
                )
        except Exception as exc:
            _notify_event(
                conn,
                "theorist_sync_failed",
                f"Run {run_id}: theorist sync exception: {exc}",
                "warning",
                run_id=run_id,
            )

    # Post-run self-reflection
    if cfg.get("reflection", {}).get("enabled", True):
        try:
            import importlib.util as _ilu

            _reflect_path = Path(__file__).parent / "reflect.py"
            if _reflect_path.exists():
                _spec = _ilu.spec_from_file_location("reflect", _reflect_path)
                _mod = _ilu.module_from_spec(_spec)
                _spec.loader.exec_module(_mod)
                reflections = _mod.post_run_reflect(
                    conn=conn,
                    cfg=cfg,
                    run_id=run_id,
                    repo=str(repo),
                    objective=objective,
                    status=status,
                    via=via,
                    tier=tier,
                    model=runtime_model or model,
                    duration=exec_duration,
                    cost=cost_usd,
                    output_path=str(output_path),
                )
                if reflections:
                    print(f"reflections={len(reflections)} patterns detected")
        except Exception as exc:
            print(f"warn: post-run reflection failed: {exc}", file=sys.stderr)

    auto_snapshot = _maybe_auto_snapshot(conn, cfg, paths, repo, rules)

    print(f"run_status={status}")
    print(f"run_log={output_path}")
    if auto_snapshot:
        print(f"auto_snapshot={auto_snapshot}")
    if fallback_meta:
        print(fallback_meta)

    # Per-run cost summary
    token_parts = []
    if input_tokens:
        token_parts.append(f"{input_tokens:,} input")
    if output_tokens:
        token_parts.append(f"{output_tokens:,} output")
    if token_parts or cost_usd > 0:
        token_str = " + ".join(token_parts) if token_parts else "unknown"
        print(
            f"tokens={token_str}  cost=${cost_usd:.4f}  via={provider}:{runtime_model or model}  duration={exec_duration}s"
        )

    if result.get("stdout"):
        print("\n--- stdout ---")
        print(
            _compact_output_for_display(
                _redact_sensitive_text(_ensure_str(result["stdout"]))
            ).rstrip()
        )
    show_stderr_on_success = bool(
        (cfg.get("logging", {}) or {}).get("show_stderr_on_success", False)
    )
    if result.get("stderr") and ((not result["ok"]) or show_stderr_on_success):
        print("\n--- stderr ---")
        print(
            _compact_output_for_display(
                _redact_sensitive_text(_ensure_str(result["stderr"]))
            ).rstrip()
        )

    # Auto-PR creation after successful run
    if result["ok"] and getattr(args, "auto_pr", False):
        try:
            from pipeline.git_ops import auto_create_pr

            pr_url, pr_number = auto_create_pr(str(repo))
            if pr_url:
                print(f"\nauto_pr_created={pr_url}")
                if pr_number:
                    print(f"auto_pr_number={pr_number}")
                _notify_event(
                    conn,
                    "auto_pr_created",
                    f"PR #{pr_number}: {pr_url}",
                    "info",
                    run_id=run_id,
                )
            else:
                print("auto_pr_skipped=not_on_feature_branch_or_no_commits")
        except Exception as exc:
            print(f"auto_pr_failed={exc}")

    conn.close()
    return 0 if result["ok"] else 1


def _session_counts(tasks: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0, "cancelled": 0}
    for task in tasks:
        status = str(task.get("status", "")).lower()
        if status in counts:
            counts[status] += 1
    return counts


def cmd_swarm(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    repo = _repo_root(args.repo)
    objective = args.objective

    if not objective:
        print("missing_objective: pass --objective")
        conn.close()
        return 2

    # Resolve swarm model from config instead of hardcoding
    swarm_model = str(
        (cfg.get("routing", {}).get("tier_models", {}) or {}).get(
            "premium", "smart-opus-4.6"
        )
    )

    preflight = run_preflight(
        cfg,
        repo,
        objective,
        args.production,
        require_swarm=True,
        planned_via="droid",
        autonomous=True,
    )
    _print_preflight(preflight)
    if not preflight["ok"]:
        _record_run(
            conn,
            repo,
            objective,
            "droid",
            "premium",
            swarm_model,
            "blocked_preflight",
            False,
            notes="swarm preflight failed",
        )
        conn.close()
        return 2

    rules = ensure_rules_check(conn, repo, cfg, force=False)
    skills = discover_skills(repo, objective, cfg)
    subtask_skills = discover_skills_for_subtasks(repo, objective, cfg)

    if (
        bool(cfg.get("autonomy", {}).get("checkpoint_required", True))
        and not args.approve
    ):
        if not bool(cfg.get("autonomy", {}).get("allow_swarm_without_approve", False)):
            payload = {
                "ts": _now_ts(),
                "repo": str(repo),
                "objective": objective,
                "mode": "swarm",
                "skills": skills,
                "subtask_skills": subtask_skills,
                "rules": rules,
                "preflight": preflight,
            }
            cp = _write_checkpoint(paths["checkpoint_dir"], payload)
            _record_run(
                conn,
                repo,
                objective,
                "droid",
                "premium",
                swarm_model,
                "checkpoint_required",
                True,
                notes=f"checkpoint={cp}",
            )
            print(f"checkpoint_created={cp}")
            print("re_run_with=swarm --approve")
            conn.close()
            return 10

    # Direct swarm execution via codex/claude
    run_id = (
        time.strftime("%Y%m%d_%H%M%S")
        + "_"
        + hashlib.sha256(os.urandom(8)).hexdigest()[:4]
    )
    swarm_via = str(cfg.get("swarm", {}).get("via", "codex")).strip().lower()
    if swarm_via not in {"claude", "codex"}:
        swarm_via = "codex"
    swarm_timeout = int(cfg.get("swarm", {}).get("timeout_seconds", 300))

    fb_model = _resolve_runtime_model(
        swarm_via, "premium", swarm_model, cfg, explicit_model=False, swarm=True
    )
    fb_cmd = _execution_cmd(
        swarm_via, fb_model, objective, cfg, tier="premium", swarm=True
    )

    print(f"run_id={run_id}")
    print(f"swarm_via={swarm_via} model={fb_model}")

    ACTIVE_RUNS[run_id] = {"pid": os.getpid(), "started_at": _now_ts()}
    result = _run_cmd(fb_cmd, cwd=repo, timeout=swarm_timeout)
    ACTIVE_RUNS.pop(run_id, None)

    result_text = (result.get("stdout") or "").strip()
    final_state = "completed" if result.get("ok") else "failed"

    redacted_result_text = _redact_sensitive_text(result_text)
    output_path = paths["runs_dir"] / f"swarm_{run_id}.log"
    output_path.write_text(redacted_result_text + "\n")

    _record_run(
        conn,
        repo,
        objective,
        swarm_via,
        "premium",
        fb_model,
        final_state,
        True,
        output_path=str(output_path),
        notes=f"run_id={run_id}",
    )

    if final_state == "completed" and bool(
        cfg.get("learning", {}).get("auto_record_successes", True)
    ):
        _record_repo_learning(
            conn,
            repo,
            pattern=f"swarm success objective: {objective}",
            fix=f"state={final_state}",
            tags="auto,success,swarm",
            source="ai-coordinator",
            confidence=float(cfg.get("learning", {}).get("default_confidence", 0.7)),
        )
    elif bool(cfg.get("learning", {}).get("auto_record_failures", True)):
        _record_repo_learning(
            conn,
            repo,
            pattern=f"swarm failure objective: {objective}",
            fix=f"state={final_state}; summary={_truncate(redacted_result_text, 180)}",
            tags="auto,failure,swarm",
            source="ai-coordinator",
            confidence=float(cfg.get("learning", {}).get("default_confidence", 0.7)),
        )

    auto_snapshot = _maybe_auto_snapshot(conn, cfg, paths, repo, rules)

    print(f"swarm_status={final_state}")
    print(f"swarm_result_log={output_path}")
    if auto_snapshot:
        print(f"auto_snapshot={auto_snapshot}")
    if result_text:
        print("\n--- result ---")
        print(redacted_result_text.rstrip())

    # Auto-PR creation after successful swarm (mirrors cmd_exec behavior)
    if final_state == "completed" and getattr(args, "auto_pr", False):
        try:
            from pipeline.git_ops import auto_create_pr

            pr_url, pr_number = auto_create_pr(str(repo))
            if pr_url:
                print(f"\nauto_pr_created={pr_url}")
                if pr_number:
                    print(f"auto_pr_number={pr_number}")
            else:
                print("auto_pr_skipped=not_on_feature_branch_or_no_commits")
        except Exception as exc:
            print(f"auto_pr_failed={exc}")

    conn.close()
    return 0 if final_state == "completed" else 1


def _check_auth_health(
    profile_type: str, profile_name: str, profile_path: str, cfg: dict[str, Any]
) -> dict[str, Any]:
    """Check auth health for a single profile. Returns {healthy, expiry_ts, error}."""
    result: dict[str, Any] = {"healthy": True, "expiry_ts": None, "error": None}

    if profile_type == "codex":
        # Check auth.json in codex home directory
        auth_file = Path(profile_path) / "auth.json"
        if not auth_file.exists():
            result["healthy"] = False
            result["error"] = "auth.json not found"
            return result
        try:
            auth_data = json.loads(auth_file.read_text())
            expires_at = auth_data.get("expires_at")
            if expires_at:
                # Try parsing as epoch or ISO string
                if isinstance(expires_at, (int, float)):
                    expiry = int(expires_at)
                else:
                    from datetime import datetime

                    expiry = int(
                        datetime.fromisoformat(
                            str(expires_at).replace("Z", "+00:00")
                        ).timestamp()
                    )
                result["expiry_ts"] = expiry
                # Unhealthy if expires in <10 minutes
                if expiry < time.time() + 600:
                    result["healthy"] = False
                    result["error"] = "token expired or expiring soon"
        except Exception as exc:
            result["healthy"] = False
            result["error"] = f"failed to parse auth.json: {exc}"

    elif profile_type == "gemini":
        # Check session file exists and is recent
        home_path = Path(profile_path)
        if not home_path.exists():
            result["healthy"] = False
            result["error"] = "profile directory not found"
            return result
        # Look for any auth-related files
        session_files = list(
            home_path.glob("**/application_default_credentials.json")
        ) + list(home_path.glob("**/credentials.json"))
        if not session_files:
            result["healthy"] = False
            result["error"] = "no credential files found"
            return result
        # Check if most recent credential file was modified in last 7 days
        newest = max(session_files, key=lambda f: f.stat().st_mtime)
        age_hours = (time.time() - newest.stat().st_mtime) / 3600
        if age_hours > 168:  # 7 days
            result["healthy"] = False
            result["error"] = f"credentials last modified {age_hours:.0f}h ago"

    elif profile_type == "claude":
        # Check source reachability using connection pool
        base_url = str(profile_path).rstrip("/")
        if not base_url:
            result["healthy"] = False
            result["error"] = "missing source URL"
            return result

        mgmt_key = os.environ.get("CLIPROXY_MANAGEMENT_KEY", "").strip()
        headers = {"Content-Type": "application/json"}
        if mgmt_key:
            headers["X-Management-Key"] = mgmt_key
        resp = _http_json(f"{base_url}/models", headers=headers, timeout=5)
        if not resp.get("ok"):
            result["healthy"] = False
            result["error"] = f"unreachable: {resp.get('error', 'unknown')}"

    return result


def cmd_auth_health(args: argparse.Namespace) -> int:
    """Show auth health status for all profiles.

    Checks run in parallel via ThreadPoolExecutor to avoid sequential
    latency when multiple profiles require network probes.
    """
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))

    # Load gateway config for profile info
    gw_path = Path.home() / ".ai-fleet" / "gateway.json"
    gw_cfg = _read_json(gw_path, {})

    # Build list of (profile_type, name, path_or_url) tuples for parallel check
    check_items: list[tuple[str, str, str]] = []
    for name, home_path in (gw_cfg.get("codex_homes") or {}).items():
        check_items.append(("codex", name, home_path))
    for name, home_path in (gw_cfg.get("gemini_homes") or {}).items():
        check_items.append(("gemini", name, home_path))
    claude_cfg = gw_cfg.get("claude") if isinstance(gw_cfg, dict) else {}
    if not isinstance(claude_cfg, dict):
        claude_cfg = {}
    for src in claude_cfg.get("usage_sources") or []:
        if not isinstance(src, dict):
            continue
        check_items.append(
            ("claude", str(src.get("name", "unknown")), str(src.get("url", "")))
        )

    # Run all health checks concurrently
    results = []
    if check_items:
        with ThreadPoolExecutor(max_workers=min(8, len(check_items))) as pool:
            future_map = {
                pool.submit(_check_auth_health, ptype, pname, ppath, cfg): (
                    ptype,
                    pname,
                )
                for ptype, pname, ppath in check_items
            }
            for future in as_completed(future_map):
                ptype, pname = future_map[future]
                try:
                    health = future.result()
                except Exception as exc:
                    health = {"healthy": False, "expiry_ts": None, "error": str(exc)}
                results.append({"type": ptype, "name": pname, **health})
                conn.execute(
                    "INSERT OR REPLACE INTO auth_health(profile_type, profile_name, last_check, healthy, expiry_ts, error) VALUES(?,?,?,?,?,?)",
                    (
                        ptype,
                        pname,
                        int(time.time()),
                        1 if health["healthy"] else 0,
                        health.get("expiry_ts"),
                        health.get("error"),
                    ),
                )

    conn.commit()
    conn.close()

    # Output
    if hasattr(args, "json") and args.json:
        print(json.dumps(results, indent=2))
        return 0

    if not results:
        print("No profiles found. Check gateway.json configuration.")
        return 0

    print(f"{'Type':<10} {'Profile':<25} {'Healthy':>7} {'Expiry':<20} {'Error'}")
    print("-" * 85)
    for r in results:
        healthy_str = "YES" if r["healthy"] else "NO"
        expiry_str = ""
        if r.get("expiry_ts"):
            from datetime import datetime

            expiry_str = datetime.fromtimestamp(r["expiry_ts"]).strftime(
                "%Y-%m-%d %H:%M"
            )
        error_str = r.get("error") or ""
        print(
            f"{r['type']:<10} {r['name']:<25} {healthy_str:>7} {expiry_str:<20} {error_str}"
        )

    healthy_count = sum(1 for r in results if r["healthy"])
    print(f"\n{healthy_count}/{len(results)} profiles healthy")
    return 0 if healthy_count == len(results) else 1


def cmd_auth_list(args: argparse.Namespace) -> int:
    _ = args
    cfg = load_config()
    print("codex_profiles")
    print("--------------")
    print(
        _run_cmd(["ai-fleet", "profiles", "list"], timeout=30)
        .get("stdout", "")
        .rstrip()
    )
    print("\ngemini_profiles")
    print("---------------")
    print(
        _run_cmd(["ai-fleet", "gemini-profiles", "list"], timeout=30)
        .get("stdout", "")
        .rstrip()
    )

    route = _gateway_route(cfg)
    health = _gateway_health(cfg)
    route_obj = route.get("json", {}) if route.get("ok") else {}
    health_obj = health.get("json", {}) if health.get("ok") else {}

    print("\nroute")
    print("-----")
    print(f"codex={route_obj.get('codex_profile', '')}")
    print(f"gemini={route_obj.get('gemini_profile', '')}")
    print(f"claude={route_obj.get('claude_source', '')}")

    usage = (health_obj.get("claude") or {}).get("usage") or {}
    sources = usage.get("sources") or []
    if sources:
        print("\nclaude_sources")
        print("--------------")
        for src in sources:
            if not isinstance(src, dict):
                continue
            print(
                f"name={src.get('name', '')} status={src.get('status', '')} "
                f"email={src.get('email', '') or 'none'} account_type={src.get('account_type', '') or 'none'} "
                f"standby={src.get('standby', False)} group={src.get('group', '') or 'none'} "
                f"ok_rate={src.get('ok_rate', '')} fail_ratio={src.get('fail_ratio', '')} score={src.get('score', '')}"
            )

    providers = health_obj.get("external_providers") or []
    if providers:
        print("\nexternal_providers")
        print("------------------")
        for p in providers:
            if not isinstance(p, dict):
                continue
            print(
                f"name={p.get('name', '')} enabled={p.get('enabled', False)} "
                f"key_present={p.get('key_present', False)} healthy={p.get('healthy', False)}"
            )
    return 0


def cmd_auth_add(args: argparse.Namespace) -> int:
    provider = args.provider
    if provider in {"codex", "gemini"} and not str(args.path or "").strip():
        print("missing_path: provide profile path")
        return 2

    if provider == "codex":
        res = _run_cmd(["ai-codex-profile", "add", args.name, args.path], timeout=30)
        sys.stdout.write(res.get("stdout", ""))
        sys.stderr.write(res.get("stderr", ""))
        if res["ok"] and args.login:
            login = _run_cmd(["ai-codex-profile", "login", args.name], timeout=600)
            sys.stdout.write(login.get("stdout", ""))
            sys.stderr.write(login.get("stderr", ""))
            return 0 if login["ok"] else 1
        return 0 if res["ok"] else 1

    if provider == "gemini":
        res = _run_cmd(["ai-gemini-profile", "add", args.name, args.path], timeout=30)
        sys.stdout.write(res.get("stdout", ""))
        sys.stderr.write(res.get("stderr", ""))
        return 0 if res["ok"] else 1

    # Claude source registration in gateway config
    source_url = (args.url or args.path or "").strip()
    mgmt_key = (
        args.management_key or os.environ.get("CLIPROXY_MANAGEMENT_KEY", "")
    ).strip()
    if not source_url:
        print("missing_source_url: pass a URL via positional path or --url")
        return 2

    gateway_cfg_path = Path.home() / ".ai-fleet" / "gateway.json"
    cfg = _read_json(gateway_cfg_path, {})
    if not isinstance(cfg, dict):
        print(f"invalid_gateway_config={gateway_cfg_path}")
        return 2

    claude_cfg = cfg.setdefault("claude", {})
    usage_sources = claude_cfg.setdefault("usage_sources", [])
    if not isinstance(usage_sources, list):
        usage_sources = []
        claude_cfg["usage_sources"] = usage_sources

    source_obj = None
    for src in usage_sources:
        if isinstance(src, dict) and str(src.get("name", "")) == args.name:
            source_obj = src
            break
    if source_obj is None:
        source_obj = {"name": args.name}
        usage_sources.append(source_obj)
    source_obj["name"] = args.name
    source_obj["url"] = source_url
    store_mgmt_keys = (
        os.environ.get("AI_COORDINATOR_STORE_MANAGEMENT_KEYS", "0").strip() == "1"
    )
    if mgmt_key and store_mgmt_keys:
        source_obj["management_key"] = mgmt_key
    if str(args.group or "").strip():
        source_obj["group"] = str(args.group).strip()
    if bool(args.standby):
        source_obj["standby"] = True
    if str(args.email or "").strip():
        source_obj["email"] = str(args.email).strip().lower()
    if str(args.account_type or "").strip():
        source_obj["account_type"] = str(args.account_type).strip().lower()

    _save_gateway_config(cfg)
    print(
        "claude_source_registered "
        f"name={args.name} url={source_url} standby={bool(args.standby)} "
        f"group={str(args.group or '').strip() or 'none'} "
        f"email={str(args.email or '').strip() or 'none'} "
        f"account_type={str(args.account_type or '').strip() or 'none'} "
        f"management_key_stored={'yes' if (mgmt_key and store_mgmt_keys) else 'no'}"
    )
    print("next=launchctl kickstart -k gui/$(id -u)/com.ai.fleet.gateway")
    return 0


def cmd_auth_detect(args: argparse.Namespace) -> int:
    _ = args
    res = _run_cmd(["ai-codex-profile", "detect"], timeout=60)
    sys.stdout.write(res.get("stdout", ""))
    sys.stderr.write(res.get("stderr", ""))
    return 0 if res["ok"] else 1


def cmd_auth_clone_gemini(args: argparse.Namespace) -> int:
    cmd = ["ai-gemini-profile", "clone-current", args.name, args.home]
    if args.model:
        cmd += ["--model", args.model]
    if args.disabled:
        cmd += ["--disabled"]
    res = _run_cmd(cmd, timeout=120)
    sys.stdout.write(res.get("stdout", ""))
    sys.stderr.write(res.get("stderr", ""))
    return 0 if res["ok"] else 1


def cmd_auth_test(args: argparse.Namespace) -> int:
    provider = str(args.provider).strip().lower()
    prompt = str(args.prompt or "Reply exactly OK.").strip()
    ok = True
    gemini_known_runtime_bug = False

    if provider in {"all", "codex"}:
        if shutil.which("ai-codex-profile"):
            cmd = ["ai-codex-profile", "test"]
            if args.name and provider == "codex":
                cmd.append(args.name)
            cmd += ["--prompt", prompt]
            res = _run_cmd(cmd, timeout=180)
        else:
            if args.name and provider == "codex":
                print(
                    "warn=codex profile name ignored (ai-codex-profile not installed)",
                    file=sys.stderr,
                )
            fallback_cmd = [
                "codex",
                "exec",
                "--full-auto",
                "--skip-git-repo-check",
                "--ephemeral",
                "-m",
                "gpt-5.1-codex-mini",
                "-c",
                'model_reasoning_effort="high"',
                prompt,
            ]
            res = _run_cmd(fallback_cmd, timeout=180)
        sys.stdout.write(res.get("stdout", ""))
        sys.stderr.write(res.get("stderr", ""))
        ok = ok and bool(res["ok"])

    if provider in {"all", "gemini"}:
        if shutil.which("ai-gemini-profile"):
            cmd = ["ai-gemini-profile", "test"]
            if args.name and provider == "gemini":
                cmd.append(args.name)
            cmd += ["--prompt", prompt]
            res = _run_cmd(cmd, timeout=180)
        else:
            if args.name and provider == "gemini":
                print(
                    "warn=gemini profile name ignored (ai-gemini-profile not installed)",
                    file=sys.stderr,
                )
            fallback_cmd = ["gemini", "-p", prompt]
            res = _run_cmd(fallback_cmd, timeout=180)

        stderr_low = _ensure_str(res.get("stderr", "")).lower()
        gemini_known_runtime_bug = (
            "error when talking to gemini api" in stderr_low
            and "syntaxerror: expected property name or '}' in json" in stderr_low
            and "code_assist/server.js" in stderr_low
        )

        sys.stdout.write(res.get("stdout", ""))
        sys.stderr.write(res.get("stderr", ""))

        if gemini_known_runtime_bug:
            print(
                "warn=gemini_cli_runtime_bug_detected (known upstream issue); treating as degraded for provider=all",
                file=sys.stderr,
            )
            if provider == "gemini":
                ok = False
        else:
            ok = ok and bool(res["ok"])

    if provider == "all" and gemini_known_runtime_bug and ok:
        print(
            "auth_test_summary=codex_ok gemini_degraded_known_runtime_bug",
            file=sys.stderr,
        )

    return 0 if ok else 1


def cmd_auth_key_status(args: argparse.Namespace) -> int:
    _ = args
    cfg = load_config()
    gateway_cfg = _load_gateway_config()
    gateway_health = _gateway_health(cfg)
    provider_health_rows = {
        str(x.get("name", "")).strip().lower(): x
        for x in (
            (gateway_health.get("json", {}) if gateway_health.get("ok") else {}).get(
                "external_providers", []
            )
            or []
        )
        if isinstance(x, dict)
    }

    specs = _provider_specs()
    print("provider_keys")
    print("-------------")
    for name in ("openrouter", "kimi", "minimax"):
        spec = specs.get(name, {})
        env_name = spec.get("env", "")
        env_value = os.getenv(env_name, "").strip()
        row = _find_external_provider(gateway_cfg, name) or {}
        cfg_key = str(row.get("api_key", "")).strip()
        key_present = bool(cfg_key or env_value)
        enabled = bool(row.get("enabled", False))
        key_source = "config" if cfg_key else ("env" if env_value else "missing")
        health_row = provider_health_rows.get(name, {})
        print(
            f"name={name} enabled={enabled} key_present={key_present} key_source={key_source} "
            f"healthy={health_row.get('healthy', False)} failures={health_row.get('failures', 0)}"
        )
        if cfg_key:
            print(f"  config_key={_mask_secret(cfg_key)}")

    return 0


def cmd_auth_key_set(args: argparse.Namespace) -> int:
    provider = str(args.provider).strip().lower()
    key = str(args.key or "").strip()
    if not key:
        print("missing_key")
        return 2

    gateway_cfg = _load_gateway_config()
    entry = _ensure_external_provider(gateway_cfg, provider)
    entry["api_key"] = key
    if args.enable:
        entry["enabled"] = True
    _save_gateway_config(gateway_cfg)
    print(
        f"key_set provider={provider} enabled={entry.get('enabled', False)} masked={_mask_secret(key)}"
    )
    print("next=launchctl kickstart -k gui/$(id -u)/com.ai.fleet.gateway")
    return 0


def cmd_auth_key_clear(args: argparse.Namespace) -> int:
    provider = str(args.provider).strip().lower()
    gateway_cfg = _load_gateway_config()
    entry = _find_external_provider(gateway_cfg, provider)
    if entry is None:
        print(f"provider_not_found={provider}")
        return 2
    entry.pop("api_key", None)
    if args.disable:
        entry["enabled"] = False
    _save_gateway_config(gateway_cfg)
    print(f"key_cleared provider={provider} enabled={entry.get('enabled', False)}")
    print("next=launchctl kickstart -k gui/$(id -u)/com.ai.fleet.gateway")
    return 0


def cmd_auth_usage(args: argparse.Namespace) -> int:
    """Show per-profile usage statistics from cost_log and gateway backend state."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    # Local cost_log stats grouped by via
    rows = conn.execute("""
        SELECT via, COUNT(*) as requests, SUM(CASE WHEN estimated_cost_usd > 0 THEN 1 ELSE 0 END) as successes,
               SUM(estimated_cost_usd) as total_cost, AVG(estimated_cost_usd) as avg_cost
        FROM cost_log GROUP BY via ORDER BY requests DESC
    """).fetchall()

    print("usage_by_provider")
    print("-" * 70)
    print(
        f"{'provider':<12} {'requests':>8} {'successes':>9} {'total_cost':>11} {'avg_cost':>10}"
    )
    print(
        f"{'--------':<12} {'--------':>8} {'---------':>9} {'----------':>11} {'--------':>10}"
    )
    for r in rows:
        via = r[0] or "?"
        reqs = r[1] or 0
        succ = r[2] or 0
        total = r[3] or 0.0
        avg = r[4] or 0.0
        print(f"{via:<12} {reqs:>8} {succ:>9} ${total:>10.4f} ${avg:>9.4f}")

    if not rows:
        print("  No usage data recorded yet.")

    # Gateway backend state (live stats)
    gateway_health = _gateway_health(cfg)
    if gateway_health.get("ok"):
        health_json = gateway_health.get("json", {})
        ext = health_json.get("external_providers", [])
        if ext:
            print("\ngateway_backend_state")
            print("-" * 70)
            print(
                f"{'backend':<16} {'requests':>8} {'successes':>9} {'failures':>8} {'latency_ms':>10}"
            )
            print(
                f"{'-------':<16} {'--------':>8} {'---------':>9} {'--------':>8} {'----------':>10}"
            )
            for p in ext:
                name = p.get("name", "?")
                print(
                    f"{name:<16} {p.get('requests', 0):>8} {p.get('successes', 0):>9} {p.get('failures', 0):>8} {p.get('avg_latency_ms', 0):>10.1f}"
                )

    conn.close()
    return 0


# --- CLIProxyAPI Usage Sync ---


def cmd_proxy_sync(args: argparse.Namespace) -> int:
    """Fetch CLIProxyAPI usage stats and log to cost_log + budget_state."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    mgmt_key = (
        os.environ.get("CLIPROXY_MANAGEMENT_KEY", "")
        or cfg.get("cliproxy", {}).get("management_key", "")
    ).strip()
    proxy_port = int(cfg.get("cliproxy", {}).get("port", 8317))
    base_url = f"http://127.0.0.1:{proxy_port}"

    if not mgmt_key:
        print(
            "cliproxy_sync_failed: missing_management_key (set CLIPROXY_MANAGEMENT_KEY)"
        )
        conn.close()
        return 1

    # Fetch usage stats
    resp = _http_json(
        f"{base_url}/v0/management/usage",
        headers={"X-Management-Key": mgmt_key},
        timeout=6,
    )
    if not resp.get("ok"):
        print(f"cliproxy_sync_failed: {resp.get('error', 'unreachable')}")
        conn.close()
        return 1

    data = resp.get("json", {})
    usage = data.get("usage", {})

    total_requests = int(usage.get("total_requests", 0))
    success_count = int(usage.get("success_count", 0))
    failure_count = int(usage.get("failure_count", 0))
    total_tokens = int(usage.get("total_tokens", 0))
    avg_latency = float(usage.get("avg_latency_ms", 0))

    # Fetch per-model breakdown if available
    models = usage.get("models", {})
    now_ts = _now_ts()
    run_id = f"proxy_sync_{time.strftime('%Y%m%d_%H%M%S')}"

    # Delete previous proxy_sync entries for today to avoid double-counting
    # (CLIProxyAPI reports cumulative stats, not deltas)
    today_start_ts = int(
        time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
    )
    conn.execute(
        "DELETE FROM cost_log WHERE via = 'cliproxy' AND timestamp >= ?",
        (today_start_ts,),
    )
    conn.commit()

    if models:
        for model_name, model_stats in models.items():
            m_tokens = int(model_stats.get("tokens", 0))
            _ = int(model_stats.get("requests", 0))  # available for future use
            provider = (
                "anthropic"
                if "claude" in model_name.lower()
                else (
                    "openai"
                    if "gpt" in model_name.lower() or "codex" in model_name.lower()
                    else "google"
                    if "gemini" in model_name.lower()
                    else "custom"
                )
            )
            # Rough cost estimate per model
            est_input = m_tokens * 3 // 4
            est_output = m_tokens // 4
            cost = _estimate_cost(cfg, model_name, est_input, est_output)
            conn.execute(
                "INSERT INTO cost_log(run_id, timestamp, provider, model, input_tokens, output_tokens, estimated_cost_usd, tier, via) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    now_ts,
                    provider,
                    model_name,
                    est_input,
                    est_output,
                    cost,
                    "proxy_sync",
                    "cliproxy",
                ),
            )
    else:
        # No per-model breakdown — log aggregate
        est_input = total_tokens * 3 // 4
        est_output = total_tokens // 4
        cost = _estimate_cost(cfg, "claude-sonnet-4-5", est_input, est_output)
        conn.execute(
            "INSERT INTO cost_log(run_id, timestamp, provider, model, input_tokens, output_tokens, estimated_cost_usd, tier, via) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                run_id,
                now_ts,
                "anthropic",
                "aggregate",
                est_input,
                est_output,
                cost,
                "proxy_sync",
                "cliproxy",
            ),
        )

    conn.commit()

    # Update monthly budget_state
    current_month = time.strftime("%Y-%m")
    monthly_cap = float(cfg.get("budget", {}).get("monthly_limit_usd", 300.0))
    month_start = int(time.mktime(time.strptime(f"{current_month}-01", "%Y-%m-%d")))
    month_cost_row = conn.execute(
        "SELECT COALESCE(SUM(estimated_cost_usd), 0) FROM cost_log WHERE timestamp >= ?",
        (month_start,),
    ).fetchone()
    month_cost = float(month_cost_row[0]) if month_cost_row else 0.0
    conn.execute(
        """
        INSERT INTO budget_state(month, cap_usd, spent_usd, last_updated)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(month) DO UPDATE SET
            spent_usd = excluded.spent_usd,
            last_updated = excluded.last_updated
    """,
        (current_month, monthly_cap, month_cost, now_ts),
    )
    conn.commit()

    # Budget enforcement for claudemax sessions
    daily_budget_usd = float(
        cfg.get("budget", {}).get("cliproxy_daily_limit_usd", 10.0)
    )
    alert_threshold_pct = float(
        cfg.get("budget", {}).get("cliproxy_alert_threshold_pct", 80)
    )

    # Sum today's proxy_sync costs
    today_start = int(time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d")))
    today_cost_row = conn.execute(
        "SELECT COALESCE(SUM(estimated_cost_usd), 0) FROM cost_log WHERE via = 'cliproxy' AND timestamp >= ?",
        (today_start,),
    ).fetchone()
    _today_proxy_cost = float(today_cost_row[0]) if today_cost_row else 0.0  # noqa: F841

    proxy_cost_today = float(today_cost_row[0]) if today_cost_row else 0.0

    budget_pct = (
        (proxy_cost_today / daily_budget_usd * 100) if daily_budget_usd > 0 else 0
    )

    # Print summary
    if hasattr(args, "json") and args.json:
        print(
            json.dumps(
                {
                    "total_requests": total_requests,
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "total_tokens": total_tokens,
                    "avg_latency_ms": avg_latency,
                    "today_cost_usd": round(proxy_cost_today, 4),
                    "daily_budget_usd": daily_budget_usd,
                    "budget_pct": round(budget_pct, 1),
                    "models": models,
                },
                indent=2,
            )
        )
    else:
        print(f"cliproxy_sync run_id={run_id}")
        print(
            f"requests={total_requests} success={success_count} failures={failure_count}"
        )
        print(f"total_tokens={total_tokens:,} avg_latency={avg_latency:.0f}ms")
        if models:
            model_parts = [f"{k}={v.get('requests', 0)}req" for k, v in models.items()]
            print(f"models: {', '.join(model_parts)}")
        print(
            f"today_cost=${proxy_cost_today:.4f} / ${daily_budget_usd:.2f} ({budget_pct:.1f}%)"
        )

    # Alert if approaching budget
    if budget_pct >= alert_threshold_pct:
        alert_msg = f"CLIProxy daily budget {budget_pct:.0f}% used (${proxy_cost_today:.2f}/${daily_budget_usd:.2f})"
        _notify_event(conn, "budget_warning", alert_msg, "warning")
        print(f"BUDGET_WARNING: {alert_msg}")
    if budget_pct >= 100:
        alert_msg = f"CLIProxy daily budget EXCEEDED: ${proxy_cost_today:.2f}/${daily_budget_usd:.2f}"
        _notify_event(conn, "budget_exceeded", alert_msg, "error")
        print(f"BUDGET_EXCEEDED: {alert_msg}")

    conn.close()
    return 0


# --- Codex Profile Usage Tracking & Scheduler ---


def _record_codex_usage(
    conn: sqlite3.Connection,
    profile_name: str,
    success: bool,
    input_tokens: int = 0,
    output_tokens: int = 0,
    error_code: int | None = None,
    error_msg: str = "",
) -> None:
    """Record a codex profile usage event."""
    now = int(time.time())
    week_start = time.strftime("%Y-W%W", time.localtime(now))
    conn.execute(
        "INSERT INTO codex_profile_usage(profile_name, ts, event, requests_delta, input_tokens, output_tokens, success, error_code, error_msg, weekly_period) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            profile_name,
            now,
            "request",
            1,
            input_tokens,
            output_tokens,
            1 if success else 0,
            error_code,
            error_msg[:500],
            week_start,
        ),
    )
    conn.commit()


def _get_profile_stats(
    conn: sqlite3.Connection, profile_name: str, hours: int = 168
) -> dict[str, Any]:
    """Get usage stats for a profile over the given window (default 7 days)."""
    cutoff = int(time.time()) - hours * 3600
    row = conn.execute(
        """SELECT COUNT(*) as total, SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) as successes,
                  SUM(CASE WHEN error_code=429 THEN 1 ELSE 0 END) as rate_limits,
                  SUM(input_tokens) as total_in, SUM(output_tokens) as total_out,
                  MIN(ts) as first_ts, MAX(ts) as last_ts
           FROM codex_profile_usage WHERE profile_name=? AND ts>=?""",
        (profile_name, cutoff),
    ).fetchone()
    if not row or not row[0]:
        return {
            "total": 0,
            "successes": 0,
            "rate_limits": 0,
            "tokens_in": 0,
            "tokens_out": 0,
            "burn_rate_per_hour": 0.0,
        }

    total, successes, rate_limits = row[0], row[1] or 0, row[2] or 0
    elapsed_hours = (
        max(1, (row[6] - row[5]) / 3600)
        if row[5] and row[6] and row[6] > row[5]
        else max(1, hours)
    )
    burn_rate = total / elapsed_hours

    return {
        "total": total,
        "successes": successes,
        "rate_limits": rate_limits,
        "tokens_in": row[3] or 0,
        "tokens_out": row[4] or 0,
        "burn_rate_per_hour": round(burn_rate, 2),
        "first_ts": row[5],
        "last_ts": row[6],
    }


def _sync_profile_schedule(
    conn: sqlite3.Connection, gateway_cfg: dict[str, Any]
) -> None:
    """Sync codex profile schedule from gateway config into DB."""
    profiles = gateway_cfg.get("codex", {}).get("profiles", [])
    for p in profiles:
        name = p.get("name", "")
        if not name:
            continue
        conn.execute(
            """INSERT INTO codex_profile_schedule(profile_name, enabled, plan, allow_overage, safety_margin_pct, weekly_resets_at)
               VALUES(?,?,?,?,?,?)
               ON CONFLICT(profile_name) DO UPDATE SET
                   plan=excluded.plan,
                   allow_overage=excluded.allow_overage,
                   safety_margin_pct=excluded.safety_margin_pct,
                   weekly_resets_at=excluded.weekly_resets_at""",
            (
                name,
                1 if p.get("enabled") else 0,
                p.get("plan", "free"),
                1 if p.get("allow_overage") else 0,
                p.get("safety_margin_pct", 10),
                p.get("weekly_resets_at", ""),
            ),
        )
    conn.commit()


def _check_profile_resets(
    conn: sqlite3.Connection, gateway_cfg: dict[str, Any]
) -> list[str]:
    """Check if any disabled profiles should be re-enabled after their reset.
    Returns list of profile names that were re-enabled."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    re_enabled = []

    profiles = gateway_cfg.get("codex", {}).get("profiles", [])
    for p in profiles:
        name = p.get("name", "")
        if not name or p.get("enabled"):
            continue  # Skip already-enabled profiles

        reset_str = p.get("weekly_resets_at", "")
        if not reset_str:
            continue

        try:
            reset_dt = datetime.fromisoformat(reset_str)
            if now >= reset_dt:
                # Reset has passed — re-enable this profile
                p["enabled"] = True
                # Update next reset (rolling 7 days from this reset)
                from datetime import timedelta

                next_reset = reset_dt + timedelta(days=7)
                p["weekly_resets_at"] = next_reset.isoformat()

                # Reset period counters in DB
                conn.execute(
                    "UPDATE codex_profile_schedule SET enabled=1, last_reset_ts=?, total_requests_this_period=0, total_429s_this_period=0, auto_disabled_reason=NULL WHERE profile_name=?",
                    (now_ts, name),
                )
                re_enabled.append(name)
        except (ValueError, TypeError):
            continue

    if re_enabled:
        conn.commit()
        # Write back to gateway config
        gateway_path = Path.home() / ".ai-fleet" / "gateway.json"
        if gateway_path.exists():
            _write_json(gateway_path, gateway_cfg)

    return re_enabled


def _check_profile_exhaustion(
    conn: sqlite3.Connection, gateway_cfg: dict[str, Any]
) -> list[str]:
    """Check if any enabled profiles should be disabled due to predicted exhaustion.
    Returns list of profile names that were disabled."""
    disabled = []
    profiles = gateway_cfg.get("codex", {}).get("profiles", [])

    for p in profiles:
        name = p.get("name", "")
        if not name or not p.get("enabled"):
            continue

        # Check recent 429 rate
        stats = _get_profile_stats(conn, name, hours=1)
        if stats["rate_limits"] >= 2:
            # Got 2+ rate limits in the last hour — profile is likely exhausted
            p["enabled"] = False
            conn.execute(
                "UPDATE codex_profile_schedule SET enabled=0, last_exhausted_ts=?, auto_disabled_reason=? WHERE profile_name=?",
                (int(time.time()), f"429x{stats['rate_limits']} in last hour", name),
            )
            disabled.append(name)
            continue

        # For non-overage profiles, estimate if approaching limit
        if not p.get("allow_overage", False):
            weekly_stats = _get_profile_stats(conn, name, hours=168)
            safety_pct = p.get("safety_margin_pct", 10)

            # If we've seen a 429 this week, we know the approximate budget
            sched = conn.execute(
                "SELECT estimated_weekly_budget, total_requests_this_period FROM codex_profile_schedule WHERE profile_name=?",
                (name,),
            ).fetchone()

            if sched and sched[0] and sched[0] > 0:
                budget = sched[0]
                used = weekly_stats["total"]
                pct_used = (used / budget) * 100
                threshold = 100 - safety_pct

                if pct_used >= threshold:
                    p["enabled"] = False
                    reason = (
                        f"predicted {pct_used:.0f}% of budget (safety={safety_pct}%)"
                    )
                    conn.execute(
                        "UPDATE codex_profile_schedule SET enabled=0, auto_disabled_reason=? WHERE profile_name=?",
                        (reason, name),
                    )
                    disabled.append(name)

    if disabled:
        conn.commit()
        gateway_path = Path.home() / ".ai-fleet" / "gateway.json"
        if gateway_path.exists():
            _write_json(gateway_path, gateway_cfg)

    return disabled


def _calibrate_weekly_budget(conn: sqlite3.Connection, profile_name: str) -> None:
    """When a profile gets 429'd, use that as a calibration point for its weekly budget."""
    # Count total successful requests since last reset
    sched = conn.execute(
        "SELECT last_reset_ts FROM codex_profile_schedule WHERE profile_name=?",
        (profile_name,),
    ).fetchone()
    since_ts = sched[0] if sched and sched[0] else int(time.time()) - 7 * 86400

    row = conn.execute(
        "SELECT COUNT(*) FROM codex_profile_usage WHERE profile_name=? AND ts>=? AND success=1",
        (profile_name, since_ts),
    ).fetchone()
    total = row[0] if row else 0

    if total > 0:
        # This is roughly the weekly budget (we hit the limit at this count)
        conn.execute(
            "UPDATE codex_profile_schedule SET estimated_weekly_budget=?, total_429s_this_period=total_429s_this_period+1 WHERE profile_name=?",
            (total, profile_name),
        )
        conn.commit()


def run_profile_scheduler(gateway_cfg: dict[str, Any], db_path: Path) -> dict[str, Any]:
    """Run one cycle of the profile scheduler. Returns summary of actions taken."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    _sync_profile_schedule(conn, gateway_cfg)
    re_enabled = _check_profile_resets(conn, gateway_cfg)
    disabled = _check_profile_exhaustion(conn, gateway_cfg)

    conn.close()
    return {"re_enabled": re_enabled, "disabled": disabled}


def cmd_codex_profiles(args: argparse.Namespace) -> int:
    """Show codex profile status, usage, and predictions."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    gateway_path = Path.home() / ".ai-fleet" / "gateway.json"
    gateway_cfg = json.loads(gateway_path.read_text()) if gateway_path.exists() else {}

    subcmd = getattr(args, "profiles_cmd", "status")

    if subcmd == "check":
        result = run_profile_scheduler(gateway_cfg, paths["db_path"])
        if result["re_enabled"]:
            print(f"Re-enabled: {', '.join(result['re_enabled'])}")
        if result["disabled"]:
            print(f"Auto-disabled: {', '.join(result['disabled'])}")
        if not result["re_enabled"] and not result["disabled"]:
            print("No profile changes needed.")
        conn.close()
        return 0

    # Default: status
    _sync_profile_schedule(conn, gateway_cfg)
    profiles = gateway_cfg.get("codex", {}).get("profiles", [])

    print(
        f"{'name':<22} {'plan':<6} {'on?':>3} {'reqs':>5} {'429s':>4} {'burn/h':>7} {'budget':>7} {'resets':>20} {'reason'}"
    )
    print("-" * 110)

    for p in profiles:
        name = p.get("name", "?")
        plan = p.get("plan", "?")
        enabled = "YES" if p.get("enabled") else "NO"

        stats = _get_profile_stats(conn, name, hours=168)
        sched = conn.execute(
            "SELECT estimated_weekly_budget, auto_disabled_reason, weekly_resets_at FROM codex_profile_schedule WHERE profile_name=?",
            (name,),
        ).fetchone()

        budget = sched[0] if sched and sched[0] else "-"
        reason = str(sched[1] or "")[:30] if sched else ""
        reset_str = p.get("weekly_resets_at") or ""
        if reset_str:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(reset_str)
                reset_str = dt.strftime("%b %d %I:%M %p")
            except (ValueError, TypeError):
                pass

        budget_str = str(budget) if isinstance(budget, int) else budget
        print(
            f"{name:<22} {plan:<6} {enabled:>3} {stats['total']:>5} {stats['rate_limits']:>4} {stats['burn_rate_per_hour']:>7.1f} {budget_str:>7} {reset_str:>20} {reason}"
        )

    # Show gateway health if available
    gateway_health = _gateway_health(cfg)
    if gateway_health.get("ok"):
        health = gateway_health.get("json", {})
        codex_health = health.get("codex", {})
        gw_profiles = codex_health.get("profiles", [])
        if gw_profiles:
            print("\ngateway_live_state")
            print(
                f"{'name':<22} {'healthy':>7} {'fails':>5} {'latency':>8} {'reqs':>5} {'ok':>5}"
            )
            print("-" * 60)
            for gp in gw_profiles:
                print(
                    f"{gp.get('name', '?'):<22} {'YES' if gp.get('healthy') else 'NO':>7} {gp.get('failures', 0):>5} {gp.get('avg_latency_ms', 0):>7.0f}ms {gp.get('requests', 0):>5} {gp.get('successes', 0):>5}"
                )

    conn.close()
    return 0


# --- Claude Profile Usage Tracking & Scheduler ---

# Claude profile definitions — known accounts and their plan metadata.
# CLIProxyAPI stores auth in ~/.cli-proxy-api/claude-<email>.json and
# antigravity-<email>.json files.  The proxy round-robins across accounts
# and auto-switches on quota exceeded (switch-project: true).
#
# This module mirrors the codex profile tracking pattern:
# 1. Per-request usage recording
# 2. Periodic schedule sync from auth directory
# 3. Weekly/session reset detection
# 4. Exhaustion prediction + auto-disable

CLAUDE_PROFILES_REGISTRY: list[dict[str, Any]] = [
    {
        "name": "primary-team",
        "email": "user@example.com",
        "context": "YourOrg",
        "plan": "team",
        "channel": "claude",
        "has_extra_usage": False,
        "extra_usage_limit_gbp": 0,
        "weekly_resets_at": "Fri 10:00",
        "session_hours": 5,
        "estimated_session_tokens": 220000,
        "estimated_weekly_tokens": 0,
    },
    {
        "name": "secondary-max",
        "email": "user2@example.com",
        "context": "Personal",
        "plan": "max",
        "channel": "claude",
        "has_extra_usage": False,
        "extra_usage_limit_gbp": 0,
        "weekly_resets_at": "Sat 02:00",
        "session_hours": 5,
        "estimated_session_tokens": 220000,
        "estimated_weekly_tokens": 0,
    },
    # Antigravity fallback profiles (Claude via Google)
    {
        "name": "ag-primary",
        "email": "user@example.com",
        "context": "Antigravity",
        "plan": "antigravity",
        "channel": "antigravity",
        "has_extra_usage": False,
        "extra_usage_limit_gbp": 0,
        "weekly_resets_at": "",
        "session_hours": 0,
        "estimated_session_tokens": 0,
        "estimated_weekly_tokens": 0,
    },
    {
        "name": "ag-secondary",
        "email": "user2@example.com",
        "context": "Antigravity",
        "plan": "antigravity",
        "channel": "antigravity",
        "has_extra_usage": False,
        "extra_usage_limit_gbp": 0,
        "weekly_resets_at": "",
        "session_hours": 0,
        "estimated_session_tokens": 0,
        "estimated_weekly_tokens": 0,
    },
]


def _record_claude_usage(
    conn: sqlite3.Connection,
    profile_name: str,
    model: str = "",
    success: bool = True,
    input_tokens: int = 0,
    output_tokens: int = 0,
    error_code: int | None = None,
    error_msg: str = "",
) -> None:
    """Record a Claude profile usage event."""
    now = int(time.time())
    week_start = time.strftime("%Y-W%W", time.localtime(now))
    conn.execute(
        "INSERT INTO claude_profile_usage(profile_name, ts, event, model, requests_delta, input_tokens, output_tokens, success, error_code, error_msg, weekly_period) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (
            profile_name,
            now,
            "request",
            model,
            1,
            input_tokens,
            output_tokens,
            1 if success else 0,
            error_code,
            error_msg[:500],
            week_start,
        ),
    )
    conn.commit()


def _get_claude_profile_stats(
    conn: sqlite3.Connection, profile_name: str, hours: int = 168
) -> dict[str, Any]:
    """Get usage stats for a Claude profile over the given window (default 7 days)."""
    cutoff = int(time.time()) - hours * 3600
    row = conn.execute(
        """SELECT COUNT(*) as total, SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) as successes,
                  SUM(CASE WHEN error_code=429 THEN 1 ELSE 0 END) as rate_limits,
                  SUM(input_tokens) as total_in, SUM(output_tokens) as total_out,
                  MIN(ts) as first_ts, MAX(ts) as last_ts
           FROM claude_profile_usage WHERE profile_name=? AND ts>=?""",
        (profile_name, cutoff),
    ).fetchone()
    if not row or not row[0]:
        return {
            "total": 0,
            "successes": 0,
            "rate_limits": 0,
            "tokens_in": 0,
            "tokens_out": 0,
            "burn_rate_per_hour": 0.0,
        }

    total, successes, rate_limits = row[0], row[1] or 0, row[2] or 0
    elapsed_hours = (
        max(1, (row[6] - row[5]) / 3600)
        if row[5] and row[6] and row[6] > row[5]
        else max(1, hours)
    )
    burn_rate = total / elapsed_hours

    return {
        "total": total,
        "successes": successes,
        "rate_limits": rate_limits,
        "tokens_in": row[3] or 0,
        "tokens_out": row[4] or 0,
        "burn_rate_per_hour": round(burn_rate, 2),
        "first_ts": row[5],
        "last_ts": row[6],
    }


def _sync_claude_profile_schedule(conn: sqlite3.Connection) -> None:
    """Sync Claude profile schedule from registry + auth directory into DB."""
    auth_dir = Path.home() / ".cli-proxy-api"

    for p in CLAUDE_PROFILES_REGISTRY:
        name = p["name"]
        email = p["email"]
        channel = p["channel"]

        # Check if auth file exists
        if channel == "claude":
            auth_file = auth_dir / f"claude-{email}.json"
        else:
            safe_email = email.replace("@", "_").replace(".", "_")
            auth_file = auth_dir / f"antigravity-{safe_email}.json"

        authed = auth_file.exists()

        conn.execute(
            """INSERT INTO claude_profile_schedule(
                   profile_name, email, context, plan, channel, enabled,
                   has_extra_usage, extra_usage_limit_gbp,
                   weekly_resets_at, last_sync_ts)
               VALUES(?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(profile_name) DO UPDATE SET
                   email=excluded.email, context=excluded.context,
                   plan=excluded.plan, channel=excluded.channel,
                   enabled=CASE WHEN claude_profile_schedule.auto_disabled_reason IS NOT NULL
                               THEN claude_profile_schedule.enabled
                               ELSE excluded.enabled END,
                   has_extra_usage=excluded.has_extra_usage,
                   extra_usage_limit_gbp=excluded.extra_usage_limit_gbp,
                   weekly_resets_at=excluded.weekly_resets_at,
                   last_sync_ts=excluded.last_sync_ts""",
            (
                name,
                email,
                p["context"],
                p["plan"],
                channel,
                1 if authed else 0,
                1 if p.get("has_extra_usage") else 0,
                p.get("extra_usage_limit_gbp", 0),
                p.get("weekly_resets_at", ""),
                int(time.time()),
            ),
        )
    conn.commit()


def _detect_claude_auth_files() -> list[dict[str, str]]:
    """Detect all Claude and Antigravity auth files in CLIProxyAPI auth directory."""
    auth_dir = Path.home() / ".cli-proxy-api"
    if not auth_dir.exists():
        return []

    results = []
    for f in sorted(auth_dir.iterdir()):
        if not f.suffix == ".json":
            continue
        try:
            data = json.loads(f.read_text())
            ftype = data.get("type", "")
            email = data.get("email", "")
            disabled = data.get("disabled", False)
            expired = data.get("expired", "")
            has_token = "access_token" in data or "token" in data

            if ftype in ("claude", "antigravity") and email:
                results.append(
                    {
                        "file": f.name,
                        "type": ftype,
                        "email": email,
                        "disabled": disabled,
                        "expired": str(expired),
                        "has_token": has_token,
                    }
                )
        except (json.JSONDecodeError, OSError):
            continue
    return results


def _check_claude_session_resets(conn: sqlite3.Connection) -> list[str]:
    """Check if any Claude profiles have had their session reset (5hr window).
    Decrements session_pct when enough time has passed."""
    now_ts = int(time.time())
    reset_profiles = []

    rows = conn.execute(
        "SELECT profile_name, session_resets_at, session_pct FROM claude_profile_schedule WHERE session_pct >= 100"
    ).fetchall()

    for row in rows:
        name, resets_at_str, _pct = row[0], row[1], row[2]
        if not resets_at_str:
            continue
        try:
            resets_at = int(resets_at_str)
            if now_ts >= resets_at:
                conn.execute(
                    "UPDATE claude_profile_schedule SET session_pct=0, session_resets_at=NULL WHERE profile_name=?",
                    (name,),
                )
                reset_profiles.append(name)
        except (ValueError, TypeError):
            continue

    if reset_profiles:
        conn.commit()
    return reset_profiles


def _check_claude_weekly_resets(conn: sqlite3.Connection) -> list[str]:
    """Check if any Claude profiles have had their weekly reset.
    Re-enables auto-disabled profiles after weekly reset passes."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    reset_profiles = []

    rows = conn.execute(
        "SELECT profile_name, weekly_resets_at, auto_disabled_reason FROM claude_profile_schedule WHERE auto_disabled_reason IS NOT NULL"
    ).fetchall()

    for row in rows:
        name, reset_str, reason = row[0], row[1] or "", row[2]
        if not reset_str:
            continue
        # Parse day+time format like "Fri 10:00" — find next occurrence
        # This is a soft check; we rely on 429 detection for hard limits
        # Weekly reset clears the disabled state
        if "weekly_exhausted" in (reason or ""):
            # Estimate if enough time has passed since last disable
            # Simple heuristic: if profile was disabled >24h ago, it likely reset
            sched = conn.execute(
                "SELECT last_sync_ts FROM claude_profile_schedule WHERE profile_name=?",
                (name,),
            ).fetchone()
            if sched and (now_ts - (sched[0] or 0)) > 86400:
                conn.execute(
                    "UPDATE claude_profile_schedule SET weekly_all_pct=0, weekly_sonnet_pct=0, "
                    "total_requests_this_period=0, total_429s_this_period=0, "
                    "auto_disabled_reason=NULL, enabled=1 WHERE profile_name=?",
                    (name,),
                )
                reset_profiles.append(name)

    if reset_profiles:
        conn.commit()
    return reset_profiles


def _check_claude_exhaustion(conn: sqlite3.Connection) -> list[str]:
    """Check if any Claude profiles should be disabled due to high 429 rate."""
    disabled = []

    rows = conn.execute(
        "SELECT profile_name, plan, has_extra_usage, extra_usage_limit_gbp, extra_usage_spent_gbp "
        "FROM claude_profile_schedule WHERE enabled=1"
    ).fetchall()

    for row in rows:
        name, _plan, has_extra, limit_gbp, spent_gbp = (
            row[0],
            row[1],
            row[2],
            row[3] or 0,
            row[4] or 0,
        )

        # Check recent 429 rate
        stats = _get_claude_profile_stats(conn, name, hours=1)
        if stats["rate_limits"] >= 2:
            reason = f"429x{stats['rate_limits']} in last hour"

            # For profiles with extra usage, check if budget is maxed
            if has_extra and limit_gbp > 0 and spent_gbp >= limit_gbp:
                reason += f" + extra maxed (£{spent_gbp:.0f}/£{limit_gbp:.0f})"

            conn.execute(
                "UPDATE claude_profile_schedule SET enabled=0, auto_disabled_reason=? WHERE profile_name=?",
                (reason, name),
            )
            disabled.append(name)

    if disabled:
        conn.commit()
    return disabled


def run_claude_profile_scheduler(db_path: Path) -> dict[str, Any]:
    """Run one cycle of the Claude profile scheduler."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    _sync_claude_profile_schedule(conn)
    session_resets = _check_claude_session_resets(conn)
    weekly_resets = _check_claude_weekly_resets(conn)
    disabled = _check_claude_exhaustion(conn)

    conn.close()
    return {
        "session_resets": session_resets,
        "weekly_resets": weekly_resets,
        "disabled": disabled,
    }


def _claude_profile_recommendation(conn: sqlite3.Connection) -> str:
    """Return the best Claude profile to use right now based on usage state."""
    rows = conn.execute(
        """SELECT profile_name, plan, channel, session_pct, weekly_all_pct,
                  has_extra_usage, extra_usage_spent_gbp, extra_usage_limit_gbp,
                  auto_disabled_reason, enabled
           FROM claude_profile_schedule
           WHERE enabled=1 AND auto_disabled_reason IS NULL
           ORDER BY
               CASE channel WHEN 'claude' THEN 0 ELSE 1 END,
               session_pct ASC,
               weekly_all_pct ASC"""
    ).fetchall()

    if not rows:
        return "(all profiles exhausted)"

    best = rows[0]
    name = best[0]
    plan = best[1]
    session = best[3] or 0
    weekly = best[4] or 0

    return f"{name} ({plan}, session={session}%, weekly={weekly}%)"


def cmd_claude_profiles(args: argparse.Namespace) -> int:
    """Show Claude profile status, usage, and recommendations."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    subcmd = getattr(args, "claude_profiles_cmd", "status")

    if subcmd == "check":
        result = run_claude_profile_scheduler(paths["db_path"])
        if result["session_resets"]:
            print(f"Session resets: {', '.join(result['session_resets'])}")
        if result["weekly_resets"]:
            print(f"Weekly resets: {', '.join(result['weekly_resets'])}")
        if result["disabled"]:
            print(f"Auto-disabled: {', '.join(result['disabled'])}")
        if not any(result.values()):
            print("No profile changes needed.")
        conn.close()
        return 0

    if subcmd == "auth":
        # Show auth file status
        auths = _detect_claude_auth_files()
        print(f"{'file':<50} {'type':<12} {'email':<30} {'token':>5} {'disabled':>8}")
        print("-" * 115)
        for a in auths:
            print(
                f"{a['file']:<50} {a['type']:<12} {a['email']:<30} {'YES' if a['has_token'] else 'NO':>5} {'YES' if a['disabled'] else 'no':>8}"
            )
        return 0

    if subcmd == "recommend":
        _sync_claude_profile_schedule(conn)
        rec = _claude_profile_recommendation(conn)
        print(f"Recommended: {rec}")
        conn.close()
        return 0

    if subcmd == "sync":
        # Manual usage sync — user provides percentages
        profile = getattr(args, "profile_name", "")
        session_pct = getattr(args, "session_pct", None)
        weekly_pct = getattr(args, "weekly_pct", None)
        sonnet_pct = getattr(args, "sonnet_pct", None)
        extra_spent = getattr(args, "extra_spent", None)

        if not profile:
            print(
                "Usage: ai-fleet claude-profiles sync <name> [--session N] [--weekly N] [--sonnet N] [--extra-spent N]"
            )
            conn.close()
            return 1

        updates = []
        params: list[Any] = []
        if session_pct is not None:
            updates.append("session_pct=?")
            params.append(session_pct)
            if session_pct >= 100:
                updates.append("session_resets_at=?")
                params.append(str(int(time.time()) + 5 * 3600))  # 5hr from now
        if weekly_pct is not None:
            updates.append("weekly_all_pct=?")
            params.append(weekly_pct)
        if sonnet_pct is not None:
            updates.append("weekly_sonnet_pct=?")
            params.append(sonnet_pct)
        if extra_spent is not None:
            updates.append("extra_usage_spent_gbp=?")
            params.append(extra_spent)

        if updates:
            params.append(profile)
            conn.execute(
                f"UPDATE claude_profile_schedule SET {', '.join(updates)} WHERE profile_name=?",
                params,
            )
            conn.commit()
            print(f"Updated {profile}")
        conn.close()
        return 0

    # Default: status
    _sync_claude_profile_schedule(conn)

    print(
        f"{'name':<24} {'plan':<8} {'ch':<5} {'on?':>3} {'sess%':>5} {'wk%':>4} {'son%':>4} {'reqs':>5} {'429s':>4} {'burn/h':>6} {'extra':>12} {'reset':>12} {'reason'}"
    )
    print("-" * 140)

    for p in CLAUDE_PROFILES_REGISTRY:
        name = p["name"]
        sched = conn.execute(
            "SELECT enabled, session_pct, weekly_all_pct, weekly_sonnet_pct, "
            "has_extra_usage, extra_usage_limit_gbp, extra_usage_spent_gbp, "
            "weekly_resets_at, auto_disabled_reason "
            "FROM claude_profile_schedule WHERE profile_name=?",
            (name,),
        ).fetchone()

        if not sched:
            continue

        enabled = "YES" if sched[0] else "NO"
        session_pct = sched[1] or 0
        weekly_pct = sched[2] or 0
        sonnet_pct = sched[3] or 0
        has_extra = sched[4]
        extra_limit = sched[5] or 0
        extra_spent = sched[6] or 0
        reset_str = sched[7] or ""
        reason = str(sched[8] or "")[:25]

        stats = _get_claude_profile_stats(conn, name, hours=168)

        extra_str = f"£{extra_spent:.0f}/£{extra_limit:.0f}" if has_extra else "-"
        print(
            f"{name:<24} {p['plan']:<8} {p['channel'][:4]:<5} {enabled:>3} {session_pct:>5} {weekly_pct:>4} {sonnet_pct:>4} {stats['total']:>5} {stats['rate_limits']:>4} {stats['burn_rate_per_hour']:>6.1f} {extra_str:>12} {reset_str:>12} {reason}"
        )

    # Show recommendation
    rec = _claude_profile_recommendation(conn)
    print(f"\nrecommended: {rec}")

    conn.close()
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    """Monitor daemon management."""
    from coordinator.monitor import (
        start_monitor,
        stop_monitor,
        is_running,
        get_recent_events,
    )

    cfg = load_config()
    paths = ensure_state(cfg)
    monitor_cfg = cfg.get("monitor", {})
    subcmd = getattr(args, "monitor_cmd", "status")

    if subcmd == "start":
        if is_running():
            print("Monitor already running.")
            return 0
        if not monitor_cfg.get("projects"):
            print(
                "No projects configured. Add to monitor.projects in coordinator.json."
            )
            return 1
        ok = start_monitor(paths["db_path"], monitor_cfg)
        if ok:
            projects = monitor_cfg.get("projects", [])
            print(
                f"Monitor started: {len(projects)} projects, interval={monitor_cfg.get('interval_seconds', 300)}s"
            )
            for p in projects:
                print(f"  - {p.get('github', p.get('repo', '?'))}")
        else:
            print("Failed to start monitor.")
        return 0 if ok else 1

    if subcmd == "stop":
        ok = stop_monitor()
        print("Monitor stopped." if ok else "Monitor was not running.")
        return 0

    if subcmd == "projects":
        projects = monitor_cfg.get("projects", [])
        if not projects:
            print("No projects configured.")
            return 0
        print(f"{'repo':<40} {'github':<30} {'sentry':<20}")
        print(f"{'----':<40} {'------':<30} {'------':<20}")
        for p in projects:
            print(
                f"{p.get('repo', '-'):<40} {p.get('github', '-'):<30} {p.get('sentry_project', '-'):<20}"
            )
        return 0

    # status (default)
    running = is_running()
    print(f"monitor_running: {running}")
    print(f"projects_configured: {len(monitor_cfg.get('projects', []))}")
    print(f"interval: {monitor_cfg.get('interval_seconds', 300)}s")
    print(f"auto_fix: {monitor_cfg.get('auto_fix_severity', ['critical'])}")
    print(f"notify: {monitor_cfg.get('notify_severity', ['critical', 'warn'])}")

    events = get_recent_events(paths["db_path"], limit=10)
    if events:
        print(f"\nrecent_events ({len(events)}):")
        for e in events:
            ts_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(e.get("ts", 0)))
            print(
                f"  [{ts_str}] [{e.get('severity', '?')}] {e.get('project', '?')}: {e.get('title', '?')}"
            )
    else:
        print("\nNo recent events.")
    return 0


def cmd_maintain(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    repo = _repo_root(args.repo)

    rules = ensure_rules_check(conn, repo, cfg, force=args.force_rules)
    snap_path = _write_repo_snapshot(conn, paths, repo, rules)

    # light maintenance
    conn.execute("VACUUM")
    conn.execute("PRAGMA optimize")
    conn.commit()
    conn.close()

    print(f"snapshot={snap_path}")
    print(f"rules_checked={rules['checked']}")
    print(f"rules_drift={rules['drift']}")
    return 0


def cmd_explain(args: argparse.Namespace) -> int:
    """Dry-run routing: show tier/via/model that would be chosen without executing."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    objective = args.objective

    tier, tier_reason = _classify_tier_with_reason(objective, cfg)
    via, via_reason, alternatives = _pick_via_with_reason(
        tier,
        cfg,
        explicit_via=getattr(args, "via", "auto"),
        objective=objective,
        conn=conn,
        repo=_repo_root(getattr(args, "repo", ".")),
    )
    model, model_reason = _pick_model_with_reason(
        tier, via, cfg, explicit_model=getattr(args, "model", "auto")
    )

    # Cost estimate
    cost_table = cfg.get("cost_estimation", {})
    model_base = model.split("/")[-1] if "/" in model else model
    pricing = cost_table.get(model_base, {})
    cost_note = (
        f"${pricing.get('input_per_m', '?')}/M in, ${pricing.get('output_per_m', '?')}/M out"
        if pricing
        else "no pricing data"
    )

    if getattr(args, "json", False):
        print(
            json.dumps(
                {
                    "tier": tier,
                    "tier_reason": tier_reason,
                    "via": via,
                    "via_reason": via_reason,
                    "model": model,
                    "model_reason": model_reason,
                    "cost_estimate": cost_note,
                    "alternatives": alternatives,
                },
                indent=2,
            )
        )
    else:
        print(f"objective: {objective[:120]}{'...' if len(objective) > 120 else ''}")
        print(f"tier={tier} reason={tier_reason}")
        print(f"via={via} reason={via_reason}")
        print(f"model={model} reason={model_reason}")
        print(f"cost_estimate: {cost_note}")
        if alternatives:
            print(f"alternatives: {json.dumps(alternatives)}")
    conn.close()
    return 0


def cmd_decisions(args: argparse.Namespace) -> int:
    """Browse routing decision history."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))

    where_clauses = ["1=1"]
    params: list[Any] = []

    if getattr(args, "tier", None):
        where_clauses.append("tier = ?")
        params.append(args.tier)
    if getattr(args, "via", None):
        where_clauses.append("via = ?")
        params.append(args.via)
    if getattr(args, "today", False):
        start_of_day = int(
            time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
        )
        where_clauses.append("timestamp >= ?")
        params.append(start_of_day)

    limit = getattr(args, "limit", 20)
    where = " AND ".join(where_clauses)
    rows = conn.execute(
        f"""SELECT run_id, timestamp, objective_hash, objective_length,
                   tier, tier_reason, via, via_reason, model, model_reason,
                   budget_check, alternatives_json
            FROM decisions
            WHERE {where}
            ORDER BY timestamp DESC LIMIT ?""",
        params + [limit],
    ).fetchall()
    conn.close()

    if getattr(args, "json", False):
        print(
            json.dumps(
                [
                    {
                        "run_id": r[0],
                        "timestamp": r[1],
                        "objective_hash": r[2],
                        "objective_length": r[3],
                        "tier": r[4],
                        "tier_reason": r[5],
                        "via": r[6],
                        "via_reason": r[7],
                        "model": r[8],
                        "model_reason": r[9],
                        "budget_check": r[10],
                        "alternatives": json.loads(r[11] or "[]"),
                    }
                    for r in rows
                ],
                indent=2,
            )
        )
    else:
        for r in rows:
            ts_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(r[1]))
            print(f"[{ts_str}] run={r[0] or '-'} tier={r[4]} via={r[6]} model={r[8]}")
            print(f"  tier_reason={r[5]}")
            print(f"  via_reason={r[7]}")
            print(f"  model_reason={r[9]}")
            if r[10]:
                print(f"  budget_check={r[10]}")
            try:
                alts = json.loads(r[11] or "[]")
            except Exception:
                alts = []
            if alts:
                print(f"  alternatives={json.dumps(alts)}")
        if not rows:
            print("No decisions recorded yet.")
    return 0


def cmd_auto(args: argparse.Namespace) -> int:
    """Toggle full_auto mode for autonomous execution."""
    cfg = load_config()
    auto_cmd = getattr(args, "auto_cmd", "status")

    if auto_cmd == "on":
        cfg.setdefault("autonomy", {})["full_auto"] = True
        _write_json(USER_CONFIG_PATH, cfg)
        print("full_auto=ON")
        print("All checkpoint gates bypassed. Premium tier tasks will auto-execute.")
        if cfg.get("autonomy", {}).get("full_auto_notify", True):
            print("Telegram notifications enabled for auto-approved runs.")
        return 0

    if auto_cmd == "off":
        cfg.setdefault("autonomy", {})["full_auto"] = False
        _write_json(USER_CONFIG_PATH, cfg)
        print("full_auto=OFF")
        print("Checkpoint gates restored. Premium tier requires --approve.")
        return 0

    # status
    autonomy = cfg.get("autonomy", {})
    full_auto = autonomy.get("full_auto", False)
    auto_tiers = autonomy.get("auto_approve_tiers", [])
    notify = autonomy.get("full_auto_notify", True)
    checkpoint = autonomy.get("checkpoint_required", True)

    print(f"full_auto: {'ON' if full_auto else 'OFF'}")
    print(f"checkpoint_required: {checkpoint}")
    print(f"auto_approve_tiers: {auto_tiers or 'none'}")
    print(f"full_auto_notify: {notify}")
    if full_auto:
        print("\nMode: FULL AUTONOMY — all tasks execute without gates")
    elif auto_tiers:
        gated = [t for t in ["simple", "coding", "premium"] if t not in auto_tiers]
        print(
            f"\nMode: SELECTIVE — {auto_tiers} auto-approve, {gated or 'none'} require gate"
        )
    else:
        print("\nMode: GATED — all autonomous tasks require checkpoint approval")
    return 0


def cmd_swarm_kill(args: argparse.Namespace) -> int:
    """Kill a running swarm by run_id."""
    run_id = args.run_id
    if run_id not in ACTIVE_RUNS:
        print(f"run_id={run_id} not found in active runs")
        print(f"active_runs: {list(ACTIVE_RUNS.keys()) or 'none'}")
        return 1

    info = ACTIVE_RUNS.pop(run_id)
    pid = info.get("pid")
    print(f"killed run_id={run_id} pid={pid}")
    return 0


def cmd_runs(args: argparse.Namespace) -> int:
    """Browse run history with filters."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))

    where_parts = ["1=1"]
    params = []

    if getattr(args, "failed", False):
        where_parts.append("status IN ('failed', 'timeout', 'error')")
    if getattr(args, "via_filter", None):
        where_parts.append("via = ?")
        params.append(args.via_filter)
    if getattr(args, "tier_filter", None):
        where_parts.append("tier = ?")
        params.append(args.tier_filter)

    ALLOWED_ORDERS = {
        "recent": "rh.ts DESC",
        "expensive": "COALESCE(rh.cost_usd, cl.total_cost, 0) DESC",
    }
    order_key = "expensive" if getattr(args, "expensive", False) else "recent"
    order = ALLOWED_ORDERS[order_key]

    limit = getattr(args, "last", 20)
    where = " AND ".join(where_parts)

    rows = conn.execute(
        f"""SELECT rh.run_id, rh.ts, rh.repo, rh.objective, rh.via, rh.tier, rh.model, rh.status,
                   rh.duration_seconds, COALESCE(rh.cost_usd, cl.total_cost, 0) as display_cost, rh.output_path
            FROM run_history rh
            LEFT JOIN (SELECT run_id, SUM(estimated_cost_usd) as total_cost FROM cost_log GROUP BY run_id) cl
              ON rh.run_id = cl.run_id
            WHERE {where} ORDER BY {order} LIMIT ?""",
        params + [limit],
    ).fetchall()
    conn.close()

    if getattr(args, "json", False):
        print(
            json.dumps(
                [
                    {
                        "run_id": r[0],
                        "ts": r[1],
                        "repo": r[2],
                        "objective": r[3][:120] if r[3] else "",
                        "via": r[4],
                        "tier": r[5],
                        "model": r[6],
                        "status": r[7],
                        "duration_seconds": r[8],
                        "cost_usd": r[9],
                        "output_path": r[10],
                    }
                    for r in rows
                ],
                indent=2,
            )
        )
    else:
        for r in rows:
            ts_str = (
                time.strftime("%Y-%m-%d %H:%M", time.localtime(r[1])) if r[1] else "?"
            )
            run_id = r[0] or "-"
            status = r[7] or "?"
            via = r[4] or "?"
            cost = f"${r[9]:.4f}" if r[9] else "-"
            duration = f"{r[8]:.1f}s" if r[8] else "-"
            obj_short = (r[3] or "")[:80]
            print(f"[{ts_str}] {run_id} {status} via={via} cost={cost} dur={duration}")
            print(f"  {obj_short}")
        if not rows:
            print("No runs found.")
    return 0


def cmd_run_detail(args: argparse.Namespace) -> int:
    """Show full detail for a specific run."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))

    row = conn.execute(
        "SELECT run_id, ts, repo, objective, via, tier, model, status, checkpoint_gate, output_path, notes, duration_seconds, exit_code, input_tokens, output_tokens, cost_usd FROM run_history WHERE run_id = ?",
        (args.run_id,),
    ).fetchone()

    if not row:
        print(f"Run {args.run_id} not found")
        conn.close()
        return 1

    conn.close()

    if getattr(args, "json", False):
        print(
            json.dumps(
                {
                    "run_id": row[0],
                    "ts": row[1],
                    "repo": row[2],
                    "objective": row[3],
                    "via": row[4],
                    "tier": row[5],
                    "model": row[6],
                    "status": row[7],
                    "checkpoint_gate": row[8],
                    "output_path": row[9],
                    "notes": row[10],
                    "duration_seconds": row[11],
                    "exit_code": row[12],
                    "input_tokens": row[13],
                    "output_tokens": row[14],
                    "cost_usd": row[15],
                },
                indent=2,
            )
        )
    else:
        ts_str = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[1]))
            if row[1]
            else "?"
        )
        print(f"run_id: {row[0]}")
        print(f"timestamp: {ts_str}")
        print(f"repo: {row[2]}")
        print(f"objective: {row[3]}")
        print(f"via: {row[4]}  tier: {row[5]}  model: {row[6]}")
        print(f"status: {row[7]}  exit_code: {row[12]}")
        print(
            f"duration: {row[11]}s  tokens: {row[13]}in/{row[14]}out  cost: ${row[15] or 0:.4f}"
        )
        if row[10]:
            print(f"notes: {row[10]}")
        if row[9]:
            output_file = Path(row[9])
            if output_file.exists():
                content = output_file.read_text()[:4000]
                print("\n--- output (first 4K) ---")
                print(content)
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    """Re-execute a previous run with optionally different via/model."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))

    row = conn.execute(
        "SELECT objective, repo, via, model FROM run_history WHERE run_id = ?",
        (args.run_id,),
    ).fetchone()
    conn.close()

    if not row:
        print(f"Run {args.run_id} not found")
        return 1

    objective, repo, orig_via, orig_model = row
    via = args.via if args.via != "auto" else orig_via
    model = args.model if args.model != "auto" else "auto"

    print(f"Replaying run {args.run_id}")
    print(f"  objective: {objective[:120]}")
    print(f"  via: {orig_via} -> {via}")
    if model != "auto":
        print(f"  model: {orig_model} -> {model}")

    # Build exec args namespace
    import argparse as _ap

    exec_args = _ap.Namespace(
        repo=repo or ".",
        objective=objective,
        via=via,
        model=model,
        autonomous=False,
        approve=False,
        checkpoint="",
        tier=None,
        production=False,
        swarm=False,
        timeout=900,
    )
    return cmd_exec(exec_args)


def cmd_budget(args: argparse.Namespace) -> int:
    """Budget cap management commands."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))
    conn.row_factory = sqlite3.Row
    month = _current_month()

    subcmd = args.budget_cmd

    if subcmd == "show":
        budget = _get_or_create_budget(conn, month)
        cap = budget["cap_usd"]
        spent = budget["spent_usd"]
        remaining = max(0.0, cap - spent)
        pct = (spent / cap * 100) if cap > 0 else 0.0

        if hasattr(args, "json") and args.json:
            print(json.dumps(dict(budget), indent=2))
        else:
            print(f"Budget for {month}")
            print(f"  Cap:       ${cap:.2f}")
            print(f"  Spent:     ${spent:.4f}")
            print(f"  Remaining: ${remaining:.4f}")
            print(f"  Used:      {pct:.1f}%")
            alerts = []
            for level in [70, 80, 90, 100]:
                if budget[f"alert_{level}_sent"]:
                    alerts.append(f"{level}%")
            if alerts:
                print(f"  Alerts sent: {', '.join(alerts)}")

    elif subcmd == "set-cap":
        amount = args.amount
        conn.execute(
            "INSERT INTO budget_state(month, cap_usd, last_updated) VALUES(?,?,?) "
            "ON CONFLICT(month) DO UPDATE SET cap_usd=?, last_updated=?",
            (month, amount, _now_ts(), amount, _now_ts()),
        )
        conn.commit()
        print(f"Budget cap for {month} set to ${amount:.2f}")

    elif subcmd == "history":
        rows = conn.execute(
            "SELECT * FROM budget_state ORDER BY month DESC LIMIT 6"
        ).fetchall()
        if not rows:
            print("No budget history.")
        else:
            print(f"{'Month':<10} {'Cap':>10} {'Spent':>12} {'%Used':>8}")
            print("-" * 42)
            for r in rows:
                cap = r["cap_usd"]
                spent = r["spent_usd"]
                pct = (spent / cap * 100) if cap > 0 else 0.0
                print(f"{r['month']:<10} ${cap:>9.2f} ${spent:>11.4f} {pct:>7.1f}%")

    elif subcmd == "reset":
        conn.execute(
            "UPDATE budget_state SET alert_70_sent=0, alert_80_sent=0, alert_90_sent=0, alert_100_sent=0, last_updated=? WHERE month=?",
            (_now_ts(), month),
        )
        conn.commit()
        print(f"Alerts reset for {month}")

    conn.close()
    return 0


def cmd_budget_dashboard(args: argparse.Namespace) -> int:
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))
    conn.row_factory = sqlite3.Row
    month = _current_month()
    now = _now_ts()

    budget = _get_or_create_budget(conn, month)
    cap = budget["cap_usd"]
    spent = budget["spent_usd"]

    # Daily spend rate from cost_log
    week_ago = now - 7 * 86400
    week_rows = conn.execute(
        "SELECT SUM(estimated_cost_usd) as total, COUNT(*) as reqs FROM cost_log WHERE timestamp >= ?",
        (week_ago,),
    ).fetchone()
    week_total = (week_rows["total"] or 0.0) if week_rows else 0.0
    week_reqs = (week_rows["reqs"] or 0) if week_rows else 0
    daily_rate = week_total / 7.0

    # Days remaining in month
    import calendar

    year, mon = int(month[:4]), int(month[5:])
    days_in_month = calendar.monthrange(year, mon)[1]
    from datetime import datetime

    day_of_month = datetime.now().day
    days_left = max(1, days_in_month - day_of_month)

    # Projections
    projected_total = spent + (daily_rate * days_left)
    remaining = max(0.0, cap - spent)
    exhaustion_days = (remaining / daily_rate) if daily_rate > 0 else float("inf")

    if hasattr(args, "json") and args.json:
        print(
            json.dumps(
                {
                    "month": month,
                    "cap_usd": cap,
                    "spent_usd": spent,
                    "daily_rate_usd": round(daily_rate, 4),
                    "projected_total_usd": round(projected_total, 4),
                    "days_to_exhaustion": round(exhaustion_days, 1)
                    if exhaustion_days != float("inf")
                    else None,
                    "days_left_in_month": days_left,
                    "requests_last_7d": week_reqs,
                },
                indent=2,
            )
        )
        conn.close()
        return 0

    print(f"Budget Dashboard — {month}")
    print(f"  Cap:              ${cap:.2f}")
    print(f"  Spent:            ${spent:.4f}")
    print(f"  Daily burn rate:  ${daily_rate:.4f}/day (7-day avg)")
    print(f"  Requests (7d):    {week_reqs}")
    print()
    print(f"  Projected EOM:    ${projected_total:.4f}")
    if projected_total > cap and cap > 0:
        overage = projected_total - cap
        print(f"  ** OVER BUDGET by ${overage:.4f} at current rate **")
    elif cap > 0:
        headroom = cap - projected_total
        print(f"  Headroom:         ${headroom:.4f}")

    if exhaustion_days != float("inf"):
        print(f"  Days to exhaust:  {exhaustion_days:.1f}")
    else:
        print("  Days to exhaust:  N/A (no spend)")

    # Top cost models
    top_models = conn.execute(
        """
        SELECT model, SUM(estimated_cost_usd) as total
        FROM cost_log WHERE timestamp >= ?
        GROUP BY model ORDER BY total DESC LIMIT 5
    """,
        (week_ago,),
    ).fetchall()

    if top_models:
        print()
        print("  Top models (7d):")
        for r in top_models:
            print(f"    {r['model'] or 'unknown':<30} ${r['total'] or 0:.4f}")

    # Recommendations
    recommendations = []
    if daily_rate > 0 and projected_total > cap * 0.9:
        recommendations.append("Consider switching expensive stages to cheaper models")
    if week_reqs > 100 and daily_rate > cap / 30:
        recommendations.append("High request volume — batch operations where possible")
    if exhaustion_days < 7 and exhaustion_days != float("inf"):
        recommendations.append(
            f"Budget exhausts in ~{exhaustion_days:.0f} days — reduce usage or raise cap"
        )

    if recommendations:
        print()
        print("  Recommendations:")
        for r in recommendations:
            print(f"    - {r}")

    conn.close()
    return 0


def cmd_rate_limits(args: argparse.Namespace) -> int:
    """Show rate limit state for all profiles."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = sqlite3.connect(str(paths["db_path"]))
    conn.row_factory = sqlite3.Row

    conditions = []
    params = []
    if hasattr(args, "type") and args.type:
        conditions.append("profile_type = ?")
        params.append(args.type)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM rate_limit_state{where} ORDER BY profile_type, profile_name",
        params,
    ).fetchall()
    conn.close()

    if hasattr(args, "json") and args.json:
        print(json.dumps([dict(r) for r in rows], indent=2))
        return 0

    if not rows:
        print("No rate limit data recorded yet.")
        return 0

    now = _now_ts()
    print(
        f"{'Type':<8} {'Profile':<20} {'Remaining':>10} {'Limit':>8} {'Cooldown':>12} {'Last 429':>12}"
    )
    print("-" * 72)
    for r in rows:
        cooldown = ""
        if r["cooldown_until"] and r["cooldown_until"] > now:
            secs = r["cooldown_until"] - now
            cooldown = f"{secs}s left"
        elif r["cooldown_until"] and r["cooldown_until"] > 0:
            cooldown = "expired"

        last_429 = ""
        if r["last_429_ts"] and r["last_429_ts"] > 0:
            ago = now - r["last_429_ts"]
            if ago < 60:
                last_429 = f"{ago}s ago"
            elif ago < 3600:
                last_429 = f"{ago // 60}m ago"
            else:
                last_429 = f"{ago // 3600}h ago"

        remaining = (
            str(r["remaining_requests"]) if r["remaining_requests"] >= 0 else "-"
        )
        limit = str(r["limit_requests"]) if r["limit_requests"] >= 0 else "-"

        print(
            f"{r['profile_type']:<8} {r['profile_name']:<20} {remaining:>10} {limit:>8} {cooldown:>12} {last_429:>12}"
        )

    return 0


def cmd_config_validate(args: argparse.Namespace) -> int:
    """Validate both coordinator and gateway configs against schemas."""
    config_root = REPO_DEFAULT_CONFIG.parent.parent / "config"
    coordinator_schema = config_root / "coordinator.schema.json"
    gateway_schema = config_root / "gateway.schema.json"

    all_errors = []

    # Validate coordinator config
    print("Validating coordinator config...")
    coord_cfg = load_config(validate=False)
    coord_errors = _validate_config(coord_cfg, str(coordinator_schema))
    all_errors.extend([{"config": "coordinator", **e} for e in coord_errors])

    # Validate gateway config
    print("Validating gateway config...")
    gateway_path = Path.home() / ".ai-fleet" / "gateway.json"
    if gateway_path.exists():
        gw_cfg = _read_json(gateway_path, {})
        gw_errors = _validate_config(gw_cfg, str(gateway_schema))
        all_errors.extend([{"config": "gateway", **e} for e in gw_errors])
    else:
        print(f"  [SKIP] gateway config not found at {gateway_path}")

    # Print results
    if args.json:
        print(json.dumps({"errors": all_errors}, indent=2))
        return 1 if any(e["severity"] == "error" for e in all_errors) else 0

    errors = [e for e in all_errors if e["severity"] == "error"]
    warnings = [e for e in all_errors if e["severity"] == "warning"]

    if errors:
        print(f"\n{len(errors)} errors found:")
        for e in errors:
            print(f"  [{e['config']}] {e['path']}: {e['message']}")

    if warnings:
        print(f"\n{len(warnings)} warnings:")
        for w in warnings:
            print(f"  [{w['config']}] {w['message']}")

    if not errors and not warnings:
        print("\nAll configs valid.")
        return 0

    return 1 if errors else 0


def cmd_config_migrate(args: argparse.Namespace) -> int:
    """Add missing fields with defaults from schema."""
    print("Migrating coordinator config...")

    # Backup current config
    backup_path = Path(str(USER_CONFIG_PATH) + f".backup.{int(time.time())}")
    if USER_CONFIG_PATH.exists():
        import shutil

        shutil.copy2(USER_CONFIG_PATH, backup_path)
        print(f"Backup created: {backup_path}")

    # Load configs
    default_cfg = _read_json(REPO_DEFAULT_CONFIG, {})
    user_cfg = _read_json(USER_CONFIG_PATH, {})

    # Deep merge with defaults
    merged = _deep_merge(default_cfg, user_cfg)

    # Write merged config
    USER_CONFIG_PATH.write_text(json.dumps(merged, indent=2) + "\n")

    # Report changes
    changes = []

    def _find_new_keys(
        default: dict[str, Any], user: dict[str, Any], prefix: str = ""
    ) -> None:
        for key, val in default.items():
            path = f"{prefix}.{key}" if prefix else key
            if key not in user:
                changes.append((path, val))
            elif isinstance(val, dict) and isinstance(user.get(key), dict):
                _find_new_keys(val, user[key], path)

    _find_new_keys(default_cfg, user_cfg)

    if args.json:
        print(
            json.dumps({"backup": str(backup_path), "changes": len(changes)}, indent=2)
        )
        return 0

    if changes:
        print(f"\n{len(changes)} fields added:")
        for path, val in changes[:10]:  # limit output
            val_str = (
                json.dumps(val)
                if not isinstance(val, (str, int, float, bool))
                else str(val)
            )
            if len(val_str) > 60:
                val_str = val_str[:57] + "..."
            print(f"  {path} = {val_str}")
        if len(changes) > 10:
            print(f"  ... and {len(changes) - 10} more")
    else:
        print("No changes needed.")

    print(f"\nConfig written to: {USER_CONFIG_PATH}")
    return 0


def cmd_submit(args: argparse.Namespace) -> int:
    """Submit a task to the queue without blocking."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    task_id = _submit_task(
        conn,
        objective=args.objective,
        repo=args.repo,
        via=args.via,
        model=args.model,
        priority=args.priority,
        notes=args.notes,
    )

    print(f"Task submitted: {task_id}")
    print("  Status: pending")
    print(f"  Priority: {args.priority}")
    print(f"  Objective: {_truncate(args.objective, 120)}")

    conn.close()
    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    """Task queue management."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    subcmd = args.queue_cmd

    if subcmd == "list":
        conditions = []
        params = []
        if hasattr(args, "status") and args.status:
            conditions.append("status = ?")
            params.append(args.status)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        limit = getattr(args, "limit", 20)

        rows = conn.execute(
            f"SELECT * FROM task_queue{where} ORDER BY priority ASC, submitted_at DESC LIMIT ?",
            params + [limit],
        ).fetchall()

        if hasattr(args, "json") and args.json:
            print(json.dumps([dict(r) for r in rows], indent=2))
        else:
            if not rows:
                print("No tasks in queue.")
            else:
                for r in rows:
                    ts_str = time.strftime(
                        "%Y-%m-%d %H:%M", time.localtime(r["submitted_at"])
                    )
                    print(
                        f"[{ts_str}] {r['task_id']} priority={r['priority']} status={r['status']}"
                    )
                    print(f"  {_truncate(r['objective'], 80)}")

    elif subcmd == "cancel":
        cancelled = _cancel_task(conn, args.task_id)
        if cancelled:
            print(f"Task {args.task_id} cancelled.")
        else:
            print(f"Task {args.task_id} not found or not pending.")
            conn.close()
            return 1

    elif subcmd == "detail":
        row = conn.execute(
            "SELECT * FROM task_queue WHERE task_id = ?", (args.task_id,)
        ).fetchone()

        if not row:
            print(f"Task {args.task_id} not found.")
            conn.close()
            return 1

        if hasattr(args, "json") and args.json:
            print(json.dumps(dict(row), indent=2))
        else:
            print(f"Task: {row['task_id']}")
            print(f"  Status: {row['status']}")
            print(f"  Priority: {row['priority']}")
            print(f"  Objective: {row['objective']}")
            print(f"  Repo: {row['repo']}")
            print(f"  Via: {row['via']}")
            print(f"  Model: {row['model']}")
            print(
                f"  Submitted: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row['submitted_at']))}"
            )
            if row["started_at"]:
                print(
                    f"  Started: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row['started_at']))}"
                )
            if row["completed_at"]:
                print(
                    f"  Completed: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row['completed_at']))}"
                )
            if row["run_id"]:
                print(f"  Run ID: {row['run_id']}")
            if row["exit_code"] is not None:
                print(f"  Exit code: {row['exit_code']}")
            if row["output_path"]:
                print(f"  Output: {row['output_path']}")
            if row["error"]:
                print(f"  Error: {row['error']}")
            if row["outcome_details"]:
                print(f"  Outcome: {_truncate(str(row['outcome_details']), 200)}")
            if row["notes"]:
                print(f"  Notes: {row['notes']}")

    elif subcmd == "drain":
        result = conn.execute(
            "UPDATE task_queue SET status = 'cancelled', completed_at = ? WHERE status = 'pending'",
            (_now_ts(),),
        )
        conn.commit()
        count = result.rowcount
        print(f"Cancelled {count} pending tasks.")

    conn.close()
    return 0


def _execute_task_worker(task: dict, cfg: dict, db_path: Path) -> dict:
    """Execute a single task in a worker thread. Returns {task_id, exit_code, error}.

    Each worker owns its own DB connection. Heartbeat is updated before and after
    subprocess execution. The actual task runs as a subprocess for full isolation.
    """
    task_id = task["task_id"]
    conn = _open_db(db_path)

    def _update_heartbeat():
        try:
            conn.execute(
                "UPDATE task_queue SET heartbeat_ts = ? WHERE task_id = ?",
                (_now_ts(), task_id),
            )
            conn.commit()
        except Exception:
            pass  # Non-fatal

    _update_heartbeat()

    # Pre-flight git check: skip stale objectives
    should_exec, skip_reason = _preflight_git_check(task, conn)
    if not should_exec:
        _complete_task(conn, task_id, 0, "", "", f"skipped: {skip_reason}")
        _notify_event(
            conn,
            "task_skipped",
            f"Task {task_id} skipped: {skip_reason}",
            "info",
            task_id,
        )
        conn.close()
        return {"task_id": task_id, "exit_code": 0, "error": f"skipped: {skip_reason}"}

    # Session-aware dedup: skip tasks that overlap with recently completed work
    has_overlap, overlap_reason = _check_task_overlap(task, conn)
    if has_overlap:
        _complete_task(conn, task_id, 0, "", "", f"skipped (dedup): {overlap_reason}")
        _notify_event(
            conn,
            "task_dedup",
            f"Task {task_id} deduped: {overlap_reason}",
            "info",
            task_id,
        )
        conn.close()
        return {
            "task_id": task_id,
            "exit_code": 0,
            "error": f"skipped (dedup): {overlap_reason}",
        }

    # Build subprocess command
    coordinator_path = Path(__file__).resolve()
    cmd = [
        sys.executable,
        str(coordinator_path),
        "exec",
        "--objective",
        task["objective"],
        "--repo",
        task.get("repo") or ".",
        "--via",
        task.get("via") or "auto",
        "--model",
        task.get("model") or "auto",
        "--timeout",
        "900",
    ]

    exit_code = 1
    run_id = ""
    output_path = ""
    error_msg = ""
    stdout = ""
    stderr = ""

    proc = None
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # Poll subprocess and heartbeat every 30s
        last_heartbeat = time.time()
        while proc.poll() is None:
            if time.time() - last_heartbeat > 30:
                _update_heartbeat()
                last_heartbeat = time.time()
            time.sleep(1)

        stdout, stderr = proc.communicate(timeout=10)
        exit_code = proc.returncode

        # Try to get run_id from latest run_history
        last_run = conn.execute(
            "SELECT run_id, output_path FROM run_history ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if last_run:
            run_id = last_run["run_id"] or ""
            output_path = last_run["output_path"] or ""

    except subprocess.TimeoutExpired:
        if proc:
            proc.kill()
        try:
            if proc:
                stdout, stderr = proc.communicate(timeout=5)
        except Exception:
            pass  # best-effort capture
        exit_code = 124
        error_msg = "task timeout (900s)"
    except Exception as e:
        exit_code = 1
        error_msg = str(e)
    finally:
        if proc and proc.poll() is None:
            proc.kill()

    # Token usage alerts (parse subprocess output)
    _check_token_usage(stdout + stderr, task_id, cfg, conn)

    # Complete task with structured outcome details for auto-remediation
    outcome = {
        "task_id": task_id,
        "run_id": run_id,
        "status": "completed" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "output_path": output_path,
        "error": error_msg,
        "stdout_tail": _truncate(_ensure_str(stdout), 1200),
        "stderr_tail": _truncate(_ensure_str(stderr), 1200),
        "timestamp": _now_ts(),
    }
    _complete_task(
        conn,
        task_id,
        exit_code,
        run_id,
        output_path,
        error_msg,
        json.dumps(outcome, ensure_ascii=False),
    )

    # Auto-push if configured and task succeeded
    if exit_code == 0 and cfg.get("queue", {}).get("auto_push", False):
        repo_path = task.get("repo", "").strip()
        if repo_path and repo_path != ".":
            _auto_push(repo_path, task_id, conn)

    # Notify
    status_label = (
        "completed successfully" if exit_code == 0 else f"failed (exit={exit_code})"
    )
    severity = "info" if exit_code == 0 else "error"
    event_type = "task_complete" if exit_code == 0 else "task_failed"
    _notify_event(
        conn, event_type, f"Task {task_id} {status_label}", severity, task_id, run_id
    )

    conn.close()
    return {"task_id": task_id, "exit_code": exit_code, "error": error_msg}


def _cmd_process_queue_dispatch(args: argparse.Namespace) -> int:
    """Route to single or parallel queue processing based on --workers flag."""
    workers = getattr(args, "workers", 0)
    if (
        workers > 0
        or getattr(args, "drain", False)
        or getattr(args, "continuous", False)
    ):
        return cmd_process_queue_parallel(args)
    return cmd_process_queue(args)


def cmd_process_queue(args: argparse.Namespace) -> int:
    """Process next task in queue."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    task = _claim_next_task(conn)
    if not task:
        print("No pending tasks.")
        conn.close()
        return 0

    task_id = task["task_id"]
    print(f"Processing task: {task_id}")
    print(f"  Objective: {_truncate(task['objective'], 120)}")

    # Build exec args
    import argparse as _ap

    exec_args = _ap.Namespace(
        repo=task["repo"] or ".",
        objective=task["objective"],
        via=task["via"],
        model=task["model"],
        autonomous=False,
        approve=False,
        checkpoint="",
        tier=None,
        production=False,
        swarm=False,
        timeout=900,
    )

    # Execute task
    exit_code = 0
    run_id = ""
    output_path = ""
    error_msg = ""

    try:
        exit_code = cmd_exec(exec_args)
        # Try to get run_id from last run
        last_run = conn.execute(
            "SELECT run_id, output_path FROM run_history ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if last_run:
            run_id = last_run["run_id"] or ""
            output_path = last_run["output_path"] or ""
    except Exception as e:
        exit_code = 1
        error_msg = str(e)

    # Complete task with structured outcome details for auto-remediation
    outcome = {
        "task_id": task_id,
        "run_id": run_id,
        "status": "completed" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "output_path": output_path,
        "error": error_msg,
        "timestamp": _now_ts(),
    }
    _complete_task(
        conn,
        task_id,
        exit_code,
        run_id,
        output_path,
        error_msg,
        json.dumps(outcome, ensure_ascii=False),
    )

    # Notify
    if exit_code == 0:
        _notify_event(
            conn,
            "task_complete",
            f"Task {task_id} completed successfully",
            "info",
            task_id,
            run_id,
        )
    else:
        _notify_event(
            conn,
            "task_complete",
            f"Task {task_id} failed with exit code {exit_code}",
            "error",
            task_id,
            run_id,
        )

    conn.close()
    return exit_code


def cmd_process_queue_parallel(args: argparse.Namespace) -> int:
    """Process queue with parallel workers using ThreadPoolExecutor."""
    cfg = load_config()
    paths = ensure_state(cfg)
    db_path = paths["db_path"]
    max_workers = getattr(args, "workers", 0) or cfg.get("queue", {}).get(
        "max_parallel_workers", 4
    )
    drain = getattr(args, "drain", False)
    continuous = getattr(args, "continuous", False)

    # Graceful shutdown support for continuous mode
    _shutdown_requested = threading.Event()

    if continuous:

        def _signal_handler(sig, frame):
            print("\n[INFO] Shutdown signal received, stopping...", file=sys.stderr)
            _shutdown_requested.set()

        import signal as _signal

        _signal.signal(_signal.SIGINT, _signal_handler)
        _signal.signal(_signal.SIGTERM, _signal_handler)

    # In continuous mode, also start Telegram listener and monitor if configured
    if continuous:
        autonomy = cfg.get("autonomy", {})
        if not autonomy.get("full_auto", False):
            print(
                "[WARN] Continuous mode works best with full_auto=true. Run: ai-fleet auto on",
                file=sys.stderr,
            )
        from pipeline.telegram import start_command_listener

        tg_started = start_command_listener(db_path=str(db_path))
        if tg_started:
            print("[INFO] Telegram command listener started")

        monitor_cfg = cfg.get("monitor", {})
        if monitor_cfg.get("enabled", False) and monitor_cfg.get("projects"):
            from coordinator.monitor import start_monitor

            if start_monitor(db_path, monitor_cfg):
                print(
                    f"[INFO] Monitor started: {len(monitor_cfg['projects'])} projects"
                )

    conn = _open_db(db_path)

    # Requeue stale tasks at startup
    stale_timeout = cfg.get("queue", {}).get("stale_task_timeout", 14400)
    requeued = _requeue_stale_tasks(conn, stale_timeout)
    if requeued:
        print(f"[WARN] Requeued {requeued} stale tasks", file=sys.stderr)

    conn.close()

    completed = 0
    failed = 0
    poll_interval = cfg.get("queue", {}).get("poll_interval_seconds", 30)
    stale_sweep_interval = cfg.get("queue", {}).get("stale_sweep_interval_seconds", 300)
    last_stale_sweep = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}

        while not _shutdown_requested.is_set():
            # Continuous mode: periodically recover stale tasks without requiring a process restart.
            if (
                continuous
                and stale_sweep_interval > 0
                and (time.time() - last_stale_sweep) >= stale_sweep_interval
            ):
                sweep_conn = _open_db(db_path)
                try:
                    requeued = _requeue_stale_tasks(sweep_conn, stale_timeout)
                    if requeued:
                        print(
                            f"[WARN] Requeued {requeued} stale tasks (periodic sweep)",
                            file=sys.stderr,
                        )
                finally:
                    sweep_conn.close()
                last_stale_sweep = time.time()

            # Fill pool up to max_workers
            claim_conn = _open_db(db_path)
            while len(futures) < max_workers:
                task = _claim_next_task_with_budget(claim_conn)
                if not task:
                    break
                future = pool.submit(_execute_task_worker, task, cfg, db_path)
                futures[future] = task["task_id"]
                print(
                    f"[CLAIMED] {task['task_id']}: {_truncate(task['objective'], 80)}"
                )
            claim_conn.close()

            if not futures:
                if continuous and not _shutdown_requested.is_set():
                    # Run profile scheduler check each idle cycle
                    try:
                        gateway_path = Path.home() / ".ai-fleet" / "gateway.json"
                        if gateway_path.exists():
                            gw_cfg = json.loads(gateway_path.read_text())
                            sched_result = run_profile_scheduler(gw_cfg, db_path)
                            if sched_result["re_enabled"]:
                                for pname in sched_result["re_enabled"]:
                                    print(
                                        f"[PROFILE] Re-enabled {pname} (weekly reset passed)"
                                    )
                                    sched_conn = _open_db(db_path)
                                    _notify_event(
                                        sched_conn,
                                        "profile_enabled",
                                        f"Codex profile {pname} re-enabled after weekly reset",
                                        "info",
                                    )
                                    sched_conn.close()
                            if sched_result["disabled"]:
                                for pname in sched_result["disabled"]:
                                    print(
                                        f"[PROFILE] Auto-disabled {pname} (approaching limit)",
                                        file=sys.stderr,
                                    )
                                    sched_conn = _open_db(db_path)
                                    _notify_event(
                                        sched_conn,
                                        "profile_disabled",
                                        f"Codex profile {pname} auto-disabled: nearing usage limit",
                                        "warn",
                                    )
                                    sched_conn.close()
                    except Exception as e:
                        print(f"debug: profile scheduler error: {e}", file=sys.stderr)

                    # In continuous mode, wait and poll again
                    time.sleep(poll_interval)
                    continue
                elif drain:
                    print("Queue empty, drain complete.")
                else:
                    print("No pending tasks.")
                break

            # Wait for completions
            for future in as_completed(futures):
                task_id = futures.pop(future)
                try:
                    result = future.result()
                    exit_code = result.get("exit_code", 1)
                    if exit_code == 0:
                        completed += 1
                        print(f"[DONE] {task_id} exit=0")
                    else:
                        failed += 1
                        err = result.get("error", "")
                        print(
                            f"[FAIL] {task_id} exit={exit_code} {err}", file=sys.stderr
                        )
                        # Notify on failure in continuous mode
                        if continuous:
                            notify_conn = _open_db(db_path)
                            _notify_event(
                                notify_conn,
                                "task_failed",
                                f"Task {task_id} failed (exit={exit_code}): {err}",
                                "error",
                                task_id,
                            )
                            notify_conn.close()
                except Exception as e:
                    failed += 1
                    print(f"[FAIL] {task_id} error={e}", file=sys.stderr)

                # If draining or continuous, try to fill the slot with a new task
                if drain or continuous:
                    refill_conn = _open_db(db_path)
                    new_task = _claim_next_task_with_budget(refill_conn)
                    refill_conn.close()
                    if new_task:
                        new_future = pool.submit(
                            _execute_task_worker, new_task, cfg, db_path
                        )
                        futures[new_future] = new_task["task_id"]
                        print(
                            f"[CLAIMED] {new_task['task_id']}: {_truncate(new_task['objective'], 80)}"
                        )

            if not drain and not continuous:
                break

    if continuous:
        from pipeline.telegram import stop_command_listener
        from coordinator.monitor import stop_monitor

        stop_command_listener()
        stop_monitor()

    print(f"\nSummary: {completed} completed, {failed} failed")
    return 1 if failed > 0 and completed == 0 else 0


def cmd_notify(args: argparse.Namespace) -> int:
    """Notification channel management."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    subcmd = args.notify_cmd

    if subcmd == "channels":
        # Ensure default channels exist
        for ch in ["terminal", "slack", "telegram"]:
            conn.execute(
                "INSERT OR IGNORE INTO notification_channels(channel_type, enabled, config_json) VALUES(?,?,?)",
                (ch, 0, "{}"),
            )
        conn.commit()

        rows = conn.execute(
            "SELECT * FROM notification_channels ORDER BY channel_type"
        ).fetchall()

        if hasattr(args, "json") and args.json:
            print(json.dumps([dict(r) for r in rows], indent=2))
        else:
            print(f"{'Channel':<12} {'Enabled':<8} {'Last Delivery':<20}")
            print("-" * 42)
            for r in rows:
                enabled = "yes" if r["enabled"] else "no"
                last_del = ""
                if r["last_delivery"] and r["last_delivery"] > 0:
                    last_del = time.strftime(
                        "%Y-%m-%d %H:%M", time.localtime(r["last_delivery"])
                    )
                print(f"{r['channel_type']:<12} {enabled:<8} {last_del:<20}")

    elif subcmd == "enable":
        conn.execute(
            "INSERT OR REPLACE INTO notification_channels(channel_type, enabled, config_json) "
            "VALUES(?, 1, COALESCE((SELECT config_json FROM notification_channels WHERE channel_type = ?), '{}'))",
            (args.channel_type, args.channel_type),
        )
        conn.commit()
        print(f"Channel {args.channel_type} enabled.")

    elif subcmd == "disable":
        conn.execute(
            "UPDATE notification_channels SET enabled = 0 WHERE channel_type = ?",
            (args.channel_type,),
        )
        conn.commit()
        print(f"Channel {args.channel_type} disabled.")

    elif subcmd == "config":
        # Get existing config
        row = conn.execute(
            "SELECT config_json FROM notification_channels WHERE channel_type = ?",
            (args.channel_type,),
        ).fetchone()

        cfg_dict = {}
        if row:
            try:
                cfg_dict = json.loads(row["config_json"] or "{}")
            except Exception:
                pass

        cfg_dict[args.key] = args.value

        conn.execute(
            "INSERT OR REPLACE INTO notification_channels(channel_type, config_json, enabled) "
            "VALUES(?, ?, COALESCE((SELECT enabled FROM notification_channels WHERE channel_type = ?), 0))",
            (args.channel_type, json.dumps(cfg_dict), args.channel_type),
        )
        conn.commit()
        print(f"Config set: {args.channel_type}.{args.key} = {args.value}")

    elif subcmd == "test":
        # Ensure channel exists
        conn.execute(
            "INSERT OR IGNORE INTO notification_channels(channel_type, enabled, config_json) VALUES(?,?,?)",
            (args.channel_type, 0, "{}"),
        )
        conn.commit()

        notif_id = _create_notification(
            conn, "test", "Test notification from ai-coordinator", "info"
        )

        row = conn.execute(
            "SELECT * FROM notification_channels WHERE channel_type = ?",
            (args.channel_type,),
        ).fetchone()

        cfg_dict = {}
        try:
            cfg_dict = json.loads(row["config_json"] or "{}")
        except Exception:
            pass

        channels = [{"channel_type": args.channel_type, "config": cfg_dict}]
        success = _deliver_notification(conn, notif_id, channels)

        if success:
            print(f"Test notification sent to {args.channel_type}.")
        else:
            print(f"Failed to send test notification to {args.channel_type}.")

    conn.close()
    return 0


def cmd_notifications(args: argparse.Namespace) -> int:
    """Browse notification history."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])

    conditions = []
    params = []

    if hasattr(args, "event_type") and args.event_type:
        conditions.append("event_type = ?")
        params.append(args.event_type)

    if hasattr(args, "severity") and args.severity:
        conditions.append("severity = ?")
        params.append(args.severity)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    limit = getattr(args, "limit", 20)

    rows = conn.execute(
        f"SELECT * FROM notifications{where} ORDER BY created_at DESC LIMIT ?",
        params + [limit],
    ).fetchall()

    conn.close()

    if hasattr(args, "json") and args.json:
        print(json.dumps([dict(r) for r in rows], indent=2))
        return 0

    if not rows:
        print("No notifications found.")
        return 0

    print(f"{'Time':<20} {'Type':<20} {'Severity':<10} {'Status':<12} {'Message':<50}")
    print("-" * 112)
    for r in rows:
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["created_at"]))
        msg = _truncate(r["message"], 50)
        print(
            f"{ts_str:<20} {r['event_type']:<20} {r['severity']:<10} {r['delivery_status']:<12} {msg:<50}"
        )

    return 0


def cmd_fleetmax(args: argparse.Namespace) -> int:
    """Shortcut: create and run a fleetmax pipeline."""
    objective = " ".join(args.objective)
    title = args.title or f"Fleetmax: {objective[:80]}"
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        from pipeline.engine import create_pipeline, run_pipeline_to_completion

        pipeline_config: dict[str, Any] | None = None
        if args.config:
            try:
                pipeline_config = json.loads(args.config)
            except json.JSONDecodeError as e:
                print(f"error=invalid --config JSON: {e}", file=sys.stderr)
                return 1
        if pipeline_config is None:
            pipeline_config = {}

        repo_path = Path(args.repo).expanduser().resolve()
        auto_loop = _should_auto_use_experiment_loop(objective, cfg, pipeline_config)
        if auto_loop:
            loop_start = _start_experiment_loop(
                objective, repo_path, cfg, pipeline_config
            )
            launch = loop_start.get("launch", {})
            if launch.get("ok"):
                payload_json = launch.get("json", {}) or {}
                loop_id = str(payload_json.get("id", "")).strip()
                print("route=experiment_loop")
                print(f"experiment_loop_id={loop_id}")
                print(f"status_url={payload_json.get('status_url', '')}")
                if loop_id:
                    exp_cfg = _resolve_experiment_loop_cfg(cfg, pipeline_config)
                    timeout_seconds = int(exp_cfg.get("poll_timeout_seconds", 7200))
                    request_timeout_seconds = int(
                        exp_cfg.get("poll_request_timeout_seconds", 20)
                    )
                    result = _poll_experiment_loop(
                        loop_id,
                        timeout_seconds=timeout_seconds,
                        request_timeout_seconds=request_timeout_seconds,
                    )
                    print(json.dumps(result, indent=2, default=str))
                    return 0 if result.get("ok") else 1
                print("error=experiment loop launched without id", file=sys.stderr)
                return 1
            else:
                err = (
                    launch.get("error") or launch.get("raw") or "unknown launch failure"
                )
                print(
                    f"[WARN] experiment-loop auto-route failed, falling back to fleetmax pipeline: {err}",
                    file=sys.stderr,
                )

        pipeline_id = create_pipeline(
            conn, "fleetmax", args.repo, title, objective, config=pipeline_config
        )
        print(f"pipeline_id={pipeline_id}")
        print("type=fleetmax")
        print("status=created")
        result = run_pipeline_to_completion(conn, pipeline_id, cfg)
        print(json.dumps(result, indent=2, default=str))
        return 0
    except Exception as e:
        print(f"error={e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_pipeline_create(args: argparse.Namespace) -> int:
    """Create a new pipeline."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        from pipeline.engine import create_pipeline

        pipeline_config = None
        if args.config:
            try:
                pipeline_config = json.loads(args.config)
            except json.JSONDecodeError as e:
                print(f"error=invalid --config JSON: {e}", file=sys.stderr)
                return 1

        pipeline_id = create_pipeline(
            conn,
            args.pipeline_type,
            args.repo,
            args.title,
            args.objective,
            config=pipeline_config,
        )
        print(f"pipeline_id={pipeline_id}")
        print(f"type={args.pipeline_type}")
        print("status=created")
        return 0
    except Exception as e:
        print(f"error={e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_pipeline_list(args: argparse.Namespace) -> int:
    """List pipelines."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        from pipeline.engine import list_pipelines

        pipelines = list_pipelines(conn, status=args.status, limit=args.limit)
        if args.json:
            print(json.dumps(pipelines, indent=2))
        else:
            for p in pipelines:
                status_icon = {
                    "created": "⬜",
                    "running": "🔵",
                    "paused": "⏸️",
                    "completed": "✅",
                    "failed": "❌",
                    "cancelled": "⛔",
                }.get(p["status"], "❓")
                print(
                    f"{status_icon} {p['pipeline_id']}  {p['pipeline_type']:10s}  {p['status']:12s}  {p['title'][:50]}"
                )
            if not pipelines:
                print("No pipelines found.")
        return 0
    except Exception as e:
        print(f"error={e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_pipeline_status(args: argparse.Namespace) -> int:
    """Show pipeline status."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        from pipeline.engine import get_pipeline

        p = get_pipeline(conn, args.pipeline_id)
        if not p:
            print(f"pipeline {args.pipeline_id} not found", file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps(p, indent=2))
        else:
            print(f"pipeline_id={p['pipeline_id']}")
            print(f"type={p['pipeline_type']}")
            print(f"title={p['title']}")
            print(f"status={p['status']}")
            print(f"current_stage={p.get('current_stage', 'none')}")
            print(f"cycle_count={p['cycle_count']}")
            print(f"total_cost_usd={p['total_cost_usd']:.4f}")
            print("stages:")
            for s in p.get("stages", []):
                icon = {
                    "pending": "⬜",
                    "running": "🔵",
                    "waiting_human": "🟡",
                    "completed": "✅",
                    "failed": "❌",
                    "skipped": "⏭️",
                }.get(s["status"], "❓")
                print(
                    f"  {icon} {s['stage_name']:12s}  cycle={s['cycle']}  {s['status']}"
                )
        return 0
    except Exception as e:
        print(f"error={e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_pipeline_approve(args: argparse.Namespace) -> int:
    """Approve a pipeline gate."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        from pipeline.engine import approve_gate

        result = approve_gate(conn, args.pipeline_id, args.input or "")
        if result["ok"]:
            print(f"approved={result['stage']}")
            return 0
        else:
            print(f"error={result['error']}", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"error={e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_pipeline_reject(args: argparse.Namespace) -> int:
    """Reject a pipeline at a human gate."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        from pipeline.engine import reject_gate

        result = reject_gate(conn, args.pipeline_id, args.reason or "")
        if result["ok"]:
            print(f"rejected={result['stage']}")
            return 0
        else:
            print(f"error={result['error']}", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"error={e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_pipeline_cancel(args: argparse.Namespace) -> int:
    """Cancel a pipeline."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        from pipeline.engine import cancel_pipeline

        result = cancel_pipeline(conn, args.pipeline_id)
        if result["ok"]:
            print("status=cancelled")
            return 0
        else:
            print(f"error={result['error']}", file=sys.stderr)
            return 1
    except Exception as e:
        print(f"error={e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_pipeline_run(args: argparse.Namespace) -> int:
    """Run a pipeline to completion or next human gate."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        from pipeline.engine import run_pipeline_to_completion

        result = run_pipeline_to_completion(conn, args.pipeline_id, cfg)
        print(json.dumps(result, indent=2, default=str))
        return 0
    except Exception as e:
        print(f"error={e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_reflect(args: argparse.Namespace) -> int:
    """Run manual reflection over recent history."""
    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        import importlib.util as _ilu

        _reflect_path = Path(__file__).parent / "reflect.py"
        if not _reflect_path.exists():
            print("error: coordinator/reflect.py not found", file=sys.stderr)
            return 1
        _spec = _ilu.spec_from_file_location("reflect", _reflect_path)
        _mod = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        run_manual_reflect = _mod.run_manual_reflect
        hours = getattr(args, "hours", 24)
        repo_filter = getattr(args, "repo", "")
        if repo_filter == ".":
            repo_filter = str(_repo_root("."))
        dry_run = getattr(args, "dry_run", False)

        patterns = run_manual_reflect(
            conn, cfg, hours=hours, repo_filter=repo_filter, dry_run=dry_run
        )
        if not patterns:
            print("No patterns detected.")
            return 0

        print(
            f"{'[DRY RUN] ' if dry_run else ''}Detected {len(patterns)} pattern(s):\n"
        )
        for i, pat in enumerate(patterns, 1):
            skip = pat.get("skipped", "")
            status = f" (skipped: {skip})" if skip else ""
            print(
                f"  {i}. [{pat['category']}] {pat['insight']}  confidence={pat.get('confidence', 0):.2f}{status}"
            )
            if pat.get("data"):
                for k, v in pat["data"].items():
                    print(f"       {k}: {v}")
        return 0
    except Exception as exc:
        print(f"error={exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_goal(args: argparse.Namespace) -> int:
    """Handle 'goal' subcommand — submit a 'get this done' objective."""
    from pipeline.goal_gateway import GoalGateway, GoalRequest

    objective = " ".join(args.objective) if args.objective else ""
    if not objective:
        print("error=objective required", file=sys.stderr)
        return 1

    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        gateway = GoalGateway(conn, cfg)
        request = GoalRequest(
            objective=objective,
            repo_path=str(Path(args.repo).resolve()),
            deployment_mode=args.mode,
            autonomous=not args.no_autonomous,
            budget_limit_usd=args.budget,
            dry_run=args.dry_run,
        )
        status = gateway.submit(request)
        status_icon = {
            "planning": "📋",
            "executing": "🔵",
            "completed": "✅",
            "failed": "❌",
            "paused": "⏸️",
        }.get(status.status, "❓")
        print(f"{status_icon} goal_id={status.goal_id}")
        print(f"objective={status.objective[:100]}")
        print(f"status={status.status}")
        print(f"current_phase={status.current_phase}")
        print(f"tasks={status.tasks_completed}/{status.tasks_total}")
        print(f"waves={status.current_wave}/{status.total_waves}")
        print(
            f"cost=${status.cost_usd:.4f}  budget_remaining=${status.budget_remaining_usd:.2f}"
        )
        print(f"progress={status.progress_pct:.1f}%")
        if status.status == "failed" and status.evidence_summary.get("error"):
            print(f"error={status.evidence_summary['error']}")
        return 0 if status.status != "failed" else 1
    except Exception as exc:
        print(f"error={exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_goal_status(args: argparse.Namespace) -> int:
    """Handle 'goal-status' subcommand."""
    from pipeline.goal_gateway import GoalGateway

    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        gateway = GoalGateway(conn, cfg)
        status = gateway.get_status(args.goal_id)
        if args.json:
            import dataclasses

            print(json.dumps(dataclasses.asdict(status), indent=2, default=str))
        else:
            status_icon = {
                "planning": "📋",
                "executing": "🔵",
                "completed": "✅",
                "failed": "❌",
                "paused": "⏸️",
            }.get(status.status, "❓")
            print(f"{status_icon} goal_id={status.goal_id}")
            print(f"objective={status.objective[:100]}")
            print(f"status={status.status}")
            print(f"current_phase={status.current_phase}")
            print(f"progress={status.progress_pct:.1f}%")
            print(
                f"tasks={status.tasks_completed}/{status.tasks_total} (failed={status.tasks_failed})"
            )
            print(f"waves={status.current_wave}/{status.total_waves}")
            print(
                f"elapsed={status.elapsed_seconds:.0f}s  remaining≈{status.estimated_remaining_seconds:.0f}s"
            )
            print(
                f"cost=${status.cost_usd:.4f}  budget_remaining=${status.budget_remaining_usd:.2f}"
            )
            ev = status.evidence_summary
            if ev.get("total"):
                print(
                    f"evidence={ev['passed']}/{ev['total']} passed ({ev.get('pass_rate', 0):.0%})"
                )
        return 0
    except Exception as exc:
        print(f"error={exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_goal_list(args: argparse.Namespace) -> int:
    """Handle 'goal-list' subcommand."""
    from pipeline.goal_gateway import GoalGateway

    cfg = load_config()
    paths = ensure_state(cfg)
    conn = _open_db(paths["db_path"])
    try:
        gateway = GoalGateway(conn, cfg)
        goals = gateway.list_active()
        if args.json:
            import dataclasses

            print(
                json.dumps(
                    [dataclasses.asdict(g) for g in goals], indent=2, default=str
                )
            )
        else:
            if not goals:
                print("No active goals.")
                return 0
            for g in goals:
                status_icon = {
                    "planning": "📋",
                    "executing": "🔵",
                    "completed": "✅",
                    "failed": "❌",
                    "paused": "⏸️",
                }.get(g.status, "❓")
                print(
                    f"{status_icon} {g.goal_id}  {g.status:12s}  {g.progress_pct:5.1f}%  ${g.cost_usd:.4f}  {g.objective[:60]}"
                )
        return 0
    except Exception as exc:
        print(f"error={exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Local AI coordinator with gates, learning, and multi-tool orchestration"
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="Initialize coordinator config/state/database")
    sp.add_argument(
        "--force",
        action="store_true",
        help="Rewrite user config and global rules from defaults",
    )
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("status", help="Show coordinator + stack status")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser(
        "doctor",
        help="Run health checks — show what's working, what's missing, how to fix",
    )
    sp.set_defaults(func=cmd_doctor)

    sp = sub.add_parser(
        "setup",
        help="Interactive setup wizard — create configs, check services, guide auth",
    )
    sp.set_defaults(func=cmd_setup)

    auto_p = sub.add_parser("auto", help="Toggle full autonomy mode")
    auto_sub = auto_p.add_subparsers(dest="auto_cmd")
    auto_sub.add_parser("on", help="Enable full_auto — bypass all checkpoint gates")
    auto_sub.add_parser("off", help="Disable full_auto — restore checkpoint gates")
    auto_sub.add_parser("status", help="Show current autonomy level")
    auto_p.set_defaults(func=cmd_auto, auto_cmd="status")

    sp = sub.add_parser(
        "observe",
        help="Show observability snapshot (runs/failures/fallbacks/providers)",
    )
    sp.add_argument("--repo", default=".")
    sp.add_argument("--hours", type=int, default=24)
    sp.add_argument("--limit", type=int, default=10)
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_observe)

    sp = sub.add_parser("preflight", help="Run dependency/endpoint/env checks")
    sp.add_argument("--repo", default=".")
    sp.add_argument("--objective", default="")
    sp.add_argument("--production", action="store_true")
    sp.add_argument("--require-swarm", action="store_true")
    sp.add_argument(
        "--via", default="auto", choices=["auto", "claude", "codex", "droid", "gemini"]
    )
    sp.add_argument("--autonomous", action="store_true")
    sp.set_defaults(func=cmd_preflight)

    sp = sub.add_parser(
        "skills", help="Find best matching local skills for an objective"
    )
    sp.add_argument("--repo", default=".")
    sp.add_argument("--objective", required=True)
    sp.add_argument("--limit", type=int, default=8)
    sp.add_argument(
        "--with-subtasks",
        action="store_true",
        help="Also run skill matching for derived subtask queries",
    )
    sp.set_defaults(func=cmd_skills)

    sp = sub.add_parser(
        "plan", help="Compute route plan (tier/via/model) with preflight"
    )
    sp.add_argument("--repo", default=".")
    sp.add_argument("--objective", required=True)
    sp.add_argument(
        "--via", default="auto", choices=["auto", "claude", "codex", "droid", "gemini"]
    )
    sp.add_argument("--model", default="auto")
    sp.add_argument("--production", action="store_true")
    sp.set_defaults(func=cmd_plan)

    sp = sub.add_parser(
        "exec", help="Execute a task via Claude/Codex/Droid with learning + gates"
    )
    sp.add_argument("--repo", default=".")
    sp.add_argument("--objective", default="")
    sp.add_argument(
        "--via", default="auto", choices=["auto", "claude", "codex", "droid", "gemini"]
    )
    sp.add_argument("--model", default="auto")
    sp.add_argument(
        "--autonomous",
        action="store_true",
        help="Enable autonomous mode (checkpoint gate may apply)",
    )
    sp.add_argument(
        "--approve", action="store_true", help="Approve execution past checkpoint gate"
    )
    sp.add_argument(
        "--checkpoint", default="", help="Resume from a checkpoint JSON created earlier"
    )
    sp.add_argument(
        "--tier",
        default=None,
        choices=["simple", "coding", "premium"],
        help="Override auto-classified tier",
    )
    sp.add_argument("--production", action="store_true")
    sp.add_argument(
        "--swarm", action="store_true", help="When via=droid, use swarm model lane"
    )
    sp.add_argument(
        "--auto-pr",
        action="store_true",
        dest="auto_pr",
        help="Auto-create PR after successful run if on a feature branch",
    )
    sp.add_argument("--timeout", type=int, default=900)
    sp.set_defaults(func=cmd_exec)

    sp = sub.add_parser("swarm", help="Run swarm orchestration with visibility")
    sp.add_argument("--repo", default=".")
    sp.add_argument("--objective", required=True)
    sp.add_argument("--approve", action="store_true")
    sp.add_argument("--production", action="store_true")
    sp.add_argument(
        "--no-fallback",
        action="store_true",
        help="Disable direct fallback when swarm execution fails",
    )
    sp.set_defaults(func=cmd_swarm)

    sp = sub.add_parser("rules-check", help="Force/check rules drift state")
    sp.add_argument("--repo", default=".")
    sp.add_argument("--force", action="store_true")
    sp.set_defaults(func=cmd_rules_check)

    sp = sub.add_parser(
        "maintain", help="Run memory/rules maintenance and write repo snapshot"
    )
    sp.add_argument("--repo", default=".")
    sp.add_argument("--force-rules", action="store_true")
    sp.set_defaults(func=cmd_maintain)

    learn = sub.add_parser("learn", help="Manual learning memory operations")
    lsub = learn.add_subparsers(dest="learn_cmd", required=True)

    sp = lsub.add_parser("add", help="Add a global or repo learning")
    sp.add_argument("--scope", choices=["global", "repo"], required=True)
    sp.add_argument("--repo", default=".")
    sp.add_argument("--rule", default="")
    sp.add_argument("--rationale", default="")
    sp.add_argument("--pattern", default="")
    sp.add_argument("--fix", default="")
    sp.add_argument("--tags", default="")
    sp.add_argument("--source", default="manual")
    sp.add_argument("--confidence", type=float, default=None)
    sp.set_defaults(func=cmd_learn_add)

    sp = lsub.add_parser("list", help="List recent global or repo learnings")
    sp.add_argument("--scope", choices=["global", "repo"], required=True)
    sp.add_argument("--repo", default=".")
    sp.add_argument("--limit", type=int, default=20)
    sp.set_defaults(func=cmd_learn_list)

    sp = lsub.add_parser("query", help="Query learnings with decay-adjusted confidence")
    sp.add_argument("--scope", choices=["global", "repo"], required=True)
    sp.add_argument("--repo", default=".")
    sp.add_argument("--tags", default="", help="Comma-separated tags to filter")
    sp.add_argument("--source", default="", help="Filter by source")
    sp.add_argument("--min-confidence", type=float, default=0.0, dest="min_confidence")
    sp.add_argument("--limit", type=int, default=50)
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_learn_query)

    sp = lsub.add_parser(
        "compact", help="Remove stale learnings below confidence threshold"
    )
    sp.add_argument(
        "--threshold",
        type=float,
        default=0.15,
        help="Minimum effective confidence to keep",
    )
    sp.add_argument("--dry-run", action="store_true", dest="dry_run")
    sp.set_defaults(func=cmd_learn_compact)

    sp = lsub.add_parser("stats", help="Show pattern statistics from learning tables")
    sp.set_defaults(func=cmd_learn_stats)

    auth = sub.add_parser("auth", help="Auth/profile helper commands")
    asub = auth.add_subparsers(dest="auth_cmd", required=True)

    sp = asub.add_parser("list", help="List codex + gemini profiles")
    sp.set_defaults(func=cmd_auth_list)

    sp = asub.add_parser("detect", help="Detect codex profiles from ~/.codex* homes")
    sp.set_defaults(func=cmd_auth_detect)

    sp = asub.add_parser(
        "clone-gemini", help="Clone current gemini auth into a new profile home"
    )
    sp.add_argument("name")
    sp.add_argument("home")
    sp.add_argument("--model", default="")
    sp.add_argument("--disabled", action="store_true")
    sp.set_defaults(func=cmd_auth_clone_gemini)

    sp = asub.add_parser("test", help="Test codex/gemini profiles")
    sp.add_argument("--provider", choices=["all", "codex", "gemini"], default="all")
    sp.add_argument(
        "--name", default="", help="Optional profile name for provider-specific test"
    )
    sp.add_argument("--prompt", default="Reply exactly OK.")
    sp.set_defaults(func=cmd_auth_test)

    sp = asub.add_parser("add", help="Add codex/gemini profile")
    sp.add_argument("provider", choices=["codex", "gemini", "claude"])
    sp.add_argument("name")
    sp.add_argument("path", nargs="?", default="")
    sp.add_argument(
        "--login", action="store_true", help="For codex, run login immediately"
    )
    sp.add_argument("--url", default="", help="For claude provider, usage source URL")
    sp.add_argument(
        "--management-key",
        default="",
        help="For claude provider, management key for usage endpoint",
    )
    sp.add_argument(
        "--group", default="", help="Optional logical group tag (e.g. antigravity)"
    )
    sp.add_argument(
        "--standby", action="store_true", help="Mark claude source as standby metadata"
    )
    sp.add_argument(
        "--email", default="", help="Optional owner email metadata for claude source"
    )
    sp.add_argument(
        "--account-type",
        default="",
        choices=["", "org", "personal"],
        help="Optional claude account type metadata",
    )
    sp.set_defaults(func=cmd_auth_add)

    sp = asub.add_parser(
        "key-status", help="Show OpenRouter/Kimi/Minimax key readiness"
    )
    sp.set_defaults(func=cmd_auth_key_status)

    sp = asub.add_parser("key-set", help="Set provider key in local config")
    sp.add_argument("provider", choices=["openrouter", "kimi", "minimax"])
    sp.add_argument("key")
    sp.add_argument(
        "--enable",
        action="store_true",
        help="Enable provider when setting key (external providers only)",
    )
    sp.set_defaults(func=cmd_auth_key_set)

    sp = asub.add_parser("key-clear", help="Clear provider key from local config")
    sp.add_argument("provider", choices=["openrouter", "kimi", "minimax"])
    sp.add_argument(
        "--disable",
        action="store_true",
        help="Disable provider after key clear (external providers only)",
    )
    sp.set_defaults(func=cmd_auth_key_clear)

    sp = asub.add_parser("usage", help="Show per-profile usage statistics")
    sp.set_defaults(func=cmd_auth_usage)

    sp = asub.add_parser("auth-health", help="Show auth health status for all profiles")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_auth_health)

    sp = sub.add_parser(
        "explain", help="Dry-run routing: show tier/via/model without executing"
    )
    sp.add_argument("--objective", required=True)
    sp.add_argument(
        "--via", default="auto", choices=["auto", "claude", "codex", "droid", "gemini"]
    )
    sp.add_argument("--model", default="auto")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_explain)

    sp = sub.add_parser("decisions", help="Browse routing decision history")
    sp.add_argument("--tier", default=None, choices=["simple", "coding", "premium"])
    sp.add_argument("--via", default=None)
    sp.add_argument("--today", action="store_true")
    sp.add_argument("--limit", "--last", type=int, default=20, dest="limit")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_decisions)

    sp = sub.add_parser("swarm-kill", help="Kill a running swarm by run_id")
    sp.add_argument("run_id")
    sp.set_defaults(func=cmd_swarm_kill)

    sp = sub.add_parser("runs", help="Browse run history")
    sp.add_argument(
        "--last", type=int, default=20, help="Number of recent runs to show"
    )
    sp.add_argument("--failed", action="store_true", help="Show only failed runs")
    sp.add_argument("--expensive", action="store_true", help="Sort by cost descending")
    sp.add_argument("--via", default=None, dest="via_filter", help="Filter by provider")
    sp.add_argument(
        "--tier",
        default=None,
        dest="tier_filter",
        choices=["simple", "coding", "premium"],
    )
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_runs)

    sp = sub.add_parser("run", help="Show full detail for a specific run")
    sp.add_argument("run_id")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_run_detail)

    sp = sub.add_parser("replay", help="Re-execute a previous run")
    sp.add_argument("run_id")
    sp.add_argument(
        "--via", default="auto", choices=["auto", "claude", "codex", "droid", "gemini"]
    )
    sp.add_argument("--model", default="auto")
    sp.set_defaults(func=cmd_replay)

    sp = sub.add_parser("submit", help="Submit a task to the queue without blocking")
    sp.add_argument("objective")
    sp.add_argument("--repo", default=".")
    sp.add_argument(
        "--via", default="auto", choices=["auto", "claude", "codex", "droid", "gemini"]
    )
    sp.add_argument("--model", default="auto")
    sp.add_argument(
        "--priority",
        type=int,
        default=5,
        help="Task priority (lower = higher priority)",
    )
    sp.add_argument("--notes", default="", help="Optional notes for the task")
    sp.set_defaults(func=cmd_submit)

    queue_p = sub.add_parser("queue", help="Task queue management")
    queue_sub = queue_p.add_subparsers(dest="queue_cmd", required=True)

    sp = queue_sub.add_parser("list", help="List tasks in queue")
    sp.add_argument("--status", default="", help="Filter by status")
    sp.add_argument("--limit", type=int, default=20)
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_queue)

    sp = queue_sub.add_parser("detail", help="Show task detail")
    sp.add_argument("task_id")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_queue)

    sp = queue_sub.add_parser("cancel", help="Cancel a pending task")
    sp.add_argument("task_id")
    sp.set_defaults(func=cmd_queue)

    sp = queue_sub.add_parser("drain", help="Cancel all pending tasks")
    sp.set_defaults(func=cmd_queue)

    sp = sub.add_parser("process-queue", help="Process tasks from queue")
    sp.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of parallel workers (0 = single task, default)",
    )
    sp.add_argument(
        "--drain", action="store_true", help="Keep processing until queue is empty"
    )
    sp.add_argument(
        "--continuous",
        "--daemon",
        action="store_true",
        help="Run continuously (alias: --daemon) — poll for new tasks, enable TG listener and monitor",
    )
    sp.set_defaults(func=_cmd_process_queue_dispatch)

    cfg_p = sub.add_parser("config", help="Config validation and migration")
    cfg_sub = cfg_p.add_subparsers(dest="config_cmd", required=True)

    sp = cfg_sub.add_parser(
        "validate", help="Validate coordinator and gateway configs against schemas"
    )
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_config_validate)

    sp = cfg_sub.add_parser(
        "migrate", help="Add missing fields with defaults from schema"
    )
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_config_migrate)

    budget_p = sub.add_parser("budget", help="Budget cap management")
    budget_sub = budget_p.add_subparsers(dest="budget_cmd", required=True)

    sp = budget_sub.add_parser("show", help="Show current month budget")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_budget)

    sp = budget_sub.add_parser("set-cap", help="Set monthly budget cap in USD")
    sp.add_argument("amount", type=float, help="Monthly cap in USD")
    sp.set_defaults(func=cmd_budget)

    sp = budget_sub.add_parser("history", help="Show last 6 months of budget data")
    sp.set_defaults(func=cmd_budget)

    sp = budget_sub.add_parser("reset", help="Reset alerts for current month")
    sp.set_defaults(func=cmd_budget)

    sp = budget_sub.add_parser(
        "dashboard", help="Budget projections and recommendations"
    )
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_budget_dashboard)

    cost_p = sub.add_parser("cost", help="Cost tracking and breakdown")
    cost_p.add_argument("--today", action="store_true", help="Show today's costs")
    cost_p.add_argument("--week", action="store_true", help="Show this week's costs")
    cost_p.add_argument("--month", action="store_true", help="Show this month's costs")
    cost_p.add_argument("--since", help="Show costs since date (YYYY-MM-DD)")
    cost_p.add_argument("--by-model", action="store_true", help="Group by model")
    cost_p.add_argument("--by-provider", action="store_true", help="Group by provider")
    cost_p.add_argument("--by-tier", action="store_true", help="Group by tier")
    cost_p.add_argument("--json", action="store_true", help="Output as JSON")
    cost_p.set_defaults(func=cmd_cost)

    sp = sub.add_parser("rate-limits", help="Show rate limit state for all profiles")
    sp.add_argument(
        "--type", choices=["codex", "gemini", "claude"], help="Filter by profile type"
    )
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_rate_limits)

    # CLIProxyAPI usage sync
    proxy_sync_p = sub.add_parser(
        "proxy-sync", help="Sync CLIProxyAPI usage stats to cost_log and check budgets"
    )
    proxy_sync_p.add_argument("--json", action="store_true", help="Output as JSON")
    proxy_sync_p.set_defaults(func=cmd_proxy_sync)

    # Monitor commands
    monitor_p = sub.add_parser("monitor", help="Project monitor daemon")
    monitor_sub = monitor_p.add_subparsers(dest="monitor_cmd")
    monitor_sub.add_parser("start", help="Start background project monitor")
    monitor_sub.add_parser("stop", help="Stop project monitor")
    monitor_sub.add_parser("status", help="Show monitor status and recent events")
    monitor_sub.add_parser("projects", help="List monitored projects")
    monitor_p.set_defaults(func=cmd_monitor, monitor_cmd="status")

    # Codex profile management
    profiles_p = sub.add_parser(
        "profiles", help="Codex profile usage tracking and scheduling"
    )
    profiles_sub = profiles_p.add_subparsers(dest="profiles_cmd")
    profiles_sub.add_parser("status", help="Show all codex profiles with usage stats")
    profiles_sub.add_parser(
        "check", help="Run profile scheduler (re-enable resets, disable exhausted)"
    )
    profiles_p.set_defaults(func=cmd_codex_profiles, profiles_cmd="status")

    # Claude profile management
    cp = sub.add_parser(
        "claude-profiles", help="Claude profile usage tracking and scheduling"
    )
    csub = cp.add_subparsers(dest="claude_profiles_cmd")
    csub.add_parser("status", help="Show all Claude profiles with usage stats")
    csub.add_parser("check", help="Run Claude profile scheduler")
    csub.add_parser("auth", help="Show Claude auth file status from CLIProxyAPI")
    csub.add_parser("recommend", help="Show recommended profile to use right now")
    sync_p = csub.add_parser(
        "sync", help="Manually sync usage percentages for a profile"
    )
    sync_p.add_argument("profile_name", help="Profile name (e.g. primary-team)")
    sync_p.add_argument(
        "--session", dest="session_pct", type=int, help="Current session usage %%"
    )
    sync_p.add_argument(
        "--weekly", dest="weekly_pct", type=int, help="Weekly all-models usage %%"
    )
    sync_p.add_argument(
        "--sonnet", dest="sonnet_pct", type=int, help="Weekly Sonnet-only usage %%"
    )
    sync_p.add_argument(
        "--extra-spent", dest="extra_spent", type=float, help="Extra usage spent (GBP)"
    )
    cp.set_defaults(func=cmd_claude_profiles, claude_profiles_cmd="status")

    # Pipeline commands
    pipeline_p = sub.add_parser("pipeline", help="Pipeline management")
    pipeline_sub = pipeline_p.add_subparsers(dest="pipeline_cmd", required=True)

    sp = pipeline_sub.add_parser("create", help="Create a new pipeline")
    sp.add_argument(
        "--type",
        dest="pipeline_type",
        required=True,
        choices=["feature", "bugfix", "design", "refactor", "fleetmax"],
    )
    sp.add_argument("--repo", default=".")
    sp.add_argument("--title", required=True)
    sp.add_argument("--objective", required=True)
    sp.add_argument("--config", default="", help="Optional JSON config for pipeline")
    sp.set_defaults(func=cmd_pipeline_create)

    sp = pipeline_sub.add_parser("list", help="List pipelines")
    sp.add_argument("--status", default="")
    sp.add_argument("--limit", type=int, default=20)
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_pipeline_list)

    sp = pipeline_sub.add_parser("status", help="Show pipeline status")
    sp.add_argument("pipeline_id")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_pipeline_status)

    sp = pipeline_sub.add_parser("approve", help="Approve a pipeline gate")
    sp.add_argument("pipeline_id")
    sp.add_argument("--input", default="")
    sp.set_defaults(func=cmd_pipeline_approve)

    sp = pipeline_sub.add_parser("reject", help="Reject a pipeline at a human gate")
    sp.add_argument("pipeline_id")
    sp.add_argument("--reason", default="")
    sp.set_defaults(func=cmd_pipeline_reject)

    sp = pipeline_sub.add_parser("cancel", help="Cancel a pipeline")
    sp.add_argument("pipeline_id")
    sp.set_defaults(func=cmd_pipeline_cancel)

    sp = pipeline_sub.add_parser("run", help="Run pipeline to completion")
    sp.add_argument("pipeline_id")
    sp.set_defaults(func=cmd_pipeline_run)

    # Fleetmax shortcut (delegates to pipeline create --type fleetmax + run)
    fm = sub.add_parser("fleetmax", help="Shortcut: create and run a fleetmax pipeline")
    fm.add_argument("objective", nargs="+", help="Pipeline objective")
    fm.add_argument("--repo", default=".")
    fm.add_argument("--title", default="")
    fm.add_argument("--config", default="", help="Optional JSON config")
    fm.set_defaults(func=cmd_fleetmax)

    # Reflect command
    sp = sub.add_parser("reflect", help="Run self-reflection over recent runs")
    sp.add_argument("--hours", type=int, default=24, help="Look-back window in hours")
    sp.add_argument("--repo", default="", help="Filter by repo path")
    sp.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect patterns without recording or filing issues",
    )
    sp.set_defaults(func=cmd_reflect)

    # --- Goal commands ---
    sp = sub.add_parser("goal", help="Submit a 'get this done' goal")
    sp.add_argument("objective", nargs="*", help="What to accomplish")
    sp.add_argument("--repo", default=".", help="Repository path")
    sp.add_argument(
        "--mode", choices=["auto", "micro", "sprint", "full"], default="auto"
    )
    sp.add_argument("--budget", type=float, default=10.0, help="Budget limit in USD")
    sp.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Plan only, don't execute",
    )
    sp.add_argument("--no-autonomous", action="store_true", help="Require human gates")
    sp.set_defaults(func=cmd_goal)

    sp = sub.add_parser("goal-status", help="Check goal execution status")
    sp.add_argument("goal_id", help="Goal ID to check")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_goal_status)

    sp = sub.add_parser("goal-list", help="List active goals")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_goal_list)

    return p


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    # argument guards for learn add
    if args.cmd == "learn" and args.learn_cmd == "add":
        if args.scope == "global" and not args.rule.strip():
            parser.error("learn add --scope global requires --rule")
        if args.scope == "repo" and (not args.pattern.strip() or not args.fix.strip()):
            parser.error("learn add --scope repo requires --pattern and --fix")

    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
