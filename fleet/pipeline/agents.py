from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import threading
import time
from pathlib import Path

from pipeline.policy import evaluate_command_policy
from pipeline import claude_sdk_bridge
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from pipeline.exec_broker import ExecBroker

log = logging.getLogger("aifleet.agents")

# Module-level broker instance — shims installed lazily on first use
_exec_broker: ExecBroker | None = None
_exec_broker_initialized = False

# Cache for model availability checks (avoid hammering APIs)
_MODEL_AVAILABILITY_CACHE: dict[tuple[str, str], tuple[bool, float, float]] = {}
_CACHE_TTL = 60  # seconds
_MODEL_PROBE_MAX_ATTEMPTS = 3
_MODEL_PROBE_BASE_DELAY_SECONDS = 0.2
_GATEWAY_TIMEOUT_WINDOW_SECONDS = 300
_GATEWAY_TIMEOUT_THRESHOLD = 2
_GATEWAY_COOLDOWN_SECONDS = 180
_GATEWAY_PRESPAWN_TIMEOUT_SECONDS = 10
_gateway_timeout_events: list[float] = []
_gateway_pause_until_ts: float = 0.0
_gateway_guard_lock = threading.Lock()

STAGE_MODELS: dict[str, tuple[str, str]] = {
    "intake": ("claude", "claude-sonnet-4-6"),
    "refine": ("claude", "claude-sonnet-4-6"),
    "research": ("claude", "claude-opus-4-6"),
    "spec": ("claude", "claude-opus-4-6"),
    "plan": ("claude", "claude-opus-4-6"),
    "issues": ("claude", "claude-sonnet-4-6"),
    "implement": ("codex", "o3"),
    "test": ("claude", "claude-sonnet-4-6"),
    "fix": ("codex", "o3"),
    "review": ("claude", "claude-opus-4-6"),
    "merge": ("none", "none"),
    "deploy": ("none", "none"),
}

# Pricing per 1M tokens (input, output) — must stay in sync with coordinator.default.json
MODEL_PRICING: dict[str, tuple[float, float]] = {
    "claude-opus-4-6": (15.0, 75.0),
    "claude-opus-4-5": (15.0, 75.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-sonnet-4-5": (3.0, 15.0),
    "claude-haiku-4-5": (0.8, 4.0),
    "o3": (2.0, 8.0),
    "o3-mini": (1.1, 4.4),
    "gpt-5.3": (2.0, 8.0),
    "gemini-2.5-pro": (1.25, 5.0),
    "gemini-2.5-flash": (0.15, 0.6),
    "gemini-3-pro": (2.0, 8.0),
}

STAGE_WORKTREE: dict[str, str] = {
    "implement": "code",
    "test": "tests",
    "fix": "code",
    "review": "code",
    "ui_implement": "ui",
    "ui_test": "uitest",
}

# Fallback chains: if primary model unavailable, try these in order
MODEL_FALLBACK_CHAINS: dict[str, list[str]] = {
    "claude-opus-4-6": ["claude-sonnet-4-6", "claude-opus-4-5", "claude-sonnet-4-5"],
    "claude-opus-4-5": ["claude-opus-4-6", "claude-sonnet-4-6", "claude-sonnet-4-5"],
    "claude-sonnet-4-6": ["claude-sonnet-4-5", "claude-haiku-4-5"],
    "claude-sonnet-4-5": ["claude-haiku-4-5"],
    "claude-sonnet-4-5-20250514": [
        "claude-sonnet-4-6",
        "claude-sonnet-4-5",
        "claude-haiku-4-5",
    ],
    "o3": ["o3-mini", "gpt-5.3"],
    "o3-mini": ["o3", "gpt-5.3"],
    "gemini-3-pro": ["gemini-2.5-pro", "gemini-2.5-flash"],
    "gemini-2.5-pro": ["gemini-2.5-flash"],
}

STAGE_TIMEOUTS: dict[str, int] = {
    "intake": 30,
    "refine": 300,
    "research": 900,
    "spec": 300,
    "plan": 900,
    "implement": 2400,
    "test": 2400,
    "fix": 2400,
    "review": 300,
}


def _stage_timeout(stage: str, via: str | None = None) -> int:
    default_timeout = STAGE_TIMEOUTS.get(stage, 600)
    stage_override = os.getenv(f"AIFLEET_STAGE_TIMEOUT_{stage.upper()}")
    if stage_override and stage_override.isdigit():
        return max(30, int(stage_override))
    provider_override = os.getenv(f"AIFLEET_STAGE_TIMEOUT_{(via or '').upper()}")
    if provider_override and provider_override.isdigit():
        return max(30, int(provider_override))
    global_override = os.getenv("AIFLEET_STAGE_TIMEOUT_DEFAULT")
    if global_override and global_override.isdigit():
        return max(30, int(global_override))
    return default_timeout


def _use_claude_sdk_bridge(stage: str, cfg: dict) -> bool:
    bridge_cfg = (cfg or {}).get("claude_sdk_bridge", {}) or {}
    enabled = bool(bridge_cfg.get("enabled", False))

    env_toggle = (os.getenv("AIFLEET_CLAUDE_SDK_BRIDGE") or "").strip().lower()
    if env_toggle in {"1", "true", "yes", "on"}:
        enabled = True
    elif env_toggle in {"0", "false", "no", "off"}:
        enabled = False

    if not enabled:
        return False

    stages = bridge_cfg.get(
        "stages",
        ["test", "review", "spec", "plan", "research", "intake", "refine", "issues"],
    )
    if stage not in set(stages):
        return False

    return claude_sdk_bridge.is_available()


# Max token budget per stage (prompt + response). Prevents runaway costs.
STAGE_TOKEN_BUDGETS: dict[str, int] = {
    "intake": 2_000,
    "refine": 8_000,
    "research": 16_000,
    "spec": 32_000,
    "plan": 16_000,
    "issues": 8_000,
    "implement": 64_000,
    "test": 32_000,
    "fix": 64_000,
    "review": 16_000,
}

# Complexity thresholds for downgrading model
COMPLEXITY_THRESHOLDS = {
    "simple_max_chars": 500,
    "medium_max_chars": 2000,
}


def _now_ts() -> int:
    return int(time.time())


def select_model_for_stage(stage: str, pipeline: dict, cfg: dict) -> tuple[str, str]:
    """Select model for a stage with complexity-aware routing.
    Returns (via, model). Downgrades to cheaper models for simple tasks.
    Explicit overrides in cfg.stage_models are never downgraded."""
    stage_models_override = cfg.get("stage_models", {})
    is_explicit_override = stage in stage_models_override
    via, model = stage_models_override.get(
        stage, STAGE_MODELS.get(stage, ("none", "none"))
    )

    # Optional local-sensitive path for fleetmax: force local OpenAI-compatible model
    # via Codex CLI (configured by ai-fleet fleetmax --local-sensitive).
    if cfg.get("local_sensitive") and stage in {
        "refine",
        "research",
        "spec",
        "plan",
        "implement",
        "test",
        "fix",
        "review",
    }:
        local_model = str(
            cfg.get("local_model", "qwen2.5:7b-instruct-q4_K_M")
            or "qwen2.5:7b-instruct-q4_K_M"
        )
        return "codex", local_model

    # Local-sensitive runs get a less expensive/faster refine default if model is unset.
    if (
        cfg.get("local_sensitive")
        and stage == "refine"
        and (not model or model == "none")
    ):
        return "codex", "qwen2.5:3b-instruct-q4_K_M"

    if via == "none" or is_explicit_override:
        return via, model

    objective = pipeline.get("structured_objective", pipeline.get("title", ""))
    obj_len = len(objective) if objective else 0

    # Downgrade for simple objectives on non-critical stages
    if obj_len < COMPLEXITY_THRESHOLDS["simple_max_chars"] and stage in (
        "research",
        "issues",
    ):
        if model == "claude-opus-4-6":
            model = "claude-sonnet-4-5"
            log.info(
                "Downgraded %s to %s for simple objective (%d chars)",
                stage,
                model,
                obj_len,
            )
        elif model == "o3":
            model = "o3-mini"
            log.info(
                "Downgraded %s to %s for simple objective (%d chars)",
                stage,
                model,
                obj_len,
            )

    return via, model


def get_fallback_model(model: str) -> str | None:
    """Get the next fallback model for a given model. Returns None if no fallback."""
    chain = MODEL_FALLBACK_CHAINS.get(model, [])
    return chain[0] if chain else None


def get_pipeline_cost(conn, pipeline_id: str) -> float:
    """Get total accumulated cost for a pipeline by summing cost_usd from pipeline_stages."""
    if conn is None:
        return 0.0

    try:
        row = conn.execute(
            """
            SELECT SUM(cost_usd) FROM pipeline_stages
            WHERE pipeline_id = ?
        """,
            (pipeline_id,),
        ).fetchone()

        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        log.warning("Failed to get pipeline cost: %s", e)
        return 0.0


def get_cost_summary(conn, pipeline_id: str | None = None) -> dict:
    """Get cost breakdown by stage/model for a pipeline, or aggregate for last 30 days.

    Returns:
        If pipeline_id given: {"total": float, "by_stage": {stage: cost}, "by_model": {model: cost}}
        If None: {"total": float, "by_model": {model: cost}, "period_days": 30}
    """
    if conn is None:
        return (
            {"total": 0.0, "by_model": {}, "by_stage": {}}
            if pipeline_id
            else {"total": 0.0, "by_model": {}, "period_days": 30}
        )

    try:
        if pipeline_id:
            # Per-pipeline breakdown
            total = get_pipeline_cost(conn, pipeline_id)

            by_stage = {}
            rows = conn.execute(
                """
                SELECT stage_name, SUM(cost_usd) FROM pipeline_stages
                WHERE pipeline_id = ?
                GROUP BY stage_name
            """,
                (pipeline_id,),
            ).fetchall()
            for stage, cost in rows:
                by_stage[stage] = float(cost) if cost else 0.0

            by_model = {}
            rows = conn.execute(
                """
                SELECT model, SUM(estimated_cost_usd) FROM cost_log
                WHERE run_id IN (
                    SELECT DISTINCT run_id FROM run_history
                    WHERE run_id LIKE 'run_%' AND repo = (
                        SELECT project_repo FROM pipeline_runs WHERE pipeline_id = ?
                    ) AND ts >= (
                        SELECT created_at FROM pipeline_runs WHERE pipeline_id = ?
                    )
                )
                GROUP BY model
            """,
                (pipeline_id, pipeline_id),
            ).fetchall()
            for model, cost in rows:
                by_model[model] = float(cost) if cost else 0.0

            return {"total": total, "by_stage": by_stage, "by_model": by_model}
        else:
            # Last 30 days aggregate
            cutoff = int((datetime.now() - timedelta(days=30)).timestamp())

            total_row = conn.execute(
                """
                SELECT SUM(estimated_cost_usd) FROM cost_log
                WHERE timestamp >= ?
            """,
                (cutoff,),
            ).fetchone()
            total = float(total_row[0]) if total_row and total_row[0] else 0.0

            by_model = {}
            rows = conn.execute(
                """
                SELECT model, SUM(estimated_cost_usd) FROM cost_log
                WHERE timestamp >= ?
                GROUP BY model
            """,
                (cutoff,),
            ).fetchall()
            for model, cost in rows:
                by_model[model] = float(cost) if cost else 0.0

            return {"total": total, "by_model": by_model, "period_days": 30}
    except Exception as e:
        log.warning("Failed to get cost summary: %s", e)
        return (
            {"total": 0.0, "by_model": {}, "by_stage": {}}
            if pipeline_id
            else {"total": 0.0, "by_model": {}, "period_days": 30}
        )


def check_model_available(via: str, model: str) -> tuple[bool, float]:
    """Check if a model endpoint is reachable. Returns (available, latency_ms).
    Results are cached for 60 seconds to avoid hammering APIs."""
    global _MODEL_AVAILABILITY_CACHE

    cache_key = (via, model)
    now = time.time()

    # Check cache
    if cache_key in _MODEL_AVAILABILITY_CACHE:
        available, latency, cached_at = _MODEL_AVAILABILITY_CACHE[cache_key]
        if now - cached_at < _CACHE_TTL:
            return available, latency

    # Map via to endpoint
    endpoint_map = {
        "claude": "https://api.anthropic.com/v1/messages",
        "codex": "https://api.openai.com/v1/chat/completions",
    }

    endpoint = endpoint_map.get(via)
    if not endpoint:
        # Unknown via, assume available
        _MODEL_AVAILABILITY_CACHE[cache_key] = (True, 0.0, now)
        return True, 0.0

    # Quick HEAD check with bounded retry/backoff for transient network flakiness.
    # Conservative behavior: still small timeout, with only a couple retries.
    max_attempts = _MODEL_PROBE_MAX_ATTEMPTS
    base_delay = _MODEL_PROBE_BASE_DELAY_SECONDS
    req = Request(endpoint, method="HEAD")
    last_latency = 0.0

    for attempt in range(1, max_attempts + 1):
        start = time.time()
        try:
            with urlopen(req, timeout=3):
                latency = (time.time() - start) * 1000
                _MODEL_AVAILABILITY_CACHE[cache_key] = (True, latency, now)
                return True, latency
        except (URLError, HTTPError, TimeoutError) as e:
            last_latency = (time.time() - start) * 1000
            if attempt < max_attempts:
                sleep_s = base_delay * (2 ** (attempt - 1))
                log.info(
                    "Model availability check retry %d/%d for %s/%s after %.1fs: %s",
                    attempt,
                    max_attempts,
                    via,
                    model,
                    sleep_s,
                    e,
                )
                time.sleep(sleep_s)
                continue

            log.warning(
                "Model availability check failed for %s/%s after %d attempts: %s",
                via,
                model,
                max_attempts,
                e,
            )
            _MODEL_AVAILABILITY_CACHE[cache_key] = (False, last_latency, now)
            return False, last_latency

    # Defensive fallback; loop always returns above.
    _MODEL_AVAILABILITY_CACHE[cache_key] = (False, last_latency, now)
    return False, last_latency


def select_model_cost_aware(
    stage: str, pipeline: dict, cfg: dict, conn
) -> tuple[str, str]:
    """Select model with cost-awareness.

    - If pipeline cost > 80% of per-pipeline limit, downgrade to cheapest model
    - If monthly budget > 90% used, force all stages to cheapest model
    - Falls back to select_model_for_stage if no conn or no budget data

    Returns (via, model)
    """
    if conn is None:
        return select_model_for_stage(stage, pipeline, cfg)

    pipeline_id = pipeline.get("pipeline_id", "")
    if not pipeline_id:
        return select_model_for_stage(stage, pipeline, cfg)

    # Get per-pipeline limit from config (default $5)
    per_pipeline_limit = cfg.get("per_pipeline_cost_limit", 5.0)

    # Get current pipeline cost
    current_cost = get_pipeline_cost(conn, pipeline_id)

    # Check monthly budget
    monthly_budget = None
    try:
        budget_row = conn.execute("""
            SELECT limit_usd, used_usd FROM budgets
            WHERE period = 'monthly' AND active = 1
            ORDER BY created_at DESC LIMIT 1
        """).fetchone()
        if budget_row:
            monthly_budget = {
                "limit": float(budget_row[0]),
                "used": float(budget_row[1]),
            }
    except Exception as e:
        log.debug("Could not fetch monthly budget (table may not exist): %s", e)

    # Determine if we need to force cheapest model
    force_cheapest = False

    if current_cost > per_pipeline_limit * 0.8:
        log.warning(
            "Pipeline %s cost ($%.2f) exceeds 80%% of limit ($%.2f), forcing cheapest model",
            pipeline_id,
            current_cost,
            per_pipeline_limit,
        )
        force_cheapest = True

    if monthly_budget and monthly_budget["used"] > monthly_budget["limit"] * 0.9:
        log.warning(
            "Monthly budget ($%.2f used of $%.2f limit) exceeds 90%%, forcing cheapest model",
            monthly_budget["used"],
            monthly_budget["limit"],
        )
        force_cheapest = True

    # Get base model selection
    via, model = select_model_for_stage(stage, pipeline, cfg)

    if via == "none":
        return via, model

    # If forcing cheapest, downgrade to lowest cost model in fallback chain
    if force_cheapest:
        cheapest_models = {
            "claude": "claude-haiku-4-5",
            "codex": "o3-mini",
        }
        cheapest = cheapest_models.get(via, model)
        if cheapest != model:
            log.info(
                "Downgrading %s from %s to %s due to budget constraints",
                stage,
                model,
                cheapest,
            )
            model = cheapest

    return via, model


def estimate_token_budget_cost(stage: str, model: str) -> float:
    """Estimate max cost for a stage based on its token budget and model pricing."""
    budget = STAGE_TOKEN_BUDGETS.get(stage, 32_000)
    # Assume 30% input, 70% output for worst-case estimate
    input_tokens = budget * 0.3
    output_tokens = budget * 0.7

    if model in MODEL_PRICING:
        input_rate, output_rate = MODEL_PRICING[model]
    else:
        input_rate, output_rate = 3.0, 15.0  # conservative fallback

    return (input_tokens * input_rate / 1_000_000) + (
        output_tokens * output_rate / 1_000_000
    )


_CODEX_PROFILES = [
    os.path.expanduser("~/.codex-main"),
    os.path.expanduser("~/.codex-alt-4"),
]
_codex_profile_idx = 0
_profile_lock = threading.Lock()


def _rotate_profile(via: str, env: dict) -> bool:
    """Rotate to next available auth profile for the given provider.

    Mutates env dict in-place. Returns True if rotation happened.
    """
    global _codex_profile_idx
    if via == "codex":
        with _profile_lock:
            _codex_profile_idx = (_codex_profile_idx + 1) % len(_CODEX_PROFILES)
            new_home = _CODEX_PROFILES[_codex_profile_idx]
        if os.path.isdir(new_home):
            env["CODEX_HOME"] = new_home
            log.info("Rotated Codex profile to %s", new_home)
            return True
    elif via == "claude":
        # Claude rotation is handled by CLIProxyAPI round-robin — just retry
        log.info("Claude rate-limited; CLIProxyAPI auto-rotates on retry")
        return True
    return False


def _extract_ao_session(stdout: str) -> str:
    m = re.search(r"SESSION=([A-Za-z0-9_-]+)", stdout or "")
    return m.group(1) if m else ""


def _ao_status_json(env: dict) -> list[dict]:
    result = subprocess.run(
        ["ao", "status", "--json"],
        capture_output=True,
        text=True,
        timeout=20,
        env=env,
    )
    if result.returncode != 0:
        return []
    try:
        data = json.loads(result.stdout or "[]")
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _inject_proxy_env(env: dict) -> None:
    """Inject fullstackOS proxy env vars so AO routes through our infrastructure.

    Reads live configs to pick up port/key changes. Falls back to defaults
    if config files are missing or unreadable.
    """
    # Orchestrator (Anthropic path)
    orch_port = 8318
    orch_key = "your-proxy-key"
    try:
        import yaml

        with open(os.path.expanduser("~/.claudemax/config.yaml")) as f:
            orch_cfg = yaml.safe_load(f) or {}
        orch_port = orch_cfg.get("port", 8318)
        keys = orch_cfg.get("api_keys", [])
        # Pick first concrete key (skip wildcards)
        for k in keys:
            if isinstance(k, str) and "*" not in k:
                orch_key = k
                break
    except Exception:
        pass

    # Fleet Gateway (OpenAI path)
    gw_port = 4105
    gw_key = "your-fleet-key"
    try:
        gw_path = os.path.expanduser("~/.ai-fleet/gateway.json")
        with open(gw_path) as f:
            gw_cfg = json.load(f)
        gw_port = gw_cfg.get("server", {}).get("port", 4105)
        gw_keys = gw_cfg.get("server", {}).get("api_keys", [])
        if gw_keys:
            gw_key = gw_keys[0]
    except Exception:
        pass

    env["ANTHROPIC_BASE_URL"] = f"http://127.0.0.1:{orch_port}"
    env["ANTHROPIC_API_KEY"] = orch_key
    env["OPENAI_BASE_URL"] = f"http://127.0.0.1:{gw_port}/v1"
    env["OPENAI_API_KEY"] = gw_key


def _inject_exec_broker_env(env: dict) -> None:
    """Inject exec broker PATH shims into agent subprocess environment.

    Installs shims on first call (lazy init). Prepends ~/.ai-fleet/shims/
    to PATH so agent subprocesses route high-risk binaries through policy checks.
    """
    global _exec_broker, _exec_broker_initialized
    if not _exec_broker_initialized:
        try:
            _exec_broker = ExecBroker()
            _exec_broker.install_shims()
            _exec_broker_initialized = True
            log.info(
                "Exec broker initialized: %d shims installed",
                len(_exec_broker._real_paths),
            )
        except Exception as exc:
            log.warning("Exec broker init failed (non-blocking): %s", exc)
            _exec_broker_initialized = True  # Don't retry on every call
            return
    if _exec_broker is not None:
        shims_str = str(_exec_broker.shims_dir)
        current_path = env.get("PATH", "")
        if shims_str not in current_path.split(os.pathsep):
            env["PATH"] = shims_str + os.pathsep + current_path


def _spawn_ao_agent(
    stage: str,
    pipeline: dict,
    prompt: str,
    run_id: str,
    stage_timeout: int,
    env: dict,
    worktree_path: str | None = None,
) -> dict:
    # Pre-check: AO requires agent-orchestrator.yaml in the workspace
    repo_path = worktree_path or pipeline.get("project_repo") or "."
    ao_config = os.path.join(os.path.abspath(repo_path), "agent-orchestrator.yaml")
    if not os.path.isfile(ao_config):
        log.warning(
            "AO config missing at %s — skipping AO, will fall back to native", ao_config
        )
        return {
            "ok": False,
            "stdout": "",
            "stderr": f"agent-orchestrator.yaml not found in {os.path.abspath(repo_path)}; AO requires per-project config",
            "exit_code": 1,
            "run_id": run_id,
            "duration_seconds": 0.0,
            "cost_usd": 0.0,
            "via": "ao",
            "model": "ao",
            "input_tokens": 0,
            "output_tokens": 0,
        }

    objective = pipeline.get("structured_objective", pipeline.get("title", ""))
    if isinstance(objective, str):
        text = objective.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
                objective = (
                    parsed.get("title")
                    or parsed.get("description")
                    or pipeline.get("title", "")
                )
            except Exception:
                objective = text
    objective_text = (
        str(objective or pipeline.get("title", "")).replace("\n", " ").strip()
    )
    objective_text = re.sub(r"\s+", " ", objective_text)
    issue = f"{stage}: {objective_text}".strip()
    start = time.time()

    # Derive workspace name from repo path instead of hardcoding
    repo_path = worktree_path or pipeline.get("project_repo") or "."
    workspace = os.path.basename(os.path.abspath(repo_path))

    # AO expects issue tracker IDs (e.g., INT-1234, #42). Free-form prompts fail.
    issue_id = issue if re.match(r"^(#[0-9]+|[A-Z][A-Z0-9]+-[0-9]+)$", issue) else None
    spawn_cmd = ["ao", "spawn", workspace]
    if issue_id:
        spawn_cmd.append(issue_id)

    spawn = subprocess.run(
        spawn_cmd,
        capture_output=True,
        text=True,
        timeout=min(stage_timeout, 60),
        env=env,
    )

    if spawn.returncode != 0:
        stderr_text = spawn.stderr or "ao spawn failed"
        if not issue_id and "must exist in tracker" in stderr_text:
            stderr_text = (
                "ao spawn requires issue IDs when issue tracker is enabled; "
                "configured AO workspace rejected free-form run"
            )
        return {
            "ok": False,
            "stdout": spawn.stdout,
            "stderr": stderr_text,
            "exit_code": spawn.returncode,
            "run_id": run_id,
            "duration_seconds": round(time.time() - start, 2),
            "cost_usd": 0.0,
            "via": "ao",
            "model": "ao",
            "input_tokens": 0,
            "output_tokens": 0,
        }

    session = _extract_ao_session(spawn.stdout)
    if not session:
        return {
            "ok": False,
            "stdout": spawn.stdout,
            "stderr": "ao spawn succeeded but session id not found",
            "exit_code": 1,
            "run_id": run_id,
            "duration_seconds": round(time.time() - start, 2),
            "cost_usd": 0.0,
            "via": "ao",
            "model": "ao",
            "input_tokens": 0,
            "output_tokens": 0,
        }

    send = subprocess.run(
        ["ao", "send", "--no-wait", "--timeout", "30", session, prompt],
        capture_output=True,
        text=True,
        timeout=min(stage_timeout, 45),
        env=env,
    )
    if send.returncode != 0:
        return {
            "ok": False,
            "stdout": (spawn.stdout or "") + "\n" + (send.stdout or ""),
            "stderr": send.stderr or "ao send failed",
            "exit_code": send.returncode,
            "run_id": run_id,
            "duration_seconds": round(time.time() - start, 2),
            "cost_usd": 0.0,
            "via": "ao",
            "model": "ao",
            "input_tokens": 0,
            "output_tokens": 0,
        }

    deadline = start + stage_timeout
    poll_interval = 5.0
    last_status = "unknown"
    last_activity = ""
    last_summary = ""
    seen_session = False

    while time.time() < deadline:
        rows = _ao_status_json(env)
        row = next((r for r in rows if str(r.get("name", "")) == session), None)
        if row is None:
            if seen_session:
                break
            time.sleep(poll_interval)
            continue

        seen_session = True

        last_status = str(row.get("status", "")).lower()
        last_activity = str(row.get("activity", "")).lower()
        last_summary = str(row.get("summary") or row.get("claudeSummary") or "")

        if last_status in {"done", "completed", "success"}:
            input_tokens, output_tokens = _count_tokens(prompt, last_summary)
            return {
                "ok": True,
                "stdout": f"SESSION={session}\nSTATUS={last_status}\n{last_summary}".strip(),
                "stderr": "",
                "exit_code": 0,
                "run_id": run_id,
                "duration_seconds": round(time.time() - start, 2),
                "cost_usd": 0.0,
                "via": "ao",
                "model": "ao",
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }

        if last_status in {"error", "failed", "crashed"} or last_activity in {
            "exited",
            "error",
            "failed",
            "crashed",
        }:
            return {
                "ok": False,
                "stdout": f"SESSION={session}\nSTATUS={last_status}\nACTIVITY={last_activity}\n{last_summary}".strip(),
                "stderr": "ao session failed",
                "exit_code": 1,
                "run_id": run_id,
                "duration_seconds": round(time.time() - start, 2),
                "cost_usd": 0.0,
                "via": "ao",
                "model": "ao",
                "input_tokens": 0,
                "output_tokens": 0,
            }

        time.sleep(poll_interval)

    # Timed out waiting for AO session completion.
    return {
        "ok": False,
        "stdout": f"SESSION={session}\nSTATUS={last_status}\nACTIVITY={last_activity}\n{last_summary}".strip(),
        "stderr": f"ao session did not complete within {stage_timeout}s",
        "exit_code": -1,
        "run_id": run_id,
        "duration_seconds": round(time.time() - start, 2),
        "cost_usd": 0.0,
        "via": "ao",
        "model": "ao",
        "input_tokens": 0,
        "output_tokens": 0,
    }


def spawn_agent(
    stage: str,
    pipeline: dict,
    worktree_path: str,
    cycle_summary: str,
    cfg: dict,
    conn,
) -> dict:
    """Spawn a fresh agent subprocess for the given stage with fallback support.

    Returns dict with keys: ok, stdout, stderr, exit_code, run_id, duration_seconds, cost_usd
    """
    run_id = "run_" + os.urandom(4).hex()

    # Use cost-aware selection if conn is available
    if conn is not None:
        via, model = select_model_cost_aware(stage, pipeline, cfg, conn)
    else:
        via, model = select_model_for_stage(stage, pipeline, cfg)

    if via == "none":
        return {
            "ok": True,
            "stdout": "",
            "stderr": "",
            "exit_code": 0,
            "run_id": run_id,
            "duration_seconds": 0.0,
            "cost_usd": 0.0,
        }

    prompt = _build_stage_prompt(stage, pipeline, cycle_summary, cfg)

    # Inject pre-hook context (verification profiles, prior knowledge)
    pre_hook_ctx = pipeline.get("_pre_hook_context", "")
    if pre_hook_ctx:
        prompt = pre_hook_ctx + "\n\n" + prompt

    # Inject skill context if enabled (Phase 1: per-stage skill injection)
    if cfg.get("stage_skills", {}).get("enabled", False):
        from pipeline.stages import discover_and_inject_skills

        skill_context = discover_and_inject_skills(stage, pipeline, cfg, conn=conn)
        if skill_context:
            prompt = skill_context + "\n\n" + prompt

    executor = (cfg or {}).get("executor", "native")
    if executor == "ao" and stage in {
        "research",
        "spec",
        "plan",
        "implement",
        "test",
        "fix",
        "review",
    }:
        via = "ao"

    # Try primary model, then one fallback if it fails
    models_to_try = [model]
    fallback = get_fallback_model(model)
    if fallback and via != "ao":
        models_to_try.append(fallback)

    last_error = None
    accumulated_cost = 0.0  # High-water-mark: tracks total cost across retries

    for attempt_model in models_to_try:
        start = time.time()

        # AO path uses dedicated handler; skip per-model loop for AO.
        if via == "ao":
            stage_timeout = _stage_timeout(stage, via)
            _blocked_env_keys = {
                "CLAUDECODE",
                "CLAUDE_CODE_SESSION",
                "CLAUDE_CODE_ENTRYPOINT",
            }
            env = {k: v for k, v in os.environ.items() if k not in _blocked_env_keys}
            # Route AO's LLM calls through fullstackOS proxy (orchestrator → CLIProxyAPI)
            _inject_proxy_env(env)
            _inject_exec_broker_env(env)
            log.info("Spawning AO agent for stage %s [%s]", stage, run_id)
            outcome = _spawn_ao_agent(
                stage,
                pipeline,
                prompt,
                run_id,
                stage_timeout,
                env,
                worktree_path=worktree_path,
            )
            duration = time.time() - start
            outcome["duration_seconds"] = round(duration, 2)
            log.info(
                "AO agent %s completed: ok=%s, duration=%.1fs [%s]",
                stage,
                outcome["ok"],
                duration,
                run_id,
            )
            _actual_cycle = 1
            if conn is not None:
                try:
                    _cycle_row = conn.execute(
                        "SELECT MAX(cycle) as c FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = ?",
                        (pipeline.get("pipeline_id", ""), stage),
                    ).fetchone()
                    _actual_cycle = (_cycle_row["c"] or 1) if _cycle_row else 1
                except Exception:
                    pass
            _save_cycle_summary(
                conn, pipeline.get("pipeline_id", ""), stage, _actual_cycle, outcome
            )
            if outcome.get("ok"):
                return outcome

            last_error = outcome
            log.warning(
                "AO failed for stage %s [%s]; falling back to native provider path",
                stage,
                run_id,
            )
            via, model = select_model_for_stage(stage, pipeline, cfg)
            models_to_try = [model]
            fallback = get_fallback_model(model)
            if fallback:
                models_to_try.append(fallback)
            continue

            # continue into native command path below in the same loop iteration

        use_sdk_bridge = False
        if via == "codex":
            cmd = [
                "codex",
                "exec",
                "--full-auto",
                "--skip-git-repo-check",
                "--ephemeral",
                "-m",
                attempt_model,
                prompt,
            ]
        elif via == "claude":
            use_sdk_bridge = _use_claude_sdk_bridge(stage, cfg)
            if use_sdk_bridge:
                cmd = ["claude-sdk-bridge", attempt_model]
            else:
                cmd = [
                    "claude",
                    "--print",
                    "--dangerously-skip-permissions",
                    "--model",
                    attempt_model,
                    prompt,
                ]
        else:
            return {
                "ok": False,
                "stdout": "",
                "stderr": f"Unknown via: {via}",
                "exit_code": 1,
                "run_id": run_id,
                "duration_seconds": 0.0,
                "cost_usd": 0.0,
            }

        is_fallback = attempt_model != model
        log.info(
            "%s %s agent (model=%s) for stage %s [%s]",
            "Fallback to" if is_fallback else "Spawning",
            via,
            attempt_model,
            stage,
            run_id,
        )

        try:
            # Clear nested-session guards so claude/codex can spawn from within Claude Code
            _blocked_env_keys = {
                "CLAUDECODE",
                "CLAUDE_CODE_SESSION",
                "CLAUDE_CODE_ENTRYPOINT",
            }
            env = {k: v for k, v in os.environ.items() if k not in _blocked_env_keys}
            _inject_proxy_env(env)
            _inject_exec_broker_env(env)

            # Inject MCP profile config for skill-aware agents
            _mcp_profile = pipeline.get("_resolved_mcp_profile", "")
            if not _mcp_profile:
                try:
                    from pipeline.skill_resolver_bridge import load_skill_tree, resolve_skill_bundle
                    _tree = load_skill_tree(str(Path(cfg.get("repo_root", ".")).resolve()))
                    _objective = pipeline.get("structured_objective", pipeline.get("title", ""))
                    _bundle = resolve_skill_bundle(f"{_objective} {stage}", _tree, task_kind=stage)
                    if _bundle:
                        _mcp_profile = _bundle.get("mcp_profile", "")
                except Exception:
                    pass
            if _mcp_profile:
                _generated = Path(cfg.get("repo_root", ".")).resolve() / "generated"
                if via == "claude":
                    _profile_json = _generated / f"mcp-{_mcp_profile}.json"
                    if _profile_json.exists():
                        env["CLAUDE_MCP_CONFIG"] = str(_profile_json)
                        log.info("MCP profile %s activated for claude agent [%s]", _mcp_profile, run_id)
                elif via == "codex":
                    _profile_toml = _generated / f"mcp-{_mcp_profile}.toml"
                    if _profile_toml.exists():
                        env["CODEX_MCP_CONFIG"] = str(_profile_toml)
                        log.info("MCP profile %s activated for codex agent [%s]", _mcp_profile, run_id)

            if via == "codex":
                env["CODEX_FORCE_ALLOW_NESTED"] = "1"
                if cfg.get("local_sensitive"):
                    local_base = str(
                        cfg.get("local_base_url", "http://10.0.1.100:11434")
                        or "http://10.0.1.100:11434"
                    )
                    if local_base.endswith("/v1"):
                        local_base = local_base[:-3]
                    local_base = local_base.rstrip("/")
                    env["OPENAI_BASE_URL"] = local_base
                    env.setdefault("OPENAI_API_KEY", "ollama")
            stage_timeout = _stage_timeout(stage, via)

            # Policy checks apply only to direct shell commands, not model-runner invocations.
            # cmd is usually ["claude"|"codex"|"ao", ... prompt/objective].
            # Checking full prompt text causes false positives (e.g., words like "reboot" in task text).
            runner = (cmd[0] or "").strip().lower() if cmd else ""
            if runner not in {"claude", "codex", "ao"}:
                cmd_str = " ".join(cmd)
                policy_cfg = None
                try:
                    cfg_json = pipeline.get("config_json")
                    if isinstance(cfg_json, dict):
                        policy_cfg = cfg_json
                    elif isinstance(cfg_json, str) and cfg_json.strip():
                        policy_cfg = json.loads(cfg_json)
                except Exception:
                    policy_cfg = None
                policy = evaluate_command_policy(cmd_str, policy_cfg)
                policy_action = str(policy.get("action", "allow"))
                policy_reason = str(policy.get("reason", ""))
                policy_risk = str(policy.get("risk_tier", "medium"))
                needs_gate = bool(policy.get("needs_human_gate", False))
                profile = str(policy.get("autonomy_profile", "balanced"))

                if policy_action == "deny":
                    return {
                        "ok": False,
                        "stdout": "",
                        "stderr": f"Policy denied command: {policy_reason} (risk={policy_risk}, profile={profile})",
                        "exit_code": 126,
                        "run_id": run_id,
                        "duration_seconds": 0.0,
                        "cost_usd": 0.0,
                    }
                if policy_action == "gate" or needs_gate:
                    return {
                        "ok": False,
                        "stdout": "",
                        "stderr": f"Policy gate required: {policy_reason or 'risk policy'} (risk={policy_risk}, profile={profile})",
                        "exit_code": 125,
                        "run_id": run_id,
                        "duration_seconds": 0.0,
                        "cost_usd": 0.0,
                    }

            if use_sdk_bridge:
                bridge_result = claude_sdk_bridge.run_prompt(
                    prompt=prompt,
                    cwd=worktree_path or ".",
                    model=attempt_model,
                    timeout=stage_timeout,
                )
                duration = float(bridge_result.get("duration_seconds", 0.0) or 0.0)
                stdout_text = str(bridge_result.get("stdout", "") or "")
                stderr_text = str(bridge_result.get("stderr", "") or "")
                exit_code = int(bridge_result.get("exit_code", -1))
            else:
                result = subprocess.run(
                    cmd,
                    cwd=worktree_path or None,
                    capture_output=True,
                    text=True,
                    timeout=stage_timeout,
                    env=env,
                )
                duration = time.time() - start
                stdout_text = result.stdout
                stderr_text = result.stderr
                exit_code = result.returncode

            input_tokens, output_tokens = _count_tokens(prompt, stdout_text)
            cost = _cost_from_tokens(via, attempt_model, input_tokens, output_tokens)
            accumulated_cost += cost

            outcome = {
                "ok": exit_code == 0,
                "stdout": stdout_text,
                "stderr": stderr_text,
                "exit_code": exit_code,
                "run_id": run_id,
                "duration_seconds": round(duration, 2),
                "cost_usd": accumulated_cost,
                "via": via,
                "model": attempt_model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }

            log.info(
                "Agent %s completed: ok=%s, cost=$%.4f, duration=%.1fs, tokens=%d/%d [%s]",
                stage,
                outcome["ok"],
                cost,
                duration,
                input_tokens,
                output_tokens,
                run_id,
            )

            _actual_cycle = 1
            if conn is not None:
                try:
                    _cycle_row = conn.execute(
                        "SELECT MAX(cycle) as c FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = ?",
                        (pipeline.get("pipeline_id", ""), stage),
                    ).fetchone()
                    _actual_cycle = (_cycle_row["c"] or 1) if _cycle_row else 1
                except Exception:
                    pass
            _save_cycle_summary(
                conn, pipeline.get("pipeline_id", ""), stage, _actual_cycle, outcome
            )

            # Record cost in cost_log table
            if conn is not None:
                try:
                    record_stage_cost(
                        conn,
                        pipeline.get("pipeline_id", ""),
                        stage,
                        run_id,
                        via,
                        attempt_model,
                        input_tokens,
                        output_tokens,
                        cost,
                        duration,
                    )
                except Exception as e:
                    log.warning("Failed to record stage cost: %s", e)

            # Detect rate limiting (429) in stderr or stdout
            combined_output = (stderr_text or "") + (stdout_text or "")
            is_rate_limited = any(
                s in combined_output.lower()
                for s in (
                    "429",
                    "rate limit",
                    "too many requests",
                    "quota exceeded",
                    "rate_limit_error",
                    "overloaded",
                )
            )

            if is_rate_limited and not is_fallback and not use_sdk_bridge:
                log.warning(
                    "Rate limited on %s/%s for stage %s — rotating profile [%s]",
                    via,
                    attempt_model,
                    stage,
                    run_id,
                )
                rotated = _rotate_profile(via, env)
                if rotated:
                    log.info(
                        "Rotated %s profile, retrying stage %s [%s]", via, stage, run_id
                    )
                    # Retry with same model but rotated profile
                    try:
                        stage_timeout = _stage_timeout(stage, via)
                        result2 = subprocess.run(
                            cmd,
                            cwd=worktree_path or None,
                            capture_output=True,
                            text=True,
                            timeout=stage_timeout,
                            env=env,
                        )
                        duration2 = time.time() - start
                        in2, out2 = _count_tokens(prompt, result2.stdout)
                        cost2 = _cost_from_tokens(via, attempt_model, in2, out2)
                        accumulated_cost += cost2
                        if result2.returncode == 0:
                            return {
                                "ok": True,
                                "stdout": result2.stdout,
                                "stderr": result2.stderr,
                                "exit_code": 0,
                                "run_id": run_id,
                                "duration_seconds": round(duration2, 2),
                                "cost_usd": accumulated_cost,
                                "via": via,
                                "model": attempt_model,
                                "input_tokens": in2,
                                "output_tokens": out2,
                            }
                    except Exception as e2:
                        log.warning("Retry after rotation also failed: %s", e2)

            # If successful or this is already a fallback, return
            if outcome["ok"] or is_fallback:
                return outcome

            # Primary model failed, try fallback
            last_error = outcome
            log.warning(
                "Primary model %s failed (exit=%d), trying fallback...",
                attempt_model,
                exit_code,
            )
            continue

        except subprocess.TimeoutExpired:
            duration = time.time() - start
            last_error = {
                "ok": False,
                "stdout": "",
                "stderr": f"Agent timed out after {_stage_timeout(stage, via)}s",
                "exit_code": -1,
                "run_id": run_id,
                "duration_seconds": round(duration, 2),
                "cost_usd": 0.0,
            }
            if is_fallback:
                return last_error
            log.warning("Primary model %s timed out, trying fallback...", attempt_model)
            continue

        except Exception as e:
            duration = time.time() - start
            last_error = {
                "ok": False,
                "stdout": "",
                "stderr": str(e),
                "exit_code": -1,
                "run_id": run_id,
                "duration_seconds": round(duration, 2),
                "cost_usd": 0.0,
            }
            if is_fallback:
                return last_error
            log.warning(
                "Primary model %s failed with exception, trying fallback: %s",
                attempt_model,
                e,
            )
            continue

    # All attempts failed, return last error
    return (
        last_error
        if last_error
        else {
            "ok": False,
            "stdout": "",
            "stderr": "All model attempts failed",
            "exit_code": -1,
            "run_id": run_id,
            "duration_seconds": 0.0,
            "cost_usd": 0.0,
        }
    )


def _gateway_prespawn_check() -> tuple[bool, str]:
    """Run Agent Gateway pre-spawn health gate before starting an agent."""
    script = os.path.expanduser("~/.agent-gateway/scripts/pre-spawn-check.sh")
    if not os.path.isfile(script):
        return True, "pre-spawn check script missing; skipping gate"

    try:
        proc = subprocess.run(
            ["bash", script],
            capture_output=True,
            text=True,
            timeout=_GATEWAY_PRESPAWN_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return (
            False,
            f"agent-gateway pre-spawn check timed out after {_GATEWAY_PRESPAWN_TIMEOUT_SECONDS}s",
        )
    except Exception as e:
        return False, f"agent-gateway pre-spawn check failed: {e}"

    output = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode == 0:
        return True, output or "agent-gateway pre-spawn check passed"
    return False, output or "agent-gateway pre-spawn check blocked agent spawn"


def _gateway_circuit_state(now: float | None = None) -> tuple[bool, float]:
    """Return whether circuit is open and seconds remaining in cooldown."""
    now = now or time.time()
    with _gateway_guard_lock:
        remaining = max(0.0, _gateway_pause_until_ts - now)
        return remaining > 0.0, remaining


def _record_gateway_timeout_event(now: float | None = None) -> tuple[int, bool, float]:
    """Track Agent Gateway timeout events and open circuit if threshold reached."""
    now = now or time.time()
    cutoff = now - _GATEWAY_TIMEOUT_WINDOW_SECONDS
    with _gateway_guard_lock:
        global _gateway_timeout_events, _gateway_pause_until_ts
        _gateway_timeout_events = [
            ts for ts in _gateway_timeout_events if ts >= cutoff
        ]
        _gateway_timeout_events.append(now)
        event_count = len(_gateway_timeout_events)
        opened = False
        if (
            event_count >= _GATEWAY_TIMEOUT_THRESHOLD
            and now >= _gateway_pause_until_ts
        ):
            _gateway_pause_until_ts = now + _GATEWAY_COOLDOWN_SECONDS
            opened = True
        return event_count, opened, _gateway_pause_until_ts


def _is_gateway_timeout_error(stderr: str, stdout: str) -> bool:
    combined = f"{stderr or ''}\n{stdout or ''}".lower()
    needles = (
        "node invoke timed out",
        "gateway timeout",
        "gateway closed (1006",
        "exec failed: node invoke timed out",
    )
    return any(n in combined for n in needles)


def _objective_text(pipeline: dict) -> str:
    """Return human-readable objective text for prompts.

    Handles legacy string objectives and newer JSON-serialized structured objectives.
    """
    obj = pipeline.get("structured_objective", pipeline.get("title", ""))
    if not isinstance(obj, str):
        return str(obj)

    stripped = obj.strip()
    if not stripped:
        return stripped

    # Parse JSON objective payloads injected by fleetmax/fleetmaxao refine stage
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                title = str(parsed.get("title", "")).strip()
                desc = str(parsed.get("description", "")).strip()
                criteria = parsed.get("success_criteria", [])
                constraints = parsed.get("constraints", [])

                lines: list[str] = []
                if title:
                    lines.append(f"Title: {title}")
                if desc:
                    lines.append(f"Description: {desc}")
                if isinstance(criteria, list) and criteria:
                    lines.append("Success criteria:")
                    for item in criteria:
                        if item:
                            lines.append(f"- {item}")
                if isinstance(constraints, list) and constraints:
                    lines.append("Constraints:")
                    for item in constraints:
                        if item:
                            lines.append(f"- {item}")

                if lines:
                    return "\n".join(lines)
        except Exception:
            pass

    return stripped


def _build_stage_prompt(
    stage: str, pipeline: dict, cycle_summary: str, cfg: dict
) -> str:
    cycle_injection = ""
    if cycle_summary:
        cycle_injection = f"Previous cycle context:\n{cycle_summary}\n\n"

    objective = _objective_text(pipeline)
    raw_input = pipeline.get("raw_input", "")
    spec = pipeline.get("spec_json", "")
    plan = pipeline.get("plan_json", "")

    if stage == "implement":
        return (
            f"You are implementing a feature in a git worktree. {objective}\n\n"
            f"Plan:\n{plan}\n\n"
            f"{cycle_injection}"
            f"Rules:\n"
            f"- After each meaningful code change, run the targeted test for the changed module\n"
            f"- Do NOT batch all testing to the end — verify incrementally\n"
            f"- If a test fails after your change, fix it before moving to the next task\n"
            f"- Commit your changes when done\n"
            f"- Write clean, working code\n"
            f"- Follow existing patterns in the codebase"
        )

    elif stage == "test":
        return (
            f"You are writing and running tests for a feature. {objective}\n\n"
            f"The implementation is in this worktree. Run existing tests, add new tests, report results.\n\n"
            f"{cycle_injection}"
            f"## CI Failure Triage\n"
            f"Before reporting failures, classify each one:\n"
            f"- TRANSIENT (network errors, rate limits, binary download failures, timeouts): mark as transient, recommend re-run\n"
            f"- ENVIRONMENT (missing deps, wrong runtime version, disk space): mark as env_issue, describe fix\n"
            f"- REAL (actual test failures, lint errors, type errors, build failures): these are the real issues\n"
            f"Only REAL failures should be counted in tests_failed.\n\n"
            f'Output a JSON summary: {{"tests_total": N, "tests_passed": N, "tests_failed": N, "failures": [...], "transient_failures": [...], "env_issues": [...]}}'
        )

    elif stage == "fix":
        test_output = cfg.get("test_output", "")
        return (
            f"You are fixing failing tests. {objective}\n\n"
            f"{cycle_injection}"
            f"Previous test results:\n{test_output}\n\n"
            f"## MANDATORY: Diagnose Before Fix\n"
            f"1. REPRODUCE: Run the exact failing test command and show the error output\n"
            f"2. ROOT CAUSE: Read the failing code paths — identify WHY it fails, not just WHERE\n"
            f"3. APPROACH: If multiple fixes exist, state tradeoffs and pick the best one\n"
            f"4. IMPLEMENT: Only then write the fix\n"
            f"5. VERIFY: Run the EXACT same test that was failing and show it passing\n"
            f"Do NOT skip straight to code changes from a symptom. Do NOT claim success without proof output.\n\n"
            f"Commit your changes."
        )

    elif stage == "review":
        return (
            f"You are reviewing code changes for quality, security, and correctness.\n\n"
            f"{objective}\n\n"
            f"Review the diff and provide:\n"
            f"1. APPROVE or REQUEST_CHANGES\n"
            f"2. List of issues found (if any) with severity (critical/high/medium/low)\n"
            f"3. For each issue: provide the exact file:line and a concrete fix\n"
            f"4. Summary\n\n"
            f"## Verification Requirements\n"
            f"- For config/policy changes: verify the runtime behavior matches intent, not just the code change\n"
            f"- For bug fixes: confirm the fix addresses the root cause, not just the symptom\n"
            f"- Flag any change that claims completion without proof output\n\n"
            f'Output as JSON: {{"verdict": "APPROVE|REQUEST_CHANGES", "issues": [{{"severity": "...", "file_line": "...", "description": "...", "fix": "..."}}], "summary": "..."}}'
        )

    elif stage == "refine":
        return (
            f"You are refining a raw brain dump into a structured development objective.\n\n"
            f"Raw input:\n{raw_input}\n\n"
            f"Produce a structured objective with:\n"
            f"1. Clear title\n"
            f"2. Description of what to build\n"
            f"3. Success criteria\n"
            f"4. Technical constraints\n\n"
            f'Output as JSON: {{"title": "...", "description": "...", "success_criteria": [...], "constraints": [...]}}'
        )

    elif stage == "research":
        return (
            f"You are a deep research specialist. Investigate this objective thoroughly.\n\n"
            f"{objective}\n\n"
            f"## Research Protocol\n"
            f"1. Search for 3+ existing implementations/references before producing output\n"
            f"2. Read actual source code, documentation, and production examples — never guess\n"
            f"3. Every finding MUST include a source (URL, file path, or code reference)\n"
            f"4. Include architectural patterns, error handling approaches, and testing strategies\n"
            f"5. Flag potential pitfalls with evidence from real-world usage\n"
            f"6. Do NOT produce speculative or fabricated findings — if you cannot find evidence, say so\n\n"
            f'Output as JSON: {{"findings": [{{"title": "...", "source": "url/path", "detail": "..."}}], "recommendations": [...], "risks": [{{"risk": "...", "evidence": "...", "mitigation": "..."}}]}}'
        )

    elif stage == "spec":
        return (
            f"Generate a detailed technical specification for:\n\n"
            f"{objective}\n\n"
            f"Include: data models, API endpoints, file changes, dependencies.\n\n"
            f"Output as JSON spec document."
        )

    elif stage == "plan":
        return (
            f"Create an implementation plan for:\n\n"
            f"{objective}\n\n"
            f"Spec:\n{spec}\n\n"
            f"Output STRICT JSON object with this schema:\n"
            f'{{"tasks": [{{"task_id": "TASK-001", "summary": "...", "depends_on": [], "acceptance_criteria": ["..."], "validation_cmds": ["..."]}}]}}\n'
            f"Rules: provide at least 3 tasks, unique task_id values, and valid depends_on references only."
        )

    elif stage == "issues":
        return (
            f"Create GitHub issues from this plan:\n\n"
            f"{plan}\n\n"
            f"For each step, create an issue with title, body, and labels.\n\n"
            f'Output as JSON: {{"issues": [{{"title": "...", "body": "...", "labels": [...]}}]}}'
        )

    else:
        return f"{cycle_injection}{objective}"


def _save_cycle_summary(
    conn, pipeline_id: str, stage: str, cycle: int, result: dict
) -> None:
    """Compress agent output into a summary for the next cycle."""
    stdout = result.get("stdout", "")
    stderr = result.get("stderr", "")

    files_changed = ""
    test_results = ""
    errors = ""

    if "files changed" in stdout.lower() or "modified:" in stdout.lower():
        matches = re.findall(
            r"(modified|created|deleted):\s+([^\n]+)", stdout, re.IGNORECASE
        )
        if matches:
            files_changed = "; ".join(f"{op}: {path}" for op, path in matches[:10])

    if "test" in stdout.lower():
        test_match = re.search(
            r"(\d+)\s+passed.*?(\d+)\s+failed", stdout, re.IGNORECASE
        )
        if test_match:
            test_results = f"{test_match.group(1)} passed, {test_match.group(2)} failed"

    if stderr or result.get("exit_code", 0) != 0:
        errors = stderr[:500] if stderr else "Non-zero exit code"

    summary_parts = []
    if files_changed:
        summary_parts.append(f"Files: {files_changed}")
    if test_results:
        summary_parts.append(f"Tests: {test_results}")
    if errors:
        summary_parts.append(f"Errors: {errors}")
    if not summary_parts:
        # Evidence grounding: only report what's directly observable
        if stdout.strip():
            summary_parts.append(f"Raw output (first 1000 chars): {stdout[:1000]}")
        else:
            summary_parts.append("No concrete output observed — nothing to report")

    summary = " | ".join(summary_parts)[:2000]

    conn.execute(
        """
        INSERT OR REPLACE INTO cycle_summaries
        (pipeline_id, stage_name, cycle, summary, files_changed, test_results, errors_encountered, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            pipeline_id,
            stage,
            cycle,
            summary,
            files_changed,
            test_results,
            errors,
            _now_ts(),
        ),
    )
    conn.commit()


def get_cycle_summary(conn, pipeline_id: str, stage: str, cycle: int) -> str:
    """Get the most recent cycle summary for injection into the next agent."""
    row = conn.execute(
        """
        SELECT summary FROM cycle_summaries
        WHERE pipeline_id = ? AND stage_name = ? AND cycle < ?
        ORDER BY cycle DESC
        LIMIT 1
    """,
        (pipeline_id, stage, cycle),
    ).fetchone()

    return row[0] if row else ""


def parse_stage_output(stage: str, stdout: str) -> dict:
    """Try to parse structured JSON from agent output. Returns parsed dict or {"raw": stdout}."""
    # Try to find JSON by looking for the last complete JSON object
    # Search from end of string backwards for better accuracy
    stdout_stripped = stdout.strip()

    # First try: if the whole output is valid JSON
    try:
        parsed = json.loads(stdout_stripped)
        if isinstance(parsed, (dict, list)):
            return parsed if isinstance(parsed, dict) else {"items": parsed}
    except json.JSONDecodeError:
        pass

    # Second try: find JSON blocks from the end (most likely to be the structured output)
    for i in range(len(stdout_stripped) - 1, -1, -1):
        if stdout_stripped[i] == "}":
            # Find matching opening brace
            depth = 0
            for j in range(i, -1, -1):
                if stdout_stripped[j] == "}":
                    depth += 1
                elif stdout_stripped[j] == "{":
                    depth -= 1
                if depth == 0:
                    candidate = stdout_stripped[j : i + 1]
                    try:
                        parsed = json.loads(candidate)
                        if isinstance(parsed, dict):
                            return parsed
                    except json.JSONDecodeError:
                        pass
                    break
            break

    # Third try: find JSON array from the end
    for i in range(len(stdout_stripped) - 1, -1, -1):
        if stdout_stripped[i] == "]":
            depth = 0
            for j in range(i, -1, -1):
                if stdout_stripped[j] == "]":
                    depth += 1
                elif stdout_stripped[j] == "[":
                    depth -= 1
                if depth == 0:
                    candidate = stdout_stripped[j : i + 1]
                    try:
                        parsed = json.loads(candidate)
                        if isinstance(parsed, list):
                            return {"items": parsed}
                    except json.JSONDecodeError:
                        pass
                    break
            break

    return {"raw": stdout[:4000]}


def _estimate_cost(via: str, model: str, prompt: str, response: str) -> float:
    """Estimate cost based on character-to-token approximation and model pricing table."""
    input_tokens, output_tokens = _count_tokens(prompt, response)
    return _cost_from_tokens(via, model, input_tokens, output_tokens)


def _count_tokens(prompt: str, response: str) -> tuple[int, int]:
    """Approximate token counts from character lengths. ~4 chars per token."""
    return int(len(prompt) / 4), int(len(response) / 4)


def _cost_from_tokens(
    via: str, model: str, input_tokens: int, output_tokens: int
) -> float:
    """Calculate cost from token counts and model pricing."""
    # Look up exact model pricing first
    if model in MODEL_PRICING:
        input_rate, output_rate = MODEL_PRICING[model]
        return (input_tokens * input_rate / 1_000_000) + (
            output_tokens * output_rate / 1_000_000
        )

    # Partial match on model name (e.g., "sonnet" matches "claude-sonnet-4-5")
    for pricing_model, (input_rate, output_rate) in MODEL_PRICING.items():
        if model in pricing_model or pricing_model in model:
            return (input_tokens * input_rate / 1_000_000) + (
                output_tokens * output_rate / 1_000_000
            )

    # Fallback by via provider
    if via == "claude":
        return (input_tokens * 3 / 1_000_000) + (output_tokens * 15 / 1_000_000)
    elif via == "codex":
        return (input_tokens * 2 / 1_000_000) + (output_tokens * 8 / 1_000_000)

    return 0.0


def record_stage_cost(
    conn,
    pipeline_id: str,
    stage: str,
    run_id: str,
    via: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: float,
    duration_seconds: float,
) -> None:
    """Record detailed cost data for a stage execution into cost_log."""
    conn.execute(
        """
        INSERT INTO cost_log
        (run_id, timestamp, provider, model, input_tokens, output_tokens, estimated_cost_usd, tier, via)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            run_id,
            int(time.time()),
            via,
            model,
            input_tokens,
            output_tokens,
            cost_usd,
            stage,
            via,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Role-based agent dispatch (v2 — replaces monolithic stage prompts)
# ---------------------------------------------------------------------------


def spawn_role_agent(
    role_name: str,
    handoff,  # pipeline.handoff.RoleHandoff
    role_config,  # roles.RoleConfig
    skill_context: str,
    worktree_path: str,
    cfg: dict,
    conn,
    complexity: int = 3,
) -> dict:
    """Spawn a specialized agent with role-specific prompt, skills, and model.

    This is the v2 dispatch that replaces the monolithic stage-based dispatch.
    Each agent gets:
    - Role system prompt (domain expertise)
    - Full skill content (not 500-char truncation)
    - Minimal structured handoff (not full pipeline state)
    - Role-appropriate model selection

    Returns same dict shape as spawn_agent for backward compatibility.
    """
    run_id = "run_" + os.urandom(4).hex()

    # Model selection: role config with complexity override
    model_cfg = role_config.model
    via = model_cfg.via
    model = model_cfg.model
    if complexity > 3 and model_cfg.model_high:
        model = model_cfg.model_high

    if via == "none":
        return {
            "ok": True,
            "stdout": "",
            "stderr": "",
            "exit_code": 0,
            "run_id": run_id,
            "duration_seconds": 0.0,
            "cost_usd": 0.0,
            "via": via,
            "model": model,
        }

    # Build the prompt: system prompt + skills + handoff
    prompt_parts = []
    skill_part_idx: int | None = None

    # 1. Role system prompt
    prompt_parts.append(f"# Role: {role_config.name}\n\n{role_config.system_prompt}")

    # 2. Skill injection (full content, respecting budget)
    if skill_context:
        skill_part_idx = len(prompt_parts)
        prompt_parts.append(skill_context)

    # 3. Structured handoff (minimal context)
    prompt_parts.append(handoff.to_prompt())

    # 4. Anti-slop enforcement instructions
    if role_config.anti_slop:
        anti_slop_rules = _build_anti_slop_rules(role_config.anti_slop)
        if anti_slop_rules:
            prompt_parts.append(anti_slop_rules)

    # 5. Research-first gate
    if role_config.research_first:
        prompt_parts.append(
            "\n## REQUIRED: Research-First Gate\n"
            "Before generating ANY code or content, you MUST first:\n"
            "1. Study 3+ existing implementations or reference designs\n"
            "2. Document patterns found (URLs, file paths, code snippets)\n"
            "3. Only THEN generate your output, adapting from references\n"
            "If you skip research, your output will be rejected."
        )

    prompt = "\n\n---\n\n".join(prompt_parts)

    # Estimate tokens and log budget
    prompt_tokens = len(prompt) // 4
    budget = role_config.max_context_tokens
    if prompt_tokens > budget:
        log.warning(
            "Role %s prompt (%d tokens) exceeds budget (%d tokens) — trimming skills",
            role_name,
            prompt_tokens,
            budget,
        )
        # Trim skill context to fit budget
        excess_chars = (prompt_tokens - budget) * 4
        if (
            skill_context
            and skill_part_idx is not None
            and skill_part_idx < len(prompt_parts)
            and len(skill_context) > excess_chars
        ):
            skill_context = skill_context[: len(skill_context) - excess_chars]
            prompt_parts[skill_part_idx] = skill_context
            prompt = "\n\n---\n\n".join(prompt_parts)

    log.info(
        "Spawning role agent: role=%s via=%s model=%s complexity=%d tokens=~%d [%s]",
        role_name,
        via,
        model,
        complexity,
        prompt_tokens,
        run_id,
    )

    # Dispatch via existing subprocess infrastructure
    start = time.time()
    _blocked_env_keys = {"CLAUDECODE", "CLAUDE_CODE_SESSION", "CLAUDE_CODE_ENTRYPOINT"}
    env = {k: v for k, v in os.environ.items() if k not in _blocked_env_keys}

    if via == "codex":
        env["CODEX_FORCE_ALLOW_NESTED"] = "1"
        cmd = [
            "codex",
            "exec",
            "--full-auto",
            "--skip-git-repo-check",
            "--ephemeral",
            "-m",
            model,
            prompt,
        ]
    elif via == "gemini":
        cmd = ["gemini", "--model", model, "--yolo", "--prompt", prompt]
    elif via == "claude":
        cmd = ["claude", "--print", "--model", model, prompt]
    else:
        return {
            "ok": False,
            "stdout": "",
            "stderr": f"Unknown via: {via}",
            "exit_code": 1,
            "run_id": run_id,
            "duration_seconds": 0.0,
            "cost_usd": 0.0,
            "via": via,
            "model": model,
        }

    stage_timeout = STAGE_TIMEOUTS.get(role_name, 600)

    try:
        if worktree_path:
            env["GIT_WORK_TREE"] = worktree_path
            env["GIT_DIR"] = os.path.join(worktree_path, ".git")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=stage_timeout,
            env=env,
            cwd=worktree_path or None,
        )
        duration = time.time() - start
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        ok = result.returncode == 0

        # Cost tracking
        in_tokens, out_tokens = _count_tokens(prompt, stdout)
        cost = _cost_from_tokens(via, model, in_tokens, out_tokens)

        if conn:
            record_stage_cost(
                conn,
                handoff.task_id,
                role_name,
                run_id,
                via,
                model,
                in_tokens,
                out_tokens,
                cost,
                duration,
            )

        log.info(
            "Role agent %s completed: ok=%s duration=%.1fs cost=$%.4f [%s]",
            role_name,
            ok,
            duration,
            cost,
            run_id,
        )

        # Try fallback model on failure
        if not ok and model_cfg.fallback:
            log.info(
                "Role agent %s failed; trying fallback model %s",
                role_name,
                model_cfg.fallback,
            )
            fallback_cmd = list(cmd)
            replaced = False
            # Replace model in command
            for i, arg in enumerate(fallback_cmd):
                if arg == model:
                    fallback_cmd[i] = model_cfg.fallback
                    replaced = True
                    break
            if not replaced:
                if via == "codex" and "-m" in fallback_cmd:
                    mi = fallback_cmd.index("-m")
                    if mi + 1 < len(fallback_cmd):
                        fallback_cmd[mi + 1] = model_cfg.fallback
                        replaced = True
                elif via in ("gemini", "claude") and "--model" in fallback_cmd:
                    mi = fallback_cmd.index("--model")
                    if mi + 1 < len(fallback_cmd):
                        fallback_cmd[mi + 1] = model_cfg.fallback
                        replaced = True
            if not replaced:
                log.warning(
                    "Could not replace model for fallback; retrying original command"
                )
            try:
                fb_result = subprocess.run(
                    fallback_cmd,
                    capture_output=True,
                    text=True,
                    timeout=stage_timeout,
                    env=env,
                    cwd=worktree_path or None,
                )
                fb_duration = time.time() - start
                fb_stdout = fb_result.stdout or ""
                fb_stderr = fb_result.stderr or ""
                if fb_result.returncode == 0:
                    fb_in, fb_out = _count_tokens(prompt, fb_stdout)
                    fb_cost = _cost_from_tokens(via, model_cfg.fallback, fb_in, fb_out)
                    return {
                        "ok": True,
                        "stdout": fb_stdout,
                        "stderr": fb_stderr,
                        "exit_code": 0,
                        "run_id": run_id,
                        "duration_seconds": round(fb_duration, 2),
                        "cost_usd": cost + fb_cost,
                        "via": via,
                        "model": model_cfg.fallback,
                        "input_tokens": fb_in,
                        "output_tokens": fb_out,
                    }
            except Exception as fb_err:
                log.warning(
                    "Fallback model %s also failed: %s", model_cfg.fallback, fb_err
                )

        return {
            "ok": ok,
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": result.returncode,
            "run_id": run_id,
            "duration_seconds": round(duration, 2),
            "cost_usd": cost,
            "via": via,
            "model": model,
            "input_tokens": in_tokens,
            "output_tokens": out_tokens,
        }

    except subprocess.TimeoutExpired:
        duration = time.time() - start
        log.error(
            "Role agent %s timed out after %ds [%s]", role_name, stage_timeout, run_id
        )
        return {
            "ok": False,
            "stdout": "",
            "stderr": f"Timeout after {stage_timeout}s",
            "exit_code": -1,
            "run_id": run_id,
            "duration_seconds": round(duration, 2),
            "cost_usd": 0.0,
            "via": via,
            "model": model,
        }
    except Exception as e:
        duration = time.time() - start
        log.error("Role agent %s error: %s [%s]", role_name, e, run_id)
        return {
            "ok": False,
            "stdout": "",
            "stderr": str(e),
            "exit_code": -1,
            "run_id": run_id,
            "duration_seconds": round(duration, 2),
            "cost_usd": 0.0,
            "via": via,
            "model": model,
        }


def _build_anti_slop_rules(mechanisms: list[str]) -> str:
    """Build anti-slop enforcement rules from mechanism names."""
    rules = {
        "reference_required": "Every claim must include a URL or file path reference",
        "url_required": "Include source URLs for all external information",
        "no_prose": "Use tables and bullet points — no paragraphs of prose",
        "reference_existing": "Reference existing codebase patterns — never propose patterns not already in use",
        "reference_based": "Adapt from downloaded reference designs — never generate from scratch",
        "no_generic_css": "No generic/default CSS — every value must come from a design token or reference",
        "accessibility_check": "Verify WCAG AA compliance for all interactive elements",
        "tests_required": "Write tests alongside implementation — code without tests will be rejected",
        "no_hardcoded_values": "No hardcoded secrets, URLs, or environment-specific values",
        "severity_required": "Every issue must have a severity level: critical/high/medium/low",
        "no_style_nits": "Do not flag purely stylistic preferences — focus on correctness and security",
        "actionable_fixes": "Every issue must include specific fix instructions",
        "humanizer_pass": "Re-read your output — if it sounds AI-generated, rewrite it",
        "no_filler": "No filler words: 'it is worth noting', 'importantly', 'in conclusion'",
        "data_backed": "Every claim must be backed by data, code reference, or URL",
        "preflight_required": "Run preflight checks before any deployment action",
        "monitoring_required": "Set up monitoring/alerting alongside any deployment",
        "evidence_only": "Only report findings backed by direct evidence — never infer, speculate, or fabricate",
        "no_speculation": "If you cannot find evidence, say 'insufficient evidence' — do NOT guess or fill gaps with assumptions",
    }

    applicable = [f"- {rules[m]}" for m in mechanisms if m in rules]
    if not applicable:
        return ""

    return "## Anti-Slop Rules (ENFORCED)\n\n" + "\n".join(applicable)
