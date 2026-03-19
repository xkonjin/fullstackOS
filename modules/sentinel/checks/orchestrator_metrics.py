"""Sentinel check: Orchestrator performance metrics from UsageDB.

Reads the orchestrator's SQLite usage database directly and computes:
- Per-provider error rates (alert if >20%)
- Cost per hour estimate
- Token expiry countdown (from token files)
- Provider availability

This makes Sentinel the single pane of glass for fullstackOS health.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("sentinel.checks.orchestrator_metrics")

USAGE_DB_PATH = os.path.expanduser("~/.claudemax/usage.db")
TOKEN_DIR = os.path.expanduser("~/.cli-proxy-api")
ORCHESTRATOR_URL = "http://127.0.0.1:8318"

# Error rate threshold for alerts
ERROR_RATE_ALERT = 0.20  # 20%
# Token expiry warning threshold (hours)
TOKEN_EXPIRY_WARN_HOURS = 6


async def run_check() -> dict[str, Any]:
    """Run orchestrator metrics check. Returns structured result for sentinel."""
    results: dict[str, Any] = {
        "check": "orchestrator_metrics",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "healthy",
        "details": {},
        "alerts": [],
    }

    # 1. Check orchestrator is reachable
    orch_healthy = await _check_orchestrator_health()
    results["details"]["orchestrator_reachable"] = orch_healthy
    if not orch_healthy:
        results["status"] = "critical"
        results["alerts"].append("Orchestrator :8318 unreachable")
        return results

    # 2. Read usage metrics from SQLite
    usage = _read_usage_metrics()
    results["details"]["usage"] = usage

    # Check per-provider error rates
    for provider in usage.get("providers", []):
        if provider["error_rate"] > ERROR_RATE_ALERT and provider["request_count"] >= 5:
            results["status"] = "degraded"
            results["alerts"].append(
                f"{provider['provider']} error rate {provider['error_rate']:.0%} "
                f"({provider['error_count']}/{provider['request_count']} requests)"
            )

    # 3. Check token health
    token_health = _check_token_health()
    results["details"]["tokens"] = token_health

    for token in token_health.get("expiring_soon", []):
        results["status"] = "degraded" if results["status"] == "healthy" else results["status"]
        results["alerts"].append(
            f"Token {token['id']} expires in {token['hours_remaining']:.1f}h"
        )

    for token in token_health.get("expired", []):
        results["status"] = "critical"
        results["alerts"].append(f"Token {token['id']} EXPIRED")

    # 4. Compute cost estimate
    cost = _estimate_hourly_cost(usage)
    results["details"]["cost_per_hour_usd"] = cost

    return results


async def _check_orchestrator_health() -> bool:
    """Probe orchestrator health endpoint."""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{ORCHESTRATOR_URL}/health")
            return resp.status_code == 200
    except Exception as e:
        log.warning("Orchestrator health check failed: %s: %s", type(e).__name__, e)
        return False


def _read_usage_metrics() -> dict[str, Any]:
    """Read last-hour and last-24h metrics from UsageDB."""
    if not os.path.exists(USAGE_DB_PATH):
        return {"error": "usage.db not found", "providers": []}

    try:
        conn = sqlite3.connect(f"file:{USAGE_DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row

        now_ms = int(time.time() * 1000)
        hour_ago = now_ms - 3600_000
        day_ago = now_ms - 86400_000

        # Per-provider stats (last 24h)
        providers = []
        rows = conn.execute("""
            SELECT provider,
                   COUNT(*) as request_count,
                   SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
                   SUM(input_tokens + output_tokens) as total_tokens,
                   AVG(CASE WHEN status = 'success' THEN latency_ms ELSE NULL END) as avg_latency_ms
            FROM requests WHERE timestamp > ?
            GROUP BY provider ORDER BY request_count DESC
        """, (day_ago,)).fetchall()

        for row in rows:
            req_count = row["request_count"]
            err_count = row["error_count"]
            providers.append({
                "provider": row["provider"],
                "request_count": req_count,
                "error_count": err_count,
                "error_rate": err_count / req_count if req_count > 0 else 0,
                "total_tokens": row["total_tokens"] or 0,
                "avg_latency_ms": round(row["avg_latency_ms"] or 0),
            })

        # Aggregate stats
        agg_row = conn.execute("""
            SELECT COUNT(*) as total_requests,
                   SUM(input_tokens + output_tokens) as total_tokens
            FROM requests WHERE timestamp > ?
        """, (hour_ago,)).fetchone()

        conn.close()

        return {
            "providers": providers,
            "last_hour": {
                "requests": agg_row["total_requests"] if agg_row else 0,
                "tokens": agg_row["total_tokens"] or 0 if agg_row else 0,
            },
        }
    except Exception as e:
        log.error("Failed to read usage DB: %s", e)
        return {"error": str(e), "providers": []}


def _check_token_health() -> dict[str, Any]:
    """Scan token files for expiry status."""
    result: dict[str, Any] = {
        "total": 0,
        "healthy": 0,
        "expiring_soon": [],
        "expired": [],
    }

    if not os.path.isdir(TOKEN_DIR):
        return result

    now = time.time()

    for filename in os.listdir(TOKEN_DIR):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(TOKEN_DIR, filename)
        try:
            with open(filepath) as f:
                data = json.load(f)

            if data.get("disabled"):
                continue

            result["total"] += 1

            # Extract expiry based on token type
            expiry_str = None
            token_type = data.get("type", "")

            if token_type in ("claude", "antigravity", "codex", "kimi"):
                expiry_str = data.get("expired")
            elif token_type == "gemini":
                expiry_str = data.get("token", {}).get("expiry")

            if not expiry_str:
                result["healthy"] += 1
                continue

            expiry_ts = datetime.fromisoformat(expiry_str.replace("Z", "+00:00")).timestamp()
            hours_remaining = (expiry_ts - now) / 3600

            token_id = f"{token_type}-{filename}"

            if hours_remaining < 0:
                result["expired"].append({
                    "id": token_id,
                    "hours_remaining": hours_remaining,
                })
            elif hours_remaining < TOKEN_EXPIRY_WARN_HOURS:
                result["expiring_soon"].append({
                    "id": token_id,
                    "hours_remaining": round(hours_remaining, 1),
                })
            else:
                result["healthy"] += 1

        except (json.JSONDecodeError, KeyError, ValueError):
            continue

    return result


# Rough cost estimates per 1M tokens (input+output combined)
_COST_PER_MTOK = {
    "claude": 12.0,
    "antigravity": 12.0,
    "codex": 10.0,
    "gemini": 5.0,
    "glm": 1.5,
    "minimax": 2.0,
    "kimi": 3.0,
    "openrouter": 15.0,
}


def _estimate_hourly_cost(usage: dict[str, Any]) -> float:
    """Estimate cost per hour from last-hour token usage."""
    last_hour = usage.get("last_hour", {})
    tokens = last_hour.get("tokens", 0)
    if tokens == 0:
        return 0.0

    # Use weighted average cost across providers
    providers = usage.get("providers", [])
    if not providers:
        return 0.0

    total_cost = 0.0
    for p in providers:
        cost_per_mtok = _COST_PER_MTOK.get(p["provider"], 10.0)
        # Proportional cost based on this provider's share of tokens
        provider_tokens = p.get("total_tokens", 0)
        total_cost += (provider_tokens / 1_000_000) * cost_per_mtok

    # Scale to hourly (providers data is 24h, divide by 24)
    return round(total_cost / 24, 4)
