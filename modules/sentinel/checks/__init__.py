"""Sentinel health checks — export run_all_checks for the monitor loop."""

from __future__ import annotations

import logging

log = logging.getLogger("sentinel.checks")


async def run_all_checks() -> dict:
    """Run all health checks and return combined results.

    Returns:
        {
            "services": {name: {status, port, ipv4, ipv6, latency_ms, ...}},
            "tokens": [{file, provider, email, status, ttl_seconds, ...}],
            "launchd": {label: {status, pid, exit_code, ...}},
        }
    """
    from .port_health import check_all_ports
    from .token_health import scan_tokens

    results: dict = {}

    try:
        results["services"] = await check_all_ports()
    except Exception:
        log.exception("port health checks failed")
        results["services"] = {}

    try:
        results["tokens"] = await scan_tokens()
    except Exception:
        log.exception("token health checks failed")
        results["tokens"] = []

    try:
        from .launchd import check_launchd_agents
        results["launchd"] = await check_launchd_agents()
    except Exception:
        log.exception("launchd checks failed")
        results["launchd"] = {}

    try:
        from .orchestrator_metrics import run_check
        results["orchestrator"] = await run_check()
    except Exception:
        log.exception("orchestrator metrics check failed")
        results["orchestrator"] = {}

    return results
