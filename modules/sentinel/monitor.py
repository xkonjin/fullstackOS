"""Async monitor loop with staggered check intervals.

Port health every 5s, tokens every 30s, launchd every 60s.
Updates SharedState, pushes SSE events, triggers remediation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from .config import (
    LAUNCHD_CHECK_INTERVAL,
    PORT_CHECK_INTERVAL,
    SERVICES,
    TOKEN_CHECK_INTERVAL,
)
from .state import SharedState

log = logging.getLogger("sentinel.monitor")


class Monitor:
    """Orchestrates health checks at different intervals."""

    def __init__(self, state: SharedState):
        self.state = state
        self._last_port_check = 0.0
        self._last_token_check = 0.0
        self._last_launchd_check = 0.0
        self._remediation_engine = None
        self._db = None

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main monitor loop."""
        # Lazy imports to avoid circular deps
        from .checks.port_health import check_all_ports
        from .checks.token_health import scan_tokens
        from .checks.launchd import check_launchd_agents
        from .checks.frozen_detect import check_frozen

        try:
            from .remediate import RemediationEngine
            from .db import SentinelDB
            self._db = SentinelDB()
            self._db.open()
            self._remediation_engine = RemediationEngine(self._db)
        except Exception:
            log.warning("remediation engine unavailable")

        while not stop_event.is_set():
            now = time.time()
            try:
                # Port health — every 5s
                if now - self._last_port_check >= PORT_CHECK_INTERVAL:
                    port_results = await check_all_ports()
                    for name, info in port_results.items():
                        self.state.update_service(name, info)

                    # Check for frozen processes
                    for name, info in port_results.items():
                        if info.get("status") == "down" and info.get("ipv4") is False and info.get("ipv6") is False:
                            svc_cfg = SERVICES.get(name, {})
                            port = svc_cfg.get("port", info.get("port", 0))
                            if port:
                                frozen_result = await check_frozen(port)
                                if frozen_result.get("frozen"):
                                    info["status"] = "frozen"
                                    info["frozen_pid"] = frozen_result.get("pid")
                                    self.state.update_service(name, info)
                                    await self._handle_issue(name, "frozen", info)

                        elif info.get("status") == "down":
                            await self._handle_issue(name, "down", info)

                        # IPv4/IPv6 mismatch
                        ipv4 = info.get("ipv4", False)
                        ipv6 = info.get("ipv6", False)
                        if ipv4 != ipv6 and (ipv4 or ipv6):
                            await self._handle_issue(name, "ip_mismatch", info)

                    self._last_port_check = now

                # Token health — every 30s
                if now - self._last_token_check >= TOKEN_CHECK_INTERVAL:
                    tokens = await scan_tokens()
                    self.state.update_tokens(tokens)

                    for tok in tokens:
                        if tok.get("status") in ("expired", "error"):
                            await self._handle_issue(
                                tok.get("provider", "unknown"),
                                "token_expired",
                                tok,
                            )

                    self._last_token_check = now

                # LaunchAgent health — every 60s
                if now - self._last_launchd_check >= LAUNCHD_CHECK_INTERVAL:
                    launchd_results = await check_launchd_agents()
                    for label, info in launchd_results.items():
                        if info.get("status") != "running":
                            self.state.add_incident({
                                "severity": "warning",
                                "component": label,
                                "signature": f"launchd:{label}",
                                "summary": f"LaunchAgent {label} is {info.get('status', 'unknown')}",
                            })
                    self._last_launchd_check = now

                # Push SSE update
                snapshot = self.state.get_snapshot()
                self.state.push_sse("state_update", snapshot)

            except Exception:
                log.exception("monitor cycle failed")

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=PORT_CHECK_INTERVAL)
            except asyncio.TimeoutError:
                pass

        if self._db:
            self._db.close()

    async def _handle_issue(self, component: str, issue_type: str, context: dict) -> None:
        """Handle a detected issue — log incident, notify, optionally remediate."""
        from . import notify

        severity = "critical" if issue_type in ("frozen", "down") else "warning"

        self.state.add_incident({
            "severity": severity,
            "component": component,
            "signature": f"{issue_type}:{component}",
            "summary": _build_summary(component, issue_type, context),
        })

        if self._db:
            self._db.log_incident(
                severity=severity,
                component=component,
                signature=f"{issue_type}:{component}",
                summary=_build_summary(component, issue_type, context),
            )

        # Notify
        if severity == "critical":
            notify.send(
                f"Sentinel: {component}",
                _build_summary(component, issue_type, context),
                component=component,
            )

        # Auto-remediate if active
        if self._remediation_engine and self.state.is_auto_heal_active():
            playbook = _issue_to_playbook(issue_type)
            if playbook:
                await self._remediation_engine.execute(playbook, component, context)


def _build_summary(component: str, issue_type: str, context: dict) -> str:
    if issue_type == "frozen":
        pid = context.get("frozen_pid", "?")
        return f"{component} frozen (PID {pid}, 0% CPU, not accepting connections)"
    elif issue_type == "down":
        port = context.get("port", "?")
        return f"{component} is down (port {port} unreachable)"
    elif issue_type == "ip_mismatch":
        ipv4 = "✓" if context.get("ipv4") else "✗"
        ipv6 = "✓" if context.get("ipv6") else "✗"
        return f"{component} IPv4/IPv6 mismatch (IPv4:{ipv4} IPv6:{ipv6})"
    elif issue_type == "token_expired":
        email = context.get("email", "?")
        return f"{component} token expired for {email}"
    return f"{component}: {issue_type}"


def _issue_to_playbook(issue_type: str) -> Optional[str]:
    return {
        "frozen": "cliproxy_frozen",
        "down": "cliproxy_frozen",
        "token_expired": "token_reauth",
        "ip_mismatch": "ip_mismatch",
    }.get(issue_type)
