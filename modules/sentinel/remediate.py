"""Playbook engine with cooldowns, rate limits, and flap detection."""

from __future__ import annotations

import logging
import time
from typing import Optional

from .config import COOLDOWN_SECONDS, FLAP_THRESHOLD, MAX_RESTARTS_PER_HOUR
from .db import SentinelDB
from . import notify

log = logging.getLogger("sentinel.remediate")

# Track consecutive failures per component for flap detection
_consecutive_failures: dict[str, int] = {}
_cooldowns: dict[str, float] = {}  # component → last_action_ts
_disabled: set[str] = set()         # components with flap-disabled auto-heal


class RemediationEngine:
    """Coordinates playbook execution with safety guards."""

    def __init__(self, db: Optional[SentinelDB] = None):
        self.db = db

    def can_remediate(self, component: str) -> tuple[bool, str]:
        """Check if remediation is allowed for this component."""
        if component in _disabled:
            return False, f"auto-heal disabled for {component} (flap detected)"

        now = time.time()

        # Cooldown check
        last = _cooldowns.get(component, 0)
        if now - last < COOLDOWN_SECONDS:
            remaining = int(COOLDOWN_SECONDS - (now - last))
            return False, f"cooldown active ({remaining}s remaining)"

        # Rate limit: max N restarts per hour
        if self.db:
            one_hour_ago = now - 3600
            count = self.db.count_actions_since(one_hour_ago, component, "restart")
            if count >= MAX_RESTARTS_PER_HOUR:
                return False, f"rate limit reached ({count}/{MAX_RESTARTS_PER_HOUR} restarts/hour)"

        return True, "ok"

    async def execute(self, playbook_name: str, component: str, context: dict) -> dict:
        """Execute a remediation playbook with safety guards."""
        allowed, reason = self.can_remediate(component)
        if not allowed:
            log.warning("remediation blocked for %s: %s", component, reason)
            return {"success": False, "reason": reason, "action": "blocked"}

        # Import and run the playbook
        try:
            playbook = _get_playbook(playbook_name)
            if playbook is None:
                return {"success": False, "reason": f"unknown playbook: {playbook_name}", "action": "error"}

            log.info("executing playbook %s for %s", playbook_name, component)
            result = await playbook.execute(component, context)

            # Update cooldown
            _cooldowns[component] = time.time()

            # Log action
            if self.db:
                self.db.log_action(
                    action_type="restart" if "restart" in playbook_name else "remediate",
                    target=component,
                    result="success" if result.get("success") else "failure",
                    detail=result,
                )

            # Track flap detection
            if result.get("success"):
                _consecutive_failures[component] = 0
            else:
                failures = _consecutive_failures.get(component, 0) + 1
                _consecutive_failures[component] = failures
                if failures >= FLAP_THRESHOLD:
                    _disabled.add(component)
                    log.error(
                        "flap detected for %s (%d consecutive failures) — disabling auto-heal",
                        component, failures,
                    )
                    notify.send(
                        "Sentinel: Auto-heal Disabled",
                        f"{component} flapping ({failures} failed fixes). Manual intervention required.",
                        component=component,
                        sound="Sosumi",
                    )

            return result

        except Exception as e:
            log.exception("playbook %s failed for %s", playbook_name, component)
            failures = _consecutive_failures.get(component, 0) + 1
            _consecutive_failures[component] = failures
            if failures >= FLAP_THRESHOLD:
                _disabled.add(component)
            return {"success": False, "reason": str(e), "action": "error"}

    def reset_flap(self, component: str) -> None:
        """Manually reset flap detection for a component."""
        _disabled.discard(component)
        _consecutive_failures.pop(component, None)
        log.info("flap detection reset for %s", component)

    def is_disabled(self, component: str) -> bool:
        return component in _disabled


def _get_playbook(name: str):
    """Lazy-load a playbook module by name."""
    try:
        if name == "cliproxy_frozen":
            from .playbooks import cliproxy_frozen
            return cliproxy_frozen
        elif name == "orch_stale":
            from .playbooks import orch_stale
            return orch_stale
        elif name == "token_reauth":
            from .playbooks import token_reauth
            return token_reauth
        elif name == "ip_mismatch":
            from .playbooks import ip_mismatch
            return ip_mismatch
    except ImportError:
        log.warning("playbook %s not available", name)
    return None
