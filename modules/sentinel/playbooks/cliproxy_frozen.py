"""Playbook: CLIProxyAPI frozen process recovery.

Strategy:
  1. launchctl kickstart -k to force-restart the service
  2. Wait 3s for process to come up
  3. Re-probe health endpoint to verify recovery
"""

from __future__ import annotations

import asyncio
import logging

from ..checks.port_health import probe_service
from ..config import SERVICES
from .. import notify

log = logging.getLogger("sentinel.playbooks.cliproxy_frozen")


async def execute(component: str, context: dict) -> dict:
    """Execute frozen process recovery."""
    svc = SERVICES.get(component)
    label = svc.get("launchd_label") if svc else None

    if not label:
        notify.send(
            f"Sentinel: {component} frozen",
            f"{component} is frozen but has no LaunchAgent label for auto-restart.",
            component=component,
        )
        return {"success": False, "reason": "no launchd label", "action": "notify_only"}

    log.info("kickstarting %s (label: %s)", component, label)

    # Step 1: Force restart via launchctl
    try:
        proc = await asyncio.create_subprocess_exec(
            "launchctl", "kickstart", "-k", f"gui/{_get_uid()}/{label}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)

        if proc.returncode != 0:
            err = stderr.decode().strip()
            log.error("kickstart failed for %s: %s", label, err)
            notify.send(
                f"Sentinel: Restart failed",
                f"Could not restart {component}: {err}",
                component=component,
            )
            return {"success": False, "reason": err, "action": "kickstart_failed"}

    except asyncio.TimeoutError:
        return {"success": False, "reason": "kickstart timed out", "action": "timeout"}

    # Step 2: Wait for process to start
    await asyncio.sleep(3)

    # Step 3: Verify recovery
    if svc:
        result = await probe_service(component, svc)
        if result.get("status") == "healthy":
            log.info("%s recovered successfully", component)
            notify.send(
                f"Sentinel: {component} recovered",
                f"{component} has been restarted and is healthy.",
                component=component,
                sound="Glass",
            )
            return {"success": True, "action": "kickstart", "new_status": "healthy"}

    return {"success": False, "reason": "service did not recover after restart", "action": "kickstart_no_recovery"}


def _get_uid() -> int:
    """Get current user's UID."""
    import os
    return os.getuid()
