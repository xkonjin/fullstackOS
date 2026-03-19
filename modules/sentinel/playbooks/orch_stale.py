"""Playbook: Orchestrator stale token state recovery.

When orchestrator caches expired tokens despite fresh ones on disk,
restart it to force a rescan.
"""

from __future__ import annotations

import asyncio
import logging

from ..checks.port_health import probe_service
from ..config import SERVICES
from .. import notify

log = logging.getLogger("sentinel.playbooks.orch_stale")


async def execute(component: str, context: dict) -> dict:
    """Restart orchestrator to rescan tokens."""
    svc = SERVICES.get("Orchestrator")
    label = svc.get("launchd_label") if svc else "com.claudemax.orchestrator"

    log.info("restarting Orchestrator to clear stale token cache")

    try:
        import os
        uid = os.getuid()

        proc = await asyncio.create_subprocess_exec(
            "launchctl", "kickstart", "-k", f"gui/{uid}/{label}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)

        if proc.returncode != 0:
            err = stderr.decode().strip()
            log.error("orchestrator restart failed: %s", err)
            notify.send(
                "Sentinel: Orchestrator restart failed",
                f"Could not restart Orchestrator: {err}",
                component="Orchestrator",
            )
            return {"success": False, "reason": err, "action": "kickstart_failed"}

    except asyncio.TimeoutError:
        return {"success": False, "reason": "kickstart timed out", "action": "timeout"}

    await asyncio.sleep(3)

    # Verify
    if svc:
        result = await probe_service("Orchestrator", svc)
        if result.get("status") == "healthy":
            log.info("Orchestrator recovered — token cache refreshed")
            notify.send(
                "Sentinel: Orchestrator restarted",
                "Token cache cleared. Orchestrator is healthy.",
                component="Orchestrator",
                sound="Glass",
            )
            return {"success": True, "action": "kickstart", "new_status": "healthy"}

    return {"success": False, "reason": "orchestrator did not recover", "action": "kickstart_no_recovery"}
