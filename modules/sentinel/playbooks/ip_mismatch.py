"""Playbook: IPv4/IPv6 mismatch notification.

When a service binds only to one address family but clients use the other,
notify with specific fix guidance.
"""

from __future__ import annotations

import logging

from .. import notify

log = logging.getLogger("sentinel.playbooks.ip_mismatch")


async def execute(component: str, context: dict) -> dict:
    """Notify about IPv4/IPv6 mismatch with fix guidance."""
    ipv4 = context.get("ipv4", False)
    ipv6 = context.get("ipv6", False)
    port = context.get("port", "?")

    if ipv6 and not ipv4:
        fix = (
            f"{component} (:{port}) binds IPv6 only (*:{port} IPv6).\n"
            f"Clients using 127.0.0.1 will fail.\n"
            f"Fix: update client configs to use [::1]:{port} or configure service to bind 0.0.0.0"
        )
    elif ipv4 and not ipv6:
        fix = (
            f"{component} (:{port}) binds IPv4 only (127.0.0.1:{port}).\n"
            f"Clients using [::1] will fail.\n"
            f"Fix: configure service to bind :: (all interfaces) or update clients to use 127.0.0.1"
        )
    else:
        fix = f"{component} (:{port}) has unexpected network binding state"

    sent = notify.send(
        f"Sentinel: IP Mismatch — {component}",
        fix,
        component=component,
        sound="Purr",
    )

    log.warning("ip mismatch for %s: ipv4=%s ipv6=%s", component, ipv4, ipv6)
    return {
        "success": True,
        "action": "notify",
        "fix_guidance": fix,
        "notification_sent": sent,
    }
