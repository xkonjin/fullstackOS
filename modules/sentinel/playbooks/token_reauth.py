"""Playbook: Token re-authentication notification.

Cannot auto-fix expired OAuth tokens — notify user with re-auth instructions.
"""

from __future__ import annotations

import logging

from .. import notify

log = logging.getLogger("sentinel.playbooks.token_reauth")

_REAUTH_COMMANDS = {
    "claude": "cliproxy auth refresh --provider claude",
    "antigravity": "cliproxy auth refresh --provider antigravity",
    "codex": "cliproxy auth refresh --provider codex",
    "gemini": "cliproxy auth refresh --provider gemini",
}


async def execute(component: str, context: dict) -> dict:
    """Notify user about expired token with re-auth instructions."""
    provider = context.get("provider", component)
    email = context.get("email", "unknown")
    cmd = _REAUTH_COMMANDS.get(provider, f"cliproxy auth refresh --provider {provider}")

    message = (
        f"{provider.title()} token expired for {email}.\n"
        f"Run: {cmd}"
    )

    sent = notify.send(
        f"Sentinel: Token Expired — {provider.title()}",
        message,
        component=f"token:{provider}",
        subtitle=email,
        sound="Purr",
    )

    log.info("token reauth notification for %s/%s (sent=%s)", provider, email, sent)
    return {
        "success": True,
        "action": "notify",
        "provider": provider,
        "email": email,
        "command": cmd,
        "notification_sent": sent,
    }
