"""macOS notifications via osascript — zero external deps."""

from __future__ import annotations

import logging
import subprocess
import time

log = logging.getLogger("sentinel.notify")

# Rate limit: component → last_notify_ts
_last_notify: dict[str, float] = {}
_RATE_LIMIT = 300  # 5 minutes


def send(
    title: str,
    message: str,
    component: str = "",
    subtitle: str = "",
    sound: str = "Blow",
) -> bool:
    """Send a macOS notification. Returns True if sent, False if rate-limited."""
    now = time.time()
    key = component or title
    last = _last_notify.get(key, 0)
    if now - last < _RATE_LIMIT:
        log.debug("notification rate-limited for %s (%.0fs remaining)", key, _RATE_LIMIT - (now - last))
        return False

    parts = [f'display notification "{_escape(message)}"']
    parts.append(f'with title "{_escape(title)}"')
    if subtitle:
        parts.append(f'subtitle "{_escape(subtitle)}"')
    if sound:
        parts.append(f'sound name "{sound}"')

    script = " ".join(parts)
    try:
        subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=5,
        )
        _last_notify[key] = now
        log.info("notification sent: %s — %s", title, message)
        return True
    except Exception:
        log.exception("failed to send notification")
        return False


def _escape(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", " ")
        .replace("\r", "")
    )
