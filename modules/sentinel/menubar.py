"""Sentinel macOS menu bar integration via rumps.

rumps requires the main thread (AppKit). If rumps is not installed,
this module exposes a no-op fallback so headless mode works everywhere.
"""

from __future__ import annotations

import logging
import webbrowser
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

log = logging.getLogger("sentinel.menubar")

DASHBOARD_URL = "http://localhost:8600"

STATUS_EMOJI = {
    "healthy": "🟢",
    "degraded": "🟡",
    "critical": "🔴",
}

SERVICE_STATUS_ICONS = {
    "healthy": "✅",
    "degraded": "⚠️",
    "down": "❌",
    "frozen": "🧊",
    "unknown": "❓",
}

try:
    import rumps
except ImportError:
    rumps = None  # type: ignore[assignment]


def create_menubar(
    state_getter: Callable[[], dict],
    action_callback: Callable[[str, str], None],
) -> Optional[Any]:
    """Create and return a SentinelMenuBar instance, or None if rumps unavailable."""
    if rumps is None:
        log.info("rumps not installed — skipping menu bar")
        return None
    return SentinelMenuBar(state_getter, action_callback)


if rumps is not None:

    class SentinelMenuBar(rumps.App):
        """Menu bar status icon for Sentinel."""

        def __init__(
            self,
            state_getter: Callable[[], dict],
            action_callback: Callable[[str, str], None],
        ):
            super().__init__("🟢", quit_button=None)
            self._state_getter = state_getter
            self._action_callback = action_callback

        @rumps.timer(3)
        def refresh(self, _: Any) -> None:
            state = self._state_getter()
            self.title = STATUS_EMOJI.get(state.get("overall", "healthy"), "🟢")
            self._rebuild_menu(state)

        def _rebuild_menu(self, state: dict) -> None:
            items: list[Any] = []

            # Service status rows
            for name, info in state.get("services", {}).items():
                status = info.get("status", "unknown")
                icon = SERVICE_STATUS_ICONS.get(status, "❓")
                port = info.get("port", "")
                label = f"{name:<18} {icon}  :{port}" if port else f"{name:<18} {icon}"
                item = rumps.MenuItem(label, callback=self._on_service_click)
                item._sentinel_service = name  # stash for callback
                items.append(item)

            items.append(rumps.separator)

            # Token summary
            tokens = state.get("tokens", [])
            healthy = sum(1 for t in tokens if t.get("status") == "healthy")
            total = len(tokens)
            if total:
                token_item = rumps.MenuItem(f"Tokens ({healthy}/{total} healthy)")
                for t in tokens:
                    sub = rumps.MenuItem(
                        f"{t.get('provider', '?')} — {t.get('email', '?')} "
                        f"{'✅' if t.get('status') == 'healthy' else '⚠️'}"
                    )
                    token_item.add(sub)
                items.append(token_item)
                items.append(rumps.separator)

            # Dashboard link
            items.append(rumps.MenuItem("🌐 Open Dashboard", callback=self._open_dashboard))

            # Auto-heal toggle
            paused = state.get("auto_heal_paused", False)
            heal_label = "▶️ Resume Auto-Heal" if paused else "⏸ Pause Auto-Heal (15m)"
            items.append(rumps.MenuItem(heal_label, callback=self._toggle_auto_heal))

            # Last incident
            incidents = state.get("incidents", [])
            if incidents:
                ts = incidents[0].get("timestamp", "")
                items.append(rumps.MenuItem(f"📋 Last Incident: {ts[:19]}"))

            items.append(rumps.separator)
            items.append(rumps.MenuItem("Quit Sentinel", callback=self._quit))

            self.menu.clear()
            for item in items:
                self.menu.add(item)

        def _open_dashboard(self, _: Any) -> None:
            webbrowser.open(DASHBOARD_URL)

        def _on_service_click(self, sender: Any) -> None:
            name = getattr(sender, "_sentinel_service", None)
            if name:
                self._action_callback("restart", name)

        def _toggle_auto_heal(self, _: Any) -> None:
            self._action_callback("toggle_auto_heal", "")

        def _quit(self, _: Any) -> None:
            rumps.quit_application()
