"""Sentinel configuration — service definitions, intervals, cooldowns."""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Service definitions
# ---------------------------------------------------------------------------
SERVICES: dict[str, dict] = {
    "CLIProxyAPI": {
        "port": 8317,
        "health_path": "/",
        "launchd_label": "homebrew.mxcl.cliproxyapi",
    },
    "Orchestrator": {
        "port": 8318,
        "health_path": "/health",
        "launchd_label": "com.claudemax.orchestrator",
    },
    "Fleet Gateway": {
        "port": 4105,
        "health_path": "/v1/health",
        "launchd_label": "com.ai.fleet.gateway",
    },
}

# ---------------------------------------------------------------------------
# Check intervals (seconds)
# ---------------------------------------------------------------------------
PORT_CHECK_INTERVAL = 5
TOKEN_CHECK_INTERVAL = 30
LAUNCHD_CHECK_INTERVAL = 60

# ---------------------------------------------------------------------------
# Remediation
# ---------------------------------------------------------------------------
COOLDOWN_SECONDS = 120           # 2 min per service
MAX_RESTARTS_PER_HOUR = 3
FLAP_THRESHOLD = 3               # consecutive failed fixes → disable auto-heal

# ---------------------------------------------------------------------------
# Token paths
# ---------------------------------------------------------------------------
TOKEN_DIR = Path.home() / ".cli-proxy-api"

# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------
NOTIFY_RATE_LIMIT = 300          # 1 per component per 5 min (seconds)

# ---------------------------------------------------------------------------
# LaunchAgent labels
# ---------------------------------------------------------------------------
LAUNCHD_LABELS = [
    "homebrew.mxcl.cliproxyapi",
    "com.claudemax.orchestrator",
    "com.ai.fleet.gateway",
    "com.cliproxyapi.tokenrefresh",
    "com.cliproxyapi.healthcheck",
    "com.ai.cliproxyapi.monitor",
]

# ---------------------------------------------------------------------------
# Frozen detection
# ---------------------------------------------------------------------------
FROZEN_CPU_THRESHOLD = 0.5       # % — below this for 2 samples = frozen
FROZEN_ACCEPT_TIMEOUT = 3.0      # seconds to wait for HTTP response
TCP_CONNECT_TIMEOUT = 2.0        # seconds for TCP handshake
HTTP_PROBE_TIMEOUT = 4.0         # seconds for full HTTP probe
PROBE_SERVICE_TIMEOUT = 10.0     # seconds — overall cap per probe_service() call

# ---------------------------------------------------------------------------
# Web UI
# ---------------------------------------------------------------------------
WEB_PORT = 8600
