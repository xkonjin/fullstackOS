"""Sentinel remediation playbooks."""

from __future__ import annotations

PLAYBOOKS = {
    "cliproxy_frozen": "modules.sentinel.playbooks.cliproxy_frozen",
    "orch_stale": "modules.sentinel.playbooks.orch_stale",
    "token_reauth": "modules.sentinel.playbooks.token_reauth",
    "ip_mismatch": "modules.sentinel.playbooks.ip_mismatch",
}
