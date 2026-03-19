"""Security policy enforcement for autonomous agent commands.

Every command spawned by agents goes through check_command() which returns
one of: allow (proceed), gate (needs human approval), deny (blocked).
"""

from __future__ import annotations

import logging
import re

log = logging.getLogger("aifleet.policy")

# Commands that are always blocked — no override
DENY_PATTERNS: list[tuple[str, str]] = [
    (r"rm\s+-rf\s+/(?:\s|$)", "Recursive delete of root"),
    (r"rm\s+-rf\s+~", "Recursive delete of home"),
    (r"mkfs\.", "Filesystem format"),
    (r"dd\s+if=.+of=/dev/", "Raw disk write"),
    (r":(){ :\|:& };:", "Fork bomb"),
    (r">\s*/dev/sd[a-z]", "Direct device write"),
    (r"git\s+push\s+--force\s+(origin\s+)?(main|master)", "Force push to main/master"),
    (r"DROP\s+(TABLE|DATABASE)", "SQL drop"),
    (r"DELETE\s+FROM\s+\w+\s*;?\s*$", "Unqualified DELETE (no WHERE)"),
    (r"TRUNCATE\s+TABLE", "SQL truncate"),
    (r"launchctl\s+bootout\s+system/", "System-level launchctl bootout"),
    (r"shutdown\s", "System shutdown"),
    (r"reboot\b", "System reboot"),
]

# Commands that require human approval before execution
GATE_PATTERNS: list[tuple[str, str]] = [
    (r"git\s+push", "Git push"),
    (r"git\s+merge", "Git merge"),
    (r"git\s+reset\s+--hard", "Git hard reset"),
    (r"npm\s+publish", "npm publish"),
    (r"docker\s+push", "Docker push"),
    (r"railway\s+up", "Railway deploy"),
    (r"railway\s+deploy", "Railway deploy"),
    (r"fly\s+deploy", "Fly.io deploy"),
    (r"vercel\s+--prod", "Vercel production deploy"),
    (r"kubectl\s+apply", "Kubernetes apply"),
    (r"kubectl\s+delete", "Kubernetes delete"),
    (r"terraform\s+apply", "Terraform apply"),
    (r"terraform\s+destroy", "Terraform destroy"),
    (r"launchctl\s+(bootstrap|bootout|load|unload)", "LaunchAgent modification"),
    (r"pip\s+install\s+--upgrade", "pip upgrade"),
    (r"brew\s+upgrade", "Homebrew upgrade"),
    (r"curl\s+[^|]*\|\s*(bash|sh)", "Remote script pipe execution"),
    (r"python\s+-m\s+pip\s+install\s+[^\n]*-U", "Python package upgrade"),
]


def assess_command_risk(cmd: str) -> str:
    low = cmd.strip().lower()
    if any(
        x in low
        for x in (
            "deploy",
            "publish",
            "terraform",
            "kubectl",
            "launchctl",
            "systemctl",
            "brew upgrade",
            "pip install -u",
        )
    ):
        return "external-side-effect"
    if any(x in low for x in ("git push", "git merge", "git reset --hard")):
        return "high"
    if any(
        x in low
        for x in ("pytest", "bun test", "typecheck", "ruff", "mypy", "tsc --noemit")
    ):
        return "low"
    return "medium"


def policy_decision(cmd: str) -> dict[str, str]:
    action, reason = check_command(cmd)
    return {
        "action": action,
        "reason": reason,
        "risk_tier": assess_command_risk(cmd),
    }


def get_autonomy_profile(config: dict | None) -> str:
    if not isinstance(config, dict):
        return "balanced"
    for key in ("autonomy_profile", "autonomy_mode"):
        value = config.get(key)
        if isinstance(value, str) and value in {
            "safe",
            "balanced",
            "aggressive",
            "supervised",
            "autonomous",
            "full-auto",
        }:
            return value
    return "balanced"


def requires_human_gate(action: str, risk_tier: str, profile: str = "balanced") -> bool:
    if action in {"deny", "gate"}:
        return True
    if profile in {"safe", "supervised"}:
        return risk_tier in {"medium", "high", "external-side-effect"}
    if profile in {"balanced", "autonomous"}:
        return risk_tier in {"high", "external-side-effect"}
    if profile in {"aggressive", "full-auto"}:
        return risk_tier == "external-side-effect"
    return True


def evaluate_command_policy(
    cmd: str, config: dict | None = None
) -> dict[str, str | bool]:
    decision = policy_decision(cmd)
    profile = get_autonomy_profile(config)
    needs_gate = requires_human_gate(
        str(decision["action"]), str(decision["risk_tier"]), profile
    )
    return {
        **decision,
        "autonomy_profile": profile,
        "needs_human_gate": needs_gate,
    }


# Commands that are always safe for autonomous execution
SAFE_PATTERNS: list[str] = [
    r"git\s+add",
    r"git\s+commit",
    r"git\s+checkout\s+-b",
    r"git\s+branch",
    r"git\s+status",
    r"git\s+diff",
    r"git\s+log",
    r"git\s+stash",
    r"git\s+fetch",
    r"git\s+pull\s+--ff-only",
    r"pytest",
    r"bun\s+test",
    r"bun\s+run\s+typecheck",
    r"bun\s+run\s+build",
    r"npm\s+test",
    r"npm\s+run\s+(test|lint|typecheck|build)",
    r"eslint",
    r"ruff\s+(check|format)",
    r"mypy",
    r"tsc\s+--noEmit",
    r"curl\s",
    r"ls\s",
    r"cat\s",
    r"head\s",
    r"tail\s",
    r"wc\s",
    r"grep\s",
    r"find\s",
    r"python3?\s+-c\s",
    r"node\s+-e\s",
]


def check_command(cmd: str) -> tuple[str, str]:
    """Check a command against security policy.

    Returns:
        (action, reason) where action is 'allow', 'gate', or 'deny'.
    """
    if not cmd or not cmd.strip():
        return ("allow", "empty command")

    cmd_stripped = cmd.strip()

    # Check deny list first (highest priority)
    for pattern, reason in DENY_PATTERNS:
        if re.search(pattern, cmd_stripped, re.IGNORECASE):
            log.warning("DENY: %s — %s", reason, cmd_stripped[:80])
            return ("deny", reason)

    # Check if explicitly safe
    for pattern in SAFE_PATTERNS:
        if re.search(pattern, cmd_stripped, re.IGNORECASE):
            return ("allow", "safe command")

    # Check gate list
    for pattern, reason in GATE_PATTERNS:
        if re.search(pattern, cmd_stripped, re.IGNORECASE):
            log.info("GATE: %s — %s", reason, cmd_stripped[:80])
            return ("gate", reason)

    # Default: allow (agent subprocess commands are generally safe)
    return ("allow", "default allow")


def check_file_write(path: str) -> tuple[str, str]:
    """Check if writing to a file path is allowed.

    Returns:
        (action, reason) where action is 'allow', 'gate', or 'deny'.
    """
    deny_paths = [
        ".env",
        "credentials",
        "secrets",
        ".git/config",
        "node_modules/",
        "package-lock.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "/etc/",
        "/usr/",
        "/System/",
    ]
    gate_paths = [
        "config.yaml",
        "config.json",
        ".plist",
        "Dockerfile",
        "docker-compose",
        ".github/workflows/",
    ]

    for dp in deny_paths:
        if dp in path:
            return ("deny", f"Protected path: {dp}")

    for gp in gate_paths:
        if gp in path:
            return ("gate", f"Config file: {gp}")

    return ("allow", "safe path")
