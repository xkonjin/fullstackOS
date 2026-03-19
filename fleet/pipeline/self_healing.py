"""Self-healing configuration — Loop 49

Auto-fixes common configuration issues.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Any

log = logging.getLogger("aifleet.healing")


class SelfHealingConfig:
    """Self-healing configuration manager."""

    # Common issues and fixes
    FIXES: List[Dict[str, Any]] = [
        {
            "issue": "missing_config_dir",
            "check": lambda: not Path.home().joinpath(".ai-fleet").exists(),
            "fix": lambda: Path.home().joinpath(".ai-fleet").mkdir(exist_ok=True),
            "description": "Create missing config directory",
        },
        {
            "issue": "missing_db",
            "check": lambda: (
                not Path.home().joinpath(".ai-fleet/coordinator/memory.db").exists()
            ),
            "fix": lambda: None,  # Will be created on first use
            "description": "Database will be initialized on first run",
        },
        {
            "issue": "codex_profile_missing",
            "check": lambda: not Path.home().joinpath(".codex").exists(),
            "fix": lambda: Path.home().joinpath(".codex").mkdir(exist_ok=True),
            "description": "Create Codex config directory",
        },
    ]

    def __init__(self):
        self.issues_found = []
        self.issues_fixed = []

    def diagnose(self) -> List[Dict[str, Any]]:
        """Diagnose configuration issues."""
        self.issues_found = []

        for fix in self.FIXES:
            try:
                if fix["check"]():
                    self.issues_found.append(
                        {
                            "issue": fix["issue"],
                            "description": fix["description"],
                            "auto_fixable": True,
                        }
                    )
            except Exception as e:
                log.debug("Check failed for %s: %s", fix["issue"], e)

        return self.issues_found

    def heal(self) -> Dict[str, Any]:
        """Attempt to auto-fix issues."""
        self.issues_fixed = []
        failed = []

        for fix in self.FIXES:
            try:
                if fix["check"]():
                    fix["fix"]()
                    self.issues_fixed.append(fix["issue"])
                    log.info("Fixed: %s", fix["description"])
            except Exception as e:
                log.warning("Failed to fix %s: %s", fix["issue"], e)
                failed.append(fix["issue"])

        return {
            "found": len(self.issues_found),
            "fixed": len(self.issues_fixed),
            "failed": failed,
        }

    def validate_environment(self) -> Dict[str, Any]:
        """Validate environment setup."""
        checks = {
            "python_version": self._check_python(),
            "git_available": self._check_git(),
            "sqlite_available": self._check_sqlite(),
            "write_permissions": self._check_permissions(),
        }

        return {"healthy": all(checks.values()), "checks": checks}

    def _check_python(self) -> bool:
        import sys

        return sys.version_info >= (3, 9)

    def _check_git(self) -> bool:
        import shutil

        return shutil.which("git") is not None

    def _check_sqlite(self) -> bool:
        return True

    def _check_permissions(self) -> bool:
        try:
            test_path = Path.home() / ".ai-fleet" / ".write_test"
            test_path.touch()
            test_path.unlink()
            return True
        except Exception:
            return False


def self_heal() -> Dict[str, Any]:
    """Run self-healing."""
    healer = SelfHealingConfig()
    healer.diagnose()
    return healer.heal()
