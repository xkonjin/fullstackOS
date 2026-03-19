"""Evidence Collector — collects proof of task completion.

Every task must produce evidence, not assertions.
Evidence types: test_results, screenshots, logs, diffs, metrics.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

log = logging.getLogger("aifleet.evidence")


@dataclass
class EvidenceItem:
    """A single piece of evidence for task completion."""

    type: (
        str  # "test_result" | "log" | "diff" | "metric" | "screenshot" | "health_check"
    )
    source: str  # where this came from
    content: str  # the actual evidence
    timestamp: int
    passed: bool


class EvidenceCollector:
    """Collects and evaluates evidence of task completion."""

    def collect_test_evidence(self, cwd: str, test_command: str) -> EvidenceItem:
        """Run a test command and capture results as evidence."""
        ts = int(time.time())
        try:
            result = subprocess.run(
                test_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300,
                cwd=cwd,
            )
            output = (result.stdout + "\n" + result.stderr).strip()
            passed = result.returncode == 0
            log.info(
                "Test evidence: command=%r passed=%s cwd=%s", test_command, passed, cwd
            )
        except subprocess.TimeoutExpired:
            output = f"Test command timed out after 300s: {test_command}"
            passed = False
            log.warning("Test evidence timed out: command=%r cwd=%s", test_command, cwd)
        except Exception as exc:
            output = f"Test command failed: {exc}"
            passed = False
            log.error("Test evidence error: command=%r error=%s", test_command, exc)

        return EvidenceItem(
            type="test_result",
            source=test_command,
            content=output[:50_000],
            timestamp=ts,
            passed=passed,
        )

    def collect_diff_evidence(
        self, cwd: str, base_branch: str = "main"
    ) -> EvidenceItem:
        """Collect git diff as evidence of changes made."""
        ts = int(time.time())
        try:
            result = subprocess.run(
                ["git", "diff", f"{base_branch}..HEAD", "--stat"],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=cwd,
            )
            stat = result.stdout.strip()

            diff_result = subprocess.run(
                ["git", "diff", f"{base_branch}..HEAD"],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=cwd,
            )
            full_diff = diff_result.stdout.strip()

            content = f"## Diff stat\n{stat}\n\n## Full diff\n{full_diff}"
            passed = bool(stat)
            log.info(
                "Diff evidence: base=%s has_changes=%s cwd=%s", base_branch, passed, cwd
            )
        except Exception as exc:
            content = f"Failed to collect diff: {exc}"
            passed = False
            log.error("Diff evidence error: %s", exc)

        return EvidenceItem(
            type="diff",
            source=f"git diff {base_branch}..HEAD",
            content=content[:50_000],
            timestamp=ts,
            passed=passed,
        )

    def collect_health_evidence(self, url: str) -> EvidenceItem:
        """Check a health endpoint and capture the response."""
        ts = int(time.time())
        try:
            resp = urlopen(url, timeout=10)
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace")[:5000]
            passed = 200 <= status < 400
            content = f"HTTP {status}\n{body}"
            log.info("Health evidence: url=%s status=%d passed=%s", url, status, passed)
        except URLError as exc:
            content = f"Health check failed: {exc.reason}"
            passed = False
            log.warning("Health evidence failed: url=%s error=%s", url, exc.reason)
        except Exception as exc:
            content = f"Health check error: {exc}"
            passed = False
            log.error("Health evidence error: url=%s error=%s", url, exc)

        return EvidenceItem(
            type="health_check",
            source=url,
            content=content,
            timestamp=ts,
            passed=passed,
        )

    def collect_log_evidence(self, log_path: str, pattern: str) -> EvidenceItem:
        """Search a log file for a pattern and capture matching lines."""
        ts = int(time.time())
        p = Path(log_path)
        if not p.exists():
            return EvidenceItem(
                type="log",
                source=log_path,
                content=f"Log file not found: {log_path}",
                timestamp=ts,
                passed=False,
            )

        try:
            text = p.read_text(errors="replace")
            lines = text.splitlines()
            regex = re.compile(pattern, re.IGNORECASE)
            matches = [line for line in lines if regex.search(line)]

            content = "\n".join(matches[-200:]) if matches else "(no matches)"
            passed = len(matches) > 0
            log.info(
                "Log evidence: path=%s pattern=%r matches=%d",
                log_path,
                pattern,
                len(matches),
            )
        except Exception as exc:
            content = f"Failed to read log: {exc}"
            passed = False

        return EvidenceItem(
            type="log",
            source=f"{log_path} (pattern: {pattern})",
            content=content[:50_000],
            timestamp=ts,
            passed=passed,
        )

    def summarize(self, items: list[EvidenceItem]) -> dict:
        """Produce an evidence summary."""
        total = len(items)
        passed = sum(1 for i in items if i.passed)
        failed = total - passed

        by_type: dict[str, dict[str, int]] = {}
        for item in items:
            entry = by_type.setdefault(item.type, {"passed": 0, "failed": 0})
            if item.passed:
                entry["passed"] += 1
            else:
                entry["failed"] += 1

        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": round(passed / total, 4) if total else 0.0,
            "by_type": by_type,
            "collected_at": int(time.time()),
        }

    def passes_gate(
        self,
        items: list[EvidenceItem],
        min_pass_rate: float = 0.8,
    ) -> tuple[bool, str]:
        """Check if evidence meets the quality gate.

        Returns (passed, reason).
        """
        if not items:
            return False, "no evidence collected"

        summary = self.summarize(items)
        rate = summary["pass_rate"]

        if rate >= min_pass_rate:
            return (
                True,
                f"pass rate {rate:.0%} >= {min_pass_rate:.0%} ({summary['passed']}/{summary['total']})",
            )

        # Build failure reason with failing items
        failing = [i for i in items if not i.passed]
        reasons = [f"  - [{i.type}] {i.source}" for i in failing[:5]]
        detail = "\n".join(reasons)
        return False, (
            f"pass rate {rate:.0%} < {min_pass_rate:.0%} "
            f"({summary['failed']}/{summary['total']} failed)\n{detail}"
        )
