"""Quality Gates — evidence-based gates between pipeline phases.

Implements the NEXUS doctrine: no phase advances without evidence.
Gates are enforced between each phase transition.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from enum import Enum

log = logging.getLogger("aifleet.gates")


class GateVerdict(Enum):
    """Outcome of a quality gate evaluation."""

    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"  # passes with warnings
    SKIP = "skip"  # gate not applicable
    ESCALATE = "escalate"  # needs human review


@dataclass
class GateResult:
    """Result of evaluating a quality gate."""

    gate_name: str
    verdict: GateVerdict
    evidence: list[dict]
    failures: list[str]
    warnings: list[str]
    score: float  # 0.0-1.0 quality score
    timestamp: int
    reviewer: str  # "auto" | "human" | agent name


class QualityGate:
    """Evaluates whether a pipeline phase can advance."""

    PHASE_GATES: dict[str, dict] = {
        "discover_to_strategize": {
            "required": ["objective_clear", "scope_defined", "stakeholders_identified"],
            "min_score": 0.7,
        },
        "strategize_to_scaffold": {
            "required": ["plan_exists", "tasks_identified", "dependencies_resolved"],
            "min_score": 0.8,
        },
        "scaffold_to_build": {
            "required": ["branch_created", "structure_defined", "tests_scaffolded"],
            "min_score": 0.7,
        },
        "build_to_harden": {
            "required": ["code_compiles", "tests_pass", "no_critical_issues"],
            "min_score": 0.8,
        },
        "harden_to_launch": {
            "required": [
                "all_tests_pass",
                "review_approved",
                "security_checked",
                "evidence_collected",
            ],
            "min_score": 0.9,
        },
        "launch_to_operate": {
            "required": ["deployed", "health_check_pass", "smoke_tests_pass"],
            "min_score": 0.9,
        },
    }

    def __init__(self, conn: sqlite3.Connection, cfg: dict) -> None:
        self.conn = conn
        self.cfg = cfg
        self.max_retries = cfg.get("quality_gates", {}).get("max_retries", 3)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(
        self, gate_name: str, evidence: list[dict], context: dict
    ) -> GateResult:
        """Evaluate a quality gate with collected evidence."""
        gate_def = self.PHASE_GATES.get(gate_name)
        if not gate_def:
            log.warning("Unknown gate %s — skipping", gate_name)
            return GateResult(
                gate_name=gate_name,
                verdict=GateVerdict.SKIP,
                evidence=evidence,
                failures=[],
                warnings=[f"gate '{gate_name}' not defined"],
                score=1.0,
                timestamp=int(time.time()),
                reviewer="auto",
            )

        required: list[str] = gate_def["required"]
        min_score: float = gate_def["min_score"]
        failures: list[str] = []
        warnings: list[str] = []
        passed_count = 0

        for req in required:
            ok, reason = self._check_requirement(req, evidence, context)
            if ok:
                passed_count += 1
            else:
                failures.append(f"{req}: {reason}")

        score = passed_count / len(required) if required else 1.0
        score = round(score, 4)

        if not failures and score >= min_score:
            verdict = GateVerdict.PASS
        elif failures and score >= min_score:
            verdict = GateVerdict.WARN
            warnings.extend(failures)
            failures = []
        else:
            verdict = GateVerdict.FAIL

        log.info(
            "Gate %s: verdict=%s score=%.2f passed=%d/%d",
            gate_name,
            verdict.value,
            score,
            passed_count,
            len(required),
        )
        return GateResult(
            gate_name=gate_name,
            verdict=verdict,
            evidence=evidence,
            failures=failures,
            warnings=warnings,
            score=score,
            timestamp=int(time.time()),
            reviewer="auto",
        )

    def enforce(
        self, gate_name: str, evidence: list[dict], context: dict
    ) -> GateResult:
        """Evaluate gate and record result. Raises if gate fails after max retries."""
        pipeline_id = context.get("pipeline_id", "")
        retry_count = context.get("retry_count", 0)

        result = self.evaluate(gate_name, evidence, context)

        # Persist gate result
        self._record_gate_result(pipeline_id, result)

        if result.verdict == GateVerdict.FAIL:
            if retry_count >= self.max_retries:
                result = GateResult(
                    gate_name=result.gate_name,
                    verdict=GateVerdict.ESCALATE,
                    evidence=result.evidence,
                    failures=result.failures
                    + [f"max retries ({self.max_retries}) exceeded"],
                    warnings=result.warnings,
                    score=result.score,
                    timestamp=result.timestamp,
                    reviewer="auto",
                )
                self._record_gate_result(pipeline_id, result)
                log.error(
                    "Gate %s ESCALATED for pipeline %s after %d retries",
                    gate_name,
                    pipeline_id,
                    retry_count,
                )
            else:
                log.warning(
                    "Gate %s FAILED for pipeline %s (retry %d/%d)",
                    gate_name,
                    pipeline_id,
                    retry_count,
                    self.max_retries,
                )

        return result

    def get_gate_history(self, pipeline_id: str) -> list[GateResult]:
        """Get all gate results for a pipeline."""
        try:
            rows = self.conn.execute(
                "SELECT gate_data FROM pipeline_gate_results "
                "WHERE pipeline_id = ? ORDER BY evaluated_at ASC",
                (pipeline_id,),
            ).fetchall()
        except sqlite3.OperationalError:
            return []

        results: list[GateResult] = []
        for row in rows:
            raw = row[0] if isinstance(row, tuple) else row["gate_data"]
            try:
                data = json.loads(raw)
                results.append(
                    GateResult(
                        gate_name=data["gate_name"],
                        verdict=GateVerdict(data["verdict"]),
                        evidence=data.get("evidence", []),
                        failures=data.get("failures", []),
                        warnings=data.get("warnings", []),
                        score=data.get("score", 0.0),
                        timestamp=data.get("timestamp", 0),
                        reviewer=data.get("reviewer", "auto"),
                    )
                )
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
        return results

    # ------------------------------------------------------------------
    # Requirement checkers
    # ------------------------------------------------------------------

    def _check_requirement(
        self,
        requirement: str,
        evidence: list[dict],
        context: dict,
    ) -> tuple[bool, str]:
        """Check a single gate requirement against evidence."""
        checker = self._REQUIREMENT_CHECKERS.get(requirement)
        if not checker:
            return False, f"unknown requirement '{requirement}'"
        return checker(self, evidence, context)

    def _check_objective_clear(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            obj = item.get("objective") or item.get("content", "")
            if isinstance(obj, str) and len(obj.strip()) > 50:
                return True, ""
        obj = context.get("objective", "")
        if isinstance(obj, str) and len(obj.strip()) > 50:
            return True, ""
        return False, "no structured objective > 50 chars found"

    def _check_scope_defined(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in ("scope", "file_list", "module_list"):
                return True, ""
            files = item.get("files") or item.get("modules")
            if isinstance(files, list) and files:
                return True, ""
        if context.get("files") or context.get("modules"):
            return True, ""
        return False, "no file list or module list in evidence"

    def _check_stakeholders_identified(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") == "stakeholders" or item.get("stakeholders"):
                return True, ""
        # Auto-pass if context provides stakeholders or if this is an automated pipeline
        if context.get("stakeholders") or context.get("automated", False):
            return True, ""
        return False, "no stakeholders identified"

    def _check_plan_exists(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in ("plan", "decomposition"):
                return True, ""
            if item.get("tasks") and isinstance(item["tasks"], list):
                return True, ""
        return False, "no plan with tasks found"

    def _check_tasks_identified(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            tasks = item.get("tasks")
            if isinstance(tasks, list) and len(tasks) > 0:
                return True, ""
        return False, "task list is empty or missing"

    def _check_dependencies_resolved(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        all_task_ids: set[str] = set()
        all_deps: set[str] = set()

        for item in evidence:
            tasks = item.get("tasks")
            if not isinstance(tasks, list):
                continue
            for task in tasks:
                tid = task.get("task_id", "")
                if tid:
                    all_task_ids.add(str(tid))
                deps = task.get("depends_on", [])
                if isinstance(deps, list):
                    all_deps.update(str(d) for d in deps if d)

        if not all_task_ids:
            return False, "no tasks to validate dependencies against"

        unresolved = all_deps - all_task_ids
        if unresolved:
            return False, f"unresolved dependencies: {sorted(unresolved)}"
        return True, ""

    def _check_branch_created(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in ("branch", "git_branch"):
                return True, ""
            if item.get("branch"):
                return True, ""
        if context.get("branch"):
            return True, ""
        return False, "no git branch evidence"

    def _check_structure_defined(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in (
                "structure",
                "scaffold",
                "file_created",
                "file_modified",
            ):
                return True, ""
            if item.get("files_created") or item.get("files_modified"):
                return True, ""
        return False, "no files created or modified"

    def _check_tests_scaffolded(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            files = item.get("files_created", []) + item.get("files", [])
            for f in files:
                if isinstance(f, str) and ("test" in f.lower() or "spec" in f.lower()):
                    return True, ""
            if item.get("type") == "test_scaffold":
                return True, ""
        return False, "no test files found in evidence"

    def _check_code_compiles(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in ("build", "typecheck", "compile"):
                if item.get("passed", False):
                    return True, ""
        return False, "no successful build/typecheck evidence"

    def _check_tests_pass(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in ("test_result", "test"):
                if item.get("passed", False):
                    return True, ""
        return False, "no passing test evidence"

    def _check_no_critical_issues(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            severity = item.get("severity", "").lower()
            if severity == "critical" and not item.get("resolved", False):
                return False, f"critical issue: {item.get('description', 'unknown')}"
        return True, ""

    def _check_all_tests_pass(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        test_items = [i for i in evidence if i.get("type") in ("test_result", "test")]
        if not test_items:
            return False, "no test results in evidence"
        for item in test_items:
            if not item.get("passed", False):
                return False, f"test failed: {item.get('source', 'unknown')}"
            # Check for explicit failure count
            failures = item.get("tests_failed", 0)
            if isinstance(failures, int) and failures > 0:
                return False, f"{failures} tests failed"
        return True, ""

    def _check_review_approved(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") == "review":
                verdict = str(item.get("verdict", "")).lower()
                if verdict in ("approve", "approved", "lgtm"):
                    return True, ""
        return False, "no review with APPROVE verdict"

    def _check_security_checked(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in ("security_scan", "security_check", "security"):
                return True, ""
        return False, "no security scan evidence"

    def _check_evidence_collected(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        if len(evidence) >= 3:
            return True, ""
        return False, f"only {len(evidence)} evidence items (need ≥3)"

    def _check_deployed(self, evidence: list[dict], context: dict) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in ("deployment", "deploy"):
                if item.get("passed", False):
                    return True, ""
        return False, "no deployment evidence"

    def _check_health_check_pass(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") == "health_check":
                if item.get("passed", False):
                    return True, ""
        return False, "no passing health check evidence"

    def _check_smoke_tests_pass(
        self, evidence: list[dict], context: dict
    ) -> tuple[bool, str]:
        for item in evidence:
            if item.get("type") in ("smoke_test", "smoke"):
                if item.get("passed", False):
                    return True, ""
        return False, "no passing smoke test evidence"

    _REQUIREMENT_CHECKERS: dict[str, callable] = {
        "objective_clear": _check_objective_clear,
        "scope_defined": _check_scope_defined,
        "stakeholders_identified": _check_stakeholders_identified,
        "plan_exists": _check_plan_exists,
        "tasks_identified": _check_tasks_identified,
        "dependencies_resolved": _check_dependencies_resolved,
        "branch_created": _check_branch_created,
        "structure_defined": _check_structure_defined,
        "tests_scaffolded": _check_tests_scaffolded,
        "code_compiles": _check_code_compiles,
        "tests_pass": _check_tests_pass,
        "no_critical_issues": _check_no_critical_issues,
        "all_tests_pass": _check_all_tests_pass,
        "review_approved": _check_review_approved,
        "security_checked": _check_security_checked,
        "evidence_collected": _check_evidence_collected,
        "deployed": _check_deployed,
        "health_check_pass": _check_health_check_pass,
        "smoke_tests_pass": _check_smoke_tests_pass,
    }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _record_gate_result(self, pipeline_id: str, result: GateResult) -> None:
        """Persist a gate result to the database."""
        data = {
            "gate_name": result.gate_name,
            "verdict": result.verdict.value,
            "evidence": result.evidence,
            "failures": result.failures,
            "warnings": result.warnings,
            "score": result.score,
            "timestamp": result.timestamp,
            "reviewer": result.reviewer,
        }
        try:
            self.conn.execute(
                "INSERT INTO pipeline_gate_results "
                "(pipeline_id, gate_name, verdict, score, gate_data, evaluated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    pipeline_id,
                    result.gate_name,
                    result.verdict.value,
                    result.score,
                    json.dumps(data, default=str),
                    result.timestamp,
                ),
            )
            self.conn.commit()
        except sqlite3.OperationalError as exc:
            log.debug("Failed to record gate result: %s", exc)
