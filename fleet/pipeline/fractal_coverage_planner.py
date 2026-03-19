"""Fractal Coverage Planner — generates task DAGs from upstream analysis stages.

Consumes:
  - census: repo file/module/symbol inventory
  - topology: dependency graph, frontier modules
  - coverage: relevance scoring, gaps, complex areas
  - autoresearch: bounded research queries

Produces:
  - Task DAG compatible with stage_plan output format
  - Execution waves (topologically sorted parallel groups)
  - Per-task skill/agent/model assignments
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict

log = logging.getLogger("aifleet.fractal_planner")

# ---------------------------------------------------------------------------
# Agent/model assignment defaults
# ---------------------------------------------------------------------------

STAGE_AGENT_DEFAULTS: dict[str, tuple[str, str]] = {
    "research": ("claude", "sonnet"),
    "implement": ("codex", "codex-5.2"),
    "test": ("codex", "codex-5.1"),
    "review": ("claude", "sonnet"),
    "refactor": ("codex", "codex-5.2"),
}

COST_PER_TASK_USD: dict[str, float] = {
    "research": 0.02,
    "implement": 0.05,
    "test": 0.03,
    "review": 0.02,
    "refactor": 0.04,
}

MINUTES_PER_TASK: dict[str, float] = {
    "research": 5.0,
    "implement": 15.0,
    "test": 10.0,
    "review": 5.0,
    "refactor": 12.0,
}


def generate_fractal_plan(
    objective: str,
    census: dict,
    topology: dict,
    coverage: dict,
    autoresearch: dict,
) -> dict:
    """Generate a task DAG from upstream analysis.

    Returns a dict compatible with stage_plan output:
      {
        "tasks": [...],
        "waves": [[task_ids], ...],
        "total_estimated_minutes": float,
        "total_estimated_cost_usd": float,
      }
    """
    tasks: list[dict] = []
    task_ids_by_module: dict[str, list[str]] = defaultdict(list)

    scored_modules = coverage.get("modules", [])
    relevant = [m for m in scored_modules if m.get("relevance_score", 0) > 0]
    gaps = {g["module"] for g in coverage.get("coverage_gaps", [])}
    complex_set = {c["module"] for c in coverage.get("complex_areas", [])}
    queries = autoresearch.get("queries", [])
    dep_chains = coverage.get("dependency_chains", {})

    # Phase 1: Research tasks for autoresearch queries
    research_task_ids: list[str] = []
    for q in queries:
        tid = _task_id("research", q.get("query_id", ""))
        tasks.append(_make_task(
            task_id=tid,
            title=f"Research: {q.get('category', 'general')} — {q.get('target_module', 'repo')}",
            description=q.get("question", ""),
            stage_type="research",
            depends_on=[],
            files=[],
            module=q.get("target_module"),
        ))
        research_task_ids.append(tid)
        if q.get("target_module"):
            task_ids_by_module[q["target_module"]].append(tid)

    # Phase 2: Implementation tasks per relevant module (ordered by relevance)
    for mod in relevant:
        module_name = mod.get("module", ".")
        module_files = _get_module_files(module_name, census)
        module_deps = dep_chains.get(module_name, [])

        # Depend on research for this module (if any) + implementation of upstream deps
        depends_on = list(task_ids_by_module.get(module_name, []))

        # If this module depends on others that are also relevant, add impl dependency
        for dep_mod in module_deps:
            dep_impl_ids = [
                t for t in task_ids_by_module.get(dep_mod, [])
                if t.startswith("impl-")
            ]
            depends_on.extend(dep_impl_ids)

        # Complex modules get a research-first approach
        if module_name in complex_set:
            tid = _task_id("impl", module_name)
            tasks.append(_make_task(
                task_id=tid,
                title=f"Implement: {module_name} (complex frontier)",
                description=(
                    f"Implement changes in '{module_name}' for objective: {objective}. "
                    f"High centrality ({mod.get('centrality', 0)}) — modify with care. "
                    f"Files: {', '.join(module_files[:10])}"
                ),
                stage_type="implement",
                depends_on=depends_on,
                files=module_files,
                module=module_name,
                priority=1,
            ))
            task_ids_by_module[module_name].append(tid)

        elif module_name in gaps:
            # Untested module: implement + test in sequence
            impl_tid = _task_id("impl", module_name)
            tasks.append(_make_task(
                task_id=impl_tid,
                title=f"Implement: {module_name}",
                description=(
                    f"Implement changes in '{module_name}' for: {objective}. "
                    f"Files: {', '.join(module_files[:10])}"
                ),
                stage_type="implement",
                depends_on=depends_on,
                files=module_files,
                module=module_name,
            ))
            task_ids_by_module[module_name].append(impl_tid)

            test_tid = _task_id("test", module_name)
            tasks.append(_make_task(
                task_id=test_tid,
                title=f"Add tests: {module_name} (coverage gap)",
                description=f"Add test coverage for '{module_name}' — currently untested.",
                stage_type="test",
                depends_on=[impl_tid],
                files=module_files,
                module=module_name,
            ))
            task_ids_by_module[module_name].append(test_tid)

        else:
            # Standard module: implement only
            tid = _task_id("impl", module_name)
            tasks.append(_make_task(
                task_id=tid,
                title=f"Implement: {module_name}",
                description=(
                    f"Implement changes in '{module_name}' for: {objective}. "
                    f"Files: {', '.join(module_files[:10])}"
                ),
                stage_type="implement",
                depends_on=depends_on,
                files=module_files,
                module=module_name,
            ))
            task_ids_by_module[module_name].append(tid)

    # Phase 3: Final review task depends on all impl/test tasks
    all_impl_test = [t["task_id"] for t in tasks if t["stage_type"] in ("implement", "test")]
    if all_impl_test:
        review_tid = _task_id("review", "final")
        tasks.append(_make_task(
            task_id=review_tid,
            title="Review: final integration review",
            description="Review all implementation changes for correctness and consistency.",
            stage_type="review",
            depends_on=all_impl_test,
            files=[],
        ))

    # Compute waves
    waves = _compute_waves(tasks)

    total_minutes = sum(t.get("estimated_minutes", 0) for t in tasks)
    total_cost = sum(t.get("estimated_cost_usd", 0) for t in tasks)

    return {
        "tasks": tasks,
        "waves": waves,
        "total_estimated_minutes": round(total_minutes, 1),
        "total_estimated_cost_usd": round(total_cost, 4),
        "summary": {
            "total_tasks": len(tasks),
            "research_tasks": sum(1 for t in tasks if t["stage_type"] == "research"),
            "implement_tasks": sum(1 for t in tasks if t["stage_type"] == "implement"),
            "test_tasks": sum(1 for t in tasks if t["stage_type"] == "test"),
            "review_tasks": sum(1 for t in tasks if t["stage_type"] == "review"),
            "total_waves": len(waves),
        },
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _task_id(stage_type: str, suffix: str) -> str:
    """Generate a deterministic short task ID."""
    raw = f"{stage_type}-{suffix}"
    short_hash = hashlib.md5(raw.encode()).hexdigest()[:6]
    return f"{stage_type}-{suffix[:20]}-{short_hash}"


def _make_task(
    task_id: str,
    title: str,
    description: str,
    stage_type: str,
    depends_on: list[str],
    files: list[str],
    module: str | None = None,
    priority: int = 3,
) -> dict:
    agent, model = STAGE_AGENT_DEFAULTS.get(stage_type, ("claude", "sonnet"))
    return {
        "task_id": task_id,
        "title": title,
        "description": description,
        "depends_on": depends_on,
        "skill_match": stage_type,
        "agent_type": agent,
        "model": model,
        "estimated_minutes": MINUTES_PER_TASK.get(stage_type, 10.0),
        "estimated_cost_usd": COST_PER_TASK_USD.get(stage_type, 0.03),
        "priority": priority,
        "stage_type": stage_type,
        "files": files[:20],  # cap file list
        "constraints": [],
        "status": "pending",
        "module": module,
    }


def _get_module_files(module_name: str, census: dict) -> list[str]:
    """Get file paths for a module from census data."""
    for mod in census.get("modules", []):
        if mod.get("module") == module_name:
            return mod.get("files", [])
    return []


def _compute_waves(tasks: list[dict]) -> list[list[str]]:
    """Topological sort tasks into parallel execution waves."""
    all_ids = {t["task_id"] for t in tasks}
    completed: set[str] = set()
    waves: list[list[str]] = []

    for _ in range(len(tasks) + 1):
        wave: list[str] = []
        for task in tasks:
            tid = task["task_id"]
            if tid in completed:
                continue
            deps = [d for d in task.get("depends_on", []) if d in all_ids]
            if all(d in completed for d in deps):
                wave.append(tid)
        if not wave:
            break
        waves.append(sorted(wave))
        completed.update(wave)

    return waves
