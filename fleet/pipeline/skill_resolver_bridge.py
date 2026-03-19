"""Skill Resolver Bridge — Python-side skill routing from config/skill-tree.yaml.

Mirrors the canonical TypeScript SkillResolver logic so that Python pipeline
stages (fractal_plan, implement) can resolve skill packs without an HTTP
roundtrip to the orchestrator.

Single source of truth: config/skill-tree.yaml
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

log = logging.getLogger("aifleet.skill_resolver")

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


def load_skill_tree(repo_root: str = ".") -> dict:
    """Load and parse config/skill-tree.yaml from the repo root."""
    if yaml is None:
        log.warning("PyYAML not installed — skill resolver disabled")
        return {}

    candidates = [
        Path(repo_root) / "config" / "skill-tree.yaml",
        Path.home() / ".claude" / "skill-tree.yaml",
    ]
    for path in candidates:
        if path.exists():
            try:
                return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            except Exception as exc:
                log.warning("Failed to parse %s: %s", path, exc)
    return {}


def resolve_skill_bundle(
    text: str,
    skill_tree: dict,
    role: str | None = None,
    task_kind: str | None = None,
) -> dict | None:
    """Resolve a skill bundle for the given text/role/task_kind.

    Mirrors TypeScript resolveSkillBundle() logic:
    1. Sort routes by priority (descending)
    2. Match text against phrase/regex rules
    3. Fall back by role, then by task_kind
    4. Return skill pack metadata or None

    Returns dict with keys:
        task_family, skill_pack_id, preferred_cli, preferred_model,
        expert_role, verification_profile, skills, route_reason
    """
    routes = skill_tree.get("routes", [])
    skill_packs = skill_tree.get("skill_packs", {})
    fallbacks = skill_tree.get("fallbacks", {})

    # Sort by priority descending
    sorted_routes = sorted(routes, key=lambda r: r.get("priority", 0), reverse=True)

    # Phase 1: Match against route rules
    for route in sorted_routes:
        match_rules = route.get("match", {})
        if _match_rule(text, match_rules):
            pack_id = route.get("skill_pack", "")
            pack = skill_packs.get(pack_id, {})
            return _build_bundle(route, pack)

    # Phase 2: Fallback by role
    if role:
        by_role = fallbacks.get("by_role", {})
        pack_id = by_role.get(role)
        if pack_id and pack_id in skill_packs:
            return _build_bundle(
                {"id": f"fallback-role-{role}", "task_family": pack_id,
                 "route_reason": f"Fallback by role: {role}"},
                skill_packs[pack_id],
            )

    # Phase 3: Fallback by task_kind
    if task_kind:
        by_kind = fallbacks.get("by_task_kind", {})
        pack_id = by_kind.get(task_kind)
        if pack_id and pack_id in skill_packs:
            return _build_bundle(
                {"id": f"fallback-kind-{task_kind}", "task_family": pack_id,
                 "route_reason": f"Fallback by task_kind: {task_kind}"},
                skill_packs[pack_id],
            )

    return None


def resolve_for_task(
    task: dict,
    skill_tree: dict,
) -> dict:
    """Enrich a fractal planner task with skill bundle metadata.

    Modifies the task dict in-place, adding:
        skill_pack_id, preferred_cli, preferred_model, skills, route_reason
    """
    text = f"{task.get('title', '')} {task.get('description', '')}"
    bundle = resolve_skill_bundle(
        text=text,
        skill_tree=skill_tree,
        role=task.get("expert_role"),
        task_kind=task.get("stage_type"),
    )
    if bundle:
        task["skill_pack_id"] = bundle.get("skill_pack_id", "")
        task["preferred_cli"] = bundle.get("preferred_cli", task.get("agent_type", ""))
        task["preferred_model"] = bundle.get("preferred_model", task.get("model", ""))
        task["skills"] = bundle.get("skills", [])
        task["route_reason"] = bundle.get("route_reason", "")
        task["verification_profile"] = bundle.get("verification_profile", "")
    return task


def _match_rule(text: str, rules: dict) -> bool:
    """Check if text matches any_phrase or any_regex rules."""
    text_lower = text.lower()

    for phrase in rules.get("any_phrase", []):
        if phrase.lower() in text_lower:
            return True

    for pattern in rules.get("any_regex", []):
        try:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        except re.error:
            continue

    return False


def _build_bundle(route: dict, pack: dict) -> dict:
    return {
        "task_family": route.get("task_family", route.get("id", "")),
        "skill_pack_id": pack.get("id", route.get("skill_pack", "")),
        "expert_role": pack.get("expert_role", ""),
        "preferred_role": pack.get("preferred_role", ""),
        "preferred_cli": pack.get("preferred_cli", ""),
        "preferred_model": pack.get("preferred_model", ""),
        "resource_pack_id": pack.get("resource_pack_id", ""),
        "mcp_profile": pack.get("mcp_profile", ""),
        "verification_profile": pack.get("verification_profile", ""),
        "skills": pack.get("skills", []),
        "route_reason": route.get("route_reason", ""),
    }
