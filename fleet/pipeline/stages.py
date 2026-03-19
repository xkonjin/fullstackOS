from __future__ import annotations

import logging
import urllib.parse
from collections.abc import Callable
from pathlib import Path
import json
import sqlite3
import subprocess
import time
from datetime import datetime, timezone

from pipeline.git_ops import (
    acquire_worktree,
    create_feature_branch,
    create_pr,
    heartbeat_worktree,
    init_worktrees,
    merge_pr,
    release_worktree,
)
from pipeline.agents import (
    parse_stage_output,
    spawn_agent,
)

log = logging.getLogger("aifleet.stages")

# ---------------------------------------------------------------------------
# Stage → skill pattern map for auto-injection
# ONLY reference skills that exist in /skills/ directory.
# Plugin/MCP skills are injected via role system, not stage injection.
# ---------------------------------------------------------------------------
STAGE_SKILL_MAP: dict[str, list[str]] = {
    "intake": ["pipeline", "product-thinking"],
    "refine": ["pipeline", "product-thinking"],
    "research": ["x-research", "hn-search", "producthunt"],
    "spec": ["product-thinking", "progressive-disclosure", "ui-design-system", "figma"],
    "plan": ["fractal-planner", "pipeline"],
    "implement": ["tdd-agent", "frontend-craft", "sentry-setup", "style-saas"],
    "test": ["tdd-agent", "verify-impl"],
    "fix": ["verify-impl"],
    "adversarial": ["verify-impl", "security-auditor", "bug-hunter"],
    "review": ["design-review", "verify-impl"],
    "merge": ["pr-flow"],
    "deploy": ["post-merge-monitor", "sentry-setup"],
    "cleanup": [],
}

# Keyword → category mapping for skill classification
SKILL_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "frontend": [
        "animation",
        "css",
        "style",
        "design",
        "ui",
        "ux",
        "frontend",
        "react",
        "web",
        "layout",
    ],
    "testing": ["test", "verify", "assert", "coverage", "spec", "tdd"],
    "deployment": ["deploy", "railway", "docker", "sentry", "ci", "cd", "ship"],
    "backend": ["api", "database", "migration", "schema", "sql", "graphql", "orm", "fastapi", "express", "django"],
    "security": ["security", "auth", "encrypt", "injection", "owasp", "xss", "csrf"],
    "design": ["figma", "design", "color", "typography", "spacing", "brand"],
    "product": ["product", "ux", "user", "journey", "onboard", "flow", "progressive"],
    "seo": ["seo", "meta", "sitemap", "schema", "structured-data"],
}


def categorize_skill(name: str, path: str) -> list[str]:
    """Classify a skill into categories by name/path keywords + optional frontmatter."""
    categories: list[str] = []
    low = (name + " " + path).lower()

    for cat, keywords in SKILL_CATEGORY_KEYWORDS.items():
        if any(kw in low for kw in keywords):
            categories.append(cat)

    # Read frontmatter for explicit category tags
    skill_md = Path(path) / "SKILL.md"
    if skill_md.exists():
        try:
            text = skill_md.read_text(errors="ignore")[:800]
            for line in text.split("\n"):
                if line.strip().lower().startswith("categories:"):
                    cats = line.split(":", 1)[1].strip()
                    categories.extend(c.strip() for c in cats.split(",") if c.strip())
                    break
        except Exception:
            pass

    return list(dict.fromkeys(categories))  # dedupe preserving order


def discover_and_inject_skills(
    stage: str, pipeline: dict, cfg: dict, conn: sqlite3.Connection | None = None
) -> str:
    """Discover matching skills for a pipeline stage. Returns markdown context to prepend to prompt."""
    stage_skills_cfg = cfg.get("stage_skills", {})
    if not stage_skills_cfg.get("enabled", False):
        return ""

    max_per_stage = int(stage_skills_cfg.get("max_per_stage", 5))
    content_chars = int(stage_skills_cfg.get("content_chars", 500))

    # Get stage category patterns
    patterns = STAGE_SKILL_MAP.get(stage, [])
    if not patterns:
        return ""

    # Also extract tokens from objective for relevance matching
    objective = pipeline.get("structured_objective", pipeline.get("title", ""))
    obj_lower = objective.lower() if objective else ""

    # Scan skill roots
    skill_roots_raw = cfg.get("skills", {}).get("roots", [])
    repo = pipeline.get("project_repo", ".")
    skill_roots: list[Path] = []
    for raw in skill_roots_raw:
        expanded = str(raw).replace("{repo}", str(repo))
        expanded = expanded.replace("~", str(Path.home()))
        p = Path(expanded)
        if p.exists() and p.is_dir():
            skill_roots.append(p)

    matched: list[dict] = []
    scanned = 0
    max_scan = int(cfg.get("skills", {}).get("max_scan_files", 5000))

    for root in skill_roots:
        for skill_file in root.rglob("SKILL.md"):
            scanned += 1
            if scanned > max_scan:
                break
            skill_name = skill_file.parent.name
            skill_path = str(skill_file.parent)

            # Score: check if skill name matches any stage pattern
            score = 0
            for pattern in patterns:
                if pattern.endswith("-"):
                    if skill_name.lower().startswith(pattern):
                        score += 3
                elif pattern in skill_name.lower():
                    score += 3

            # Boost if objective tokens overlap with skill name
            if obj_lower:
                name_tokens = set(skill_name.lower().replace("-", " ").split())
                obj_tokens = set(obj_lower.replace("-", " ").split())
                overlap = len(name_tokens & obj_tokens)
                score += overlap

            if score > 0:
                matched.append(
                    {
                        "name": skill_name,
                        "path": skill_path,
                        "file": str(skill_file),
                        "score": score,
                    }
                )

        if scanned > max_scan:
            break

    if not matched:
        return ""

    matched.sort(key=lambda x: -x["score"])
    top = matched[:max_per_stage]

    # Build markdown context
    lines = [f"## Relevant Skills for {stage} stage\n"]
    for skill in top:
        lines.append(f"### {skill['name']}")
        try:
            text = Path(skill["file"]).read_text(errors="ignore")[:content_chars]
            # Skip frontmatter
            if text.startswith("---"):
                end = text.find("---", 3)
                if end != -1:
                    text = text[end + 3 :].strip()
            lines.append(text)
        except Exception:
            pass
        lines.append("")

    skill_names = [s["name"] for s in top]
    log.info("Injected skills for stage %s: %s", stage, ", ".join(skill_names))

    # Record skill usage in pipeline_skills table
    if conn is not None:
        pipeline_id = pipeline.get("pipeline_id", "")
        if pipeline_id:
            ts = int(time.time())
            for skill in top:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO pipeline_skills (pipeline_id, stage_name, skill_name, skill_path, injected_at) VALUES (?, ?, ?, ?, ?)",
                        (pipeline_id, stage, skill["name"], skill["path"], ts),
                    )
                except Exception:
                    log.debug(
                        "Failed to record skill %s for pipeline %s",
                        skill["name"],
                        pipeline_id,
                        exc_info=True,
                    )
            try:
                conn.commit()
            except Exception:
                log.warning(
                    "Failed to commit skill injection for pipeline %s",
                    pipeline_id,
                    exc_info=True,
                )

    return "\n".join(lines)


PIPELINE_STAGES: dict[str, list[str]] = {
    "feature": [
        "intake",
        "refine",
        "research",
        "spec",
        "plan",
        "issues",
        "implement",
        "test",
        "fix",
        "review",
        "merge",
        "deploy",
    ],
    "bugfix": [
        "intake",
        "refine",
        "research",
        "implement",
        "test",
        "fix",
        "review",
        "merge",
        "deploy",
    ],
    "design": [
        "intake",
        "refine",
        "research",
        "spec",
        "plan",
        "implement",
        "test",
        "fix",
        "review",
        "merge",
        "deploy",
    ],
    "refactor": [
        "intake",
        "refine",
        "plan",
        "implement",
        "test",
        "fix",
        "review",
        "merge",
        "deploy",
    ],
    "fleetmax": [
        "intake",
        "refine",
        "research",
        "spec",
        "plan",
        "implement",
        "test",
        "adversarial",
        "ralph",
        "review",
        "merge",
        "cleanup",
    ],
    "self_update": ["plan_self_update", "implement", "validate", "apply"],
}


def _now_ts() -> int:
    return int(time.time())


def _theorist_enabled(cfg: dict) -> bool:
    theorist_cfg = cfg.get("theorist") if isinstance(cfg.get("theorist"), dict) else {}
    return bool(theorist_cfg.get("enabled", True))


def _theorist_notes_dir(pipeline: dict, cfg: dict) -> Path:
    repo_root = Path(cfg.get("repo_root", pipeline.get("project_repo", ".")))
    theorist_cfg = cfg.get("theorist") if isinstance(cfg.get("theorist"), dict) else {}
    rel = theorist_cfg.get("notes_dir", "docs/theorist/notes")
    return repo_root / rel


def _write_theorist_note(note_path: Path, frontmatter: dict, body: str) -> None:
    note_path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["---"]
    for key, value in frontmatter.items():
        if isinstance(value, dict):
            lines.append(f"{key}:")
            for sub_key, sub_val in value.items():
                if isinstance(sub_val, list):
                    lines.append(f"  {sub_key}: [{', '.join(str(v) for v in sub_val)}]")
                else:
                    lines.append(f"  {sub_key}: {sub_val}")
        elif isinstance(value, list):
            lines.append(f"{key}: [{', '.join(str(v) for v in value)}]")
        else:
            lines.append(f"{key}: {value}")
    lines.append("---")
    lines.append("")
    lines.append(body.rstrip())
    lines.append("")
    note_path.write_text("\n".join(lines), encoding="utf-8")


def _write_plan_theorist_note(pipeline: dict, cfg: dict, output: dict) -> None:
    if not _theorist_enabled(cfg):
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pid = pipeline.get("pipeline_id", "unknown")
    note_id = f"th-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}-plan-{pid[:8]}"
    frontmatter = {
        "id": note_id,
        "type": "plan",
        "repo": Path(cfg.get("repo_root", pipeline.get("project_repo", "."))).name,
        "scope": "repo",
        "status": "active",
        "owners": ["fleetmax"],
        "tags": ["theorist", "fleetmax", "pipeline"],
        "links": {"relates_to": [pid], "supersedes": []},
        "timestamps": {"created_utc": now, "updated_utc": now},
        "schema_version": 1,
    }
    body = (
        f"# Pipeline Plan {pid}\n\n"
        f"## Context\n\n"
        f"Objective: {pipeline.get('structured_objective', pipeline.get('title', ''))}\n\n"
        f"## Decision / Plan\n\n"
        f"```json\n{json.dumps(output, indent=2)}\n```\n\n"
        f"## Machine Contract\n\n"
        f'```json\n{{\n  "pipeline_id": "{pid}",\n  "stage": "plan",\n  "checks": ["plan_schema_valid"]\n}}\n```\n\n'
        f"## Verification\n\n"
        f"- [ ] plan_json persisted in pipeline_runs\n\n"
        f"## Change Log\n\n"
        f"- {datetime.now(timezone.utc).strftime('%Y-%m-%d')}: generated by fleet pipeline stage_plan\n"
    )
    note_path = _theorist_notes_dir(pipeline, cfg) / f"{pid}-plan.md"
    _write_theorist_note(note_path, frontmatter, body)


def get_project_config(conn: sqlite3.Connection, project_repo: str) -> dict:
    """Get project config, creating default if not exists."""
    conn.row_factory = sqlite3.Row

    # Use INSERT OR IGNORE to avoid race condition when concurrent callers
    # both try to create the default config for the same project.
    conn.execute(
        """
        INSERT OR IGNORE INTO project_config
        (project_repo, pipeline_type_defaults, human_gates, deploy_target, deploy_config,
         worktree_config, notification_channel, max_ralph_cycles, created_at)
        VALUES (?, '{}', '["spec","review"]', 'merge', '{}', '{}', 'telegram', 5, ?)
    """,
        (project_repo, _now_ts()),
    )
    conn.commit()

    row = conn.execute(
        "SELECT * FROM project_config WHERE project_repo = ?", (project_repo,)
    ).fetchone()

    return dict(row) if row else {}


AUTONOMY_GATES: dict[str, list[str]] = {
    "supervised": ["spec", "review"],
    "autonomous": ["deploy"],
    "full-auto": [],
    "safe": ["spec", "review", "merge", "deploy", "cleanup"],
    "balanced": ["spec", "review", "deploy"],
    "aggressive": ["deploy"],
}


def _resolve_autonomy_mode(pipeline: dict | None) -> str | None:
    if not pipeline:
        return None
    config_json = pipeline.get("config_json", "{}")
    try:
        pcfg = (
            json.loads(config_json)
            if isinstance(config_json, str)
            else (config_json or {})
        )
    except (json.JSONDecodeError, TypeError):
        pcfg = {}

    for key in ("autonomy_profile", "autonomy_mode"):
        value = pcfg.get(key)
        if isinstance(value, str) and value in AUTONOMY_GATES:
            return value
    return None


def is_human_gate(
    conn: sqlite3.Connection,
    project_repo: str,
    stage_name: str,
    pipeline: dict | None = None,
) -> bool:
    """Check if this stage requires human approval for this project.

    Respects pipeline-level autonomy mode from config_json if present,
    falling back to project-level human_gates, then coordinator config default.
    """
    # Pipeline-level autonomy override
    autonomy = _resolve_autonomy_mode(pipeline)
    if autonomy:
        return stage_name in AUTONOMY_GATES[autonomy]

    if pipeline:
        config_json = pipeline.get("config_json", "{}")
        try:
            pcfg = (
                json.loads(config_json)
                if isinstance(config_json, str)
                else (config_json or {})
            )
        except (json.JSONDecodeError, TypeError):
            pcfg = {}
        if (
            isinstance(pcfg.get("autonomy_mode"), str)
            and pcfg.get("autonomy_mode") in AUTONOMY_GATES
        ):
            return stage_name in AUTONOMY_GATES[str(pcfg.get("autonomy_mode"))]

        if bool(pcfg.get("external_side_effects")) and stage_name in (
            "merge",
            "deploy",
            "cleanup",
        ):
            return True

        risk_tier = str(pcfg.get("risk_tier", "")).lower()
        if risk_tier in ("high", "external-side-effect") and stage_name in (
            "review",
            "merge",
            "deploy",
        ):
            return True

    # Project-level config
    config = get_project_config(conn, project_repo)
    gates = json.loads(config.get("human_gates", "[]"))
    return stage_name in gates


def _query_verification_profile(stage: str, pipeline: dict, cfg: dict) -> str:
    """Query verification-profiles.yaml for the stage's skill pack and return context."""
    repo_root = Path(cfg.get("repo_root", pipeline.get("project_repo", ".")))
    profiles_path = repo_root / "config" / "verification-profiles.yaml"
    tree_path = repo_root / "config" / "skill-tree.yaml"
    if not profiles_path.exists() or not tree_path.exists():
        return ""

    try:
        import yaml
        tree = yaml.safe_load(tree_path.read_text())
        profiles = yaml.safe_load(profiles_path.read_text())
    except Exception:
        return ""

    objective = pipeline.get("structured_objective", pipeline.get("title", ""))
    obj_lower = str(objective).lower() if objective else ""

    # Find best matching skill pack for this stage + objective
    matched_profile: str | None = None
    best_score = -1
    for pack_name, pack in (tree.get("skill_packs") or {}).items():
        vp = pack.get("verification_profile")
        if not vp:
            continue
        pack_skills = pack.get("skills", [])
        score = 0
        for skill in pack_skills:
            if skill.replace("-", " ").replace("_", " ") in obj_lower:
                score += 2
        stage_map_skills = STAGE_SKILL_MAP.get(stage, [])
        for skill in stage_map_skills:
            if skill in pack_skills:
                score += 1
        if score > best_score:
            best_score = score
            matched_profile = vp

    if not matched_profile or matched_profile not in (profiles.get("profiles") or {}):
        # Fallback: targeted-tests for implement/test/fix
        if stage in ("implement", "test", "fix") and "targeted-tests" in (profiles.get("profiles") or {}):
            matched_profile = "targeted-tests"
        else:
            return ""

    profile_data = profiles["profiles"][matched_profile]
    commands = profile_data.get("commands", {})
    lines = [f"## Verification Profile: {matched_profile}", ""]
    if profile_data.get("description"):
        lines.append(profile_data["description"])
        lines.append("")
    for lang, cmd in commands.items():
        lines.append(f"- {lang}: `{cmd}`")
    if profile_data.get("pre_check"):
        lines.append(f"- pre_check: `{profile_data['pre_check']}`")

    return "\n".join(lines)


def _query_mem_context(stage: str, pipeline: dict, cfg: dict) -> str:
    """Query local memory DB for prior learnings relevant to this stage."""
    mem_db = Path.home() / ".ai-fleet" / "coordinator" / "memory.db"
    if not mem_db.exists():
        return ""

    objective = pipeline.get("structured_objective", pipeline.get("title", ""))
    if not objective:
        return ""

    try:
        conn = sqlite3.connect(str(mem_db), timeout=3)
        conn.row_factory = sqlite3.Row
        # Search global_learning for stage-relevant patterns
        rows = conn.execute(
            "SELECT rule, rationale FROM global_learning WHERE tags LIKE ? ORDER BY ts DESC LIMIT 5",
            (f"%{stage}%",),
        ).fetchall()
        conn.close()
    except Exception:
        return ""

    if not rows:
        return ""

    lines = ["## Prior Learnings (memory)", ""]
    for row in rows:
        lines.append(f"- {row['rule']}")
    return "\n".join(lines)


# Stages that emit pre-hook context
STAGE_PRE_HOOKS: dict[str, list[str]] = {
    "implement": ["verification_profile", "mem_context"],
    "test": ["verification_profile"],
    "fix": ["verification_profile", "mem_context"],
    "review": ["verification_profile"],
    "spec": ["verification_profile"],
    "plan": ["mem_context"],
}


def run_stage_pre_hooks(stage: str, pipeline: dict, cfg: dict) -> str:
    """Run pre-hooks for a stage. Returns markdown context string to prepend to prompt."""
    hooks = STAGE_PRE_HOOKS.get(stage, [])
    if not hooks:
        return ""

    parts: list[str] = []
    for hook in hooks:
        try:
            if hook == "verification_profile":
                ctx = _query_verification_profile(stage, pipeline, cfg)
            elif hook == "mem_context":
                ctx = _query_mem_context(stage, pipeline, cfg)
            else:
                ctx = ""
            if ctx:
                parts.append(ctx)
        except Exception as exc:
            log.debug("Pre-hook %s failed for stage %s: %s", hook, stage, exc)

    return "\n\n".join(parts)


def run_stage(
    stage_name: str, pipeline: dict, conn: sqlite3.Connection, cfg: dict
) -> dict:
    """Run a single pipeline stage. Returns result dict with keys: ok, stage, output, error, cost_usd."""
    pipeline_id = pipeline["pipeline_id"]

    stage_func = _STAGE_DISPATCH.get(stage_name)
    if not stage_func:
        return {
            "ok": False,
            "stage": stage_name,
            "output": None,
            "error": f"Unknown stage: {stage_name}",
            "cost_usd": 0.0,
        }

    # Run pre-hooks and inject context into pipeline for agent consumption
    pre_hook_context = run_stage_pre_hooks(stage_name, pipeline, cfg)
    if pre_hook_context:
        pipeline = {**pipeline, "_pre_hook_context": pre_hook_context}

    # Get current cycle for this stage
    cycle_row = conn.execute(
        """
        SELECT MAX(cycle) as current_cycle FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = ?
    """,
        (pipeline_id, stage_name),
    ).fetchone()
    current_cycle = cycle_row[0] if cycle_row and cycle_row[0] else 1

    conn.execute(
        """
        UPDATE pipeline_stages
        SET status = 'running', started_at = ?
        WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
    """,
        (_now_ts(), pipeline_id, stage_name, current_cycle),
    )
    conn.commit()

    try:
        result = stage_func(pipeline, conn, cfg)

        status = "completed" if result.get("ok") else "failed"
        if result.get("waiting_human"):
            status = "waiting_human"

        try:
            output_json = (
                json.dumps(result.get("output", {})) if result.get("output") else None
            )
        except (TypeError, ValueError) as e:
            output_json = json.dumps(
                {
                    "error": f"output not serializable: {e}",
                    "raw": str(result.get("output", ""))[:2000],
                }
            )

        conn.execute(
            """
            UPDATE pipeline_stages
            SET status = ?, output_json = ?, cost_usd = ?, completed_at = ?, error = ?
            WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
        """,
            (
                status,
                output_json,
                result.get("cost_usd", 0.0),
                _now_ts(),
                result.get("error"),
                pipeline_id,
                stage_name,
                current_cycle,
            ),
        )

        conn.execute(
            """
            UPDATE pipeline_runs
            SET total_cost_usd = total_cost_usd + ?, current_stage = ?
            WHERE pipeline_id = ?
        """,
            (result.get("cost_usd", 0.0), stage_name, pipeline_id),
        )

        conn.commit()

        _record_pipeline_learning(
            conn,
            pipeline_id,
            stage_name,
            result.get("ok", False),
            result.get("cost_usd", 0.0),
            result.get("error"),
        )

        return {**result, "stage": stage_name}

    except Exception as e:
        conn.rollback()
        conn.execute(
            """
            UPDATE pipeline_stages
            SET status = 'failed', error = ?, completed_at = ?
            WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
        """,
            (str(e), _now_ts(), pipeline_id, stage_name, current_cycle),
        )
        conn.commit()

        _record_pipeline_learning(conn, pipeline_id, stage_name, False, 0.0, str(e))

        return {
            "ok": False,
            "stage": stage_name,
            "output": None,
            "error": str(e),
            "cost_usd": 0.0,
        }


def stage_intake(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Check if raw input is structured, route to refine or skip to research."""
    raw_input = pipeline.get("raw_input", "")

    has_json = "{" in raw_input and "}" in raw_input
    has_sections = any(
        marker in raw_input.lower()
        for marker in ["##", "objective:", "requirements:", "---"]
    )

    if has_json or has_sections:
        conn.execute(
            """
            UPDATE pipeline_runs
            SET structured_objective = ?
            WHERE pipeline_id = ?
        """,
            (raw_input, pipeline["pipeline_id"]),
        )
        conn.commit()

        return {
            "ok": True,
            "output": {
                "routing": "skip_to_research",
                "reason": "Input already structured",
            },
            "error": None,
            "cost_usd": 0.0,
        }

    return {
        "ok": True,
        "output": {"routing": "refine", "reason": "Raw input needs refinement"},
        "error": None,
        "cost_usd": 0.0,
    }


def stage_refine(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Refine raw input into structured objective."""
    result = _spawn_agent_placeholder(
        stage="refine",
        via="claude",
        model="opus",
        pipeline=pipeline,
        cfg=cfg,
        conn=conn,
    )

    if result["ok"] and result["output"]:
        # Save entire agent output as structured objective (contains title, description, etc.)
        structured = (
            json.dumps(result["output"])
            if isinstance(result["output"], dict)
            else str(result["output"])
        )
        conn.execute(
            """
            UPDATE pipeline_runs
            SET structured_objective = ?
            WHERE pipeline_id = ?
        """,
            (structured, pipeline["pipeline_id"]),
        )
        conn.commit()

    return result


def _objective_text(pipeline: dict) -> str:
    obj = pipeline.get("structured_objective", pipeline.get("title", ""))
    if not isinstance(obj, str):
        return str(obj)

    stripped = obj.strip()
    if not stripped:
        return stripped

    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                title = str(parsed.get("title", "")).strip()
                description = str(parsed.get("description", "")).strip()
                success = parsed.get("success_criteria", [])
                constraints = parsed.get("constraints", [])

                lines: list[str] = []
                if title:
                    lines.append(f"Title: {title}")
                if description:
                    lines.append(f"Description: {description}")
                if isinstance(success, list) and success:
                    lines.append("Success criteria:")
                    lines.extend([f"- {str(x)}" for x in success[:8]])
                if isinstance(constraints, list) and constraints:
                    lines.append("Constraints:")
                    lines.extend([f"- {str(x)}" for x in constraints[:8]])
                return "\n".join(lines) if lines else stripped
        except Exception:
            return stripped

    return stripped


def stage_research(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Research stage using structured objective."""
    normalized_pipeline = {
        **pipeline,
        "structured_objective": _objective_text(pipeline),
    }
    result = _spawn_agent_placeholder(
        stage="research",
        via="claude",
        model="sonnet",
        pipeline=normalized_pipeline,
        cfg=cfg,
        conn=conn,
    )

    return result


def stage_spec(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Create spec, with optional human gate."""
    normalized_pipeline = {
        **pipeline,
        "structured_objective": _objective_text(pipeline),
    }
    result = _spawn_agent_placeholder(
        stage="spec",
        via="claude",
        model="opus",
        pipeline=normalized_pipeline,
        cfg=cfg,
        conn=conn,
    )

    if result["ok"]:
        spec_json = json.dumps(result["output"])
        conn.execute(
            """
            UPDATE pipeline_runs
            SET spec_json = ?
            WHERE pipeline_id = ?
        """,
            (spec_json, pipeline["pipeline_id"]),
        )
        conn.commit()

    return result


def _extract_plan_tasks(output: dict) -> list[dict]:
    tasks = output.get("tasks")
    if isinstance(tasks, list):
        return tasks

    items = output.get("items")
    if isinstance(items, list):
        return items

    steps = output.get("steps")
    if isinstance(steps, list):
        return steps

    phases = output.get("phases")
    if isinstance(phases, list):
        return phases

    plan = output.get("plan")
    if isinstance(plan, list):
        return plan
    if isinstance(plan, dict):
        nested_tasks = plan.get("tasks")
        if isinstance(nested_tasks, list):
            return nested_tasks
        nested_steps = plan.get("steps")
        if isinstance(nested_steps, list):
            return nested_steps

    return []


def _normalize_plan_tasks(tasks: list[dict]) -> list[dict]:
    """Coerce common LLM schema drift before strict validation.

    Handles: missing task_id, scalar depends_on/acceptance_criteria/validation_cmds,
    None values for list fields, string-wrapped lists.
    """
    for idx, task in enumerate(tasks):
        if not isinstance(task, dict):
            continue

        # Coerce phase/spec objects into executable task records
        if (
            not str(task.get("task_id", "")).strip()
            and not str(task.get("id", "")).strip()
        ):
            phase_name = str(task.get("name", "")).strip()
            if phase_name:
                task["task_id"] = phase_name.lower().replace(" ", "_").replace("-", "_")

        if not str(task.get("task_id", "")).strip() and str(task.get("id", "")).strip():
            task["task_id"] = str(task.get("id", "")).strip()

        # Auto-generate task_id when LLM omits it
        if not str(task.get("task_id", "")).strip():
            task["task_id"] = f"TASK-{idx + 1:03d}"

        # Coerce list fields: None → [], scalar → [scalar], already list → keep
        for key in ("depends_on", "acceptance_criteria", "validation_cmds"):
            val = task.get(key)
            if val is None:
                task[key] = []
            elif isinstance(val, str):
                task[key] = [val] if val.strip() else []
            elif isinstance(val, dict):
                # LLM sometimes emits {"cmd": "pytest"} instead of ["pytest"]
                task[key] = [json.dumps(val)]
            elif not isinstance(val, list):
                task[key] = [str(val)]

        # Fallback from phase-style fields
        if not task.get("acceptance_criteria"):
            phase_desc = str(task.get("description", "")).strip()
            task["acceptance_criteria"] = [phase_desc] if phase_desc else []

        if not task.get("validation_cmds"):
            cmd = task.get("command")
            if isinstance(cmd, str) and cmd.strip():
                task["validation_cmds"] = [cmd.strip()]
            else:
                task["validation_cmds"] = []

        if task.get("depends_on") is None:
            task["depends_on"] = []

        # Ensure human-readable summary field for downstream consumers
        if not str(task.get("summary", "")).strip():
            task["summary"] = str(
                task.get("name", task.get("description", task.get("task_id", "")))
            )[:240]

    return tasks


def _validate_plan_schema(tasks: list[dict]) -> tuple[bool, str]:
    if not tasks:
        return False, "plan schema invalid: tasks list is empty"

    task_ids: list[str] = []
    seen: set[str] = set()
    for task in tasks:
        if not isinstance(task, dict):
            return False, "plan schema invalid: each task must be an object"

        task_id = str(task.get("task_id", "")).strip()
        if not task_id:
            return False, "plan schema invalid: task_id is required"
        if task_id in seen:
            return False, f"plan schema invalid: duplicate task_id '{task_id}'"
        seen.add(task_id)
        task_ids.append(task_id)

        depends_on = task.get("depends_on")
        if not isinstance(depends_on, list):
            return (
                False,
                f"plan schema invalid: task '{task_id}' depends_on must be a list",
            )

        acceptance = task.get("acceptance_criteria")
        if not isinstance(acceptance, list):
            return (
                False,
                f"plan schema invalid: task '{task_id}' acceptance_criteria must be a list",
            )

        validation = task.get("validation_cmds")
        if not isinstance(validation, list):
            return (
                False,
                f"plan schema invalid: task '{task_id}' validation_cmds must be a list",
            )

    task_id_set = set(task_ids)
    for task in tasks:
        task_id = str(task.get("task_id", "")).strip()
        depends_on = [
            str(d).strip() for d in task.get("depends_on", []) if str(d).strip()
        ]
        missing = [dep for dep in depends_on if dep not in task_id_set]
        if missing:
            return (
                False,
                f"plan schema invalid: task '{task_id}' references missing dependencies {missing}",
            )

    incoming = {tid: 0 for tid in task_ids}
    outgoing: dict[str, list[str]] = {tid: [] for tid in task_ids}
    for task in tasks:
        task_id = str(task.get("task_id", "")).strip()
        for dep in [
            str(d).strip() for d in task.get("depends_on", []) if str(d).strip()
        ]:
            outgoing[dep].append(task_id)
            incoming[task_id] += 1

    queue = [tid for tid in task_ids if incoming[tid] == 0]
    visited = 0
    while queue:
        current = queue.pop(0)
        visited += 1
        for nxt in outgoing[current]:
            incoming[nxt] -= 1
            if incoming[nxt] == 0:
                queue.append(nxt)

    if visited != len(task_ids):
        return False, "plan schema invalid: dependency graph has cycle or deadlock"

    return True, ""


def stage_plan(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Create implementation plan."""
    normalized_pipeline = {
        **pipeline,
        "structured_objective": _objective_text(pipeline),
    }
    result = _spawn_agent_placeholder(
        stage="plan",
        via="claude",
        model="sonnet",
        pipeline=normalized_pipeline,
        cfg=cfg,
        conn=conn,
    )

    if result["ok"]:
        output = result.get("output") if isinstance(result.get("output"), dict) else {}
        pipeline_type = str(pipeline.get("pipeline_type", "")).strip().lower()
        fleetmax_cfg = (
            cfg.get("fleetmax") if isinstance(cfg.get("fleetmax"), dict) else {}
        )
        enforce_schema = (
            bool(cfg.get("swarm_waves_enabled", False))
            or pipeline_type == "fleetmax"
            or bool(fleetmax_cfg.get("swarm_waves_enabled", False))
        )

        if enforce_schema:
            tasks = _normalize_plan_tasks(_extract_plan_tasks(output))
            valid, error = _validate_plan_schema(tasks)
            if not valid:
                return {
                    "ok": False,
                    "output": None,
                    "error": error,
                    "cost_usd": result.get("cost_usd", 0.0),
                }

            normalized = []
            for task in tasks:
                normalized.append(
                    {
                        **task,
                        "task_id": str(task.get("task_id", "")).strip(),
                        "depends_on": [
                            str(d).strip()
                            for d in task.get("depends_on", [])
                            if str(d).strip()
                        ],
                    }
                )

            remaining = {t["task_id"] for t in normalized}
            completed: set[str] = set()
            waves: list[list[str]] = []
            while remaining:
                wave = sorted(
                    [
                        t["task_id"]
                        for t in normalized
                        if t["task_id"] in remaining
                        and all(dep in completed for dep in t.get("depends_on", []))
                    ]
                )
                if not wave:
                    return {
                        "ok": False,
                        "output": None,
                        "error": "plan schema invalid: dependency graph has cycle or deadlock",
                        "cost_usd": result.get("cost_usd", 0.0),
                    }
                waves.append(wave)
                for tid in wave:
                    remaining.remove(tid)
                    completed.add(tid)

            output = {"tasks": normalized, "waves": waves}
            result["output"] = output

        plan_json = json.dumps(result["output"])
        conn.execute(
            """
            UPDATE pipeline_runs
            SET plan_json = ?
            WHERE pipeline_id = ?
        """,
            (plan_json, pipeline["pipeline_id"]),
        )
        conn.commit()

        try:
            _write_plan_theorist_note(pipeline, cfg, result["output"])
        except Exception as exc:
            log.debug(
                "Failed to write theorist plan note for %s: %s",
                pipeline.get("pipeline_id"),
                exc,
            )

    return result


def stage_issues(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Create GitHub issues from plan."""
    result = _spawn_agent_placeholder(
        stage="issues",
        via="claude",
        model="sonnet",
        pipeline=pipeline,
        cfg=cfg,
        conn=conn,
    )

    if not result["ok"]:
        return result

    issues = result["output"].get("issues", []) if isinstance(result.get("output"), dict) else []
    created_urls = []

    repo_path = cfg.get("repo_root", ".")

    for issue in issues:
        title = issue.get("title", "")
        body = issue.get("body", "")
        labels = issue.get("labels", [])

        try:
            cmd = ["gh", "issue", "create", "--title", title, "--body", body]
            for label in labels:
                cmd.extend(["--label", label])

            proc_result = subprocess.run(
                cmd, cwd=repo_path, capture_output=True, text=True, timeout=30
            )

            if proc_result.returncode == 0:
                created_urls.append(proc_result.stdout.strip())
        except subprocess.TimeoutExpired:
            pass  # skip this issue, continue with rest
        except FileNotFoundError:
            break  # gh CLI not installed, stop trying

    return {
        "ok": True,
        "output": {"issues_created": created_urls},
        "error": None,
        "cost_usd": result.get("cost_usd", 0.0),
    }


def _get_swarm_wave_state(conn: sqlite3.Connection, pipeline_id: str) -> dict | None:
    """Read swarm_state from the implement stage's input_json. Returns None if not set."""
    row = conn.execute(
        "SELECT input_json FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = 'implement' ORDER BY cycle DESC LIMIT 1",
        (pipeline_id,),
    ).fetchone()
    if not row:
        return None
    raw = row[0] if isinstance(row, tuple) else row["input_json"]
    if not raw:
        return None
    try:
        payload = json.loads(raw)
        swarm = payload.get("swarm_state") if isinstance(payload, dict) else None
        if isinstance(swarm, dict) and swarm.get("enabled"):
            return swarm
    except Exception:
        pass
    return None


def _get_plan_tasks(conn: sqlite3.Connection, pipeline_id: str) -> list[dict]:
    """Read the task list from the plan stage output."""
    row = conn.execute(
        "SELECT output_json FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = 'plan' ORDER BY cycle DESC LIMIT 1",
        (pipeline_id,),
    ).fetchone()
    if not row:
        return []
    raw = row[0] if isinstance(row, tuple) else row["output_json"]
    if not raw:
        return []
    try:
        output = json.loads(raw)
        tasks = output.get("tasks") if isinstance(output, dict) else None
        return tasks if isinstance(tasks, list) else []
    except Exception:
        return []


def stage_implement(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Implement the feature. Uses swarm parallel execution when wave state is active."""
    # Worktrees must always be rooted in the pipeline target repo, never global cfg repo_root.
    repo_root = pipeline.get("project_repo", ".")
    if repo_root and repo_root != ".":
        init_worktrees(repo_root, conn)
    worktree = acquire_worktree(
        conn, pipeline["pipeline_id"], "implement", preferred="code"
    )
    if not worktree:
        return {
            "ok": False,
            "output": None,
            "error": "No worktree available",
            "cost_usd": 0.0,
        }

    worktree_name = worktree["worktree_name"]
    try:
        worktree_path = worktree["worktree_path"]

        try:
            create_feature_branch(worktree_path, pipeline["pipeline_id"], "implement")
        except RuntimeError as e:
            release_worktree(conn, worktree_name)
            return {
                "ok": False,
                "output": None,
                "error": f"Failed to create feature branch: {e}",
                "cost_usd": 0.0,
            }

        # --- Swarm parallel path ---
        swarm = _get_swarm_wave_state(conn, pipeline["pipeline_id"])
        swarm_enabled = bool(cfg.get("swarm_waves_enabled", False)) or bool(
            (cfg.get("fleetmax") or {}).get("swarm_waves_enabled", False)
        )

        if swarm and swarm_enabled and not swarm.get("done"):
            wave_tasks = swarm.get("current_wave", [])
            if wave_tasks:
                plan_tasks = _get_plan_tasks(conn, pipeline["pipeline_id"])
                if plan_tasks:
                    from pipeline.swarm_executor import execute_wave

                    log.info(
                        "Swarm mode: executing wave of %d tasks for pipeline %s",
                        len(wave_tasks),
                        pipeline["pipeline_id"],
                    )
                    try:
                        wave_result = execute_wave(
                            pipeline=pipeline,
                            conn=conn,
                            cfg=cfg,
                            wave_task_ids=wave_tasks,
                            plan_tasks=plan_tasks,
                            primary_worktree_path=worktree_path,
                        )

                        conn.execute(
                            """
                            UPDATE pipeline_stages
                            SET worktree_name = ?
                            WHERE pipeline_id = ? AND stage_name = 'implement'
                            AND cycle = (SELECT MAX(cycle) FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = 'implement')
                        """,
                            (
                                worktree_name,
                                pipeline["pipeline_id"],
                                pipeline["pipeline_id"],
                            ),
                        )
                        conn.commit()

                        return {
                            "ok": wave_result.get("ok", False),
                            "output": {
                                "completed_task_ids": wave_result.get(
                                    "completed_task_ids", []
                                ),
                                "failed_task_ids": wave_result.get("failed_task_ids", []),
                                "wave_duration": wave_result.get("duration_seconds", 0.0),
                            },
                            "error": None
                            if wave_result.get("ok")
                            else "wave execution had failures",
                            "cost_usd": wave_result.get("total_cost_usd", 0.0),
                        }
                    finally:
                        release_worktree(conn, worktree_name)

        # --- Sequential path (original) ---
        cycle_summary = _get_cycle_summary_placeholder(pipeline, conn)

        result = _spawn_agent_placeholder(
            stage="implement",
            via="codex",
            model="o3",
            worktree_path=worktree_path,
            pipeline=pipeline,
            cfg=cfg,
            cycle_summary=cycle_summary,
            conn=conn,
        )

        conn.execute(
            """
            UPDATE pipeline_stages
            SET worktree_name = ?
            WHERE pipeline_id = ? AND stage_name = 'implement'
            AND cycle = (SELECT MAX(cycle) FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = 'implement')
        """,
            (worktree_name, pipeline["pipeline_id"], pipeline["pipeline_id"]),
        )
        conn.commit()

        return result

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        release_worktree(conn, worktree_name)
        return {"ok": False, "output": None, "error": str(e), "cost_usd": 0.0}


def stage_test(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Run tests on implemented code."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT worktree_name FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = 'implement'
        ORDER BY cycle DESC
        LIMIT 1
    """,
        (pipeline["pipeline_id"],),
    ).fetchone()

    if not row or not row["worktree_name"]:
        return {
            "ok": False,
            "output": None,
            "error": "No worktree from implement stage",
            "cost_usd": 0.0,
        }

    worktree_name = row["worktree_name"]
    worktree_row = conn.execute(
        """
        SELECT * FROM worktree_state WHERE worktree_name = ?
    """,
        (worktree_name,),
    ).fetchone()

    if not worktree_row:
        return {
            "ok": False,
            "output": None,
            "error": "Worktree not found",
            "cost_usd": 0.0,
        }

    result = _spawn_agent_placeholder(
        stage="test",
        via="claude",
        model="sonnet",
        worktree_path=worktree_row["worktree_path"],
        pipeline=pipeline,
        cfg=cfg,
        conn=conn,
    )

    return result


def stage_fix(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Fix test failures."""
    conn.row_factory = sqlite3.Row

    test_row = conn.execute(
        """
        SELECT output_json, worktree_name FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = 'test'
        ORDER BY cycle DESC
        LIMIT 1
    """,
        (pipeline["pipeline_id"],),
    ).fetchone()

    if not test_row:
        return {
            "ok": False,
            "output": None,
            "error": "No test stage results",
            "cost_usd": 0.0,
        }

    worktree_name = test_row["worktree_name"] or ""
    if not worktree_name:
        impl_row = conn.execute(
            """
            SELECT worktree_name FROM pipeline_stages
            WHERE pipeline_id = ? AND stage_name = 'implement'
            ORDER BY cycle DESC
            LIMIT 1
        """,
            (pipeline["pipeline_id"],),
        ).fetchone()

        if impl_row:
            worktree_name = impl_row["worktree_name"]

    if not worktree_name:
        return {
            "ok": False,
            "output": None,
            "error": "No worktree available for fix",
            "cost_usd": 0.0,
        }

    worktree_row = conn.execute(
        """
        SELECT * FROM worktree_state WHERE worktree_name = ?
    """,
        (worktree_name,),
    ).fetchone()

    if not worktree_row:
        return {
            "ok": False,
            "output": None,
            "error": "Worktree not found",
            "cost_usd": 0.0,
        }

    test_results = (
        json.loads(test_row["output_json"]) if test_row["output_json"] else {}
    )
    cycle_summary = _get_cycle_summary_placeholder(pipeline, conn)

    result = _spawn_agent_placeholder(
        stage="fix",
        via="codex",
        model="o3",
        worktree_path=worktree_row["worktree_path"],
        pipeline=pipeline,
        cfg=cfg,
        cycle_summary=cycle_summary,
        test_results=test_results,
        conn=conn,
    )

    # NOTE: cycle_count is managed exclusively by _ralph_loop_reset in engine.py
    # Do NOT increment here — it was double-counting before this fix.
    return result


def _coerce_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.strip():
            return int(float(value.strip()))
    except (ValueError, TypeError):
        return default
    return default


def _normalize_adversarial_output(output: object) -> dict:
    normalized = {
        "issues": [],
        "summary": {"total": 0, "by_severity": {}, "by_category": {}},
        "schema_version": "v1",
    }
    if not isinstance(output, dict):
        return normalized

    raw_issues = output.get("issues")
    if not isinstance(raw_issues, list):
        return normalized

    severities = ("critical", "high", "medium", "low")
    categories = ("persona", "edge", "error", "state")
    issues: list[dict] = []
    by_severity: dict[str, int] = {}
    by_category: dict[str, int] = {}

    for issue in raw_issues:
        if not isinstance(issue, dict):
            continue
        severity = str(issue.get("severity", "medium")).lower()
        category = str(issue.get("category", "edge")).lower()
        if severity not in severities:
            severity = "medium"
        if category not in categories:
            category = "edge"
        normalized_issue = {
            "category": category,
            "severity": severity,
            "description": str(issue.get("description", "")).strip(),
            "reproduction": str(issue.get("reproduction", "")).strip(),
            "suggested_fix": str(issue.get("suggested_fix", "")).strip(),
        }
        issues.append(normalized_issue)
        by_severity[severity] = by_severity.get(severity, 0) + 1
        by_category[category] = by_category.get(category, 0) + 1

    normalized["issues"] = issues
    normalized["summary"] = {
        "total": len(issues),
        "by_severity": by_severity,
        "by_category": by_category,
    }
    return normalized


def _normalize_ralph_output(output: object) -> dict:
    normalized = {
        "iterations_run": 0,
        "issues_found": 0,
        "issues_fixed": 0,
        "tests_added": 0,
        "tests_passing": 0,
        "tests_failing": 0,
        "learnings": [],
        "remaining_issues": [],
        "schema_version": "v1",
    }
    if not isinstance(output, dict):
        return normalized

    normalized["iterations_run"] = _coerce_int(output.get("iterations_run"), 0)
    normalized["issues_found"] = _coerce_int(output.get("issues_found"), 0)
    normalized["issues_fixed"] = _coerce_int(output.get("issues_fixed"), 0)
    normalized["tests_added"] = _coerce_int(output.get("tests_added"), 0)
    normalized["tests_passing"] = _coerce_int(output.get("tests_passing"), 0)
    normalized["tests_failing"] = _coerce_int(output.get("tests_failing"), 0)

    learnings = output.get("learnings")
    if isinstance(learnings, list):
        normalized["learnings"] = learnings

    remaining_issues = output.get("remaining_issues")
    if isinstance(remaining_issues, list):
        normalized["remaining_issues"] = [
            i for i in remaining_issues if isinstance(i, dict)
        ]

    return normalized


def _normalize_review_output(output: object) -> dict:
    normalized = {
        "verdict": "APPROVE",
        "needs_fix": False,
        "feedback": "",
        "issues": [],
        "schema_version": "v1",
    }
    if not isinstance(output, dict):
        return normalized

    verdict = str(output.get("verdict", "APPROVE")).upper()
    if verdict not in ("APPROVE", "REQUEST_CHANGES"):
        verdict = "APPROVE"

    issues_raw = output.get("issues")
    issues = (
        [
            issue
            for issue in issues_raw
            if isinstance(issue, dict) or (isinstance(issue, str) and issue.strip())
        ]
        if isinstance(issues_raw, list)
        else []
    )
    has_issues = len(issues) > 0

    if has_issues and verdict == "APPROVE":
        verdict = "REQUEST_CHANGES"

    needs_fix = bool(
        output.get("needs_fix", False) or verdict == "REQUEST_CHANGES" or has_issues
    )
    feedback = output.get("feedback")
    if feedback is None or str(feedback).strip() == "":
        feedback = output.get("summary", "")
    if (feedback is None or str(feedback).strip() == "") and has_issues:
        feedback = f"{len(issues)} issue(s) reported by reviewer"

    normalized["verdict"] = verdict
    normalized["needs_fix"] = needs_fix
    normalized["issues"] = issues
    normalized["feedback"] = str(feedback) if feedback is not None else ""
    return normalized


def stage_review(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Review implementation."""
    conn.row_factory = sqlite3.Row

    impl_row = conn.execute(
        """
        SELECT worktree_name FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = 'implement'
        ORDER BY cycle DESC
        LIMIT 1
    """,
        (pipeline["pipeline_id"],),
    ).fetchone()

    worktree_name = impl_row["worktree_name"] if impl_row else None
    worktree_path = None

    if worktree_name:
        worktree_row = conn.execute(
            """
            SELECT worktree_path FROM worktree_state WHERE worktree_name = ?
        """,
            (worktree_name,),
        ).fetchone()

        if worktree_row:
            worktree_path = worktree_row["worktree_path"]

    result = _spawn_agent_placeholder(
        stage="review",
        via="claude",
        model="opus",
        worktree_path=worktree_path,
        pipeline=pipeline,
        cfg=cfg,
        conn=conn,
    )

    if not result["ok"]:
        return result

    normalized_output = _normalize_review_output(result.get("output"))
    result["output"] = normalized_output

    project_config = get_project_config(conn, pipeline.get("project_repo", ""))
    max_cycles = project_config.get("max_ralph_cycles", 5)
    current_cycle = pipeline.get("cycle_count", 0)

    if normalized_output["needs_fix"] and current_cycle < max_cycles:
        return {**result, "needs_fix": True}

    return result


def _run_bug_hunter_pre_pr(
    pipeline: dict, conn: sqlite3.Connection, cfg: dict, worktree_path: str
) -> dict:
    """Run bug-hunter/adversarial audit before PR creation."""
    objective = str(pipeline.get("structured_objective", pipeline.get("title", "")))
    bug_hunter_prompt = (
        "Run /bug-hunter adversarial audit before opening a PR. "
        "Focus on correctness, regressions, security, and edge cases. "
        "Create or update tests for discovered defects and run relevant test commands.\n\n"
        f"Original objective:\n{objective}\n\n"
        "Return structured issues with severity/category/description/reproduction/suggested_fix."
    )

    result = _spawn_agent_placeholder(
        stage="adversarial",
        via="codex",
        model="coding",
        worktree_path=worktree_path,
        pipeline={**pipeline, "structured_objective": bug_hunter_prompt},
        cfg=cfg,
        conn=conn,
    )

    if result.get("ok"):
        output = result.get("output") or {}
        result["output"] = _normalize_adversarial_output(output)
    return result


def stage_merge(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Create and optionally merge PR."""
    conn.row_factory = sqlite3.Row

    impl_row = conn.execute(
        """
        SELECT worktree_name FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = 'implement'
        ORDER BY cycle DESC
        LIMIT 1
    """,
        (pipeline["pipeline_id"],),
    ).fetchone()

    if not impl_row or not impl_row["worktree_name"]:
        return {
            "ok": False,
            "output": None,
            "error": "No worktree from implement stage",
            "cost_usd": 0.0,
        }

    worktree_name = impl_row["worktree_name"]
    worktree_row = conn.execute(
        """
        SELECT * FROM worktree_state WHERE worktree_name = ?
    """,
        (worktree_name,),
    ).fetchone()

    if not worktree_row:
        return {
            "ok": False,
            "output": None,
            "error": "Worktree not found",
            "cost_usd": 0.0,
        }

    worktree_path = worktree_row["worktree_path"]

    bug_hunter_result = _run_bug_hunter_pre_pr(pipeline, conn, cfg, worktree_path)
    if not bug_hunter_result.get("ok"):
        return {
            "ok": False,
            "output": {
                "bug_hunter": bug_hunter_result.get("output"),
            },
            "error": f"Pre-PR bug-hunter failed: {bug_hunter_result.get('error') or 'unknown error'}",
            "cost_usd": float(bug_hunter_result.get("cost_usd", 0.0) or 0.0),
        }

    title = f"{pipeline['pipeline_type']}: {pipeline['title']}"
    body = (
        f"Pipeline: {pipeline['pipeline_id']}\n\n"
        f"{pipeline.get('structured_objective', '')}\n\n"
        f"Pre-PR bug-hunter: completed"
    )

    pr_url, pr_number = create_pr(worktree_path, title, body)

    if not pr_url:
        release_worktree(conn, worktree_name)
        return {
            "ok": False,
            "output": None,
            "error": "Failed to create PR",
            "cost_usd": 0.0,
        }

    conn.execute(
        """
        UPDATE worktree_state
        SET pr_url = ?, pr_number = ?, status = 'pr_open'
        WHERE worktree_name = ?
    """,
        (pr_url, pr_number, worktree_name),
    )
    conn.commit()

    project_config = get_project_config(conn, pipeline.get("project_repo", ""))
    auto_merge = project_config.get("deploy_config", "{}")
    if isinstance(auto_merge, str):
        auto_merge = json.loads(auto_merge)

    merged = False
    merge_error = None
    if auto_merge.get("auto_merge"):
        if pr_number:
            success, error = merge_pr(pr_number)
            if success:
                merged = True
                conn.execute(
                    """
                    UPDATE worktree_state
                    SET status = 'merged'
                    WHERE worktree_name = ?
                """,
                    (worktree_name,),
                )
                conn.commit()
            else:
                merge_error = error

    # Only release worktree if merged or no auto-merge attempted.
    # Keep worktree alive on merge failure so user can inspect/retry.
    if merged or not auto_merge.get("auto_merge"):
        release_worktree(conn, worktree_name)

    return {
        "ok": True,
        "output": {
            "pr_url": pr_url,
            "pr_number": pr_number,
            "merged": merged,
            "merge_error": merge_error,
            "pre_pr_bug_hunter": bug_hunter_result.get("output"),
        },
        "error": None,
        "cost_usd": float(bug_hunter_result.get("cost_usd", 0.0) or 0.0),
    }


def stage_deploy(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Deploy the changes."""
    project_config = get_project_config(conn, pipeline.get("project_repo", ""))
    deploy_target = project_config.get("deploy_target", "merge")

    if deploy_target == "merge":
        return {
            "ok": True,
            "output": {"deploy_type": "merge", "status": "complete"},
            "error": None,
            "cost_usd": 0.0,
        }

    conn.row_factory = sqlite3.Row
    impl_row = conn.execute(
        """
        SELECT worktree_name FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = 'implement'
        ORDER BY cycle DESC
        LIMIT 1
    """,
        (pipeline["pipeline_id"],),
    ).fetchone()

    worktree_path = None
    if impl_row and impl_row["worktree_name"]:
        worktree_row = conn.execute(
            """
            SELECT worktree_path FROM worktree_state WHERE worktree_name = ?
        """,
            (impl_row["worktree_name"],),
        ).fetchone()

        if worktree_row:
            worktree_path = worktree_row["worktree_path"]

    if not worktree_path:
        worktree_path = cfg.get("repo_root", ".")

    deploy_config = project_config.get("deploy_config", "{}")
    if isinstance(deploy_config, str):
        deploy_config = json.loads(deploy_config)

    try:
        if deploy_target == "railway":
            result = subprocess.run(
                ["railway", "up"],
                cwd=worktree_path,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                return {
                    "ok": False,
                    "output": None,
                    "error": f"Railway deploy failed: {result.stderr}",
                    "cost_usd": 0.0,
                }

            return {
                "ok": True,
                "output": {"deploy_type": "railway", "status": "deployed"},
                "error": None,
                "cost_usd": 0.0,
            }

        elif deploy_target == "docker":
            image_name = deploy_config.get("image_name", "app:latest")

            result = subprocess.run(
                ["docker", "build", "-t", image_name, "."],
                cwd=worktree_path,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                return {
                    "ok": False,
                    "output": None,
                    "error": f"Docker build failed: {result.stderr}",
                    "cost_usd": 0.0,
                }

            if deploy_config.get("push"):
                result = subprocess.run(
                    ["docker", "push", image_name],
                    cwd=worktree_path,
                    capture_output=True,
                    text=True,
                    timeout=300,
                )

                if result.returncode != 0:
                    return {
                        "ok": False,
                        "output": None,
                        "error": f"Docker push failed: {result.stderr}",
                        "cost_usd": 0.0,
                    }

            return {
                "ok": True,
                "output": {
                    "deploy_type": "docker",
                    "status": "deployed",
                    "image": image_name,
                },
                "error": None,
                "cost_usd": 0.0,
            }

        elif deploy_target == "ssh":
            script_path = deploy_config.get("script_path", "./deploy.sh")

            result = subprocess.run(
                [script_path],
                cwd=worktree_path,
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                return {
                    "ok": False,
                    "output": None,
                    "error": f"Deploy script failed: {result.stderr}",
                    "cost_usd": 0.0,
                }

            return {
                "ok": True,
                "output": {"deploy_type": "ssh", "status": "deployed"},
                "error": None,
                "cost_usd": 0.0,
            }

        else:
            return {
                "ok": False,
                "output": None,
                "error": f"Unknown deploy target: {deploy_target}",
                "cost_usd": 0.0,
            }

    except Exception as e:
        return {
            "ok": False,
            "output": None,
            "error": f"Deploy error: {str(e)}",
            "cost_usd": 0.0,
        }


def stage_adversarial(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Run adversarial testing: multi-turn persona simulation, edge cases, error injection.

    Dispatches an agent that:
    1. Identifies the project type and user personas
    2. Simulates multi-turn interactions as different user types (beginner, power user, malicious)
    3. Tests edge cases: empty states, error states, large inputs, concurrent access
    4. Injects common failure modes: network errors, invalid data, timeout scenarios
    5. Logs all failures with reproduction steps
    """
    conn.row_factory = sqlite3.Row

    impl_row = conn.execute(
        """
        SELECT worktree_name FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = 'implement'
        ORDER BY cycle DESC LIMIT 1
    """,
        (pipeline["pipeline_id"],),
    ).fetchone()

    if not impl_row or not impl_row["worktree_name"]:
        return {
            "ok": False,
            "output": None,
            "error": "No worktree from implement stage",
            "cost_usd": 0.0,
        }

    worktree_name = impl_row["worktree_name"]
    worktree_row = conn.execute(
        "SELECT * FROM worktree_state WHERE worktree_name = ?", (worktree_name,)
    ).fetchone()

    if not worktree_row:
        return {
            "ok": False,
            "output": None,
            "error": "Worktree not found",
            "cost_usd": 0.0,
        }

    # Gather test results from prior stage for context
    test_row = conn.execute(
        """
        SELECT output_json FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = 'test'
        ORDER BY cycle DESC LIMIT 1
    """,
        (pipeline["pipeline_id"],),
    ).fetchone()
    try:
        test_context = (
            json.loads(test_row["output_json"])
            if test_row and test_row["output_json"]
            else {}
        )
    except (json.JSONDecodeError, TypeError):
        test_context = {}

    structured_obj = pipeline.get("structured_objective", "")
    adversarial_prompt = f"""You are an adversarial tester. The project objective is:
{structured_obj}

Prior test results: {json.dumps(test_context)[:500]}

Run these adversarial test categories:
1. PERSONA SIMULATION: Simulate 3 user types interacting with this code:
   - Beginner (confused, typos, wrong order of operations)
   - Power user (edge cases, concurrent access, large inputs)
   - Malicious user (injection, boundary violations, auth bypass)

2. EDGE CASES: Test empty states, null inputs, maximum-length inputs,
   special characters, unicode, concurrent modifications

3. ERROR INJECTION: What happens when external services fail?
   Network timeouts, invalid responses, disk full, permission denied

4. STATE CORRUPTION: Test refresh mid-flow, duplicate submissions,
   race conditions, session expiry scenarios

For each issue found, output a structured JSON array:
[{{"category": "persona|edge|error|state", "severity": "critical|high|medium|low",
   "description": "what happened", "reproduction": "steps to reproduce",
   "suggested_fix": "how to fix"}}]

Write adversarial tests in a test file if the project has a test framework.
Run the test suite after adding tests. Report pass/fail counts."""

    result = _spawn_agent_placeholder(
        stage="adversarial",
        via="codex",
        model="coding",
        worktree_path=worktree_row["worktree_path"],
        pipeline={**pipeline, "structured_objective": adversarial_prompt},
        cfg=cfg,
        conn=conn,
    )

    if result.get("ok"):
        result["output"] = _normalize_adversarial_output(result.get("output"))

    return result


def stage_ralph(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """RALPH loop: Review → Act → Learn → Push → Harden.

    Iterative self-improvement cycle that:
    1. REVIEW: Analyze all prior stage outputs (test, adversarial, review feedback)
    2. ACT: Fix every issue found, prioritized by severity
    3. LEARN: Record what failed and what fixed it (for compound learning)
    4. PUSH: Run full test suite, commit passing changes
    5. HARDEN: Add regression tests for each fix

    Repeats until clean or max iterations reached.
    """
    conn.row_factory = sqlite3.Row

    impl_row = conn.execute(
        """
        SELECT worktree_name FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = 'implement'
        ORDER BY cycle DESC LIMIT 1
    """,
        (pipeline["pipeline_id"],),
    ).fetchone()

    if not impl_row or not impl_row["worktree_name"]:
        return {
            "ok": False,
            "output": None,
            "error": "No worktree from implement stage",
            "cost_usd": 0.0,
        }

    worktree_name = impl_row["worktree_name"]
    worktree_row = conn.execute(
        "SELECT * FROM worktree_state WHERE worktree_name = ?", (worktree_name,)
    ).fetchone()

    if not worktree_row:
        return {
            "ok": False,
            "output": None,
            "error": "Worktree not found",
            "cost_usd": 0.0,
        }

    # Collect all prior findings: test failures, adversarial issues, review feedback
    findings = []
    for prior_stage in ("test", "adversarial", "review"):
        row = conn.execute(
            """
            SELECT output_json, error FROM pipeline_stages
            WHERE pipeline_id = ? AND stage_name = ?
            ORDER BY cycle DESC LIMIT 1
        """,
            (pipeline["pipeline_id"], prior_stage),
        ).fetchone()
        if row and row["output_json"]:
            try:
                findings.append(
                    {"stage": prior_stage, "output": json.loads(row["output_json"])}
                )
            except (json.JSONDecodeError, TypeError):
                findings.append(
                    {"stage": prior_stage, "error": "malformed output_json"}
                )
        if row and row["error"]:
            findings.append({"stage": prior_stage, "error": row["error"]})

    proj_cfg = get_project_config(conn, pipeline.get("project_repo", ""))
    max_iterations = proj_cfg.get("max_ralph_cycles", 5)

    cycle_summary = _get_cycle_summary_placeholder(pipeline, conn)

    ralph_prompt = f"""You are running a RALPH loop (Review→Act→Learn→Push→Harden).

FINDINGS FROM PRIOR STAGES:
{json.dumps(findings, indent=2)[:3000]}

CYCLE HISTORY:
{cycle_summary[:1000]}

MAX ITERATIONS: {max_iterations}

Execute this loop:

1. REVIEW: Analyze all findings above. Categorize by severity (critical→low).
   List every issue that needs fixing.

2. ACT: Fix each issue, starting with critical. For each fix:
   - Identify the exact file and line
   - Apply the minimal fix
   - Verify the fix doesn't break other things

3. LEARN: For each fix, document:
   - What was wrong (root cause)
   - What fixed it (the change)
   - How to prevent it (the pattern)

4. PUSH: Run the full test suite. If tests pass, commit with message:
   "fix: [summary] — RALPH loop cycle N"
   If tests fail, go back to step 2.

5. HARDEN: For each fix applied, add a regression test that would
   catch the bug if it was reintroduced. Run tests again.

Output format:
{{"iterations_run": N, "issues_found": N, "issues_fixed": N,
  "tests_added": N, "tests_passing": N, "tests_failing": N,
  "learnings": [{{"pattern": "...", "fix": "...", "confidence": 0.8}}],
  "remaining_issues": [{{"description": "...", "severity": "..."}}]}}

Stop iterating when: all tests pass AND no remaining issues, OR max iterations reached."""

    result = _spawn_agent_placeholder(
        stage="ralph",
        via="codex",
        model="coding",
        worktree_path=worktree_row["worktree_path"],
        pipeline={**pipeline, "structured_objective": ralph_prompt},
        cfg=cfg,
        cycle_summary=cycle_summary,
        conn=conn,
    )

    if result.get("ok"):
        result["output"] = _normalize_ralph_output(result.get("output"))

    # Record learnings from RALPH output
    if result["ok"] and isinstance(result.get("output"), dict):
        learnings = result["output"].get("learnings", [])
        for learning in learnings[:10]:
            pattern = (
                learning.get("pattern", "unknown")
                if isinstance(learning, dict)
                else str(learning)
            )
            fix = learning.get("fix", "") if isinstance(learning, dict) else ""
            _record_pipeline_learning(
                conn,
                pipeline["pipeline_id"],
                f"ralph:{pattern}:{fix}"[:200],
                ok=True,
                cost=result.get("cost_usd", 0.0),
            )

    return result


def stage_cleanup(pipeline: dict, conn: sqlite3.Connection, cfg: dict) -> dict:
    """Post-merge cleanup: release worktrees, prune branches, record learnings.

    Final stage in the fleetmax pipeline that:
    1. Releases all worktrees associated with this pipeline
    2. Prunes remote tracking branches that were merged
    3. Records compound learnings from the entire pipeline run
    4. Sends completion notification
    """
    conn.row_factory = sqlite3.Row
    pipeline_id = pipeline["pipeline_id"]

    # Release all worktrees for this pipeline
    worktrees = conn.execute(
        """
        SELECT worktree_name, status FROM worktree_state
        WHERE pipeline_id = ?
    """,
        (pipeline_id,),
    ).fetchall()

    released = 0
    for wt in worktrees:
        if wt["status"] not in ("released", "merged"):
            try:
                release_worktree(conn, wt["worktree_name"])
                released += 1
            except Exception as exc:
                log.debug("Failed to release worktree %s: %s", wt["worktree_name"], exc)

    # Record compound learning from full pipeline
    stage_rows = conn.execute(
        """
        SELECT stage_name, status, cost_usd, error
        FROM pipeline_stages
        WHERE pipeline_id = ?
        ORDER BY stage_order ASC
    """,
        (pipeline_id,),
    ).fetchall()

    total_cost = sum(float(r["cost_usd"] or 0) for r in stage_rows)
    failed_stages = [r["stage_name"] for r in stage_rows if r["status"] == "failed"]
    completed_stages = [
        r["stage_name"] for r in stage_rows if r["status"] == "completed"
    ]

    # Send completion notification
    _send_notification_placeholder(
        conn,
        "pipeline_complete",
        f"Pipeline {pipeline_id} complete: {len(completed_stages)} stages, "
        f"{len(failed_stages)} failed, ${total_cost:.4f} total cost",
        severity="info",
    )

    # Record pipeline learning
    _record_pipeline_learning(
        conn,
        pipeline_id,
        "cleanup",
        ok=len(failed_stages) == 0,
        cost=total_cost,
        error=f"failed: {','.join(failed_stages)}" if failed_stages else None,
    )

    cleanup_output = {
        "worktrees_released": released,
        "total_stages": len(stage_rows),
        "completed_stages": len(completed_stages),
        "failed_stages": failed_stages,
        "total_cost_usd": total_cost,
    }

    if _theorist_enabled(cfg):
        try:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            pid = pipeline_id
            note_id = f"th-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}-decision-{pid[:8]}"
            frontmatter = {
                "id": note_id,
                "type": "decision",
                "repo": Path(
                    cfg.get("repo_root", pipeline.get("project_repo", "."))
                ).name,
                "scope": "repo",
                "status": "active" if not failed_stages else "draft",
                "owners": ["fleetmax"],
                "tags": ["theorist", "fleetmax", "cleanup"],
                "links": {"relates_to": [pid], "supersedes": []},
                "timestamps": {"created_utc": now, "updated_utc": now},
                "schema_version": 1,
            }
            body = (
                f"# Pipeline Cleanup Decision {pid}\\n\\n"
                f"## Context\\n\\n"
                f"Finalization after pipeline execution.\\n\\n"
                f"## Decision / Plan\\n\\n"
                f"```json\\n{json.dumps(cleanup_output, indent=2)}\\n```\\n\\n"
                f"## Machine Contract\\n\\n"
                f'```json\\n{{\\n  \\"pipeline_id\\": \\"{pid}\\",\\n  \\"stage\\": \\"cleanup\\",\\n  \\"checks\\": [\\"worktrees_released\\", \\"pipeline_learning_recorded\\"]\\n}}\\n```\\n\\n'
                f"## Verification\\n\\n"
                f"- [ ] worktree_state updated\\n- [ ] pipeline learning persisted\\n\\n"
                f"## Change Log\\n\\n"
                f"- {datetime.now(timezone.utc).strftime('%Y-%m-%d')}: generated by fleet pipeline stage_cleanup\\n"
            )
            note_path = _theorist_notes_dir(pipeline, cfg) / f"{pid}-cleanup.md"
            _write_theorist_note(note_path, frontmatter, body)
        except Exception as exc:
            log.debug(
                "Failed to write theorist cleanup note for %s: %s", pipeline_id, exc
            )

    return {
        "ok": True,
        "output": cleanup_output,
        "error": None,
        "cost_usd": 0.0,
    }


def _spawn_agent_placeholder(
    stage: str,
    via: str,
    model: str,
    pipeline: dict,
    cfg: dict,
    worktree_path: str = "",
    cycle_summary: str = "",
    test_results: dict = None,
    conn: sqlite3.Connection = None,
) -> dict:
    """Spawn an agent via pipeline.agents and adapt the return format."""
    if conn is not None:
        try:
            rows = conn.execute(
                "SELECT worktree_name FROM worktree_state WHERE pipeline_id = ? AND status = 'assigned'",
                (pipeline.get("pipeline_id", ""),),
            ).fetchall()
            for row in rows:
                wt = row[0] if isinstance(row, tuple) else row["worktree_name"]
                if wt:
                    heartbeat_worktree(conn, wt)
        except Exception:
            pass

    raw = spawn_agent(stage, pipeline, worktree_path, cycle_summary, cfg, conn)

    if conn is not None:
        try:
            rows = conn.execute(
                "SELECT worktree_name FROM worktree_state WHERE pipeline_id = ? AND status = 'assigned'",
                (pipeline.get("pipeline_id", ""),),
            ).fetchall()
            for row in rows:
                wt = row[0] if isinstance(row, tuple) else row["worktree_name"]
                if wt:
                    heartbeat_worktree(conn, wt)
        except Exception:
            pass

    output = parse_stage_output(stage, raw.get("stdout", "")) if raw.get("ok") else None
    return {
        "ok": raw.get("ok", False),
        "output": output,
        "error": raw.get("stderr", "") if not raw.get("ok") else None,
        "cost_usd": raw.get("cost_usd", 0.0),
    }


def _get_cycle_summary_placeholder(pipeline: dict, conn: sqlite3.Connection) -> str:
    """Get cycle summaries for injection into next agent.

    Queries across ALL stage names (not filtered by a specific stage) to get
    the most recent summary from any prior cycle. This gives the next agent
    full context of what happened in previous RALPH iterations.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT stage_name, cycle, summary FROM cycle_summaries
        WHERE pipeline_id = ?
        ORDER BY cycle DESC, created_at DESC
        LIMIT 5
    """,
        (pipeline["pipeline_id"],),
    ).fetchall()

    if not rows:
        return ""

    parts = []
    for row in rows:
        parts.append(f"[Cycle {row['cycle']}/{row['stage_name']}] {row['summary']}")
    return "\n".join(parts)


def _send_notification_placeholder(
    conn: sqlite3.Connection,
    event_type: str,
    message: str,
    severity: str = "info",
    task_id: str = "",
    run_id: str = "",
) -> None:
    """Placeholder for notification sending."""
    conn.execute(
        """
        INSERT INTO notifications
        (event_type, message, severity, task_id, run_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (event_type, message, severity, task_id, run_id, _now_ts()),
    )
    conn.commit()


def _record_pipeline_learning(
    conn: sqlite3.Connection,
    pipeline_id: str,
    stage: str,
    ok: bool,
    cost: float,
    error: str | None = None,
) -> None:
    """Record pipeline stage outcome to global_learning for cross-session learning."""
    try:
        ts = _now_ts()
        if ok:
            rule = f"pipeline:{stage} succeeded (cost=${cost:.4f})"
            rationale = f"pipeline={pipeline_id}"
            tags = "pipeline,stage,success,auto-learned"
        else:
            rule = f"pipeline:{stage} failed: {(error or 'unknown')[:120]}"
            rationale = f"pipeline={pipeline_id}"
            tags = "pipeline,stage,failure,auto-learned"
        conn.execute(
            "INSERT INTO global_learning(ts, rule, rationale, tags, source, confidence) VALUES(?,?,?,?,?,?)",
            (ts, rule, rationale, tags, "pipeline-engine", 0.6 if ok else 0.7),
        )
        conn.commit()
    except Exception as exc:
        log.debug("Failed to record learning for %s/%s: %s", pipeline_id, stage, exc)


def _lazy_self_update_stages() -> dict[str, Callable]:
    """Lazily import self_update stages to avoid circular imports."""
    from pipeline.self_update import stage_plan_self_update, stage_validate, stage_apply

    return {
        "plan_self_update": stage_plan_self_update,
        "validate": stage_validate,
        "apply": stage_apply,
    }


_STAGE_DISPATCH: dict[str, Callable] = {
    "intake": stage_intake,
    "refine": stage_refine,
    "research": stage_research,
    "spec": stage_spec,
    "plan": stage_plan,
    "issues": stage_issues,
    "implement": stage_implement,
    "test": stage_test,
    "adversarial": stage_adversarial,
    "ralph": stage_ralph,
    "fix": stage_fix,
    "review": stage_review,
    "merge": stage_merge,
    "deploy": stage_deploy,
    "cleanup": stage_cleanup,
    # self_update stages populated lazily below
}

# Populate self-update stages
try:
    _STAGE_DISPATCH.update(_lazy_self_update_stages())
except ImportError:
    log.debug("self_update stages not available")
    pass


# ---------------------------------------------------------------------------
# Role-based pipeline dispatcher (v2)
# ---------------------------------------------------------------------------


def run_role_pipeline(
    pipeline: dict,
    conn: sqlite3.Connection,
    cfg: dict,
    objective: str | None = None,
) -> dict:
    """Execute a role-based pipeline: classify → compose → dispatch roles sequentially.

    This is the v2 pipeline that replaces stage-based dispatch with specialized
    agent roles. Each role gets:
    - Domain-specific system prompt
    - Full skill content (not truncated)
    - Minimal structured handoff (not full pipeline state)
    - Role-appropriate model and tools

    Returns: {ok, results: {role_name: result}, total_cost, errors: []}
    """
    from pipeline.classifier import classify_task
    from pipeline.composer import compose_pipeline
    from pipeline.handoff import build_handoff
    from pipeline.agents import spawn_role_agent, parse_stage_output

    try:
        from roles import get_role, load_role_skills
    except ImportError:
        import sys

        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from roles import get_role, load_role_skills

    objective = objective or pipeline.get(
        "structured_objective", pipeline.get("title", "")
    )
    pipeline_id = pipeline.get("pipeline_id", "")

    # Phase 1: Classify
    classification = classify_task(objective)
    log.info(
        "Role pipeline: classified as type=%s complexity=%d",
        classification.task_type,
        classification.complexity,
    )

    # Phase 2: Compose
    composed = compose_pipeline(classification)
    log.info(
        "Role pipeline: composed %d steps: %s", len(composed.steps), composed.role_names
    )

    # Phase 3: Dispatch roles in order
    results: dict[str, dict] = {}
    previous_results: dict[str, dict] = {}
    total_cost = 0.0
    errors: list[str] = []

    worktree_path = pipeline.get("worktree_path", cfg.get("repo_root", ""))

    for step in composed.steps:
        role_name = step.role_name

        # Check dependencies
        deps_ok = True
        for dep in step.depends_on:
            if dep not in results:
                errors.append(
                    f"Role {role_name} depends on {dep} which hasn't completed"
                )
                deps_ok = False
            elif not results[dep].get("ok"):
                log.warning("Dependency %s failed — skipping %s", dep, role_name)
                errors.append(f"Skipped {role_name}: dependency {dep} failed")
                deps_ok = False

        if not deps_ok:
            continue

        # Get role config and load skills
        try:
            role_config = get_role(role_name)
        except KeyError:
            errors.append(f"Unknown role: {role_name}")
            continue

        skill_context = load_role_skills(role_config)

        # Build structured handoff
        handoff = build_handoff(
            task_id=pipeline_id,
            objective=objective,
            role_name=role_name,
            pipeline=pipeline,
            previous_results=previous_results,
            relevant_files=None,  # auto-extract from pipeline
            constraints=None,  # auto-extract from pipeline
        )

        log.info(
            "Dispatching role: %s (handoff ~%d tokens, skills ~%d chars)",
            role_name,
            handoff.token_estimate(),
            len(skill_context),
        )

        # Research-first gate check
        if role_config.research_first and "researcher" not in previous_results:
            log.warning(
                "Role %s requires research-first but no researcher output available",
                role_name,
            )

        # Dispatch
        result = spawn_role_agent(
            role_name=role_name,
            handoff=handoff,
            role_config=role_config,
            skill_context=skill_context,
            worktree_path=worktree_path,
            cfg=cfg,
            conn=conn,
            complexity=classification.complexity,
        )

        # Parse output
        if result.get("ok") and result.get("stdout"):
            parsed = parse_stage_output(role_name, result["stdout"])
            result["output"] = parsed

        results[role_name] = result
        previous_results[role_name] = result
        total_cost += result.get("cost_usd", 0.0)

        if not result.get("ok"):
            errors.append(
                f"Role {role_name} failed: {result.get('stderr', 'unknown error')[:200]}"
            )

        # Evaluator loop: if reviewer rejects, re-run the failing role
        if role_name == "reviewer" and result.get("ok"):
            review_output = result.get("output", {})
            verdict = _extract_review_verdict(review_output)
            if verdict == "REQUEST_CHANGES":
                feedback = _extract_review_feedback(review_output)
                rerun_role = _identify_rerun_target(composed, results)
                if rerun_role and rerun_role in results:
                    log.info(
                        "Reviewer requested changes — re-running %s with feedback",
                        rerun_role,
                    )
                    # Re-dispatch with reviewer feedback as constraint
                    rerun_config = get_role(rerun_role)
                    rerun_skill_context = load_role_skills(rerun_config)
                    rerun_handoff = build_handoff(
                        task_id=pipeline_id,
                        objective=objective,
                        role_name=rerun_role,
                        pipeline=pipeline,
                        previous_results=previous_results,
                        constraints=[f"REVIEWER FEEDBACK: {feedback}"],
                    )
                    rerun_result = spawn_role_agent(
                        role_name=rerun_role,
                        handoff=rerun_handoff,
                        role_config=rerun_config,
                        skill_context=rerun_skill_context,
                        worktree_path=worktree_path,
                        cfg=cfg,
                        conn=conn,
                        complexity=classification.complexity,
                    )
                    if rerun_result.get("ok") and rerun_result.get("stdout"):
                        rerun_result["output"] = parse_stage_output(
                            rerun_role, rerun_result["stdout"]
                        )
                    results[f"{rerun_role}_rerun"] = rerun_result
                    total_cost += rerun_result.get("cost_usd", 0.0)

                    if not rerun_result.get("ok"):
                        errors.append(f"Re-run of {rerun_role} failed")
                    else:
                        # Re-review rerun output; still failing should surface as an error
                        recheck_handoff = build_handoff(
                            task_id=pipeline_id,
                            objective=objective,
                            role_name="reviewer",
                            pipeline=pipeline,
                            previous_results={
                                **previous_results,
                                f"{rerun_role}_rerun": rerun_result,
                            },
                            constraints=[
                                f"REVIEW ONLY rerun of {rerun_role}; output APPROVE or REQUEST_CHANGES"
                            ],
                        )
                        recheck_result = spawn_role_agent(
                            role_name="reviewer",
                            handoff=recheck_handoff,
                            role_config=get_role("reviewer"),
                            skill_context=load_role_skills(get_role("reviewer")),
                            worktree_path=worktree_path,
                            cfg=cfg,
                            conn=conn,
                            complexity=classification.complexity,
                        )
                        if recheck_result.get("ok") and recheck_result.get("stdout"):
                            recheck_result["output"] = parse_stage_output(
                                "reviewer", recheck_result["stdout"]
                            )
                        results["reviewer_rerun_check"] = recheck_result
                        total_cost += recheck_result.get("cost_usd", 0.0)
                        if not recheck_result.get("ok"):
                            errors.append("Reviewer rerun check failed")
                        else:
                            rerun_verdict = _extract_review_verdict(
                                recheck_result.get("output", {})
                            )
                            if rerun_verdict == "REQUEST_CHANGES":
                                errors.append(
                                    f"Re-run of {rerun_role} still rejected by reviewer"
                                )

    return {
        "ok": len(errors) == 0,
        "results": results,
        "total_cost": total_cost,
        "errors": errors,
        "classification": classification.to_dict(),
        "roles_executed": list(results.keys()),
    }


def _extract_review_verdict(output: dict) -> str:
    """Extract verdict from reviewer output."""
    if isinstance(output, dict):
        verdict = output.get("verdict", "")
        if verdict:
            return verdict.upper()
        # Check raw text
        raw = output.get("raw", "")
        if "REQUEST_CHANGES" in raw.upper():
            return "REQUEST_CHANGES"
        if "APPROVE" in raw.upper():
            return "APPROVE"
    return "UNKNOWN"


def _extract_review_feedback(output: dict) -> str:
    """Extract actionable feedback from reviewer output."""
    if isinstance(output, dict):
        for key in ("feedback", "issues", "summary"):
            if key in output:
                val = output[key]
                if isinstance(val, str):
                    return val[:2000]
                if isinstance(val, list):
                    return json.dumps(val)[:2000]
    return str(output)[:2000]


def _identify_rerun_target(composed, results: dict) -> str | None:
    """Identify which role to re-run based on reviewer feedback.

    Returns the last non-reviewer role that produced output.
    """
    for step in reversed(composed.steps):
        if step.role_name != "reviewer" and step.role_name in results:
            return step.role_name
    return None
