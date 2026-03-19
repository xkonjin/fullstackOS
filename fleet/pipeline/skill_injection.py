"""Intelligent skill injection — Loop 45

Auto-selects and injects skills based on task context.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Dict, List, Optional
from pathlib import Path

log = logging.getLogger("aifleet.skills")


class SkillInjector:
    """Injects relevant skills based on task context."""

    # Skill mappings by keyword
    SKILL_MAP: Dict[str, List[str]] = {
        "react": ["frontend", "typescript", "component-design"],
        "api": ["backend", "rest-api", "documentation"],
        "database": ["sql", "data-modeling", "migrations"],
        "test": ["testing", "tdd", "pytest"],
        "deploy": ["devops", "docker", "ci-cd"],
        "auth": ["security", "oauth", "authentication"],
        "css": ["css-effects", "responsive-design"],
        "animation": ["animation-web", "frontend-craft"],
    }

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._skill_cache: Dict[str, str] = {}

    def detect_skills(self, objective: str) -> List[str]:
        """Detect relevant skills from objective."""
        objective_lower = objective.lower()
        detected = set()

        for keyword, skills in self.SKILL_MAP.items():
            if keyword in objective_lower:
                detected.update(skills)

        return list(detected)

    def load_skill_content(self, skill_name: str) -> Optional[str]:
        """Load skill content from file."""
        if skill_name in self._skill_cache:
            return self._skill_cache[skill_name]

        skill_path = Path.home() / ".claude" / "skills" / skill_name / "SKILL.md"

        if skill_path.exists():
            content = skill_path.read_text()
            self._skill_cache[skill_name] = content
            return content

        return None

    def inject_skills(self, objective: str, base_prompt: str) -> str:
        """Inject relevant skills into prompt."""
        skills = self.detect_skills(objective)

        if not skills:
            return base_prompt

        skill_contents = []
        for skill in skills[:3]:  # Max 3 skills
            content = self.load_skill_content(skill)
            if content:
                skill_contents.append(f"## Skill: {skill}\n{content[:2000]}")

        if not skill_contents:
            return base_prompt

        injected = "\n\n".join(
            ["# Relevant Skills\n", *skill_contents, "\n# Task\n", base_prompt]
        )

        log.debug("Injected %d skills", len(skill_contents))
        return injected


def inject_skills_for_task(
    conn: sqlite3.Connection, objective: str, base_prompt: str
) -> str:
    """Inject skills for a task."""
    injector = SkillInjector(conn)
    return injector.inject_skills(objective, base_prompt)
