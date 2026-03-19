"""Skill Loader — Deterministic, trigger-based skill loading.

Design principles:
1. Triggers are pattern-matched against user input/context
2. Skills are loaded ONLY when triggered (lazy loading)
3. Loaded skills are cached per-session (no re-loading)
4. Progressive disclosure: summary first, full content on demand

Usage:
    from pipeline.skill_loader import SkillLoader

    loader = SkillLoader()
    skill = loader.match_trigger("search x for recent AI news")
    if skill:
        content = loader.load_skill(skill)
"""

from __future__ import annotations

import fnmatch
import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("aifleet.skill_loader")

# Skill directories to search (in priority order)
SKILL_DIRS = [
    Path.home() / ".ai-fleet" / "skills",
    Path.home() / "Dev" / "fullstackOS" / "skills",
]


@dataclass
class SkillManifest:
    """Minimal metadata for skill matching (no content bloating)."""

    name: str
    path: Path
    description: str
    triggers: list[str]
    user_invocable: bool = False
    version: str = "1.0.0"
    hash: str = ""

    def matches(self, text: str) -> bool:
        """Check if text matches any trigger pattern."""
        text_lower = text.lower()
        for trigger in self.triggers:
            trigger_lower = trigger.lower()
            # Support glob-style patterns
            if "*" in trigger or "?" in trigger:
                if fnmatch.fnmatch(text_lower, trigger_lower):
                    return True
            # Support regex patterns (enclosed in /)
            elif trigger.startswith("/") and trigger.endswith("/"):
                try:
                    if re.search(trigger[1:-1], text, re.IGNORECASE):
                        return True
                except re.error:
                    pass
            # Single-token triggers should match whole words only to avoid false positives
            elif re.fullmatch(r"[a-z0-9_-]+", trigger_lower):
                if re.search(rf"(?<![a-z0-9_-]){re.escape(trigger_lower)}(?![a-z0-9_-])", text_lower):
                    return True
            # Multi-word triggers keep substring semantics
            elif trigger_lower in text_lower:
                return True
        return False


@dataclass
class LoadedSkill:
    """A fully loaded skill with content."""

    manifest: SkillManifest
    content: str
    frontmatter: dict[str, Any]
    loaded_at: float = 0.0


class SkillLoader:
    """Deterministic, trigger-based skill loading."""

    def __init__(self, skill_dirs: list[Path] | None = None):
        self.skill_dirs = skill_dirs or SKILL_DIRS
        self._manifests: dict[str, SkillManifest] = {}
        self._loaded: dict[str, LoadedSkill] = {}
        self._indexed = False

    def index_skills(self, force: bool = False) -> int:
        """Scan skill directories and build manifest index.

        Only reads YAML frontmatter, not full content.
        Returns count of skills indexed.
        """
        if self._indexed and not force:
            return len(self._manifests)

        self._manifests.clear()

        for skill_dir in self.skill_dirs:
            if not skill_dir.exists():
                continue

            for skill_path in skill_dir.iterdir():
                if not skill_path.is_dir():
                    continue
                if skill_path.name.startswith("."):
                    continue

                skill_file = skill_path / "SKILL.md"
                if not skill_file.exists():
                    continue

                manifest = self._parse_manifest(skill_file)
                if manifest:
                    self._manifests[manifest.name] = manifest

        self._indexed = True
        return len(self._manifests)

    def _parse_manifest(self, skill_file: Path) -> SkillManifest | None:
        """Parse only the YAML frontmatter from a skill file.

        This is O(1) - we read only until the closing ---.
        """
        try:
            content = skill_file.read_text()
            if not content.startswith("---"):
                return None

            # Extract frontmatter
            end_idx = content.find("\n---", 4)
            if end_idx == -1:
                return None

            frontmatter_text = content[4:end_idx]
            frontmatter = self._parse_yaml_frontmatter(frontmatter_text)

            name = skill_file.parent.name
            description = frontmatter.get("description", "")
            if isinstance(description, dict):
                description = description.get("description", str(description))

            triggers_raw = frontmatter.get("triggers", "")
            if isinstance(triggers_raw, str):
                triggers = [t.strip() for t in triggers_raw.split(",") if t.strip()]
            else:
                triggers = list(triggers_raw) if triggers_raw else []

            # Extract triggers from description if present (Use when: pattern)
            desc_str = str(description)
            if "Use when:" in desc_str:
                import re

                # Find quoted strings in Use when section
                use_when_match = re.search(
                    r"Use when:.*?(?=NOT for:|$)", desc_str, re.DOTALL
                )
                if use_when_match:
                    use_when_text = use_when_match.group(0)
                    # Extract quoted phrases
                    quoted = re.findall(r'"([^"]+)"', use_when_text)
                    triggers.extend(quoted)
                    # Also extract slash commands
                    slashes = re.findall(r"/([\w-]+)", use_when_text)
                    for s in slashes:
                        triggers.append(s)
                        triggers.append(f"/{s}")

            # Add implicit trigger from skill name
            if name not in triggers:
                triggers.append(name)
                triggers.append(name.replace("-", " "))

            return SkillManifest(
                name=name,
                path=skill_file.parent,
                description=str(description)[:500],
                triggers=triggers,
                user_invocable=bool(frontmatter.get("user-invocable", False)),
                version=str(frontmatter.get("version", "1.0.0")),
                hash=hashlib.md5(content.encode()).hexdigest()[:8],
            )
        except Exception as e:
            log.warning("Failed to parse skill %s: %s", skill_file, e)
            return None

    def _parse_yaml_frontmatter(self, text: str) -> dict[str, Any]:
        """Minimal YAML parser for frontmatter (no pyyaml dependency)."""
        result: dict[str, Any] = {}
        current_key = None
        current_value: list[str] = []

        for line in text.split("\n"):
            if not line.strip():
                continue

            # Key: value
            if ":" in line and not line.startswith(" "):
                # Save previous key
                if current_key:
                    result[current_key] = self._parse_value(current_value)

                key, _, val = line.partition(":")
                current_key = key.strip()
                current_value = [val.strip()] if val.strip() else []
            # Continuation
            elif current_key:
                current_value.append(line.strip())

        # Save last key
        if current_key:
            result[current_key] = self._parse_value(current_value)

        return result

    def _parse_value(self, lines: list[str]) -> Any:
        """Parse a YAML value from lines."""
        if not lines:
            return ""

        # Single value
        if len(lines) == 1:
            val = lines[0]
            # Boolean
            if val.lower() in ("true", "yes"):
                return True
            if val.lower() in ("false", "no"):
                return False
            # Quoted string
            if (val.startswith('"') and val.endswith('"')) or (
                val.startswith("'") and val.endswith("'")
            ):
                return val[1:-1]
            return val

        # Multiline - check if it's a list or folded string
        joined = " ".join(lines)
        if all(line.startswith("-") or not line for line in lines if line):
            # List
            return [line[1:].strip() for line in lines if line.startswith("-")]
        return joined

    def match_trigger(self, text: str) -> SkillManifest | None:
        """Find the first skill whose triggers match the text.

        Returns the skill manifest (not full content).
        """
        self.index_skills()

        for manifest in self._manifests.values():
            if manifest.matches(text):
                log.debug("Triggered skill: %s", manifest.name)
                return manifest

        return None

    def match_all_triggers(self, text: str) -> list[SkillManifest]:
        """Find all skills whose triggers match the text."""
        self.index_skills()
        return [m for m in self._manifests.values() if m.matches(text)]

    def load_skill(self, name_or_manifest: str | SkillManifest) -> LoadedSkill | None:
        """Load a skill's full content.

        Cached after first load - subsequent calls are O(1).
        """
        if isinstance(name_or_manifest, str):
            manifest = self._manifests.get(name_or_manifest)
            if not manifest:
                self.index_skills()
                manifest = self._manifests.get(name_or_manifest)
        else:
            manifest = name_or_manifest

        if not manifest:
            return None

        # Check cache
        if manifest.name in self._loaded:
            return self._loaded[manifest.name]

        # Load content
        skill_file = manifest.path / "SKILL.md"
        if not skill_file.exists():
            return None

        content = skill_file.read_text()

        # Parse frontmatter
        frontmatter = {}
        if content.startswith("---"):
            end_idx = content.find("\n---", 4)
            if end_idx != -1:
                frontmatter = self._parse_yaml_frontmatter(content[4:end_idx])
                content = content[end_idx + 5 :].strip()

        import time

        loaded = LoadedSkill(
            manifest=manifest,
            content=content,
            frontmatter=frontmatter,
            loaded_at=time.time(),
        )
        self._loaded[manifest.name] = loaded
        return loaded

    def list_skills(self) -> list[SkillManifest]:
        """List all available skill manifests (no content loading)."""
        self.index_skills()
        return list(self._manifests.values())

    def get_summary(self) -> dict[str, Any]:
        """Get a summary of all skills for context injection.

        This is minimal - just names and one-line descriptions.
        """
        self.index_skills()
        return {
            "total": len(self._manifests),
            "user_invocable": [
                m.name for m in self._manifests.values() if m.user_invocable
            ],
            "all": [m.name for m in self._manifests.values()],
        }

    def get_trigger_map(self) -> dict[str, list[str]]:
        """Get a map of trigger patterns to skill names.

        Useful for debugging what triggers what.
        """
        self.index_skills()
        trigger_map: dict[str, list[str]] = {}
        for manifest in self._manifests.values():
            for trigger in manifest.triggers:
                if trigger not in trigger_map:
                    trigger_map[trigger] = []
                trigger_map[trigger].append(manifest.name)
        return trigger_map


# --- Convenience functions ---


def find_skill(text: str) -> SkillManifest | None:
    """Find a skill matching the given text."""
    loader = SkillLoader()
    return loader.match_trigger(text)


def load_skill(name: str) -> LoadedSkill | None:
    """Load a skill by name."""
    loader = SkillLoader()
    return loader.load_skill(name)


def get_skill_summary() -> dict[str, Any]:
    """Get minimal skill summary for context injection."""
    loader = SkillLoader()
    return loader.get_summary()
