"""End-to-end test harness for fullstackOS.

Tests the full stack: config parsing, skill resolution, MCP profiles,
verification profiles, stage pre-hooks, theorist notes, and pipeline stages.
"""
from __future__ import annotations

import json
import re
import sqlite3
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO_ROOT / "config"
FLEET_DIR = REPO_ROOT / "fleet"
GENERATED_DIR = REPO_ROOT / "generated"


# ---------------------------------------------------------------------------
# 1. Config Integrity Tests
# ---------------------------------------------------------------------------

class TestConfigIntegrity:
    """Validate all YAML configs parse and have required structure."""

    def test_skill_tree_parses(self):
        data = yaml.safe_load((CONFIG_DIR / "skill-tree.yaml").read_text())
        assert data["version"] == 1
        assert "skill_packs" in data
        assert "routes" in data
        assert "fallbacks" in data

    def test_skill_tree_packs_have_required_fields(self):
        data = yaml.safe_load((CONFIG_DIR / "skill-tree.yaml").read_text())
        required = {"id", "expert_role", "skills"}
        for name, pack in data["skill_packs"].items():
            missing = required - set(pack.keys())
            assert not missing, f"Pack '{name}' missing: {missing}"
            assert isinstance(pack["skills"], list), f"Pack '{name}' skills must be a list"
            assert len(pack["skills"]) > 0, f"Pack '{name}' has empty skills list"

    def test_skill_tree_routes_have_match(self):
        data = yaml.safe_load((CONFIG_DIR / "skill-tree.yaml").read_text())
        for route in data["routes"]:
            assert "match" in route, f"Route '{route['id']}' missing match"
            m = route["match"]
            assert "any_phrase" in m or "any_regex" in m, (
                f"Route '{route['id']}' needs any_phrase or any_regex"
            )

    def test_skill_tree_routes_reference_existing_packs(self):
        data = yaml.safe_load((CONFIG_DIR / "skill-tree.yaml").read_text())
        packs = set(data["skill_packs"].keys())
        for route in data["routes"]:
            assert route["skill_pack"] in packs, (
                f"Route '{route['id']}' references missing pack '{route['skill_pack']}'"
            )

    def test_mcp_registry_parses(self):
        data = yaml.safe_load((CONFIG_DIR / "mcp-registry.yaml").read_text())
        assert "servers" in data
        assert "profiles" in data

    def test_mcp_profiles_reference_existing_servers(self):
        data = yaml.safe_load((CONFIG_DIR / "mcp-registry.yaml").read_text())
        servers = set(data["servers"].keys())
        for profile_name, profile in data["profiles"].items():
            for srv in profile["servers"]:
                assert srv in servers, (
                    f"Profile '{profile_name}' references unknown server '{srv}'"
                )

    def test_verification_profiles_parses(self):
        data = yaml.safe_load((CONFIG_DIR / "verification-profiles.yaml").read_text())
        assert data["version"] == 1
        assert "profiles" in data
        for name, profile in data["profiles"].items():
            assert "commands" in profile, f"Profile '{name}' missing commands"

    def test_skill_pack_mcp_profiles_exist_in_registry(self):
        tree = yaml.safe_load((CONFIG_DIR / "skill-tree.yaml").read_text())
        registry = yaml.safe_load((CONFIG_DIR / "mcp-registry.yaml").read_text())
        profile_names = set(registry["profiles"].keys())
        for name, pack in tree["skill_packs"].items():
            if "mcp_profile" in pack:
                assert pack["mcp_profile"] in profile_names, (
                    f"Pack '{name}' references MCP profile '{pack['mcp_profile']}' "
                    f"not in registry. Available: {profile_names}"
                )

    def test_skill_pack_verification_profiles_exist(self):
        tree = yaml.safe_load((CONFIG_DIR / "skill-tree.yaml").read_text())
        vp = yaml.safe_load((CONFIG_DIR / "verification-profiles.yaml").read_text())
        vp_names = set(vp["profiles"].keys())
        for name, pack in tree["skill_packs"].items():
            if "verification_profile" in pack:
                assert pack["verification_profile"] in vp_names, (
                    f"Pack '{name}' references verification profile "
                    f"'{pack['verification_profile']}' not in config. Available: {vp_names}"
                )


# ---------------------------------------------------------------------------
# 2. Skill Resolver Tests
# ---------------------------------------------------------------------------

class TestSkillResolver:
    """Test deterministic skill routing via resolver bridge."""

    @pytest.fixture(autouse=True)
    def _load_tree(self):
        try:
            from pipeline.skill_resolver_bridge import load_skill_tree
            self.tree = load_skill_tree(str(REPO_ROOT))
        except ImportError:
            pytest.skip("pipeline.skill_resolver_bridge not available")

    def _resolve(self, text, **kw):
        from pipeline.skill_resolver_bridge import resolve_skill_bundle
        return resolve_skill_bundle(text, self.tree, **kw)

    @pytest.mark.parametrize("query,expected_pack", [
        ("dispatch fleet agents to run the workflow", "orchestration_infra"),
        ("write architecture documentation for the runbook", "docs_theorist"),
        ("implement this design from figma", "ui_implement"),
        ("review this UI and polish it", "ui_review"),
        ("write a product spec for the onboarding flow", "product_design_router"),
        ("animate this hero illustration", "ui_assets_motion"),
        ("add a new API endpoint and database migration", "backend_implement"),
        ("check for XSS vulnerabilities in the auth flow", "security_audit"),
        ("what are people saying about AI coding assistants", "research_deep"),
        ("create a test strategy and coverage plan", "test_strategy"),
    ])
    def test_route_resolution(self, query, expected_pack):
        bundle = self._resolve(query)
        assert bundle is not None, f"No match for: {query}"
        assert bundle["skill_pack_id"] == expected_pack

    @pytest.mark.parametrize("role,expected_pack", [
        ("backend_lead", "backend_implement"),
        ("security_lead", "security_audit"),
        ("qa_lead", "test_strategy"),
        ("researcher", "research_deep"),
        ("design_lead", "ui_implement"),
        ("infra_lead", "orchestration_infra"),
    ])
    def test_role_fallback(self, role, expected_pack):
        bundle = self._resolve("generic xyz123 no match", role=role)
        assert bundle is not None, f"No fallback for role={role}"
        assert bundle["skill_pack_id"] == expected_pack

    @pytest.mark.parametrize("task_kind,expected_pack", [
        ("backend", "backend_implement"),
        ("security", "security_audit"),
        ("testing", "test_strategy"),
        ("research", "research_deep"),
    ])
    def test_task_kind_fallback(self, task_kind, expected_pack):
        bundle = self._resolve("generic xyz123", task_kind=task_kind)
        assert bundle is not None, f"No fallback for task_kind={task_kind}"
        assert bundle["skill_pack_id"] == expected_pack

    def test_bundle_has_mcp_profile(self):
        bundle = self._resolve("add a database migration")
        assert bundle is not None
        assert "mcp_profile" in bundle
        assert bundle["mcp_profile"] == "backend-impl"

    def test_bundle_has_verification_profile(self):
        bundle = self._resolve("check for XSS vulnerabilities")
        assert bundle is not None
        assert bundle["verification_profile"] == "security-scan"


# ---------------------------------------------------------------------------
# 3. Stage Skill Injection Tests
# ---------------------------------------------------------------------------

class TestStageSkillInjection:
    """Test unified skill injection with resolver bridge as primary."""

    @pytest.fixture
    def conn(self):
        c = sqlite3.connect(":memory:")
        c.execute("""CREATE TABLE pipeline_skills (
            pipeline_id TEXT, stage_name TEXT, skill_name TEXT,
            skill_path TEXT, injected_at INTEGER,
            PRIMARY KEY (pipeline_id, stage_name, skill_name)
        )""")
        return c

    def _make_pipeline(self, objective):
        return {
            "pipeline_id": "test-e2e-001",
            "structured_objective": objective,
            "project_repo": str(REPO_ROOT),
        }

    def _make_cfg(self):
        skills_root = Path.home() / ".claude" / "skills"
        return {
            "stage_skills": {"enabled": True, "max_per_stage": 5, "content_chars": 500},
            "skills": {"roots": [str(skills_root)], "max_scan_files": 5000},
            "repo_root": str(REPO_ROOT),
        }

    def test_implement_stage_gets_skills(self, conn):
        from pipeline.stages import discover_and_inject_skills
        result = discover_and_inject_skills(
            "implement",
            self._make_pipeline("fix the database migration"),
            self._make_cfg(),
            conn=conn,
        )
        assert len(result) > 0, "implement stage should inject skills"
        assert "## Relevant Skills" in result

    def test_research_stage_gets_skills(self, conn):
        from pipeline.stages import discover_and_inject_skills
        result = discover_and_inject_skills(
            "research",
            self._make_pipeline("what are people saying about AI"),
            self._make_cfg(),
            conn=conn,
        )
        assert len(result) > 0
        assert "x-research" in result.lower() or "research" in result.lower()

    def test_cleanup_stage_gets_no_skills(self, conn):
        from pipeline.stages import discover_and_inject_skills
        result = discover_and_inject_skills(
            "cleanup",
            self._make_pipeline("clean up after pipeline"),
            self._make_cfg(),
            conn=conn,
        )
        assert result == ""

    def test_db_records_created(self, conn):
        from pipeline.stages import discover_and_inject_skills
        discover_and_inject_skills(
            "implement",
            self._make_pipeline("add API endpoint"),
            self._make_cfg(),
            conn=conn,
        )
        rows = conn.execute("SELECT skill_name FROM pipeline_skills").fetchall()
        assert len(rows) > 0, "Skills should be recorded in DB"


# ---------------------------------------------------------------------------
# 4. Pre-Hooks Tests
# ---------------------------------------------------------------------------

class TestPreHooks:
    """Test stage pre-hooks inject verification context."""

    def test_implement_gets_verification_profile(self):
        try:
            from pipeline.stages import _query_verification_profile
        except ImportError:
            pytest.skip("_query_verification_profile not available")
        result = _query_verification_profile(
            "implement",
            {"structured_objective": "add API endpoint and database migration", "project_repo": str(REPO_ROOT)},
            {"repo_root": str(REPO_ROOT)},
        )
        assert "Verification Profile" in result
        assert "targeted-tests" in result

    def test_cleanup_gets_no_hooks(self):
        try:
            from pipeline.stages import run_stage_pre_hooks
        except ImportError:
            pytest.skip("run_stage_pre_hooks not available")
        result = run_stage_pre_hooks(
            "cleanup",
            {"structured_objective": "cleanup", "project_repo": str(REPO_ROOT)},
            {"repo_root": str(REPO_ROOT)},
        )
        assert result == ""

    def test_test_stage_gets_verification(self):
        try:
            from pipeline.stages import run_stage_pre_hooks
        except ImportError:
            pytest.skip("run_stage_pre_hooks not available")
        result = run_stage_pre_hooks(
            "test",
            {"structured_objective": "test the API endpoint", "project_repo": str(REPO_ROOT)},
            {"repo_root": str(REPO_ROOT)},
        )
        assert "Verification Profile" in result


# ---------------------------------------------------------------------------
# 5. Theorist Note ID Validation
# ---------------------------------------------------------------------------

class TestTheoristNoteID:
    """Validate theorist note ID generation produces valid IDs."""

    VALID_ID_PATTERN = re.compile(r"^th-\d{4}-\d{2}-\d{2}-[a-z0-9-]+$")

    @pytest.mark.parametrize("pid", [
        "pip_18c3d9a9",
        "pip-abc12345",
        "pip_UPPER_case",
        "unknown",
        "a1b2c3d4e5f6",
    ])
    def test_plan_note_id_valid(self, pid):
        """Plan note IDs must match ^th-YYYY-MM-DD-[a-z0-9-]+$."""
        from datetime import datetime, timezone
        note_id = f"th-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}-plan-{pid[:8].replace('_', '-').lower()}"
        assert self.VALID_ID_PATTERN.match(note_id), f"Invalid ID: {note_id}"

    @pytest.mark.parametrize("pid", [
        "pip_18c3d9a9",
        "pip-abc12345",
        "task_xyz_123",
    ])
    def test_decision_note_id_valid(self, pid):
        from datetime import datetime, timezone
        note_id = f"th-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}-decision-{pid[:8].replace('_', '-').lower()}"
        assert self.VALID_ID_PATTERN.match(note_id), f"Invalid ID: {note_id}"


# ---------------------------------------------------------------------------
# 6. Generated MCP Profile Tests
# ---------------------------------------------------------------------------

class TestGeneratedMCPProfiles:
    """Validate generated per-profile MCP configs exist and are valid."""

    @pytest.fixture(autouse=True)
    def _check_generated(self):
        if not GENERATED_DIR.exists():
            pytest.skip("generated/ dir not present — run generate-agent-stack.ts first")

    def test_all_profiles_have_json(self):
        registry = yaml.safe_load((CONFIG_DIR / "mcp-registry.yaml").read_text())
        for profile_name in registry["profiles"]:
            path = GENERATED_DIR / f"mcp-{profile_name}.json"
            assert path.exists(), f"Missing generated config: {path.name}"

    def test_all_profiles_have_toml(self):
        registry = yaml.safe_load((CONFIG_DIR / "mcp-registry.yaml").read_text())
        for profile_name in registry["profiles"]:
            path = GENERATED_DIR / f"mcp-{profile_name}.toml"
            assert path.exists(), f"Missing generated TOML: {path.name}"

    def test_json_profiles_are_valid(self):
        for path in GENERATED_DIR.glob("mcp-*.json"):
            if path.name == "mcp-profiles.json":
                continue
            data = json.loads(path.read_text())
            assert "mcpServers" in data, f"{path.name} missing mcpServers key"
            assert isinstance(data["mcpServers"], dict)

    def test_profile_servers_match_registry(self):
        registry = yaml.safe_load((CONFIG_DIR / "mcp-registry.yaml").read_text())
        for profile_name, profile in registry["profiles"].items():
            path = GENERATED_DIR / f"mcp-{profile_name}.json"
            if not path.exists():
                continue
            data = json.loads(path.read_text())
            generated_servers = set(data["mcpServers"].keys())
            expected_servers = set(profile["servers"])
            assert generated_servers == expected_servers, (
                f"Profile '{profile_name}': generated={generated_servers}, expected={expected_servers}"
            )


# ---------------------------------------------------------------------------
# 7. STAGE_SKILL_MAP Consistency
# ---------------------------------------------------------------------------

class TestStageSkillMapConsistency:
    """Validate STAGE_SKILL_MAP references skills that exist on disk."""

    # Known plugin/marketplace skills that don't have local SKILL.md dirs
    PLUGIN_SKILLS = {
        "spec-writer", "onboarding-audit", "growth-teardown",
        "ai-workflow-planner", "security-auditor",
    }

    def test_all_mapped_skills_exist(self):
        from pipeline.stages import STAGE_SKILL_MAP
        skills_dir = Path.home() / ".claude" / "skills"
        if not skills_dir.exists():
            pytest.skip("~/.claude/skills not present")

        available = {d.name for d in skills_dir.iterdir() if d.is_dir() and (d / "SKILL.md").exists()}
        missing = []
        for stage, skills in STAGE_SKILL_MAP.items():
            for skill in skills:
                normalized = skill.replace("_", "-")
                if (
                    skill not in available
                    and normalized not in available
                    and skill not in self.PLUGIN_SKILLS
                ):
                    missing.append(f"{stage}: {skill}")
        assert not missing, f"STAGE_SKILL_MAP references missing skills:\n" + "\n".join(missing)


# ---------------------------------------------------------------------------
# 8. Theorist Validator Integration
# ---------------------------------------------------------------------------

class TestTheoristValidator:
    """Run the theorist validator script if available."""

    def test_validator_exists(self):
        validator = REPO_ROOT / "scripts" / "theorist" / "validate.py"
        assert validator.exists(), "scripts/theorist/validate.py not found"

    def test_committed_notes_pass_validation(self):
        validator = REPO_ROOT / "scripts" / "theorist" / "validate.py"
        notes_dir = REPO_ROOT / "docs" / "theorist"
        if not validator.exists() or not notes_dir.exists():
            pytest.skip("Validator or notes dir missing")

        result = subprocess.run(
            ["python3", str(validator), "--root", str(notes_dir)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            assert False, f"Theorist validation failed:\n{result.stdout}\n{result.stderr}"
