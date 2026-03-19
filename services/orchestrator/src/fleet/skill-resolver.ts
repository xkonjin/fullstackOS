import { existsSync, readFileSync } from "fs";
import { isAbsolute, join, resolve } from "path";
import { parse as parseYaml } from "yaml";
import type { AgentCLI } from "./types.ts";
import type { FleetRole, RoutingTaskKind } from "../types.ts";

interface SkillMetadata {
  id: string;
  platforms?: string[];
  task_families?: string[];
  priority?: number;
  exclusive_with?: string[];
  requires_mcp?: string[];
  requires_resources?: string[];
  preferred_role?: string;
  preferred_cli?: AgentCLI;
  preferred_model?: string;
  verification_profile?: string;
}

interface SkillPackManifest {
  id: string;
  expert_role?: string;
  preferred_role?: string;
  preferred_cli?: AgentCLI;
  preferred_model?: string;
  resource_pack_id?: string;
  mcp_profile?: string;
  verification_profile?: string;
  skills: string[];
}

interface RouteMatch {
  any_phrase?: string[];
  any_regex?: string[];
}

interface RouteRule {
  id: string;
  priority?: number;
  task_family: string;
  skill_pack: string;
  route_reason?: string;
  match: RouteMatch;
}

interface SkillTreeManifest {
  version: number;
  defaults?: {
    mode?: "deterministic" | "fallback_fuzzy";
    resource_pack_root?: string;
    fallback_fuzzy?: boolean;
  };
  resource_packs?: Record<string, { id: string; path: string; summary?: string }>;
  skill_packs?: Record<string, SkillPackManifest>;
  routes?: RouteRule[];
  fallbacks?: {
    by_role?: Record<string, string>;
    by_task_kind?: Record<string, string>;
  };
}

export interface ResolvedSkillMatch {
  id: string;
  name: string;
  path: string;
  score?: number;
  summary?: string;
}

export interface ResolvedSkillBundle {
  task_family: string;
  skill_pack_id: string;
  expert_role?: string;
  preferred_role?: string;
  preferred_cli?: AgentCLI;
  preferred_model?: string;
  resource_pack_id?: string;
  resource_pack_path?: string;
  mcp_profile?: string;
  verification_profile?: string;
  route_reason: string;
  selected_skills: ResolvedSkillMatch[];
}

function resolveRepoPath(cwd: string, maybeRelative: string): string {
  const candidate = maybeRelative.replace(/^~(?=\/)/, process.env.HOME ?? "~");
  return isAbsolute(candidate) ? candidate : resolve(cwd, candidate);
}

function readJson<T>(path: string): T | null {
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8")) as T;
}

function readYaml<T>(path: string): T | null {
  if (!existsSync(path)) return null;
  return parseYaml(readFileSync(path, "utf8")) as T;
}

function summarizeSkill(skillDir: string): string | undefined {
  const skillFile = join(skillDir, "SKILL.md");
  if (!existsSync(skillFile)) return undefined;
  const content = readFileSync(skillFile, "utf8").split("\n").slice(0, 12).join(" ");
  return content.replace(/\s+/g, " ").trim().slice(0, 180);
}

function matchRule(text: string, rule: RouteRule): boolean {
  const phraseMatch = (rule.match.any_phrase ?? []).some((phrase) =>
    text.includes(phrase.toLowerCase()),
  );
  const regexMatch = (rule.match.any_regex ?? []).some((pattern) => {
    try {
      return new RegExp(pattern, "i").test(text);
    } catch {
      return false;
    }
  });
  return phraseMatch || regexMatch;
}

function loadSkillMetadata(skillRoots: string[], skillId: string): ResolvedSkillMatch | null {
  for (const root of skillRoots) {
    const skillDir = join(root, skillId);
    const meta = readJson<SkillMetadata>(join(skillDir, "skill.json"));
    if (!meta) continue;
    return {
      id: meta.id,
      name: meta.id,
      path: skillDir,
      score: meta.priority,
      summary: summarizeSkill(skillDir),
    };
  }
  return null;
}

function resolveSkillRoots(cwd: string, roots?: string[]): string[] {
  const defaults = [join(cwd, "skills")];
  return (roots && roots.length ? roots : defaults).map((root) => resolveRepoPath(cwd, root));
}

export function loadSkillTreeManifest(cwd: string, manifestPath?: string): SkillTreeManifest | null {
  const candidate = resolveRepoPath(cwd, manifestPath ?? "config/skill-tree.yaml");
  return readYaml<SkillTreeManifest>(candidate);
}

export function resolveSkillBundle(options: {
  cwd: string;
  text: string;
  role?: FleetRole | string;
  taskKind?: RoutingTaskKind | string;
  manifestPath?: string;
  skillRoots?: string[];
}): ResolvedSkillBundle | null {
  const manifest = loadSkillTreeManifest(options.cwd, options.manifestPath);
  if (!manifest) return null;

  const text = options.text.toLowerCase();
  const routes = [...(manifest.routes ?? [])].sort(
    (left, right) => (right.priority ?? 0) - (left.priority ?? 0),
  );

  let taskFamily: string | undefined;
  let skillPackId: string | undefined;
  let routeReason = "";

  for (const route of routes) {
    if (!matchRule(text, route)) continue;
    taskFamily = route.task_family;
    skillPackId = route.skill_pack;
    routeReason = route.route_reason ?? `Matched route ${route.id}`;
    break;
  }

  if (!skillPackId && options.role) {
    const fallbackPack = manifest.fallbacks?.by_role?.[String(options.role)];
    if (fallbackPack) {
      taskFamily = fallbackPack;
      skillPackId = fallbackPack;
      routeReason = `Matched role fallback for ${String(options.role)}`;
    }
  }

  if (!skillPackId && options.taskKind) {
    const fallbackPack = manifest.fallbacks?.by_task_kind?.[String(options.taskKind)];
    if (fallbackPack) {
      taskFamily = fallbackPack;
      skillPackId = fallbackPack;
      routeReason = `Matched task-kind fallback for ${String(options.taskKind)}`;
    }
  }

  if (!skillPackId) return null;
  const pack = manifest.skill_packs?.[skillPackId];
  if (!pack) return null;

  const skillRoots = resolveSkillRoots(options.cwd, options.skillRoots);
  const selectedSkills = pack.skills
    .map((skillId) => loadSkillMetadata(skillRoots, skillId))
    .filter((value): value is ResolvedSkillMatch => !!value);

  const resourcePack = pack.resource_pack_id
    ? manifest.resource_packs?.[pack.resource_pack_id]
    : undefined;

  return {
    task_family: taskFamily ?? skillPackId,
    skill_pack_id: skillPackId,
    expert_role: pack.expert_role,
    preferred_role: pack.preferred_role,
    preferred_cli: pack.preferred_cli,
    preferred_model: pack.preferred_model,
    resource_pack_id: pack.resource_pack_id,
    resource_pack_path: resourcePack ? resolveRepoPath(options.cwd, resourcePack.path) : undefined,
    mcp_profile: pack.mcp_profile,
    verification_profile: pack.verification_profile,
    route_reason: routeReason,
    selected_skills: selectedSkills,
  };
}
