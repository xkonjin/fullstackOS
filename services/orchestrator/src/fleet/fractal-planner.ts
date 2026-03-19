/**
 * Fractal Fleet Planner
 *
 * Decomposes high-level objectives into executable task trees using an LLM,
 * then dispatches leaf tasks via the SwarmCoordinator with role-aware,
 * quality-first CLI/model assignments.
 */

import { existsSync, readdirSync, readFileSync } from "fs";
import { homedir } from "os";
import { join, resolve } from "path";
import type { Config, FleetRole, RoutingLane, RoutingTaskKind } from "../types.ts";
import type { TaskRouter } from "../router/task-router.ts";
import type { BudgetGuard } from "../budget-guard.ts";
import type { LearningRouter } from "../router/learning-router.ts";
import type { SwarmCoordinator } from "./swarm.ts";
import { classifyTask } from "./task-classifier.ts";
import type { TaskComplexity } from "./task-classifier.ts";
import {
  buildRoutingIntent,
  canonicalizeModel,
} from "../router/routing-policy.ts";
import type {
  AgentCLI,
  FractalTask,
  FractalPlanRequest,
  FractalPlanResponse,
  FleetSwarmRequest,
  SwarmTaskSpec,
} from "./types.ts";
import { resolveSkillBundle } from "./skill-resolver.ts";

interface DecomposedTask {
  id: string;
  description: string;
  depends_on: string[];
}

interface SkillMatch {
  name: string;
  path: string;
  score: number;
  summary?: string;
}

const MODEL_COST_WEIGHTS: Record<string, number> = {
  "glm-4.7-flash": 0.0,
  "gemini-2.5-flash-lite": 0.01,
  "gemini-2.5-flash": 0.03,
  "MiniMax-M2.1-highspeed": 0.08,
  "kimi-for-coding": 0.1,
  "glm-4.7": 0.12,
  "MiniMax-M2.5-highspeed": 0.13,
  "MiniMax-M2.5": 0.13,
  "claude-haiku-4-5-20251001": 0.19,
  "glm-5": 0.16,
  "gpt-5.1-codex-mini": 0.4,
  "gemini-3.1-pro-low": 0.53,
  "claude-sonnet-4-6": 1.0,
  "gpt-5.3-codex": 0.93,
  "gemini-3.1-pro-high": 1.07,
  "gpt-5.4": 1.53,
  "claude-opus-4-6": 1.75,
};

const CLI_COST_WEIGHTS: Record<string, number> = {
  claude: 1.0,
  codex: 0.67,
  gemini: 0.33,
  kimi: 0.17,
  glm: 0.05,
  minimax: 0.13,
};

const AGENT_CANDIDATES: Record<TaskComplexity, Array<{ cli: AgentCLI; model: string }>> = {
  trivial: [
    { cli: "kimi", model: "kimi-k2.5" },
    { cli: "claude", model: "glm-4.7-flash" },
    { cli: "claude", model: "MiniMax-M2.1-highspeed" },
  ],
  standard: [
    { cli: "gemini", model: "gemini-2.5-flash" },
    { cli: "kimi", model: "kimi-for-coding" },
    { cli: "claude", model: "glm-4.7" },
    { cli: "claude", model: "MiniMax-M2.5-highspeed" },
  ],
  complex: [
    { cli: "codex", model: "gpt-5.1-codex-mini" },
    { cli: "gemini", model: "gemini-3.1-pro-low" },
    { cli: "claude", model: "claude-sonnet-4-6" },
  ],
  deep: [
    { cli: "codex", model: "gpt-5.4" },
    { cli: "claude", model: "claude-opus-4-6" },
    { cli: "gemini", model: "gemini-3.1-pro-high" },
  ],
};

export class FractalPlanner {
  private config: Config;
  private router: TaskRouter;
  private swarmCoordinator: SwarmCoordinator;
  private budgetGuard: BudgetGuard | undefined;
  private learningRouter: LearningRouter | undefined;
  private plans: Map<string, FractalPlanResponse> = new Map();

  constructor(
    config: Config,
    router: TaskRouter,
    swarmCoordinator: SwarmCoordinator,
    budgetGuard?: BudgetGuard,
    learningRouter?: LearningRouter,
  ) {
    this.config = config;
    this.router = router;
    this.swarmCoordinator = swarmCoordinator;
    this.budgetGuard = budgetGuard;
    this.learningRouter = learningRouter;
  }

  getPlan(planId: string): FractalPlanResponse | undefined {
    return this.plans.get(planId);
  }

  async plan(request: FractalPlanRequest): Promise<FractalPlanResponse> {
    const planId = crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    const maxDepth = request.max_depth ?? 3;
    const plannerModel = request.planner_model ?? this.config.fleet_policy?.roles?.planner?.preferred_models?.[0] ?? "gpt-5.4";

    const decomposition = await this.decompose(request.objective, maxDepth, plannerModel);
    const tree = this.buildTree(decomposition, request.objective);
    const leaves = this.collectLeaves(tree).filter((leaf) => leaf.id !== "0");
    const depsMap = new Map(decomposition.map((task) => [task.id, task.depends_on]));
    const cwd = request.cwd ?? process.cwd();

    for (const leaf of leaves) {
      const classification = classifyTask({
        cli: "claude",
        prompt: leaf.description,
      });
      const routingIntent = buildRoutingIntent(this.config, {
        model: classification.recommended_model ?? leaf.assigned_model ?? "claude-sonnet-4-6",
        messages: [{ role: "user", content: leaf.description }],
        metadata: {
          task_kind: classification.task_kind,
          claudemax_lane: classification.lane,
        },
      });
      leaf.complexity = classification.complexity;
      leaf.role = this.normalizeRole(classification.role, leaf.id, routingIntent.taskKind);
      leaf.lane = this.normalizeLane(classification.lane, leaf.role, routingIntent.lane);
      leaf.task_kind = classification.task_kind ?? routingIntent.taskKind;
      leaf.risk_level = classification.risk_level ?? routingIntent.riskLevel;
      leaf.quality_floor = classification.quality_floor ?? routingIntent.qualityFloor;
      leaf.review_required = classification.review_required ?? ["reviewer", "ui_reviewer"].includes(leaf.role);
      leaf.synthesis_required = classification.synthesis_required ?? ["planner", "architect", "design_lead", "synthesizer"].includes(leaf.role);
      leaf.fallback_band = leaf.lane === "worker_budget" ? "worker_budget" : "premium_adjacent";
      leaf.design_artifact_type = this.detectDesignArtifactType(leaf.description, leaf.task_kind);
      leaf.required_skill_queries = this.deriveSkillQueries(request.objective, leaf);
      const deterministicBundle = this.resolveDeterministicSkillBundle(cwd, request.objective, leaf);
      if (deterministicBundle?.preferred_role) {
        leaf.role = this.normalizeRole(deterministicBundle.preferred_role as FleetRole, leaf.id, leaf.task_kind);
        leaf.lane = this.normalizeLane(undefined, leaf.role, leaf.lane);
      }
      leaf.expert_role = deterministicBundle?.expert_role;
      leaf.skill_pack_id = deterministicBundle?.skill_pack_id;
      leaf.resource_pack_id = deterministicBundle?.resource_pack_id;
      leaf.resource_pack_path = deterministicBundle?.resource_pack_path;
      leaf.mcp_profile = deterministicBundle?.mcp_profile;
      leaf.route_reason = deterministicBundle?.route_reason;
      leaf.verification_profile = deterministicBundle?.verification_profile;
      leaf.selected_skills = deterministicBundle
        ? deterministicBundle.selected_skills.map((skill) => ({
            name: skill.id,
            path: skill.path,
            score: skill.score ?? 0,
            summary: skill.summary,
          }))
        : this.discoverSkills(cwd, leaf.required_skill_queries);

      const assignment = this.assignAgent(
        leaf.complexity,
        leaf.role,
        leaf.lane,
        leaf.task_kind,
        deterministicBundle,
      );
      leaf.assigned_cli = assignment.cli;
      leaf.assigned_model = assignment.model;
    }

    const leafTasks = leaves.map((leaf) => ({
      id: leaf.id,
      description: leaf.description,
      complexity: leaf.complexity!,
      role: leaf.role,
      lane: leaf.lane,
      task_kind: leaf.task_kind,
      risk_level: leaf.risk_level,
      quality_floor: leaf.quality_floor,
      synthesis_required: leaf.synthesis_required,
      review_required: leaf.review_required,
      fallback_band: leaf.fallback_band,
      design_artifact_type: leaf.design_artifact_type,
      required_skill_queries: leaf.required_skill_queries,
      selected_skills: leaf.selected_skills,
      expert_role: leaf.expert_role,
      skill_pack_id: leaf.skill_pack_id,
      resource_pack_id: leaf.resource_pack_id,
      resource_pack_path: leaf.resource_pack_path,
      mcp_profile: leaf.mcp_profile,
      route_reason: leaf.route_reason,
      verification_profile: leaf.verification_profile,
      assigned_cli: leaf.assigned_cli!,
      assigned_model: leaf.assigned_model!,
      depends_on: depsMap.get(leaf.id) ?? [],
    }));

    const byComplexity: Record<string, number> = {};
    const byCli: Record<string, number> = {};
    for (const task of leafTasks) {
      byComplexity[task.complexity] = (byComplexity[task.complexity] ?? 0) + 1;
      byCli[task.assigned_cli] = (byCli[task.assigned_cli] ?? 0) + 1;
    }

    const originalCost = Math.max(1, leafTasks.length);
    const optimizedCost = leafTasks.reduce(
      (sum, task) =>
        sum +
        (MODEL_COST_WEIGHTS[task.assigned_model] ??
          CLI_COST_WEIGHTS[task.assigned_cli] ??
          1.0),
      0,
    );
    const costReduction = Math.round((1 - optimizedCost / originalCost) * 100);

    const response: FractalPlanResponse = {
      plan_id: planId,
      tree,
      leaf_tasks: leafTasks,
      stats: {
        total_tasks: this.countNodes(tree),
        leaf_tasks: leafTasks.length,
        by_complexity: byComplexity,
        by_cli: byCli,
        estimated_cost_reduction_pct: Math.max(0, costReduction),
      },
      status: "planned",
    };

    this.plans.set(planId, response);

    if (request.auto_execute) {
      const swarmId = await this.execute(planId, request);
      response.swarm_id = swarmId;
      response.status = "executing";
    }

    return response;
  }

  async execute(planId: string, request?: FractalPlanRequest): Promise<string> {
    const plan = this.plans.get(planId);
    if (!plan) throw new Error(`Plan ${planId} not found`);
    if (plan.status === "executing") throw new Error(`Plan ${planId} already executing`);

    const cwd = request?.cwd ?? process.cwd();

    const swarmTasks: SwarmTaskSpec[] = plan.leaf_tasks.map((leaf) => ({
      id: leaf.id,
      depends_on: leaf.depends_on.length > 0 ? leaf.depends_on : undefined,
      dispatch: {
        cli: leaf.assigned_cli,
        prompt: this.augmentPromptWithSkills(leaf.description, leaf.selected_skills ?? []),
        model: leaf.assigned_model,
        cwd,
        one_shot: true,
        role: leaf.role,
        task_kind: leaf.task_kind,
        metadata: {
          fractal_plan_id: planId,
          fractal_task_id: leaf.id,
          fractal_complexity: leaf.complexity,
          fractal_role: leaf.role,
          fractal_lane: leaf.lane,
          fractal_task_kind: leaf.task_kind,
          fractal_risk_level: leaf.risk_level,
          fractal_quality_floor: leaf.quality_floor,
          fractal_fallback_band: leaf.fallback_band,
          fractal_review_required: leaf.review_required,
          fractal_synthesis_required: leaf.synthesis_required,
          fractal_required_skill_queries: leaf.required_skill_queries,
          fractal_selected_skills: leaf.selected_skills,
          expert_role: leaf.expert_role,
          skill_pack_id: leaf.skill_pack_id,
          resource_pack_id: leaf.resource_pack_id,
          resource_pack_path: leaf.resource_pack_path,
          mcp_profile: leaf.mcp_profile,
          route_reason: leaf.route_reason,
          verification_profile: leaf.verification_profile,
          design_artifact_type: leaf.design_artifact_type,
        },
      },
    }));

    const swarmRequest: FleetSwarmRequest = {
      tasks: swarmTasks,
      max_concurrency: request?.max_concurrency ?? 4,
      fail_fast: request?.fail_fast ?? false,
      metadata: {
        fractal_plan_id: planId,
        ...request?.metadata,
      },
    };

    const swarm = await this.swarmCoordinator.create(swarmRequest);
    plan.swarm_id = swarm.id;
    plan.status = "executing";

    return swarm.id;
  }

  private async decompose(
    objective: string,
    maxDepth: number,
    plannerModel: string,
  ): Promise<DecomposedTask[]> {
    const systemPrompt = `You are a JSON-only task decomposition API. You receive an objective and return ONLY a raw JSON array of tasks. Never output markdown, prose, headings, or explanations. Your entire response must be parseable by JSON.parse().

Format: [{"id": "1", "description": "specific task", "depends_on": []}, ...]
Rules: atomic tasks, max depth ${maxDepth}, prefer parallel, self-contained descriptions.`;

    const response = await fetch("http://localhost:8318/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Bearer your-proxy-key",
      },
      body: JSON.stringify({
        model: canonicalizeModel(this.config, plannerModel),
        max_tokens: 4096,
        system: systemPrompt,
        metadata: {
          task_kind: "code_deep",
          claudemax_lane: "interactive_premium",
        },
        messages: [
          {
            role: "user",
            content: `Return a JSON array of atomic coding tasks for this objective. NO markdown, NO prose, ONLY the JSON array starting with [ and ending with ]:\n\n${objective}`,
          },
        ],
      }),
    });

    if (!response.ok) {
      throw new Error(`Planner LLM call failed: ${response.status}`);
    }

    const data = (await response.json()) as { content?: { type: string; text?: string }[] };
    const rawText = data.content?.find((block) => block.type === "text")?.text ?? "";
    const text = rawText.replace(/```(?:json)?\n?/g, "").trim();

    let raw: unknown[];
    try {
      const parsed = JSON.parse(text);
      if (!Array.isArray(parsed)) throw new Error("not array");
      raw = parsed;
    } catch {
      const jsonMatch = text.match(/\[[\s\S]*\]/);
      if (!jsonMatch) throw new Error("Planner LLM did not return valid JSON task array");
      try {
        raw = JSON.parse(jsonMatch[0]) as unknown[];
      } catch {
        const start = text.indexOf("[");
        if (start === -1) throw new Error("No JSON array found in planner response");
        let depth = 0;
        let end = -1;
        for (let index = start; index < text.length; index++) {
          if (text[index] === "[") depth++;
          else if (text[index] === "]") {
            depth--;
            if (depth === 0) {
              end = index;
              break;
            }
          }
        }
        if (end === -1) throw new Error("Unclosed JSON array in planner response");
        raw = JSON.parse(text.slice(start, end + 1)) as unknown[];
      }
    }

    return raw.map((item: any, index: number) => ({
      id: String(item.id ?? index + 1),
      description: String(item.description ?? item.title ?? item.task ?? `Task ${index + 1}`),
      depends_on: (item.depends_on ?? item.dependencies ?? []).map((dependency: any) => String(dependency)),
    }));
  }

  private buildTree(tasks: DecomposedTask[], objective: string): FractalTask {
    const root: FractalTask = {
      id: "0",
      depth: 0,
      description: objective,
      kind: "composite",
      status: "ready",
      lineage: [],
      role: "planner",
      lane: "interactive_premium",
      task_kind: "code_deep",
      risk_level: "high",
      quality_floor: "premium",
      synthesis_required: true,
      review_required: false,
      required_skill_queries: [objective, "architecture planning"],
      selected_skills: [],
      children: tasks.map((task) => ({
        id: task.id,
        depth: 1,
        description: task.description,
        kind: "atomic" as const,
        status: "pending" as const,
        lineage: [objective],
        children: [],
      })),
    };
    return root;
  }

  private collectLeaves(task: FractalTask): FractalTask[] {
    if (task.children.length === 0) return [task];
    return task.children.flatMap((child) => this.collectLeaves(child));
  }

  private countNodes(task: FractalTask): number {
    return 1 + task.children.reduce((sum, child) => sum + this.countNodes(child), 0);
  }

  private normalizeRole(role: FleetRole | undefined, id: string, taskKind: RoutingTaskKind): FleetRole {
    if (role) return role;
    if (id === "1") return "planner";
    if (["design_visual", "design_system", "product_ux"].includes(taskKind)) return "design_lead";
    if (taskKind === "review") return "reviewer";
    return "implementer";
  }

  private normalizeLane(lane: RoutingLane | undefined, role: FleetRole, fallbackLane: RoutingLane): RoutingLane {
    if (lane) return lane;
    if (["design_lead", "ui_reviewer"].includes(role)) return "design_premium";
    if (["planner", "architect", "reviewer", "synthesizer"].includes(role)) return "interactive_premium";
    if (["researcher", "extractor"].includes(role)) return "worker_standard";
    return fallbackLane;
  }

  private detectDesignArtifactType(description: string, taskKind?: RoutingTaskKind): string | undefined {
    if (!["design_visual", "design_system", "product_ux"].includes(taskKind ?? "")) return undefined;
    if (/landing page|homepage|marketing/i.test(description)) return "landing_page";
    if (/dashboard|admin/i.test(description)) return "dashboard";
    if (/mobile|ios|android/i.test(description)) return "mobile_ui";
    if (/design system|tokens|component/i.test(description)) return "design_system";
    return "ui_flow";
  }

  private deriveSkillQueries(objective: string, leaf: FractalTask): string[] {
    const queries = new Set<string>();
    queries.add(leaf.description);
    if (leaf.role) queries.add(leaf.role.replace(/_/g, " "));
    if (leaf.task_kind) queries.add(leaf.task_kind.replace(/_/g, " "));
    if (leaf.lineage.length > 0) queries.add(leaf.lineage[0]!);

    if (leaf.role === "design_lead" || leaf.role === "ui_reviewer") {
      queries.add("design review");
      queries.add("product thinking");
      queries.add("progressive disclosure");
      queries.add("ui mastery");
    }
    if (leaf.role === "planner" || leaf.role === "architect") {
      queries.add("architecture");
      queries.add("gitnexus exploring");
    }
    if (leaf.role === "reviewer" || leaf.role === "validator") {
      queries.add("security review");
      queries.add("test validation");
    }
    if (leaf.role === "researcher") {
      queries.add("research");
    }

    if (objective) queries.add(objective);
    return [...queries].slice(0, 4);
  }

  private resolveDeterministicSkillBundle(cwd: string, objective: string, leaf: FractalTask) {
    if (this.config.skills_policy?.mode === "fallback_fuzzy") return null;
    return resolveSkillBundle({
      cwd,
      text: [objective, leaf.description, leaf.design_artifact_type, leaf.task_kind, leaf.role]
        .filter(Boolean)
        .join("\n"),
      role: leaf.role,
      taskKind: leaf.task_kind,
      manifestPath: this.config.skills_policy?.manifest_path,
      skillRoots: this.skillRoots(cwd),
    });
  }

  private skillRoots(cwd: string): string[] {
    const configured = this.config.skills_policy?.roots ?? [
      "~/.agents/skills",
      "~/.codex/skills",
      "~/.claude/skills",
      join(cwd, "skills"),
    ];
    return configured.map((root) => resolve(root.replace(/^~(?=\/)/, homedir())));
  }

  private discoverSkills(cwd: string, queries: string[]): SkillMatch[] {
    if (this.config.skills_policy?.enable_subtask_skill_discovery === false) return [];
    const tokens = new Set(
      queries
        .join(" ")
        .toLowerCase()
        .split(/[^a-z0-9]+/)
        .filter((token) => token.length >= 3),
    );
    const matches: SkillMatch[] = [];
    const maxSelected = this.config.skills_policy?.max_selected_skills ?? 4;

    for (const root of this.skillRoots(cwd)) {
      if (!existsSync(root)) continue;
      for (const skillDir of this.walkSkillDirs(root)) {
        const skillFile = join(skillDir, "SKILL.md");
        if (!existsSync(skillFile)) continue;
        const content = readFileSync(skillFile, "utf8").slice(0, 2000).toLowerCase();
        const name = skillDir.split("/").pop() ?? skillDir;
        let score = 0;
        for (const token of tokens) {
          if (name.toLowerCase().includes(token)) score += 3;
          if (content.includes(token)) score += 1;
        }
        if (score > 0) {
          matches.push({
            name,
            path: skillDir,
            score,
            summary: readFileSync(skillFile, "utf8").split("\n").slice(0, 3).join(" ").slice(0, 160),
          });
        }
      }
    }

    matches.sort((left, right) => right.score - left.score || left.name.localeCompare(right.name));
    const unique = new Map<string, SkillMatch>();
    for (const match of matches) {
      if (!unique.has(match.path)) unique.set(match.path, match);
    }
    return [...unique.values()].slice(0, maxSelected);
  }

  private walkSkillDirs(root: string): string[] {
    const queue = [root];
    const results: string[] = [];
    while (queue.length > 0) {
      const current = queue.shift()!;
      let entries: ReturnType<typeof readdirSync>;
      try {
        entries = readdirSync(current, { withFileTypes: true }) as any;
      } catch {
        continue;
      }
      let hasSkill = false;
      for (const entry of entries as any[]) {
        if (entry.isFile?.() && entry.name === "SKILL.md") {
          hasSkill = true;
        }
      }
      if (hasSkill) {
        results.push(current);
        continue;
      }
      for (const entry of entries as any[]) {
        if (entry.isDirectory?.()) {
          queue.push(join(current, entry.name));
        }
      }
    }
    return results;
  }

  private augmentPromptWithSkills(description: string, skills: SkillMatch[]): string {
    if (!skills.length || this.config.skills_policy?.inject_by_role === false) {
      return description;
    }
    const inlineLimit = this.config.skills_policy?.max_inline_skill_snippets ?? 2;
    const selected = skills.slice(0, inlineLimit);
    const skillLines = selected.map((skill) => `- ${skill.name}: ${skill.summary ?? skill.path}`);
    return `${description}\n\nRelevant skills:\n${skillLines.join("\n")}`;
  }

  private assignAgent(
    complexity: TaskComplexity,
    role?: FleetRole,
    lane?: RoutingLane,
    taskKind?: RoutingTaskKind,
    deterministicBundle?: { preferred_cli?: AgentCLI; preferred_model?: string } | null,
  ): { cli: AgentCLI; model: string } {
    if (deterministicBundle?.preferred_cli && deterministicBundle?.preferred_model) {
      return { cli: deterministicBundle.preferred_cli, model: deterministicBundle.preferred_model };
    }
    if (role === "design_lead" || role === "ui_reviewer" || lane === "design_premium") {
      return { cli: "gemini", model: "gemini-3.1-pro-high" };
    }
    if (role === "reviewer") {
      return { cli: "claude", model: "claude-opus-4-6" };
    }
    if (role === "planner" || role === "architect") {
      return { cli: "codex", model: "gpt-5.4" };
    }
    if (role === "synthesizer") {
      return { cli: "claude", model: "claude-opus-4-6" };
    }
    if (taskKind === "research_synthesis") {
      return { cli: "claude", model: "claude-opus-4-6" };
    }
    if (complexity === "deep") {
      return { cli: "codex", model: "gpt-5.4" };
    }
    if (complexity === "complex") {
      return { cli: "codex", model: "gpt-5.1-codex-mini" };
    }
    const candidates = AGENT_CANDIDATES[complexity];
    return candidates[0]!;
  }
}
