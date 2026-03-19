/**
 * Task Complexity Classifier for Fleet Dispatch
 *
 * Analyzes fleet dispatch prompts to determine task complexity and recommend
 * the cheapest capable CLI + model combination. This is the core of the
 * "fractal fleet" cost optimization: expensive models (Claude, Codex) handle
 * deep/complex work, while cheap models (Kimi, GLM, MiniMax) handle routine tasks.
 *
 * Based on research:
 * - NVIDIA (2025): "SLMs are sufficiently powerful for many agentic tasks"
 * - BudgetMLAgent: 94.2% cost reduction with multi-agent cascade
 * - Zywot et al.: Tool-augmented 4B models outperform 32B without tools
 *
 * Quality guarantee: complex tasks NEVER get downgraded. When in doubt, keep
 * the original routing. Cascade on failure is handled by the dispatcher.
 */

import type { AgentCLI, FleetDispatchRequest } from "./types.ts";
import type {
  FleetRole,
  RiskLevel,
  RoutingLane,
  RoutingTaskKind,
} from "../types.ts";

export type TaskComplexity = "trivial" | "standard" | "complex" | "deep";

export interface FleetAvailability {
  /** Providers with all accounts exhausted (rate-limited or circuit-open) */
  exhaustedProviders: Set<string>;
  /** Providers with budget stopped (>95% daily limit) */
  budgetStoppedProviders: Set<string>;
  /** Providers with budget in alert zone (>80% daily limit) */
  budgetAlertProviders: Set<string>;
  /** Learning scores by provider (higher = better) — from LearningRouter */
  providerScores: Map<string, number>;
}

// CLI-to-provider mapping (1:1 for fleet agents)
const CLI_TO_PROVIDER: Record<string, string> = {
  codex: "codex",
  kimi: "kimi",
  claude: "claude",
  gemini: "gemini",
};

export interface ClassificationResult {
  complexity: TaskComplexity;
  score: number;
  reasons: string[];
  role?: FleetRole;
  lane?: RoutingLane;
  task_kind?: RoutingTaskKind;
  risk_level?: RiskLevel;
  quality_floor?: string;
  review_required?: boolean;
  synthesis_required?: boolean;
  /** Recommended CLI if different from requested */
  recommended_cli?: AgentCLI;
  /** Recommended model for the recommended CLI */
  recommended_model?: string;
  /** Whether the classifier changed the routing */
  rerouted: boolean;
  /** Original CLI before rerouting */
  original_cli?: AgentCLI;
  /** Original model before rerouting */
  original_model?: string;
}

// Candidate agent entry — `provider` overrides CLI→provider mapping for
// proxy-routed models (GLM/MiniMax run through claude CLI via :8318)
interface CheapAgent {
  cli: AgentCLI;
  model: string;
  /** Actual provider when different from CLI (e.g. glm via claude CLI) */
  provider?: string;
  /** Model quality score (0-1). Derived from the orchestrator's tier system:
   *  premium models ~0.95, standard ~0.70, fast ~0.45, budget ~0.25 */
  quality: number;
  /** Normalized cost score (0-1). 0 = free, 1 = most expensive in the fleet.
   *  Based on blended per-MTok cost: (input + output*2) / 3 */
  cost: number;
}

// Models ranked by VALUE (quality/cost ratio) per complexity tier.
// GLM/MiniMax have no CLI binary — they route through claude CLI via orchestrator
// proxy (:8318), which detects the model prefix and handles cross-provider translation.
//
// Available fleet CLIs: codex, kimi, claude, gemini
// Available proxy providers: claude, codex, gemini, glm, minimax, openrouter
//
// Quality tiers (from orchestrator's tier system):
//   premium (0.95): opus, glm-5, gemini-3.1-pro-high, codex-5.3/5.4
//   standard (0.70): sonnet, glm-4.7, gemini-3.1-pro-low, codex-5.1, kimi
//   fast (0.45): haiku, gemini-2.5-flash, glm-4.7-flashx, minimax-fast, codex-mini
//   budget (0.25): glm-4.7-flash (FREE), gemini-2.5-flash-lite, MiniMax-M2.1-highspeed
//
// Cost (normalized 0-1, based on blended $/MTok: (in + out*2)/3):
//   glm-4.7-flash: 0.00 (FREE) | gemini-2.5-flash-lite: 0.01 | gemini-2.5-flash: 0.02
//   MiniMax-M2.1-highspeed: 0.05 | kimi: 0.07 | glm-4.7: 0.08 | MiniMax-M2.5: 0.09
//   claude-haiku: 0.13 | MiniMax-M2.5-highspeed: 0.09 | codex-mini: 0.27
//   glm-5: 0.11 | claude-sonnet: 0.50 | gemini-3.1-pro-low: 0.37
//   codex-5.3: 0.63 | gemini-3.1-pro-high: 0.73 | codex-5.4: 1.00
//
// Ordering principle: best value first (quality per dollar spent).
// For trivial tasks, any model works — optimize for cost.
// For standard tasks, need decent quality — balance quality & cost.
// For complex/deep tasks, quality matters most — premium models first.
const CHEAP_AGENTS: Record<TaskComplexity, CheapAgent[]> = {
  // Trivial: any model can handle these — pure cost optimization
  trivial: [
    {
      cli: "claude",
      model: "glm-4.7-flash",
      provider: "glm",
      quality: 0.25,
      cost: 0.0,
    }, // FREE, good enough for trivial
    { cli: "gemini", model: "gemini-2.5-flash-lite", quality: 0.2, cost: 0.01 }, // near-free
    { cli: "gemini", model: "gemini-2.5-flash", quality: 0.45, cost: 0.02 }, // great value
    {
      cli: "claude",
      model: "MiniMax-M2.1-highspeed",
      provider: "minimax",
      quality: 0.25,
      cost: 0.05,
    }, // cheap
    {
      cli: "kimi",
      model: "kimi-code/kimi-for-coding",
      quality: 0.65,
      cost: 0.07,
    }, // solid quality/cost
  ],
  // Standard: need reliable results — balance quality and cost
  // Ordered by value: gemini-flash and kimi give best quality/$ for real coding work
  standard: [
    { cli: "gemini", model: "gemini-2.5-flash", quality: 0.45, cost: 0.02 }, // best value for coding
    {
      cli: "kimi",
      model: "kimi-code/kimi-for-coding",
      quality: 0.65,
      cost: 0.07,
    }, // strong coder, cheap
    {
      cli: "claude",
      model: "glm-4.7",
      provider: "glm",
      quality: 0.7,
      cost: 0.08,
    }, // standard-tier quality, very cheap
    {
      cli: "claude",
      model: "glm-4.7-flash",
      provider: "glm",
      quality: 0.25,
      cost: 0.0,
    }, // FREE fallback (lower quality)
    {
      cli: "claude",
      model: "MiniMax-M2.5-highspeed",
      provider: "minimax",
      quality: 0.4,
      cost: 0.09,
    }, // decent fast
    {
      cli: "claude",
      model: "claude-haiku-4-5-20251001",
      quality: 0.45,
      cost: 0.13,
    }, // reliable, tool-capable
    { cli: "codex", model: "gpt-5.1-codex-mini", quality: 0.45, cost: 0.27 }, // good but pricier
    { cli: "gemini", model: "gemini-3.1-pro-low", quality: 0.7, cost: 0.37 }, // high quality, moderate cost
  ],
  // Complex: quality matters — stronger models first, cost is secondary
  complex: [
    { cli: "gemini", model: "gemini-3.1-pro-low", quality: 0.7, cost: 0.37 }, // best quality/cost for complex
    {
      cli: "claude",
      model: "glm-4.7",
      provider: "glm",
      quality: 0.7,
      cost: 0.08,
    }, // standard-tier, very cheap
    { cli: "claude", model: "claude-sonnet-4-6", quality: 0.8, cost: 0.5 }, // strong, trusted
    { cli: "codex", model: "gpt-5.3-codex", quality: 0.9, cost: 0.63 }, // near-premium
    {
      cli: "claude",
      model: "claude-haiku-4-5-20251001",
      quality: 0.45,
      cost: 0.13,
    }, // fast fallback
    {
      cli: "claude",
      model: "MiniMax-M2.5",
      provider: "minimax",
      quality: 0.55,
      cost: 0.09,
    }, // decent mid-tier
    { cli: "gemini", model: "gemini-3.1-pro-high", quality: 0.95, cost: 0.73 }, // premium fallback
  ],
  // Deep: premium quality required — never skimp
  deep: [
    { cli: "claude", model: "claude-sonnet-4-6", quality: 0.8, cost: 0.5 }, // best value for deep work
    {
      cli: "claude",
      model: "glm-5",
      provider: "glm",
      quality: 0.95,
      cost: 0.11,
    }, // premium quality, incredible value
    { cli: "gemini", model: "gemini-3.1-pro-high", quality: 0.95, cost: 0.73 }, // premium
    { cli: "codex", model: "gpt-5.4", quality: 0.95, cost: 1.0 }, // top-tier
  ],
};

// CLIs that are considered "expensive" and candidates for downgrade
const EXPENSIVE_CLIS = new Set<AgentCLI>(["claude", "codex"]);

// --- Pattern banks ---

// Trivial: rename, typo, format, single-line change, doc update
const TRIVIAL_PATTERNS: [RegExp, string][] = [
  [/\b(rename|typo|spelling|grammar|capitali[sz])\b/i, "rename/typo"],
  [/\b(format|lint|prettier|eslint|ruff)\b/i, "formatting"],
  [/\b(bump|update)\s+(version|dep|dependency)\b/i, "version-bump"],
  [/\b(add|update|fix)\s+(comment|docstring|jsdoc|readme)\b/i, "doc-update"],
  [/\b(import|export)\s+(add|remove|fix|missing)\b/i, "import-fix"],
  [/\b(one-?liner|single.?line|small\s+fix)\b/i, "one-liner"],
  [
    /\b(env|config)\s+(var|variable|value)\s+(change|update|set)\b/i,
    "config-change",
  ],
  [/\b(log|print|console\.log)\s+(add|remove|clean)\b/i, "logging-change"],
  [/\bupdate\s+(the\s+)?copy\b/i, "copy-update"],
];

// Standard: typical coding tasks — implement a function, fix a bug, write a test
const STANDARD_PATTERNS: [RegExp, string][] = [
  [
    /\b(implement|create|build|write)\s+(a\s+)?(simple|basic|small)\b/i,
    "simple-implement",
  ],
  [
    /\b(add|create)\s+(a\s+)?(new\s+)?(endpoint|route|handler|component)\b/i,
    "add-endpoint",
  ],
  [
    /\b(fix|resolve|debug)\b.*\b(bug|issue|error|warning|fail|broken)\b/i,
    "bug-fix",
  ],
  [/\b(write|add|create)\s+(unit\s+)?test/i, "write-test"],
  [/\b(add|implement)\s+(validation|check|guard)\b/i, "add-validation"],
  [
    /\b(extract|move)\s+(to|into)\s+(a\s+)?(function|method|util|helper)\b/i,
    "extract-function",
  ],
  [/\b(wrap|add)\s+(try|error\s+handling|catch)\b/i, "error-handling"],
  [/\binstall\s+(and\s+)?(configure|setup|wire)/i, "install-configure"],
  [/\b(generate|scaffold|stub)\b/i, "scaffold"],
  [/\bunit\s+test/i, "unit-test"],
];

// Complex: multi-file, refactoring, review, significant feature
const COMPLEX_PATTERNS: [RegExp, string][] = [
  [/\b(refactor|rewrite|restructure)\b/i, "refactor"],
  [/\bcode\s+review\b/i, "code-review"],
  [/\b(across|multiple)\s+(files?|modules?|components?)\b/i, "multi-file"],
  [
    /\bacross\s+(the\s+)?(entire\s+)?(codebase|project|repo)\b/i,
    "codebase-wide",
  ],
  [/\bmulti-?file\b/i, "multi-file"],
  [/\b(test\s+suite|test\s+strategy|integration\s+test)\b/i, "test-strategy"],
  [/\b(feature|workflow)\b.*\b(implement|build|create)\b/i, "feature-impl"],
  [/\b(api|module|service)\s+(design|redesign)\b/i, "module-design"],
  [/\btype\s+(system|safety|checking)\b/i, "type-system"],
  [/\b(database|schema)\s+(migration|change)\b/i, "db-migration"],
  [/\b(performance|optimize|bottleneck)\b/i, "performance"],
];

// Deep: architecture, security, system design, complex debugging
const DEEP_PATTERNS: [RegExp, string][] = [
  [/\b(architect|architecture)\b/i, "architecture"],
  [/\bsystem\s+design\b/i, "system-design"],
  [/\bsecurity\s*(audit|review|vulnerabilit|analys)/i, "security-audit"],
  [/\bvulnerabilit/i, "vulnerability"],
  [/\b(migration|migrate)\s+(strategy|plan)\b/i, "migration-strategy"],
  [/\b(race\s+condition|deadlock|heisenbug)\b/i, "concurrency-bug"],
  [/\b(memory\s+leak|corruption|segfault)\b/i, "deep-debug"],
  [/\bbreaking\s+change/i, "breaking-change"],
  [/\b(design\s+pattern|trade-?off|decision)\b/i, "design-decision"],
  [
    /\b(plan|proposal|rfc)\b.*\b(implement|feature|system|remediat)\b/i,
    "planning",
  ],
  [/\bdesign\s+(a\s+)?(remediat\w*|strateg\w*|approach)\b/i, "design-planning"],
  [/\bdistributed\s+(system|architecture|consensus)\b/i, "distributed"],
  [/\b(backwards?\s*compat|deprecat)\b/i, "deprecation"],
  [/\b(orchestrat|coordinat|multi-?agent)\b/i, "orchestration"],
  [/\bmicroservices?\s+(migration|architect)/i, "microservices"],
  [/\b(product\s+strate?g|go.to.market|gtm)\b/i, "product-strategy"],
  [/\b(prd|product\s+requirements?\s+doc)/i, "prd"],
  [/\b(growth\s+loop|growth\s+model|north\s+star)/i, "growth-strategy"],
  [/\b(competitive\s+analy|market\s+siz|positioning)\b/i, "market-analysis"],
  [/\b(pricing\s+strate?g|monetization\s+strate?g)\b/i, "pricing-strategy"],
];

/**
 * Classify a fleet dispatch request by task complexity.
 * Returns the classification and optionally a cheaper CLI/model recommendation.
 */
export function classifyTask(
  req: FleetDispatchRequest,
  availability?: FleetAvailability,
): ClassificationResult {
  const prompt = req.prompt;
  const { complexity, score, reasons } = scorePrompt(prompt);

  const taskKind =
    (req.task_kind as RoutingTaskKind | undefined) ?? inferTaskKind(prompt);
  const role =
    (req.role as FleetRole | undefined) ?? inferRole(taskKind, prompt);
  const lane = inferLane(taskKind, role);
  const riskLevel = inferRiskLevel(complexity, taskKind);
  const result: ClassificationResult = {
    complexity,
    score,
    reasons,
    role,
    lane,
    task_kind: taskKind,
    risk_level: riskLevel,
    quality_floor: lane === "worker_budget" ? "budget" : "premium",
    review_required: ["reviewer", "ui_reviewer"].includes(role),
    synthesis_required: [
      "planner",
      "architect",
      "design_lead",
      "synthesizer",
    ].includes(role),
    rerouted: false,
  };

  const explicitlyPinned =
    typeof req.metadata?.["provider_lock"] === "string" ||
    req.metadata?.["no_reroute"] === true;

  if (
    !explicitlyPinned &&
    ["design_visual", "design_system", "product_ux"].includes(taskKind)
  ) {
    if (req.cli !== "gemini" || req.model !== "gemini-3.1-pro-high") {
      result.recommended_cli = "gemini";
      result.recommended_model = "gemini-3.1-pro-high";
      result.original_cli = req.cli;
      result.original_model = req.model;
      result.rerouted = true;
    }
    return result;
  }

  if (!explicitlyPinned && taskKind === "product_strategy") {
    if (req.cli !== "claude" || !req.model?.includes("opus")) {
      result.recommended_cli = "claude";
      result.recommended_model = "claude-opus-4-6";
      result.original_cli = req.cli;
      result.original_model = req.model;
      result.rerouted = true;
    }
    return result;
  }

  if (isPremiumProtectedRole(role)) {
    return result;
  }

  // Only reroute if:
  // 1. The requested CLI is expensive (claude/codex)
  // 2. The task is trivial or standard (won't benefit from expensive model)
  // 3. No explicit opt-out via metadata
  if (
    EXPENSIVE_CLIS.has(req.cli) &&
    (complexity === "trivial" || complexity === "standard") &&
    req.metadata?.["no_reroute"] !== true &&
    req.metadata?.["cost_sensitivity"] !== "none"
  ) {
    const candidates = CHEAP_AGENTS[complexity];
    const pick = availability
      ? pickBestCandidate(candidates, availability, complexity)
      : candidates[0];

    if (pick) {
      result.recommended_cli = pick.cli;
      result.recommended_model = pick.model;
      result.original_cli = req.cli;
      result.original_model = req.model;
      result.rerouted = true;
    }
  }

  return result;
}

function inferTaskKind(prompt: string): RoutingTaskKind {
  if (/(design system|component library|design tokens|ui kit)/i.test(prompt))
    return "design_system";
  if (
    /(ui|ux|wireframe|mockup|visual design|landing page|responsive|mobile-first)/i.test(
      prompt,
    )
  )
    return "design_visual";
  if (
    /(user flow|onboarding|product ux|information architecture|usability)/i.test(
      prompt,
    )
  )
    return "product_ux";
  if (
    /(product\s+strate?g|product\s+spec|prd|feature\s+spec|competitive\s+analy|market\s+siz|go.to.market|gtm|positioning|personas?|jobs?.to?.be?.done|jtbd|growth\s+loop|north\s+star|pricing\s+strate?g|feature\s+prioriti[sz]|rice\s+scor|ice\s+scor)/i.test(
      prompt,
    )
  )
    return "product_strategy";
  if (/(review|audit|pr review|code review|security review)/i.test(prompt))
    return "review";
  if (/(research|compare|synthesis|summari[sz]e|investigate)/i.test(prompt))
    return "research_synthesis";
  if (
    /(refactor|architecture|migration|distributed|orchestrat|system design|race condition|performance)/i.test(
      prompt,
    )
  )
    return "code_deep";
  if (/(rename|format|typo|small|minor|lint|one-line)/i.test(prompt))
    return "small_transform";
  return "general";
}

function inferRole(taskKind: RoutingTaskKind, prompt: string): FleetRole {
  const metadataRole =
    /(planner|architect|implementer|researcher|extractor|validator|reviewer|synthesizer|design lead|ui reviewer)/i
      .exec(prompt)?.[1]
      ?.toLowerCase();
  if (metadataRole === "planner") return "planner";
  if (metadataRole === "architect") return "architect";
  if (metadataRole === "researcher") return "researcher";
  if (metadataRole === "extractor") return "extractor";
  if (metadataRole === "validator") return "validator";
  if (metadataRole === "reviewer") return "reviewer";
  if (metadataRole === "synthesizer") return "synthesizer";
  if (metadataRole === "design lead") return "design_lead";
  if (metadataRole === "ui reviewer") return "ui_reviewer";
  if (
    metadataRole === "product strategist" ||
    metadataRole === "product_strategist"
  )
    return "product_strategist";
  switch (taskKind) {
    case "design_visual":
    case "design_system":
    case "product_ux":
      return "design_lead";
    case "review":
      return "reviewer";
    case "product_strategy":
      return "product_strategist";
    case "research_synthesis":
      return "researcher";
    case "code_deep":
      return "architect";
    case "small_transform":
      return "implementer";
    default:
      return "implementer";
  }
}

function inferLane(taskKind: RoutingTaskKind, role: FleetRole): RoutingLane {
  if (role === "reviewer") return "review_premium";
  if (role === "product_strategist") return "strategy_premium";
  if (["design_lead", "ui_reviewer"].includes(role)) return "design_premium";
  if (taskKind === "small_transform") return "worker_budget";
  if (role === "researcher" || role === "extractor") return "worker_standard";
  return "interactive_premium";
}

function inferRiskLevel(
  complexity: TaskComplexity,
  taskKind: RoutingTaskKind,
): RiskLevel {
  if (
    complexity === "deep" ||
    taskKind === "review" ||
    taskKind === "code_deep"
  )
    return "high";
  if (
    complexity === "complex" ||
    [
      "design_visual",
      "design_system",
      "product_ux",
      "product_strategy",
      "research_synthesis",
    ].includes(taskKind)
  )
    return "medium";
  return "low";
}

function isPremiumProtectedRole(role: FleetRole): boolean {
  return [
    "planner",
    "architect",
    "reviewer",
    "synthesizer",
    "design_lead",
    "ui_reviewer",
    "product_strategist",
  ].includes(role);
}

// Quality weight per complexity tier — how much quality matters vs cost.
// Higher = prefer quality, lower = prefer cost savings.
const QUALITY_WEIGHT: Record<TaskComplexity, number> = {
  trivial: 0.2, // cost dominates — any model works
  standard: 0.5, // balanced — need decent results
  complex: 0.75, // quality dominates — bad output wastes more than it saves
  deep: 0.9, // near-total quality focus — failure is expensive
};

/**
 * Compute a composite value score for a candidate.
 * Blends model quality, cost, and live provider performance data.
 *
 * Formula: qualityWeight * qualitySignal + (1 - qualityWeight) * costSignal
 *   where qualitySignal = model.quality * providerBoost
 *   and   costSignal = 1 - model.cost (lower cost → higher score)
 */
function candidateValueScore(
  c: CheapAgent,
  complexity: TaskComplexity,
  providerScores: Map<string, number>,
): number {
  const qw = QUALITY_WEIGHT[complexity];

  // Provider learning boost: scale model quality by live provider reliability.
  // No score data → neutral (1.0). Score range 0-1 maps to 0.5-1.5 multiplier
  // so a high-performing provider boosts quality and a failing one dampens it.
  const providerScore = providerScores.get(candidateProvider(c));
  const providerMultiplier =
    providerScore !== undefined
      ? 0.5 + providerScore // 0→0.5, 0.5→1.0, 1.0→1.5
      : 1.0;

  const qualitySignal = c.quality * providerMultiplier;
  const costSignal = 1 - c.cost; // lower cost → higher score

  return qw * qualitySignal + (1 - qw) * costSignal;
}

/**
 * Filter and rank candidates using live availability data and quality-cost balance.
 * Returns the best candidate, or undefined if none survive filtering.
 */
/** Resolve the actual provider for a candidate (uses provider override or CLI mapping) */
function candidateProvider(c: CheapAgent): string {
  return c.provider ?? CLI_TO_PROVIDER[c.cli] ?? c.cli;
}

function pickBestCandidate(
  candidates: CheapAgent[],
  availability: FleetAvailability,
  complexity: TaskComplexity = "standard",
): CheapAgent | undefined {
  const {
    exhaustedProviders,
    budgetStoppedProviders,
    budgetAlertProviders,
    providerScores,
  } = availability;

  // Filter out exhausted and budget-stopped providers
  const viable = candidates.filter((c) => {
    const provider = candidateProvider(c);
    if (exhaustedProviders.has(provider)) return false;
    if (budgetStoppedProviders.has(provider)) return false;
    return true;
  });

  if (viable.length === 0) return undefined;

  // Partition into normal and alert-zone
  const normal: typeof viable = [];
  const alert: typeof viable = [];
  for (const c of viable) {
    const provider = candidateProvider(c);
    if (budgetAlertProviders.has(provider)) {
      alert.push(c);
    } else {
      normal.push(c);
    }
  }

  // Prefer normal over alert; within each group, sort by composite value score
  const pool = normal.length > 0 ? normal : alert;

  pool.sort((a, b) => {
    const scoreA = candidateValueScore(a, complexity, providerScores);
    const scoreB = candidateValueScore(b, complexity, providerScores);
    return scoreB - scoreA;
  });

  return pool[0];
}

/**
 * Score a prompt for complexity. Higher score = more complex.
 *
 * Thresholds:
 *   0-9   → trivial
 *   10-29 → standard
 *   30-59 → complex
 *   60+   → deep
 */
function scorePrompt(prompt: string): {
  complexity: TaskComplexity;
  score: number;
  reasons: string[];
} {
  let score = 0;
  const reasons: string[] = [];

  // Deep signals (+30 each)
  for (const [pattern, label] of DEEP_PATTERNS) {
    if (pattern.test(prompt)) {
      score += 30;
      reasons.push(label);
    }
  }

  // Complex signals (+20 each)
  for (const [pattern, label] of COMPLEX_PATTERNS) {
    if (pattern.test(prompt)) {
      score += 20;
      reasons.push(label);
    }
  }

  // Standard signals (+15 each)
  for (const [pattern, label] of STANDARD_PATTERNS) {
    if (pattern.test(prompt)) {
      score += 15;
      reasons.push(label);
    }
  }

  // Trivial anti-signals (-5 each, to push trivial tasks lower)
  for (const [pattern, label] of TRIVIAL_PATTERNS) {
    if (pattern.test(prompt)) {
      score -= 5;
      reasons.push(label);
    }
  }

  // Structural complexity signals
  const codeBlockCount = Math.floor((prompt.match(/```/g) ?? []).length / 2);
  if (codeBlockCount >= 3) {
    score += 15;
    reasons.push(`${codeBlockCount} code blocks`);
  }

  const fileRefs = prompt.match(
    /\b[\w/.-]+\.(ts|js|py|go|rs|java|yaml|json|tsx|jsx|vue|svelte)\b/g,
  );
  const uniqueFiles = fileRefs ? new Set(fileRefs).size : 0;
  if (uniqueFiles >= 5) {
    score += 25;
    reasons.push(`${uniqueFiles} file references`);
  } else if (uniqueFiles >= 3) {
    score += 15;
    reasons.push(`${uniqueFiles} file references`);
  } else if (uniqueFiles >= 2) {
    score += 10;
    reasons.push(`${uniqueFiles} file references`);
  }

  // Prompt length as a soft signal
  if (prompt.length > 3000) {
    score += 15;
    reasons.push(`long prompt (${prompt.length} chars)`);
  } else if (prompt.length > 1500) {
    score += 8;
    reasons.push(`medium prompt (${prompt.length} chars)`);
  } else if (prompt.length < 100) {
    score -= 2;
    reasons.push(`short prompt (${prompt.length} chars)`);
  }

  // Determine complexity tier
  let complexity: TaskComplexity;
  if (score >= 60) {
    complexity = "deep";
  } else if (score >= 30) {
    complexity = "complex";
  } else if (score >= 10) {
    complexity = "standard";
  } else {
    complexity = "trivial";
  }

  return { complexity, score, reasons };
}

/**
 * Apply classification to a dispatch request, mutating it in-place.
 * Returns the classification result for logging/observability.
 *
 * Call this in the fleet dispatch handler BEFORE dispatching.
 * The original request is preserved in the result for audit trail.
 */
export function applyClassification(
  req: FleetDispatchRequest,
  result: ClassificationResult,
): void {
  if (!result.rerouted) return;

  // Mutate the request to use the cheaper agent
  if (result.recommended_cli) {
    req.cli = result.recommended_cli;
  }
  if (result.recommended_model) {
    req.model = result.recommended_model;
  }

  // Stash the original routing in metadata for cascade fallback
  if (!req.metadata) req.metadata = {};
  req.metadata["fractal_rerouted"] = true;
  req.metadata["fractal_original_cli"] = result.original_cli;
  req.metadata["fractal_original_model"] = result.original_model;
  req.metadata["fractal_complexity"] = result.complexity;
  req.metadata["fractal_score"] = result.score;
  req.metadata["fractal_reasons"] = result.reasons;
  req.metadata["fractal_role"] = result.role;
  req.metadata["fractal_lane"] = result.lane;
  req.metadata["fractal_task_kind"] = result.task_kind;
  req.metadata["fractal_risk_level"] = result.risk_level;
  req.metadata["fractal_quality_floor"] = result.quality_floor;
}
