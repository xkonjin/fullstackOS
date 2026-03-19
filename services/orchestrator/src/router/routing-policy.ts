import type {
  Config,
  FleetRole,
  Provider,
  RoutingLane,
  RoutingTaskKind,
  Tier,
} from "../types.ts";
import type { MessagesRequest } from "../types.ts";

const DEFAULT_PROVIDER_BANDS: Record<RoutingLane, Provider[]> = {
  interactive_premium: ["codex", "gemini", "antigravity", "claude"],
  design_premium: ["gemini", "codex", "claude"],
  review_premium: ["codex", "gemini", "antigravity", "claude"],
  strategy_premium: ["claude", "gemini", "codex"],
  interactive_fast: ["gemini", "codex", "glm", "minimax"],
  worker_standard: ["gemini", "kimi", "codex", "glm"],
  worker_budget: ["kimi", "glm", "minimax", "gemini"],
  last_resort: ["openrouter"],
};

const DEFAULT_TASK_HEADS: Record<
  RoutingLane,
  Partial<Record<RoutingTaskKind, string[]>>
> = {
  interactive_premium: {
    code_deep: ["gpt-5.4", "gpt-5.3-codex", "claude-opus-4-6"],
    research_synthesis: ["gpt-5.4", "gemini-2.5-pro", "claude-opus-4-6"],
    review: ["gpt-5.4", "gpt-5.3-codex", "claude-opus-4-6"],
    general: ["gpt-5.4", "gemini-2.5-pro", "claude-opus-4-6"],
  },
  design_premium: {
    design_visual: ["gemini-3.1-pro-high", "gemini-2.5-pro", "claude-opus-4-6"],
    design_system: ["gemini-3.1-pro-high", "gpt-5.4", "claude-opus-4-6"],
    product_ux: ["gemini-3.1-pro-high", "gpt-5.4", "claude-opus-4-6"],
    general: ["gemini-3.1-pro-high", "gemini-2.5-pro", "claude-opus-4-6"],
  },
  review_premium: {
    review: ["gpt-5.4", "gpt-5.3-codex", "claude-opus-4-6"],
    general: ["gpt-5.4", "gpt-5.3-codex", "claude-opus-4-6"],
  },
  strategy_premium: {
    product_strategy: ["claude-opus-4-6", "gemini-3.1-pro-high", "gpt-5.4"],
    research_synthesis: ["claude-opus-4-6", "gemini-2.5-pro", "gpt-5.4"],
    general: ["claude-opus-4-6", "gemini-3.1-pro-high", "gpt-5.4"],
  },
  interactive_fast: {
    small_transform: ["gpt-5.2-codex", "gemini-2.5-pro", "claude-sonnet-4-6"],
    general: ["gpt-5.2-codex", "gemini-2.5-pro", "claude-sonnet-4-6"],
  },
  worker_standard: {
    general: ["gemini-2.5-flash", "kimi-for-coding", "gpt-5.1-codex-mini"],
  },
  worker_budget: {
    small_transform: ["kimi-k2.5", "glm-4.7", "MiniMax-M2.5-highspeed"],
    general: ["kimi-k2.5", "glm-4.7", "MiniMax-M2.5-highspeed"],
  },
  last_resort: {
    general: ["anthropic/claude-sonnet-4-6"],
  },
};

const MODEL_PROVIDER_HINTS: Array<[RegExp, Provider]> = [
  [/^gpt-5(\.|-)/i, "codex"],
  [/^claude-/i, "claude"],
  [/^gemini-/i, "gemini"],
  [/^glm-/i, "glm"],
  [/^kimi-/i, "kimi-api"],
  [/^minimax/i, "minimax"],
  [/^anthropic\//i, "openrouter"],
  [/^openai\//i, "openrouter"],
  [/^google\//i, "openrouter"],
];

export interface RoutingIntent {
  lane: RoutingLane;
  taskKind: RoutingTaskKind;
  role: FleetRole;
  riskLevel: "low" | "medium" | "high";
  preferredModels: string[];
  providerBand: Provider[];
  qualityFloor: Tier;
  explanation: string[];
}

function getTaskHeads(
  config: Config,
  lane: RoutingLane,
): Partial<Record<RoutingTaskKind, string[]>> {
  return {
    ...(DEFAULT_TASK_HEADS[lane] ?? {}),
    ...(config.routing_policy?.lanes?.[lane]?.task_heads ?? {}),
  };
}

export function getProviderBand(config: Config, lane: RoutingLane): Provider[] {
  return (
    config.routing_policy?.lanes?.[lane]?.provider_band ??
    DEFAULT_PROVIDER_BANDS[lane]
  ).filter(Boolean) as Provider[];
}

export function getLaneQualityFloor(config: Config, lane: RoutingLane): Tier {
  return (
    config.routing_policy?.lanes?.[lane]?.quality_floor ??
    (lane === "worker_budget"
      ? "budget"
      : lane === "interactive_fast"
        ? "fast"
        : lane === "last_resort"
          ? "last_resort"
          : "premium")
  );
}

export function canonicalizeModel(config: Config, model: string): string {
  const aliases = config.model_aliases ?? {};
  return aliases[model] ?? aliases[model.replace(/[-.]/g, "_")] ?? model;
}

export function modelToProvider(model: string): Provider | undefined {
  for (const [pattern, provider] of MODEL_PROVIDER_HINTS) {
    if (pattern.test(model)) return provider;
  }
  return undefined;
}

function textFromRequest(request: MessagesRequest): string {
  return (request.messages ?? [])
    .map((message) => {
      if (typeof message.content === "string") return message.content;
      if (!Array.isArray(message.content)) return "";
      return message.content
        .map((block) =>
          block && typeof block === "object" && typeof block.text === "string"
            ? block.text
            : "",
        )
        .join("\n");
    })
    .join("\n")
    .toLowerCase();
}

export function inferTaskKind(request: MessagesRequest): RoutingTaskKind {
  const explicit = request.metadata?.["task_kind"];
  if (typeof explicit === "string") return explicit as RoutingTaskKind;

  const text = textFromRequest(request);
  if (/(design system|component library|ui system|design tokens)/i.test(text))
    return "design_system";
  if (
    /(product\s+strate?g|product\s+spec|product\s+requirements?|prd|feature\s+spec|competitive\s+analy|market\s+siz|go.to.market|gtm|positioning|personas?|customer\s+research|user\s+research|jobs?.to?.be?.done|jtbd|product.market\s+fit|pmf|growth\s+loop|growth\s+model|north\s+star\s+metric|pricing\s+strate?g|feature\s+prioriti[sz]|rice\s+scor|ice\s+scor)/i.test(
      text,
    )
  )
    return "product_strategy";
  if (
    /(ui|ux|wireframe|mockup|visual design|landing page|responsive|mobile-first|design review)/i.test(
      text,
    )
  )
    return "design_visual";
  if (
    /(user flow|product ux|information architecture|onboarding|usability|interaction design)/i.test(
      text,
    )
  )
    return "product_ux";
  if (/(review|audit|pr review|code review|security review)/i.test(text))
    return "review";
  if (/(research|compare|synthesis|summari[sz]e|investigate)/i.test(text))
    return "research_synthesis";
  if (
    /(architecture|multi-file|refactor|migration|orchestr|system design|distributed|deep debug|race condition)/i.test(
      text,
    )
  )
    return "code_deep";
  if (/(rename|format|typo|small|one-line|lint|minor)/i.test(text))
    return "small_transform";
  return "general";
}

export function inferRoleFromTaskKind(taskKind: RoutingTaskKind): FleetRole {
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
    case "small_transform":
      return "implementer";
    case "code_deep":
      return "architect";
    default:
      return "implementer";
  }
}

export function inferLane(
  request: MessagesRequest,
  taskKind: RoutingTaskKind,
  role?: FleetRole,
): RoutingLane {
  const explicitLane =
    request.metadata?.["claudemax_lane"] ?? request.metadata?.["routing_lane"];
  if (typeof explicitLane === "string") return explicitLane as RoutingLane;
  if (role === "reviewer") return "review_premium";
  if (role === "product_strategist" || taskKind === "product_strategy")
    return "strategy_premium";
  if (["design_visual", "design_system", "product_ux"].includes(taskKind))
    return "design_premium";
  if (taskKind === "review") return "review_premium";
  return "interactive_premium";
}

export function preferredModelsForIntent(
  config: Config,
  lane: RoutingLane,
  taskKind: RoutingTaskKind,
): string[] {
  const taskHeads = getTaskHeads(config, lane);
  return taskHeads[taskKind] ?? taskHeads.general ?? [];
}

export function buildRoutingIntent(
  config: Config,
  request: MessagesRequest,
): RoutingIntent {
  const taskKind = inferTaskKind(request);
  const role = inferRoleFromTaskKind(taskKind);
  const lane = inferLane(request, taskKind, role);
  const preferredModels = preferredModelsForIntent(config, lane, taskKind).map(
    (model) => canonicalizeModel(config, model),
  );
  const providerBand = getProviderBand(config, lane);
  const qualityFloor = getLaneQualityFloor(config, lane);
  const riskLevel = ["code_deep", "review"].includes(taskKind)
    ? "high"
    : [
          "design_visual",
          "design_system",
          "product_ux",
          "product_strategy",
          "research_synthesis",
        ].includes(taskKind)
      ? "medium"
      : "low";

  const explanation = [
    `lane=${lane}`,
    `task_kind=${taskKind}`,
    `role=${role}`,
    `quality_floor=${qualityFloor}`,
  ];
  if (preferredModels.length > 0) {
    explanation.push(`preferred_models=${preferredModels.join(" -> ")}`);
  }
  if (providerBand.length > 0) {
    explanation.push(`provider_band=${providerBand.join(",")}`);
  }

  return {
    lane,
    taskKind,
    role,
    riskLevel,
    preferredModels,
    providerBand,
    qualityFloor,
    explanation,
  };
}

export function roleRequiresPremium(role: FleetRole): boolean {
  return [
    "planner",
    "architect",
    "reviewer",
    "ui_reviewer",
    "design_lead",
    "synthesizer",
    "product_strategist",
  ].includes(role);
}
