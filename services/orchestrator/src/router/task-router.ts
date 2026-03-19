import type {
  Config,
  Message,
  MessagesRequest,
  Provider,
  ProviderAccount,
  RoutingLane,
  RoutingTaskKind,
  Tier,
} from "../types.ts";
import {
  buildRoutingIntent,
  canonicalizeModel,
  modelToProvider,
} from "./routing-policy.ts";
import type { TokenManager } from "../auth/token-manager.ts";
import type { UsageDB } from "../db/usage.ts";

// Providers that natively support Anthropic's extended thinking mode
const NATIVE_THINKING_PROVIDERS = new Set<Provider>(["claude", "antigravity"]);
// Providers that support reasoning (OpenAI reasoning_effort) AND return
// reasoning_content in responses so Claude Code can display thinking traces.
const REASONING_CAPABLE_PROVIDERS = new Set<Provider>([
  "claude",
  "antigravity",
  "codex",
  "gemini",
]);
// Providers that only work via CLI (fleet dispatch), not HTTP API.
// kimi: now works via HTTP through CLIProxyAPI's openai-compat path (OAuth tokens).
const CLI_ONLY_PROVIDERS = new Set<Provider>([]);
const ALL_PROVIDERS: Provider[] = [
  "claude",
  "antigravity",
  "codex",
  "gemini",
  "kimi",
  "glm",
  "minimax",
  "openrouter",
];
const FALLBACK_ORDER: Tier[] = [
  "premium",
  "standard",
  "fast",
  "budget",
  "last_resort",
];

export class TaskRouter {
  private config: Config;
  private tokenManager: TokenManager;
  private usageDB: UsageDB | null;
  // Round-robin index per tier
  private rrIndex: Map<string, number> = new Map();
  private usageCache: { data: Map<string, number>; ts: number } | null = null;
  private lastRouteExplanation: Record<string, unknown> | null = null;
  private readonly USAGE_CACHE_TTL = 30_000;

  constructor(config: Config, tokenManager: TokenManager, usageDB?: UsageDB) {
    this.config = config;
    this.tokenManager = tokenManager;
    this.usageDB = usageDB ?? null;
  }

  /**
   * Check if ALL accounts for a provider are rate-limited or unhealthy.
   * Used to proactively skip providers instead of wasting retry attempts.
   */
  isProviderExhausted(provider: Provider): boolean {
    const all = this.tokenManager
      .getAllAccounts()
      .filter((a) => a.provider === provider);
    if (all.length === 0) return true;
    const healthy = this.tokenManager.getHealthyAccounts(provider);
    return healthy.length === 0;
  }

  private getDailyUsage(): Map<string, number> {
    const now = Date.now();
    if (this.usageCache && now - this.usageCache.ts < this.USAGE_CACHE_TTL) {
      return this.usageCache.data;
    }
    if (!this.usageDB) return new Map();
    const data = this.usageDB.getTodayTokensByAccount();
    this.usageCache = { data, ts: now };
    return data;
  }

  private pickLeastUsed(
    available: ProviderAccount[],
    rrKey: string,
  ): ProviderAccount {
    const dailyUsage = this.getDailyUsage();

    if (dailyUsage.size > 0 && available.length > 1) {
      // Build scored list: lower score = better candidate
      const scored = available.map((acct, idx) => {
        const usage = dailyUsage.get(acct.id) ?? 0;

        // Proactive budget pressure: penalize accounts approaching rate limits.
        // quotaRemaining/quotaLimit from response headers give real-time signal.
        let budgetPenalty = 0;
        if (
          acct.quotaRemaining !== null &&
          acct.quotaLimit !== null &&
          acct.quotaLimit > 0
        ) {
          const usedFraction = 1 - acct.quotaRemaining / acct.quotaLimit;
          if (usedFraction > 0.9)
            budgetPenalty = 1_000_000; // near-exhausted
          else if (usedFraction > 0.75) budgetPenalty = 500_000;
          else if (usedFraction > 0.5) budgetPenalty = 100_000;
        }

        // Penalize accounts with recent rate limit hits (last hour)
        let rateLimitPenalty = 0;
        if (acct.rateLimitHits > 0 && acct.lastRateLimitAt) {
          const msSinceLimit = Date.now() - acct.lastRateLimitAt;
          if (msSinceLimit < 3600_000) {
            rateLimitPenalty = acct.rateLimitHits * 200_000;
          }
        }

        return {
          idx,
          acct,
          score: usage + budgetPenalty + rateLimitPenalty,
        };
      });

      scored.sort((a, b) => a.score - b.score);
      const best = scored[0]!;
      const worst = scored[scored.length - 1]!;

      // If there's a meaningful difference, use the scored result
      if (worst.score > 0 && best.score < worst.score * 0.9) {
        console.log(
          `[router] Budget-aware: picked ${best.acct.id} (score=${best.score}) over ${worst.acct.id} (score=${worst.score})`,
        );
        return best.acct;
      }
    }

    // Fall back to round-robin when usage is similar or no data
    const idx = (this.rrIndex.get(rrKey) ?? 0) % available.length;
    this.rrIndex.set(rrKey, idx + 1);
    return available[idx]!;
  }

  private dedupeProviders(
    providers: Array<Provider | undefined | null>,
  ): Provider[] {
    const seen = new Set<Provider>();
    const ordered: Provider[] = [];
    for (const provider of providers) {
      if (!provider || seen.has(provider)) continue;
      seen.add(provider);
      ordered.push(provider);
    }
    return ordered;
  }

  private getPolicyProviderPriority(intent: {
    preferredModels: string[];
    providerBand: Provider[];
  }): Provider[] {
    const modelProviders = intent.preferredModels
      .map((model) => modelToProvider(model))
      .filter(Boolean) as Provider[];
    return this.dedupeProviders([...modelProviders, ...intent.providerBand]);
  }

  private getTierProviderOrder(
    tier: Tier,
    policyProviders?: Provider[],
  ): Provider[] {
    const configured = this.config.routing.tiers[tier] ?? [];
    return this.dedupeProviders([
      ...(policyProviders ?? []),
      ...(configured as Provider[]),
    ]);
  }

  updateConfig(config: Config): void {
    this.config = config;
  }

  getLastRouteExplanation(): Record<string, unknown> | null {
    return this.lastRouteExplanation;
  }

  classifyTier(request: MessagesRequest): Tier {
    const model = canonicalizeModel(this.config, request.model).toLowerCase();
    const hasThinking = !!request.thinking;
    const explicitTaskKind = request.metadata?.["task_kind"] as
      | RoutingTaskKind
      | undefined;
    const explicitLane =
      request.metadata?.["claudemax_lane"] ??
      request.metadata?.["routing_lane"];

    if (explicitLane === "worker_budget") return "budget";
    if (explicitLane === "interactive_fast") return "fast";
    if (explicitLane === "worker_standard") return "standard";
    if (
      explicitLane === "review_premium" ||
      explicitLane === "design_premium"
    ) {
      return "premium";
    }

    if (
      explicitTaskKind === "design_visual" ||
      explicitTaskKind === "design_system" ||
      explicitTaskKind === "product_ux" ||
      explicitTaskKind === "review" ||
      explicitTaskKind === "research_synthesis" ||
      explicitTaskKind === "code_deep"
    ) {
      return "premium";
    }

    // Premium: opus, thinking mode, gemini 3.1/3 pro high, codex 5.4/5.3, glm-5
    if (model.includes("opus") || hasThinking) return "premium";
    if (model.includes("gemini-3.1-pro-high")) return "premium";
    if (model.includes("gemini-3-pro")) return "premium";
    if (
      model === "gpt-5.4" ||
      model === "gpt-5.4-codex" ||
      model.includes("5.3-codex")
    )
      return "premium";
    if (model === "glm-5" || model === "glm-4.5-x") return "premium";

    // GLM flash — differentiate paid vs free before generic flash check
    if (model === "glm-4.7-flash" || model === "glm-4.5-flash") return "budget";
    if (model === "glm-4.7-flashx" || model === "glm-4.5-air") return "fast";

    // Fast: haiku, flash, codex-mini, minimax — explicit "I want fast"
    if (model.includes("haiku")) return "fast";
    if (model.includes("flash")) return "fast";
    if (model.includes("codex-mini") || model.includes("5.1-codex-mini")) {
      return "fast";
    }
    if (model.includes("minimax")) {
      if (model.includes("m2.1")) return "budget";
      return "fast";
    }

    if (model.includes("kimi")) {
      if (model === "kimi-k2.5") return "standard";
      return "fast";
    }

    if (
      request.metadata?.["agent_type"] === "sub_agent" &&
      request.metadata?.["cost_sensitivity"] === "high"
    ) {
      return "budget";
    }

    const isStandardModel =
      model.includes("sonnet") ||
      model.includes("gemini-3.1-pro-low") ||
      model.includes("gemini-2.5-pro") ||
      model.includes("5.2") ||
      model.includes("5.1-codex-max") ||
      model.includes("5.1-codex") ||
      model === "glm-4.7" ||
      model === "glm-4.5" ||
      model === "glm-4.5-airx";

    const { mode, score, reasons } = analyzeContentComplexity(request.messages);
    if (mode === "deep") {
      console.log(
        `[router] Content upgrade → premium (score=${score}, reasons: ${reasons.join(", ")})`,
      );
      return "premium";
    }

    if (!isStandardModel) {
      console.warn(`Unknown model "${model}" defaulting to standard tier`);
    }

    return "standard";
  }

  /**
   * Returns true if classifyTier would upgrade the request based on content analysis
   * (i.e., the model name alone would be standard, but content made it premium).
   */
  isContentUpgraded(request: MessagesRequest): boolean {
    const model = request.model.toLowerCase();
    // If the model is explicitly premium, it's not a content upgrade
    if (model.includes("opus") || !!request.thinking) return false;
    if (model.includes("gemini-3.1-pro-high") || model.includes("gemini-3-pro"))
      return false;
    if (
      model === "gpt-5.4" ||
      model === "gpt-5.4-codex" ||
      model.includes("5.3-codex")
    )
      return false;
    if (model === "glm-5" || model === "glm-4.5-x") return false;
    // If it's fast/budget, content analysis doesn't apply
    if (model.includes("haiku") || model.includes("flash")) return false;
    if (model.includes("codex-mini")) return false;
    // Check if content analysis would upgrade
    const tier = this.classifyTier(request);
    return tier === "premium";
  }

  /**
   * Compute the quality floor tier — the lowest acceptable tier for this request.
   * Premium/deep tasks should never silently degrade to budget models.
   */
  getFloorTier(request: MessagesRequest, tier: Tier): Tier {
    const intent = buildRoutingIntent(this.config, request);

    // Budget and last_resort stay at their own floor
    if (tier === "budget" || tier === "last_resort") {
      return tier;
    }

    // Fast requests can fall back to budget (but not last_resort)
    // This prevents fast→premium upgrades when budget providers are available
    if (tier === "fast") {
      return intent.qualityFloor === "premium" ? "premium" : "budget";
    }

    if (intent.qualityFloor === "premium") return "premium";

    // Premium requests → floor at standard (allow premium→standard fallback, not further)
    if (tier === "premium") return "standard";

    // Standard requests → floor at standard (don't fall to fast/budget)
    // Unless explicitly overridden via metadata
    if (request.metadata?.["allow_degrade"] === true) return "last_resort";

    return "standard";
  }

  /**
   * Returns the next account AND the model name to use for that provider.
   * When falling back cross-provider, the model name is translated.
   * When thinking mode is enabled, only thinking-capable providers are considered.
   */
  getNextRoute(
    request: MessagesRequest,
    excludeIds?: Set<string>,
    excludeProviders?: Set<string>,
    tierOverride?: Tier,
    opts?: { bypassFloor?: boolean },
  ): {
    account: ProviderAccount;
    model: string;
    tier: Tier;
    floorTier: Tier;
    degraded: boolean;
    degradeReason?: string;
  } | null {
    const normalizedRequest = {
      ...request,
      model: canonicalizeModel(this.config, request.model),
    };
    const tier = tierOverride ?? this.classifyTier(normalizedRequest);
    const contentUpgraded = tierOverride
      ? false
      : this.isContentUpgraded(normalizedRequest);
    const intent = buildRoutingIntent(this.config, normalizedRequest);
    const floorTier = opts?.bypassFloor
      ? "last_resort"
      : this.getFloorTier(normalizedRequest, tier);
    const hasThinking = !!normalizedRequest.thinking;

    // Provider affinity: only honor explicit provider hints or non-Claude native models.
    // claude-* model names are treated as abstract quality labels, NOT provider affinity —
    // the orchestrator translates them to equivalent models on cheaper providers.
    // This prevents Claude from dominating all traffic just because clients send claude-* names.
    const codexPreferred =
      normalizedRequest.metadata?.["codex_preferred"] === true ||
      normalizedRequest.metadata?.["execution_route"] === "codex_first";
    const explicitLock = normalizedRequest.metadata?.["provider_lock"] as
      | Provider
      | undefined;
    const nativeProvider = detectNativeProvider(normalizedRequest.model);
    const policyProviderPriority = this.getPolicyProviderPriority(intent);
    const intentPreferredProvider = policyProviderPriority[0];
    const taskHeadAffinityKinds = new Set<RoutingTaskKind>([
      "code_deep",
      "research_synthesis",
      "review",
      "design_visual",
      "design_system",
      "product_ux",
      "product_strategy",
    ]);
    const preferredProvider = explicitLock
      ? explicitLock
      : codexPreferred
        ? "codex"
        : taskHeadAffinityKinds.has(intent.taskKind) && intentPreferredProvider
          ? intentPreferredProvider
          : nativeProvider &&
              nativeProvider !== "claude" &&
              nativeProvider !== "antigravity"
            ? nativeProvider
            : undefined;

    // If thinking is enabled, prefer native thinking providers first,
    // but allow reasoning-capable providers (Codex) as fallback
    const effectiveExclude = new Set(excludeProviders ?? []);
    const providerBand = new Set(intent.providerBand);
    const honorProviderBand =
      !opts?.bypassFloor &&
      !explicitLock &&
      !(
        nativeProvider &&
        nativeProvider !== "claude" &&
        nativeProvider !== "antigravity"
      );
    if (providerBand.size > 0 && honorProviderBand) {
      for (const providerName of ALL_PROVIDERS) {
        if (!providerBand.has(providerName)) {
          effectiveExclude.add(providerName);
        }
      }
    }

    // CLI-only providers (e.g. kimi) only work via fleet dispatch, not HTTP API.
    // Exclude them for non-fleet requests to avoid routing to providers that will always fail.
    const isFleetRequest =
      normalizedRequest.metadata?.["agent_type"] === "fleet" ||
      normalizedRequest.metadata?.["dispatch_mode"] === "cli";
    if (!isFleetRequest) {
      for (const p of CLI_ONLY_PROVIDERS) {
        effectiveExclude.add(p);
      }
    }

    if (hasThinking) {
      const enforceNativeThinkingFirst =
        !preferredProvider || NATIVE_THINKING_PROVIDERS.has(preferredProvider);

      if (enforceNativeThinkingFirst) {
        const nativeExclude = new Set(effectiveExclude);
        for (const p of ALL_PROVIDERS) {
          if (!NATIVE_THINKING_PROVIDERS.has(p)) {
            nativeExclude.add(p);
          }
        }
        const nativeRoute = this.getAccountForTier(
          tier,
          excludeIds,
          nativeExclude,
          preferredProvider,
          floorTier,
          policyProviderPriority,
        );
        if (nativeRoute) {
          const model = translateModel(
            request.model,
            nativeRoute.account.provider,
            tier,
            contentUpgraded,
          );
          return {
            account: nativeRoute.account,
            model,
            tier,
            floorTier,
            degraded: nativeRoute.selectedTier !== tier,
            degradeReason:
              nativeRoute.selectedTier !== tier
                ? `${tier}→${nativeRoute.selectedTier} (${nativeRoute.account.provider})`
                : undefined,
          };
        }
        console.log(
          `[router] Native thinking providers exhausted, falling back to reasoning-capable`,
        );
      } else {
        console.log(
          `[router] Thinking request honoring preferred provider=${preferredProvider}; skipping native-thinking-only pass`,
        );
      }

      for (const p of ALL_PROVIDERS) {
        if (!REASONING_CAPABLE_PROVIDERS.has(p)) {
          effectiveExclude.add(p);
        }
      }
    }

    const accountRoute = this.getAccountForTier(
      tier,
      excludeIds,
      effectiveExclude,
      preferredProvider,
      floorTier,
      policyProviderPriority,
    );
    if (!accountRoute) {
      console.log(
        `[router] ⛔ No accounts available for tier=${tier} (floor=${floorTier}). Excluded providers: [${[...effectiveExclude].join(",")}], excluded accounts: ${excludeIds?.size ?? 0}`,
      );
      return null;
    }

    const account = accountRoute.account;
    // Detect if we degraded from the requested tier
    const accountTier = accountRoute.selectedTier;
    const degraded = accountTier !== tier;
    const degradeReason = degraded
      ? `${tier}→${accountTier} (${account.provider})`
      : undefined;
    if (degraded) {
      console.log(
        `[router] ⚠️ Degraded: ${degradeReason} — floor=${floorTier}`,
      );
    }

    const model = selectRoutedModel(
      this.config,
      normalizedRequest.model,
      account.provider,
      tier,
      contentUpgraded,
      intent.preferredModels,
    );
    this.lastRouteExplanation = {
      lane: intent.lane,
      task_kind: intent.taskKind,
      role: intent.role,
      risk_level: intent.riskLevel,
      requested_model: request.model,
      normalized_model: normalizedRequest.model,
      selected_provider: account.provider,
      selected_model: model,
      tier,
      floor_tier: floorTier,
      degraded,
      degrade_reason: degradeReason ?? null,
      provider_band: [...providerBand],
      preferred_models: intent.preferredModels,
      provider_priority: policyProviderPriority,
      excluded_providers: [...effectiveExclude],
      explanation: intent.explanation,
    };
    return { account, model, tier, floorTier, degraded, degradeReason };
  }

  private getAccountEffectiveTier(
    account: ProviderAccount,
    requestedTier: Tier,
  ): Tier {
    const tiers = this.config.routing.tiers;
    const requestedIdx = FALLBACK_ORDER.indexOf(requestedTier);
    // Search from the requested tier downward — a provider in both fast and budget
    // should report the tier closest to the request, not the highest tier it appears in
    for (let i = requestedIdx; i < FALLBACK_ORDER.length; i++) {
      const t = FALLBACK_ORDER[i]!;
      if (tiers[t]?.includes(account.provider)) return t;
    }
    // Also check tiers above (upward fallback case)
    for (let i = requestedIdx - 1; i >= 0; i--) {
      const t = FALLBACK_ORDER[i]!;
      if (tiers[t]?.includes(account.provider)) return t;
    }
    return "last_resort";
  }

  // Legacy compat
  getNextAccount(request: MessagesRequest): ProviderAccount | null {
    const route = this.getNextRoute(request);
    return route?.account ?? null;
  }

  getAccountForTier(
    tier: Tier,
    excludeIds?: Set<string>,
    excludeProviders?: Set<string>,
    preferredProvider?: Provider,
    floorTier?: Tier,
    policyProviders?: Provider[],
  ): { account: ProviderAccount; selectedTier: Tier } | null {
    // Provider affinity: when a model explicitly belongs to a provider, try it first.
    if (preferredProvider && !excludeProviders?.has(preferredProvider)) {
      const available = this.tokenManager
        .getHealthyAccounts(preferredProvider)
        .filter((a) => !excludeIds?.has(a.id));
      if (available.length > 0) {
        const rrKey = `${tier}-${preferredProvider}`;
        console.log(
          `[router] Provider affinity: ${preferredProvider} for tier=${tier}`,
        );
        return {
          account: this.pickLeastUsed(available, rrKey),
          selectedTier: tier,
        };
      }
      console.log(
        `[router] Provider affinity: ${preferredProvider} exhausted, using tier priorities`,
      );
    }

    const tierProviders = this.getTierProviderOrder(tier, policyProviders);

    // Try each provider in the tier's priority order
    for (const providerName of tierProviders) {
      if (excludeProviders?.has(providerName)) continue;
      const available = this.tokenManager
        .getHealthyAccounts(providerName as Provider)
        .filter((a) => !excludeIds?.has(a.id));
      if (available.length === 0) continue;

      const rrKey = `${tier}-${providerName}`;
      return {
        account: this.pickLeastUsed(available, rrKey),
        selectedTier: tier,
      };
    }

    // Cross-tier fallback — respect quality floor
    const floorIdx = floorTier
      ? FALLBACK_ORDER.indexOf(floorTier)
      : FALLBACK_ORDER.length - 1;
    const currentIdx = FALLBACK_ORDER.indexOf(tier);

    // Try tiers below current, but stop at floor
    for (let i = currentIdx + 1; i < FALLBACK_ORDER.length; i++) {
      const fallbackTier = FALLBACK_ORDER[i]!;

      // Quality floor: don't go below the floor tier
      if (i > floorIdx) {
        console.log(
          `[router] 🛑 Quality floor hit: won't degrade below ${floorTier} (requested=${tier}, would-be=${fallbackTier})`,
        );
        break;
      }

      const fallbackProviders = this.getTierProviderOrder(
        fallbackTier,
        policyProviders,
      );
      for (const providerName of fallbackProviders) {
        if (excludeProviders?.has(providerName)) continue;
        const available = this.tokenManager
          .getHealthyAccounts(providerName as Provider)
          .filter((a) => !excludeIds?.has(a.id));
        if (available.length > 0) {
          const rrKey = `${fallbackTier}-${providerName}`;
          console.log(
            `[router] Falling back from ${tier} to ${fallbackTier} (${providerName})`,
          );
          return {
            account: this.pickLeastUsed(available, rrKey),
            selectedTier: fallbackTier,
          };
        }
      }
    }

    // Also try tiers above (premium request falling back to... shouldn't happen, but be safe)
    for (let i = currentIdx - 1; i >= 0; i--) {
      const fallbackTier = FALLBACK_ORDER[i]!;
      const fallbackProviders = this.getTierProviderOrder(
        fallbackTier,
        policyProviders,
      );
      for (const providerName of fallbackProviders) {
        if (excludeProviders?.has(providerName)) continue;
        const available = this.tokenManager
          .getHealthyAccounts(providerName as Provider)
          .filter((a) => !excludeIds?.has(a.id));
        if (available.length > 0) {
          const rrKey = `${fallbackTier}-${providerName}`;
          console.log(
            `[router] Falling back from ${tier} up to ${fallbackTier} (${providerName})`,
          );
          return {
            account: this.pickLeastUsed(available, rrKey),
            selectedTier: fallbackTier,
          };
        }
      }
    }

    return null;
  }

  getAvailableModels(): { provider: string; model: string; tier: Tier }[] {
    const models: { provider: string; model: string; tier: Tier }[] = [];
    const allAccounts = this.tokenManager.getAllAccounts();
    const seenProviders = new Set<string>();

    for (const account of allAccounts) {
      if (account.health === "dead" || account.health === "expired") continue;
      if (seenProviders.has(account.provider)) continue;
      seenProviders.add(account.provider);

      for (const model of getProviderModels(account.provider)) {
        models.push({ provider: account.provider, model, tier: account.tier });
      }
    }

    return models;
  }
}

/**
 * Scored content complexity analysis.
 * Analyzes last 2 user messages for weighted signals, returns mode + score + reasons.
 * "deep" tasks get routed to premium models. Scoring prevents false negatives
 * from narrow keyword matching.
 */
function analyzeContentComplexity(messages: Message[]): {
  mode: "rush" | "smart" | "deep";
  score: number;
  reasons: string[];
} {
  // Collect text from last 2 user messages for multi-turn context
  const userMessages = messages.filter((m) => m.role === "user");
  const recentUsers = userMessages.slice(-2);
  if (recentUsers.length === 0)
    return { mode: "smart", score: 0, reasons: ["no user messages"] };

  const extractText = (msg: Message): string =>
    typeof msg.content === "string"
      ? msg.content
      : Array.isArray(msg.content)
        ? msg.content
            .map((b) =>
              "text" in b && typeof b.text === "string" ? b.text : "",
            )
            .join(" ")
        : "";

  const lastText = extractText(recentUsers[recentUsers.length - 1]!);
  const combinedText = recentUsers.map(extractText).join("\n");
  const preview = lastText.slice(0, 50);

  let score = 0;
  const reasons: string[] = [];

  // --- Deep signals (high weight) ---

  // Planning & architecture (+30 each)
  const planningPatterns: [RegExp, string][] = [
    [/\b(architect|architecture)\b/i, "architecture"],
    [
      /\b(plan|proposal|rfc)\b.*\b(implement|feature|system|design)\b/i,
      "planning",
    ],
    [/\bdesign\s+(system|pattern|approach)\b/i, "design"],
    [/\b(migration|migrate)\s+(strategy|plan)?\b/i, "migration"],
    [/\b(trade-?off|decision|rollout|rollback)\b/i, "tradeoff"],
    [/\bbreaking\s+change/i, "breaking-change"],
  ];
  for (const [pattern, label] of planningPatterns) {
    if (pattern.test(combinedText)) {
      score += 30;
      reasons.push(label);
    }
  }

  // Deep coding & debugging (+25 each)
  const deepCodingPatterns: [RegExp, string][] = [
    [/\b(refactor|rewrite|restructure)\b/i, "refactor"],
    [
      /\b(race\s+condition|deadlock|heisenbug|nondeterministic)\b/i,
      "concurrency-bug",
    ],
    [/\b(memory\s+leak|corruption|segfault)\b/i, "deep-debug"],
    [/\bperformance\s*(optim|bottleneck|profil)/i, "perf-optimization"],
    [/\bsecurity\s*(audit|review|vulnerabilit)/i, "security"],
    [/\b(deprecate|backwards?\s*compat)/i, "deprecation"],
    [/\bmulti-?file\b|\bacross\s+(the\s+)?codebase\b/i, "multi-file"],
  ];
  for (const [pattern, label] of deepCodingPatterns) {
    if (pattern.test(combinedText)) {
      score += 25;
      reasons.push(label);
    }
  }

  // Code review & testing strategy (+25 each)
  const reviewPatterns: [RegExp, string][] = [
    [/\bcode\s+review\b/i, "code-review"],
    [/\btest\s+(strategy|plan|coverage|suite)\b/i, "test-strategy"],
    [/\b(edge\s+cases?|invariants?|regression)\b/i, "edge-cases"],
    [/\b(audit|compliance)\b/i, "audit"],
    [
      /\b(implement|build|create)\s+(the\s+)?(entire|full|complete)\b/i,
      "full-implementation",
    ],
  ];
  for (const [pattern, label] of reviewPatterns) {
    if (pattern.test(combinedText)) {
      score += 25;
      reasons.push(label);
    }
  }

  // Structural complexity signals (+15 each)
  const codeBlockCount = Math.floor(
    (combinedText.match(/```/g) ?? []).length / 2,
  );
  if (codeBlockCount >= 3) {
    score += 15;
    reasons.push(`${codeBlockCount} code blocks`);
  } else if (codeBlockCount >= 2) {
    score += 10;
    reasons.push(`${codeBlockCount} code blocks`);
  }

  // Multiple file paths mentioned → multi-file task
  const filePathMatches = combinedText.match(
    /\b\w+\.(ts|js|py|go|rs|java|yaml|json|tsx|jsx|vue|svelte)\b/g,
  );
  if (filePathMatches && new Set(filePathMatches).size >= 3) {
    score += 15;
    reasons.push(`${new Set(filePathMatches).size} file references`);
  }

  // Stack traces or diffs
  if (/diff --git|@@\s*[-+]\d/.test(combinedText)) {
    score += 10;
    reasons.push("diff/patch");
  }
  if (/^\s+at\s+\S+\s+\(/m.test(combinedText)) {
    score += 10;
    reasons.push("stack-trace");
  }

  // Long message → depth signal (but NOT the sole factor)
  if (lastText.length > 3000) {
    score += 15;
    reasons.push(`long (${lastText.length} chars)`);
  } else if (lastText.length > 1500) {
    score += 8;
    reasons.push(`medium-long (${lastText.length} chars)`);
  }

  // Tool calls in history suggest multi-step workflow
  const toolUseCount = messages.filter(
    (m) =>
      Array.isArray(m.content) && m.content.some((b) => b.type === "tool_use"),
  ).length;
  if (toolUseCount >= 5) {
    score += 10;
    reasons.push(`${toolUseCount} tool calls in history`);
  }

  // --- Determine mode ---

  if (score >= 40) {
    console.log(
      `[router] Content analysis: "${preview}" → deep (score=${score}, reasons: ${reasons.join(", ")})`,
    );
    return { mode: "deep", score, reasons };
  }

  // Rush: only if short AND no complexity signals AND matches simple Q&A pattern
  const rushPatterns = [
    /^(what is|what's|explain|how to|how do|list|show me|translate|convert)\b/i,
  ];
  if (
    score === 0 &&
    lastText.length < 80 &&
    rushPatterns.some((p) => p.test(lastText.trim()))
  ) {
    console.log(
      `[router] Content analysis: "${preview}" → rush (score=0, short simple question)`,
    );
    return { mode: "rush", score: 0, reasons: ["short simple question"] };
  }

  console.log(
    `[router] Content analysis: "${preview}" → smart (score=${score})`,
  );
  return {
    mode: "smart",
    score,
    reasons: reasons.length > 0 ? reasons : ["default"],
  };
}

/**
 * Detect the native provider for a model name.
 * Only returns affinity for providers that aren't already top-priority in most tiers
 * (claude/antigravity/codex don't need affinity — they're already first in line).
 * Returns null for unknown models or high-priority providers.
 */
function detectNativeProvider(model: string): Provider | undefined {
  const lower = model.toLowerCase();
  if (lower.startsWith("claude-")) return "claude";
  if (lower.startsWith("gpt-")) return "codex";
  if (lower.startsWith("glm-")) return "glm";
  if (lower.startsWith("gemini-")) return "gemini";
  if (lower.startsWith("kimi-")) return "kimi-api";
  if (lower.startsWith("minimax")) return "minimax";
  return undefined;
}

/**
 * Translate a requested model to the equivalent model for a given provider.
 * e.g., "claude-sonnet-4-6" on codex → "gpt-5.2-codex"
 *
 * When contentUpgraded=true (content analysis upgraded the tier), the model
 * is force-translated to the tier's default even if it's native to the provider.
 * Without this, "claude-sonnet-4-6" on antigravity with tier=premium would
 * pass through as sonnet instead of being upgraded to opus.
 */
function translateModel(
  requestedModel: string,
  targetProvider: Provider,
  tier: Tier,
  contentUpgraded?: boolean,
): string {
  const model = requestedModel.toLowerCase();

  // If the model already belongs to the target provider, use as-is
  // UNLESS content analysis upgraded the tier — then force the tier's default model
  if (isNativeModel(model, targetProvider) && !contentUpgraded)
    return requestedModel;

  // Cross-provider model mapping by tier
  const tierMap: Record<Tier, Record<Provider, string>> = {
    premium: {
      claude: "claude-opus-4-6",
      antigravity: "claude-opus-4-6",
      codex: "gpt-5.4",
      gemini: "gemini-3.1-pro-high",
      kimi: "kimi-k2.5",
      "kimi-api": "kimi-k2.5",
      glm: "glm-5",
      minimax: "MiniMax-M2.5",
      openrouter: "anthropic/claude-opus-4-6",
    },
    standard: {
      claude: "claude-sonnet-4-6",
      antigravity: "claude-sonnet-4-6",
      codex: "gpt-5.2-codex",
      gemini: "gemini-3.1-pro-low",
      kimi: "kimi-k2.5",
      "kimi-api": "kimi-k2.5",
      glm: "glm-4.7",
      minimax: "MiniMax-M2.5",
      openrouter: "anthropic/claude-sonnet-4-6",
    },
    fast: {
      claude: "claude-haiku-4-5-20251001",
      antigravity: "claude-haiku-4-5-20251001",
      codex: "gpt-5.1-codex-mini",
      gemini: "gemini-2.5-flash",
      kimi: "kimi-k2",
      "kimi-api": "kimi-k2",
      glm: "glm-4.7-flashx",
      minimax: "MiniMax-M2.5-highspeed",
      openrouter: "google/gemini-2.5-flash",
    },
    budget: {
      claude: "claude-haiku-4-5-20251001",
      antigravity: "claude-haiku-4-5-20251001",
      codex: "gpt-5.1-codex-mini",
      gemini: "gemini-2.5-flash-lite",
      kimi: "kimi-k2",
      "kimi-api": "kimi-k2",
      glm: "glm-4.7-flash",
      minimax: "MiniMax-M2.1-highspeed",
      openrouter: "google/gemini-2.5-flash",
    },
    last_resort: {
      claude: "claude-sonnet-4-6",
      antigravity: "claude-sonnet-4-6",
      codex: "gpt-5.1-codex",
      gemini: "gemini-2.5-pro",
      kimi: "kimi-k2.5",
      "kimi-api": "kimi-k2.5",
      glm: "glm-4.7",
      minimax: "MiniMax-M2.5",
      openrouter: "anthropic/claude-sonnet-4-6",
    },
  };

  const translated = tierMap[tier]?.[targetProvider] ?? requestedModel;
  if (translated !== requestedModel) {
    console.log(
      `[router] Translated ${requestedModel} → ${translated} for ${targetProvider} (tier=${tier})`,
    );
  }
  return translated;
}

function selectRoutedModel(
  config: Config,
  requestedModel: string,
  targetProvider: Provider,
  tier: Tier,
  contentUpgraded: boolean,
  preferredModels: string[],
): string {
  for (const preferredModel of preferredModels) {
    const nativeProvider = modelToProvider(preferredModel);
    if (nativeProvider === targetProvider) {
      return preferredModel;
    }
  }

  const preferredHead = preferredModels[0];
  if (preferredHead) {
    return translateModel(
      canonicalizeModel(config, preferredHead),
      targetProvider,
      tier,
      true,
    );
  }

  return translateModel(requestedModel, targetProvider, tier, contentUpgraded);
}

function isNativeModel(model: string, provider: Provider): boolean {
  // Strict check: only pass through models we explicitly know about
  // This prevents old/unknown model IDs (e.g. claude-opus-4-0520) from
  // being sent untranslated to providers that don't support them
  const knownModels = getProviderModels(provider);
  const lower = model.toLowerCase();
  return knownModels.some((known) => lower === known.toLowerCase());
}

function getProviderModels(provider: Provider): string[] {
  switch (provider) {
    case "claude":
    case "antigravity":
      return [
        "claude-opus-4-6",
        "claude-opus-4-5-20251101",
        "claude-opus-4-1-20250805",
        "claude-sonnet-4-6",
        "claude-sonnet-4-5-20250929",
        "claude-sonnet-4-20250514",
        "claude-haiku-4-5-20251001",
      ];
    case "codex":
      return [
        "gpt-5.4",
        "gpt-5.3-codex",
        "gpt-5.3-codex-spark",
        "gpt-5.2-codex",
        "gpt-5.2",
        "gpt-5.1-codex-max",
        "gpt-5.1-codex",
        "gpt-5.1-codex-mini",
        "gpt-5.1",
        "gpt-5-codex",
        "gpt-5-codex-mini",
        "gpt-5",
      ];
    case "gemini":
      return [
        "gemini-3.1-pro-high",
        "gemini-3.1-pro-low",
        "gemini-3-pro-preview",
        "gemini-3-flash-preview",
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite",
      ];
    case "kimi":
    case "kimi-api":
      return ["kimi-k2.5", "kimi-k2", "kimi-for-coding"];
    case "glm":
      return [
        "glm-5",
        "glm-4.7",
        "glm-4.7-flashx",
        "glm-4.7-flash",
        "glm-4.5",
        "glm-4.5-x",
        "glm-4.5-air",
        "glm-4.5-airx",
        "glm-4.5-flash",
      ];
    case "minimax":
      return [
        "MiniMax-M2.5",
        "MiniMax-M2.5-highspeed",
        "MiniMax-M2.1",
        "MiniMax-M2.1-highspeed",
      ];
    case "openrouter":
      return [
        "anthropic/claude-opus-4-6",
        "anthropic/claude-sonnet-4-6",
        "google/gemini-3-pro-preview",
        "google/gemini-2.5-pro",
        "openai/gpt-5.4",
        "openai/gpt-5.3-codex",
      ];
    default:
      return [];
  }
}
