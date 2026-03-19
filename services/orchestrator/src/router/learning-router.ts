/**
 * LearningRouter — closes the feedback loop between usage data and routing decisions.
 *
 * Reads the last 24h of request data from UsageDB every hour, computes per-provider
 * performance scores (success rate, latency, cost efficiency), and reorders the
 * tier priority lists in config.yaml. The orchestrator hot-reloads config, so
 * routing updates take effect without restart.
 *
 * This is the ONE component that was missing: the system collects all the data
 * needed to make smart routing decisions, but never fed it back into the router.
 */

import { parse as parseYaml, stringify as stringifyYaml } from "yaml";
import { renameSync } from "fs";
import { homedir } from "os";
import { join } from "path";
import { EventEmitter } from "events";
import type { Config, Provider, Tier } from "../types.ts";
import { getProviderBand } from "./routing-policy.ts";
import type { UsageDB } from "../db/usage.ts";
import type { TokenManager } from "../auth/token-manager.ts";

interface ProviderScore {
  provider: string;
  successRate: number; // 0-1
  avgLatencyMs: number;
  p95LatencyMs: number;
  totalTokens: number;
  requestCount: number;
  errorCount: number;
  healthyAccounts: number; // from token manager
  compositeScore: number; // weighted combination
}

interface LearningState {
  lastRun: number;
  scores: ProviderScore[];
  tierUpdates: Record<string, string[]>;
  guardrailHits: string[];
}

// Minimum requests per provider before we trust the data enough to reorder
const MIN_REQUESTS_FOR_LEARNING = 10;
// Minimum providers a tier must retain — prevents single-provider tiers
const MIN_PROVIDERS_PER_TIER = 2;
// Quarantine guardrails to avoid overreacting to transient auth/token incidents
const QUARANTINE_MIN_REQUESTS = 80;
const QUARANTINE_MAX_SUCCESS_RATE = 0.2;
const QUARANTINE_MAX_HEALTHY_ACCOUNTS = 0;

// How much we weight each factor (must sum to 1.0)
const WEIGHTS = {
  successRate: 0.3, // reliability matters but shouldn't dominate
  latency: 0.15, // faster is better
  availability: 0.15, // healthy accounts matter
  costEfficiency: 0.2, // prefer cheap providers to preserve Claude allowance
  loadBalance: 0.2, // penalize over-used providers to spread load
};

// Estimated cost per 1M tokens (input+output combined) for each provider
const COST_PER_MTOK: Record<string, number> = {
  claude: 12.0,
  antigravity: 12.0,
  codex: 10.0,
  gemini: 5.0,
  glm: 1.5, // mostly free tier
  minimax: 2.0,
  kimi: 3.0,
  openrouter: 15.0, // premium for last resort
};

const CONFIG_PATH = join(homedir(), ".claudemax", "config.yaml");

export class LearningRouter extends EventEmitter {
  private config: Config;
  private usageDB: UsageDB;
  private tokenManager: TokenManager;
  private timer: ReturnType<typeof setInterval> | null = null;
  private startTimeout: ReturnType<typeof setTimeout> | null = null;
  private state: LearningState | null = null;
  private learnInProgress = false;
  private lastGuardrailHits: string[] = [];

  // Interval in ms — default 1 hour
  private readonly LEARNING_INTERVAL = 60 * 60 * 1000;
  private readonly START_DELAY_MS = 30_000;
  private readonly START_JITTER_MS = 5_000;
  private readonly MAX_CYCLE_MS = 7_500;
  // Lookback window for scoring — 24 hours
  private readonly LOOKBACK_MS = 24 * 3600_000;

  constructor(config: Config, usageDB: UsageDB, tokenManager: TokenManager) {
    super();
    this.config = config;
    this.usageDB = usageDB;
    this.tokenManager = tokenManager;
  }

  updateConfig(config: Config): void {
    this.config = config;
  }

  private isEnabled(): boolean {
    return this.config.learning?.enabled !== false;
  }

  start(): void {
    if (!this.isEnabled()) {
      console.log(
        "[learning-router] Disabled via config.learning.enabled=false",
      );
      return;
    }

    const intervalMs =
      this.config.learning?.interval_ms ?? this.LEARNING_INTERVAL;
    const baseStartDelay =
      this.config.learning?.start_delay_ms ?? this.START_DELAY_MS;
    const startDelay = baseStartDelay + Math.floor(Math.random() * this.START_JITTER_MS);

    // First run after a delayed, jittered startup window (to avoid synchronized
    // maintenance bursts with refresh/rescan tasks), then every interval.
    this.startTimeout = setTimeout(
      () => {
        this.startTimeout = null;
        this.learn();
        // Then every interval
        this.timer = setInterval(() => this.learn(), intervalMs);
      },
      startDelay,
    );

    console.log(
      `[learning-router] Started — first analysis in ${Math.round(startDelay / 1000)}s, then every ${Math.round(intervalMs / 1000)}s`,
    );
  }

  stop(): void {
    if (this.startTimeout) {
      clearTimeout(this.startTimeout);
      this.startTimeout = null;
    }
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  getState(): LearningState | null {
    return this.state;
  }

  /**
   * Core learning loop: score providers, reorder tiers, write config.
   */
  private async learn(): Promise<void> {
    if (this.learnInProgress) return;

    // Skip learning while token maintenance is active to avoid event-loop contention
    // and scoring against partially refreshed account state.
    const maintenance = this.tokenManager.getMaintenanceState();
    if (maintenance.refresh_in_progress || maintenance.rescan_in_progress) {
      console.log(
        "[learning-router] Skipping cycle: token maintenance in progress",
      );
      return;
    }

    this.learnInProgress = true;
    const cycleStart = Date.now();
    const maxCycleMs = this.config.learning?.max_cycle_ms ?? this.MAX_CYCLE_MS;

    const exceededBudget = (phase: string): boolean => {
      const elapsed = Date.now() - cycleStart;
      if (elapsed <= maxCycleMs) return false;
      console.warn(
        `[learning-router] Skipping cycle after ${elapsed}ms (budget ${maxCycleMs}ms) during ${phase}`,
      );
      return true;
    };

    try {
      // Reload config from disk to avoid overwriting manual edits.
      // The in-memory this.config may be stale if someone edited config.yaml
      // between learning cycles.
      try {
        const file = Bun.file(CONFIG_PATH);
        if (await file.exists()) {
          const raw = await file.text();
          const diskConfig = parseYaml(raw) as Record<string, unknown>;
          const diskTiers = (diskConfig.routing as Record<string, unknown>)?.tiers as Record<string, string[]> | undefined;
          if (diskTiers) {
            this.config = {
              ...this.config,
              routing: { ...this.config.routing, tiers: diskTiers as Config["routing"]["tiers"] },
            };
          }
        }
      } catch (e) {
        console.warn("[learning-router] Failed to reload config from disk, using in-memory:", e);
      }
      if (exceededBudget("config_reload")) return;

      const scores = this.scoreProviders();
      if (exceededBudget("score_providers")) return;

      if (scores.length === 0) {
        console.log("[learning-router] No usage data yet — skipping");
        return;
      }

      // Only reorder if we have enough data
      const minRequests =
        this.config.learning?.min_requests_for_reorder ??
        MIN_REQUESTS_FOR_LEARNING;
      const sufficientData = scores.filter(
        (s) => s.requestCount >= minRequests,
      );

      if (sufficientData.length < 2) {
        console.log(
          `[learning-router] Only ${sufficientData.length} provider(s) with ≥${minRequests} requests — need ≥2 to reorder`,
        );
        return;
      }

      const tierUpdates = this.computeTierPriorities(scores);
      if (exceededBudget("compute_tiers")) return;
      const changed = this.hasChanged(tierUpdates);

      this.state = {
        lastRun: Date.now(),
        scores,
        tierUpdates,
        guardrailHits: this.lastGuardrailHits,
      };

      if (changed) {
        if (exceededBudget("pre_write")) return;
        await this.writeConfig(tierUpdates);
        if (exceededBudget("write_config")) return;
        this.logScoreSummary(scores, tierUpdates);
      } else {
        console.log("[learning-router] No priority changes needed");
      }
    } catch (e) {
      console.error("[learning-router] Error during learning cycle:", e);
    } finally {
      this.learnInProgress = false;
    }
  }

  /**
   * Score each provider based on recent performance data + account health.
   */
  private scoreProviders(): ProviderScore[] {
    const since = Date.now() - this.LOOKBACK_MS;
    const rawScores = this.usageDB.getProviderScores(since);

    if (rawScores.length === 0) return [];

    // Get healthy account counts from token manager
    const accounts = this.tokenManager.getAllAccounts();
    const healthyByProvider = new Map<string, number>();
    const now = Date.now();
    for (const acct of accounts) {
      if (acct.health === "healthy" && !acct.disabled) {
        // M8: accounts under rate-limit or circuit-open are not truly available
        const isRateLimited =
          acct.rateLimitedUntil != null && acct.rateLimitedUntil > now;
        const isCircuitOpen =
          acct.circuitOpenUntil != null && acct.circuitOpenUntil > now;
        if (!isRateLimited && !isCircuitOpen) {
          healthyByProvider.set(
            acct.provider,
            (healthyByProvider.get(acct.provider) ?? 0) + 1,
          );
        }
      }
    }

    // Normalize metrics across providers for fair comparison
    const maxLatency = Math.max(...rawScores.map((s) => s.p95_latency_ms), 1);
    const maxAccounts = Math.max(...[...healthyByProvider.values()], 1);
    const totalRequests = rawScores.reduce((sum, s) => sum + s.request_count, 1);

    return rawScores.map((raw) => {
      const healthyAccounts = healthyByProvider.get(raw.provider) ?? 0;
      const costPerMTok = COST_PER_MTOK[raw.provider] ?? 10.0;

      // Normalize each dimension to 0-1 (higher = better)
      const successScore = raw.success_rate; // already 0-1
      const latencyScore = 1 - Math.min(raw.p95_latency_ms / maxLatency, 1);
      const availabilityScore = Math.min(healthyAccounts / maxAccounts, 1);
      const costScore = 1 - Math.min(costPerMTok / 20.0, 1); // 20.0 = worst case
      // Load balance: penalize providers handling disproportionate traffic share
      // A provider handling 90% of requests gets loadScore ≈ 0.1 (heavily penalized)
      const trafficShare = raw.request_count / totalRequests;
      const loadScore = 1 - trafficShare;

      const compositeScore =
        WEIGHTS.successRate * successScore +
        WEIGHTS.latency * latencyScore +
        WEIGHTS.availability * availabilityScore +
        WEIGHTS.costEfficiency * costScore +
        WEIGHTS.loadBalance * loadScore;

      const successRate = raw.success_rate;
      const requestCount = raw.request_count;

      // C2: hard quarantine — provider is excluded from all routing.
      // Guarded to avoid transient auth/token incidents causing full quarantine.
      let finalScore = Math.round(compositeScore * 1000) / 1000;
      const shouldQuarantine =
        successRate <= QUARANTINE_MAX_SUCCESS_RATE &&
        requestCount >= QUARANTINE_MIN_REQUESTS &&
        healthyAccounts <= QUARANTINE_MAX_HEALTHY_ACCOUNTS;
      if (shouldQuarantine) {
        finalScore = 0;
        this.emit("provider_quarantined", {
          provider: raw.provider,
          successRate,
          requestCount,
          healthyAccounts,
        });
      }

      return {
        provider: raw.provider,
        successRate,
        avgLatencyMs: raw.avg_latency_ms,
        p95LatencyMs: raw.p95_latency_ms,
        totalTokens: raw.total_tokens,
        requestCount,
        errorCount: raw.error_count,
        healthyAccounts,
        compositeScore: finalScore,
      };
    });
  }

  /**
   * Compute new tier priorities based on provider scores.
   * Preserves structural constraints:
   * - Each tier only includes providers that are configured for it
   * - Providers with insufficient data keep their current position
   * - OpenRouter always stays last in last_resort
   */
  private computeTierPriorities(
    scores: ProviderScore[],
  ): Record<string, string[]> {
    const scoreMap = new Map(scores.map((s) => [s.provider, s]));
    const currentTiers = this.config.routing.tiers;
    const result: Record<string, string[]> = {};
    const guardrailHits: string[] = [];

    for (const [tier, providers] of Object.entries(currentTiers)) {
      // last_resort is always openrouter — don't reorder
      if (tier === "last_resort") {
        result[tier] = [...providers];
        continue;
      }

      // Split into scored (enough data) and unscored (keep position)
      const scored: { provider: string; score: number }[] = [];
      const unscored: { provider: string; originalIdx: number }[] = [];
      const quarantined: { provider: string; originalIdx: number }[] = [];

      for (let i = 0; i < providers.length; i++) {
        const p = providers[i]!;
        const s = scoreMap.get(p);
        const minRequests =
          this.config.learning?.min_requests_for_reorder ??
          MIN_REQUESTS_FOR_LEARNING;
        // C2: track quarantined providers separately for min-provider guard
        if (s && s.compositeScore === 0) {
          quarantined.push({ provider: p, originalIdx: i });
          continue;
        }
        if (s && s.requestCount >= minRequests) {
          scored.push({ provider: p, score: s.compositeScore });
        } else {
          unscored.push({ provider: p, originalIdx: i });
        }
      }

      // Min-provider guard: if removing quarantined providers would leave
      // fewer than MIN_PROVIDERS_PER_TIER, keep the least-bad quarantined
      // providers at the END of the list instead of dropping them entirely
      const activeCount = scored.length + unscored.length;
      if (activeCount < MIN_PROVIDERS_PER_TIER && quarantined.length > 0) {
        const needed = MIN_PROVIDERS_PER_TIER - activeCount;
        const rescued = quarantined.splice(0, needed);
        for (const q of rescued) {
          console.warn(
            `[learning-router] ⚠️ Keeping quarantined provider ${q.provider} in ${tier} (min-provider guard: ${activeCount} active < ${MIN_PROVIDERS_PER_TIER})`,
          );
          // Add with score -1 so they sort to the end
          scored.push({ provider: q.provider, score: -1 });
        }
      }

      // Sort scored providers by composite score (descending = best first)
      scored.sort((a, b) => b.score - a.score);

      // Allowance preservation: never let Claude/antigravity take slot #1
      // in premium/standard tiers — they burn Anthropic allowance.
      // Push them after the first non-Anthropic provider.
      const ANTHROPIC_PROVIDERS = new Set(["claude", "antigravity"]);
      if (
        tier === "standard" &&
        scored.length >= 2 &&
        ANTHROPIC_PROVIDERS.has(scored[0]?.provider ?? "")
      ) {
        const firstNonAnthropic = scored.findIndex(
          (s) => !ANTHROPIC_PROVIDERS.has(s.provider),
        );
        if (firstNonAnthropic > 0) {
          const [nonA] = scored.splice(firstNonAnthropic, 1);
          scored.unshift(nonA!);
          console.log(
            `[learning-router] 🛡️ Allowance guard: moved ${nonA!.provider} ahead of claude/antigravity in ${tier}`,
          );
        }
      }

      // Merge: scored providers fill top slots, unscored keep relative position
      // Total slots = unscored (at original positions) + scored (filling gaps)
      const totalSlots = Math.max(
        providers.length,
        scored.length + unscored.length,
      );
      const merged: string[] = new Array(totalSlots);
      const unscoredSlots: number[] = [];

      // First, place unscored at their original positions
      for (const u of unscored) {
        if (u.originalIdx < totalSlots) {
          merged[u.originalIdx] = u.provider;
        }
      }

      // Collect empty slots for scored providers
      for (let i = 0; i < totalSlots; i++) {
        if (!merged[i]) unscoredSlots.push(i);
      }

      // Place scored providers in empty slots (best score gets lowest index)
      for (let i = 0; i < scored.length; i++) {
        merged[unscoredSlots[i]!] = scored[i]!.provider;
      }

      const configSet = new Set(providers);
      // C1: drop any provider that wasn't in the original config tier
      let filtered = [...new Set(merged.filter((p) => Boolean(p) && configSet.has(p)))];

      const guardrails = this.config.learning?.guardrails;
      const budgetishProviders = new Set(["glm", "minimax", "kimi", "kimi-api"]);
      const premiumBand = new Set(getProviderBand(this.config, "interactive_premium"));
      const premiumPolicyOrder = getProviderBand(this.config, "interactive_premium")
        .filter((provider) => !budgetishProviders.has(provider));
      const healthyProviders = new Set(
        scores
          .filter((score) => score.healthyAccounts > 0 && score.compositeScore > 0)
          .map((score) => score.provider as Provider),
      );
      const designAllowlist = new Set(guardrails?.design_head_allowlist ?? ["gemini", "claude", "codex"]);

      if (tier === "premium") {
        const rescuedProviders = premiumPolicyOrder.filter(
          (provider) => healthyProviders.has(provider) && !filtered.includes(provider),
        );
        if (rescuedProviders.length > 0) {
          filtered = [...filtered, ...rescuedProviders];
          guardrailHits.push(`premium:rescued ${rescuedProviders.join(",")}`);
          console.log(
            `[learning-router] 🛡️ Premium rescue: restored ${rescuedProviders.join(", ")} to premium tier`,
          );
        }
        filtered = premiumPolicyOrder.filter((provider) => filtered.includes(provider));
      }

      if (tier === "premium" && filtered.length > 0) {
        const removedProviders = filtered.filter(
          (provider) => !premiumBand.has(provider as Provider) || budgetishProviders.has(provider),
        );
        if (removedProviders.length > 0) {
          filtered = filtered.filter(
            (provider) => premiumBand.has(provider as Provider) && !budgetishProviders.has(provider),
          );
          guardrailHits.push(`premium:removed ${removedProviders.join(",")}`);
          console.log(
            `[learning-router] 🛡️ Premium band guard: removed ${removedProviders.join(", ")} from premium tier`,
          );
        }
      }

      if ((tier === "premium" || tier === "standard") && filtered.length > 1) {
        const currentHead = filtered[0];
        const mustProtectHead = tier === "premium" || guardrails?.forbid_budget_provider_heading_premium;
        if (mustProtectHead && currentHead && (!premiumBand.has(currentHead as Provider) || budgetishProviders.has(currentHead))) {
          const replacementIndex = filtered.findIndex((provider) => premiumBand.has(provider as Provider) && !budgetishProviders.has(provider));
          if (replacementIndex > 0) {
            const [replacement] = filtered.splice(replacementIndex, 1);
            filtered.unshift(replacement!);
            guardrailHits.push(`${tier}:promoted premium-safe head ${replacement}`);
            console.log(`[learning-router] 🛡️ Premium head guard: moved ${replacement} ahead of ${currentHead} in ${tier}`);
          }
        }
      }

      if (tier === "premium" && filtered.length > 0 && guardrails?.forbid_cross_band_promotion !== false) {
        filtered = filtered.filter((provider) => premiumBand.has(provider as Provider) && !budgetishProviders.has(provider));
      }

      if (filtered.length > 0 && !designAllowlist.has(filtered[0] as Provider) && guardrails?.forbid_non_gemini_heading_design_lane) {
        guardrailHits.push(`design-head:${filtered[0]} blocked`);
      }

      // Safety guard: never allow a non-empty tier to become empty.
      // Empty tiers cause 529 errors for all requests in that tier.
      if (filtered.length === 0 && providers.length > 0) {
        console.warn(
          `[learning-router] ⚠️ Tier ${tier} would become empty — preserving original: ${providers.join(", ")}`,
        );
        result[tier] = [...providers];
      } else {
        result[tier] = filtered;
      }
    }

    this.lastGuardrailHits = guardrailHits;
    return result;
  }

  /**
   * Check if computed priorities differ from current config.
   */
  private hasChanged(tierUpdates: Record<string, string[]>): boolean {
    const current = this.config.routing.tiers;
    for (const [tier, providers] of Object.entries(tierUpdates)) {
      const currentProviders = current[tier as Tier];
      if (!currentProviders) continue;
      if (providers.length !== currentProviders.length) return true;
      for (let i = 0; i < providers.length; i++) {
        if (providers[i] !== currentProviders[i]) return true;
      }
    }
    return false;
  }

  /**
   * Write updated tier priorities to config.yaml.
   * Only touches `routing.tiers` — preserves all other config.
   */
  private async writeConfig(
    tierUpdates: Record<string, string[]>,
  ): Promise<void> {
    try {
      const file = Bun.file(CONFIG_PATH);
      if (!(await file.exists())) {
        console.warn(
          "[learning-router] Config file not found — skipping write",
        );
        return;
      }

      const raw = await file.text();
      const parsed = parseYaml(raw) as Record<string, unknown>;

      // Update only routing.tiers
      if (!parsed.routing || typeof parsed.routing !== "object") {
        parsed.routing = {};
      }

      // Write-time safety: reject any update that would zero out a tier
      const existingTiers = (parsed.routing as Record<string, unknown>).tiers as Record<string, string[]> | undefined;
      for (const [tier, providers] of Object.entries(tierUpdates)) {
        if (providers.length === 0) {
          const existing = existingTiers?.[tier];
          if (existing && existing.length > 0) {
            console.warn(
              `[learning-router] ⚠️ Write-time guard: refusing to zero out ${tier} tier, keeping: ${existing.join(", ")}`,
            );
            tierUpdates[tier] = [...existing];
          }
        }
      }

      (parsed.routing as Record<string, unknown>).tiers = tierUpdates;

      // Add learning metadata as comment-safe field
      (parsed as Record<string, unknown>)._learning = {
        last_updated: new Date().toISOString(),
        note: "Tier priorities auto-adjusted by LearningRouter based on 24h usage data",
      };

      const tmpPath = `${CONFIG_PATH}.tmp.${process.pid}.${Date.now()}`;
      await Bun.write(tmpPath, stringifyYaml(parsed));
      renameSync(tmpPath, CONFIG_PATH);
      console.log(
        "[learning-router] Config updated — hot-reload will pick up changes",
      );
    } catch (e) {
      console.error("[learning-router] Failed to write config:", e);
    }
  }

  private logScoreSummary(
    scores: ProviderScore[],
    tierUpdates: Record<string, string[]>,
  ): void {
    console.log("[learning-router] === Provider Scores (24h) ===");
    const sorted = [...scores].sort(
      (a, b) => b.compositeScore - a.compositeScore,
    );
    for (const s of sorted) {
      console.log(
        `[learning-router]   ${s.provider.padEnd(14)} score=${s.compositeScore.toFixed(3)} ` +
          `success=${(s.successRate * 100).toFixed(1)}% ` +
          `p95=${s.p95LatencyMs}ms ` +
          `reqs=${s.requestCount} ` +
          `accounts=${s.healthyAccounts}`,
      );
    }
    console.log("[learning-router] === Updated Tier Priorities ===");
    for (const [tier, providers] of Object.entries(tierUpdates)) {
      console.log(`[learning-router]   ${tier}: ${providers.join(" → ")}`);
    }
  }
}
