import type {
  Config,
  ErrorClass,
  MessagesRequest,
  Provider,
  ProviderAccount,
  Tier,
  UsageRecord,
} from "./types.ts";
import type { TokenManager } from "./auth/token-manager.ts";
import type { TaskRouter } from "./router/task-router.ts";
import type { UsageDB } from "./db/usage.ts";
import { BaseProvider } from "./providers/base.ts";
import { ClaudeProvider } from "./providers/claude.ts";
import { OpenAICompatProvider } from "./providers/openai-compat.ts";
import { GeminiProvider } from "./providers/gemini.ts";
import { AnthropicCompatProvider } from "./providers/anthropic-compat.ts";
import { FleetDispatcher } from "./fleet/dispatcher.ts";
import { TaskBoardManager } from "./fleet/task-board.ts";
import type {
  FleetDispatchRequest,
  FleetSwarmRequest,
  FractalPlanRequest,
} from "./fleet/types.ts";
import { SwarmCoordinator } from "./fleet/swarm.ts";
import { FractalPlanner } from "./fleet/fractal-planner.ts";
import { ExperimentLoopController } from "./fleet/experiment-loop.ts";
import {
  classifyTask,
  applyClassification,
  type FleetAvailability,
} from "./fleet/task-classifier.ts";
import { Agent GatewaySandboxAdapter } from "./sandbox/gateway-adapter.ts";
import type { Agent GatewaySandboxIntent } from "./sandbox/gateway-adapter.ts";
import type { BudgetGuard } from "./budget-guard.ts";
import type { LearningRouter } from "./router/learning-router.ts";
import {
  INTERACTIVE_BACKOFF,
  FLEET_BACKOFF,
  getBackoffMs,
  sleep,
  type BackoffConfig,
} from "./backoff.ts";
import { Semaphore, QueueFullError, QueueTimeoutError } from "./semaphore.ts";
import type { WatchdogService } from "./watchdog.ts";

// --- Retry mode configs ---
interface RetryConfig {
  maxRetries: number;
  backoff: BackoffConfig;
}

const INTERACTIVE_RETRY: RetryConfig = {
  maxRetries: 5,
  backoff: INTERACTIVE_BACKOFF,
};

const FLEET_RETRY: RetryConfig = {
  maxRetries: 8,
  backoff: FLEET_BACKOFF,
};

// --- Large-context providers for context_length fallback ---
// Gemini: 1M-2M tokens, Codex: 200K+, Kimi: 200K+, OpenRouter: routes to Gemini
const LARGE_CONTEXT_PROVIDERS = new Set<string>([
  "gemini",
  "codex",
  "kimi",
  "openrouter",
]);

function normalizeJsonSchema(schema: unknown): Record<string, unknown> {
  const visit = (node: unknown, root = false): Record<string, unknown> => {
    if (!node || typeof node !== "object" || Array.isArray(node)) {
      return root
        ? { type: "object", properties: {} }
        : ((node as Record<string, unknown>) ?? {});
    }

    const out = { ...(node as Record<string, unknown>) };

    if (out.type === "object") {
      const props = out.properties;
      if (!props || typeof props !== "object" || Array.isArray(props)) {
        out.properties = {};
      }
      if (out.required !== undefined && !Array.isArray(out.required)) {
        delete out.required;
      }
    }

    const properties = out.properties;
    if (
      properties &&
      typeof properties === "object" &&
      !Array.isArray(properties)
    ) {
      for (const [key, value] of Object.entries(
        properties as Record<string, unknown>,
      )) {
        (properties as Record<string, unknown>)[key] = visit(value, false);
      }
    }

    const items = out.items;
    if (items !== undefined) {
      if (Array.isArray(items)) {
        out.items = items.map((item) => visit(item, false));
      } else {
        out.items = visit(items, false);
      }
    }

    for (const key of ["allOf", "anyOf", "oneOf"] as const) {
      const value = out[key];
      if (Array.isArray(value)) {
        out[key] = value.map((entry) => visit(entry, false));
      }
    }

    const defs = out.$defs;
    if (defs && typeof defs === "object" && !Array.isArray(defs)) {
      for (const [key, value] of Object.entries(
        defs as Record<string, unknown>,
      )) {
        (defs as Record<string, unknown>)[key] = visit(value, false);
      }
    }

    return out;
  };

  const normalized = visit(schema, true);
  if (normalized.type !== "object") normalized.type = "object";
  if (
    !normalized.properties ||
    typeof normalized.properties !== "object" ||
    Array.isArray(normalized.properties)
  ) {
    normalized.properties = {};
  }
  if (
    normalized.required !== undefined &&
    !Array.isArray(normalized.required)
  ) {
    delete normalized.required;
  }
  return normalized;
}

// --- Per-tier timeouts ---
const TIER_TIMEOUT: Record<Tier, number> = {
  premium: 180_000,
  standard: 120_000,
  fast: 45_000,
  budget: 45_000,
  last_resort: 90_000,
};

// --- Concurrency limits ---
const GLOBAL_CONCURRENCY = 20;
const GLOBAL_MAX_PENDING = 80; // reject immediately if queue deeper than this
const QUEUE_TIMEOUT_MS = 60_000; // abort if waiting in queue > 60s
const PROVIDER_CONCURRENCY: Partial<Record<Provider, number>> = {
  claude: 4,
  antigravity: 4,
  codex: 12,
  gemini: 4,
};
const DEFAULT_PROVIDER_CONCURRENCY = 4;
const PROVIDER_SEM_TIMEOUT_MS = 10_000; // fail-fast to next provider instead of blocking
const PROXY_PROVIDERS = new Set<string>([
  "claude",
  "antigravity",
  "codex",
  "gemini",
  "openrouter",
]);
const HEALTH_SNAPSHOT_INTERVAL_MS = 1_000;
const EVENT_LOOP_SAMPLE_INTERVAL_MS = 500;
const EVENT_LOOP_WARN_MS = 250;
const EVENT_LOOP_CRITICAL_MS = 1_000;
const EVENT_LOOP_WARN_THROTTLE_MS = 30_000;

function extractContentText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";

  const parts: string[] = [];
  for (const block of content) {
    if (!block || typeof block !== "object") continue;
    const record = block as Record<string, unknown>;
    if (typeof record.text === "string") parts.push(record.text);
    if (typeof record.name === "string") parts.push(record.name);
    if (record.input !== undefined) parts.push(JSON.stringify(record.input));
    if (record.content !== undefined && typeof record.content !== "string") {
      parts.push(JSON.stringify(record.content));
    }
  }

  return parts.join("\n");
}

function estimateInputTokens(body: Partial<MessagesRequest>): number {
  const chunks: string[] = [];

  if (typeof body.model === "string") chunks.push(body.model);
  if (typeof body.system === "string") {
    chunks.push(body.system);
  } else if (body.system) {
    chunks.push(extractContentText(body.system));
  }

  if (Array.isArray(body.messages)) {
    for (const message of body.messages) {
      chunks.push(message.role);
      chunks.push(extractContentText(message.content));
    }
  }

  const tools = (body as Record<string, unknown>).tools;
  if (Array.isArray(tools)) {
    for (const tool of tools) chunks.push(JSON.stringify(tool));
  }

  if (body.metadata) chunks.push(JSON.stringify(body.metadata));

  const raw = chunks.filter(Boolean).join("\n");
  return Math.max(1, Math.ceil(raw.length / 4));
}

export class ProxyServer {
  private config: Config;
  private tokenManager: TokenManager;
  private router: TaskRouter;
  private usageDB: UsageDB;
  private providers: Map<string, BaseProvider> = new Map();
  private fleet: FleetDispatcher;
  private taskBoard: TaskBoardManager;
  private swarmCoordinator: SwarmCoordinator;
  private fractalPlanner: FractalPlanner;
  private experimentLoops: ExperimentLoopController;
  private agent-gatewaySandbox: Agent GatewaySandboxAdapter;
  private budgetGuard: BudgetGuard | null;
  private learningRouter: LearningRouter | null;
  private watchdog: WatchdogService | null = null;
  // CLIProxyAPI health latch — when set, skip all proxy providers immediately
  // to avoid N×M retry stampede against a broken proxy
  private cliProxyUnhealthyUntil = 0;
  private server: ReturnType<typeof Bun.serve> | null = null;
  private startTime = Date.now();
  private telemetry = {
    queue_rejects: 0,
    provider_exclusions: 0,
    proxy_resets: 0,
    route_decisions: {} as Record<string, number>,
    fallback_routes: 0,
    degraded_routes: 0,
    gated_on_degrade: 0,
  };
  private healthSnapshot = {
    status: "degraded",
    uptime: 0,
    accounts: { healthy: 0, total: 0 },
    snapshot_age_ms: 0,
    snapshot_ts: 0,
    event_loop: {
      last_lag_ms: 0,
      max_lag_ms: 0,
      p95_lag_ms: 0,
      histogram: {
        lt50: 0,
        lt100: 0,
        lt250: 0,
        lt500: 0,
        gte500: 0,
      },
    },
  };
  private healthSnapshotTimer: ReturnType<typeof setInterval> | null = null;
  private eventLoopLagTimer: ReturnType<typeof setInterval> | null = null;
  private lastLagSampleAt = Date.now();
  private lastLagWarnAt = 0;
  private lagSamples: number[] = [];

  private shouldGateOnDegrade(route: {
    degraded: boolean;
    tier: Tier;
    floorTier: Tier;
  }): boolean {
    const autonomy = this.config.autonomy;
    if (!route.degraded) return false;
    if (autonomy?.gate_on_degrade === false) return false;
    const profile = autonomy?.profile ?? "balanced";
    if (profile === "aggressive") return false;
    return true;
  }

  private getProviderErrorRate(provider: string): number {
    const rows = this.usageDB.getErrorRates();
    const row = rows.find((r) => r.provider === provider);
    if (!row || row.total <= 0) return 0;
    return row.errors / row.total;
  }

  private shouldExcludeForSlo(provider: string): boolean {
    if (!this.config.slo?.enabled) return false;
    const threshold = this.config.slo.max_provider_error_rate ?? 0.25;
    const rate = this.getProviderErrorRate(provider);
    return rate > threshold;
  }

  private getTierConcurrencyLimit(tier: Tier): number {
    return (
      this.config.autonomy?.max_parallel_by_tier?.[tier] ?? GLOBAL_CONCURRENCY
    );
  }

  private isTierAtConcurrencyLimit(tier: Tier): boolean {
    const tierLimit = this.getTierConcurrencyLimit(tier);
    return this.globalSemaphore.active >= tierLimit;
  }

  // Concurrency control
  private globalSemaphore = new Semaphore(
    GLOBAL_CONCURRENCY,
    GLOBAL_MAX_PENDING,
  );
  private providerSemaphores: Map<string, Semaphore> = new Map();

  constructor(
    config: Config,
    tokenManager: TokenManager,
    router: TaskRouter,
    usageDB: UsageDB,
    budgetGuard?: BudgetGuard,
    learningRouter?: LearningRouter,
  ) {
    this.config = config;
    this.tokenManager = tokenManager;
    this.router = router;
    this.usageDB = usageDB;
    this.budgetGuard = budgetGuard ?? null;
    this.learningRouter = learningRouter ?? null;
    this.fleet = new FleetDispatcher();
    this.taskBoard = new TaskBoardManager();
    this.swarmCoordinator = new SwarmCoordinator(this.fleet);
    this.fractalPlanner = new FractalPlanner(
      this.config,
      this.router,
      this.swarmCoordinator,
      this.budgetGuard ?? undefined,
      this.learningRouter ?? undefined,
    );
    this.experimentLoops = new ExperimentLoopController(this.swarmCoordinator);
    this.agent-gatewaySandbox = new Agent GatewaySandboxAdapter();
    this.initProviders();
    this.initSemaphores();
  }

  updateConfig(config: Config): void {
    this.config = config;
    // Re-initialize semaphores for any providers added/changed since startup
    for (const [name] of this.providers) {
      if (!this.providerSemaphores.has(name)) {
        const limit =
          PROVIDER_CONCURRENCY[name as Provider] ??
          DEFAULT_PROVIDER_CONCURRENCY;
        this.providerSemaphores.set(name, new Semaphore(limit));
      }
    }
  }

  setWatchdog(wd: WatchdogService): void {
    this.watchdog = wd;
  }

  private snapshotIntervalMs(): number {
    return (
      this.config.health.snapshot_interval_ms ?? HEALTH_SNAPSHOT_INTERVAL_MS
    );
  }

  private eventLoopWarnMs(): number {
    return this.config.health.event_loop_lag_warn_ms ?? EVENT_LOOP_WARN_MS;
  }

  private eventLoopCriticalMs(): number {
    return (
      this.config.health.event_loop_lag_critical_ms ?? EVENT_LOOP_CRITICAL_MS
    );
  }

  private eventLoopSuspendThresholdMs(): number {
    return this.config.health.event_loop_suspend_threshold_ms ?? 60_000;
  }

  private startRuntimeMonitors(): void {
    this.stopRuntimeMonitors();
    this.refreshHealthSnapshot();
    this.lastLagSampleAt = Date.now();

    this.healthSnapshotTimer = setInterval(
      () => this.refreshHealthSnapshot(),
      this.snapshotIntervalMs(),
    );

    this.eventLoopLagTimer = setInterval(() => {
      const now = Date.now();
      const expected = this.lastLagSampleAt + EVENT_LOOP_SAMPLE_INTERVAL_MS;
      const lagMs = Math.max(0, now - expected);
      this.lastLagSampleAt = now;

      // Ignore huge suspend/wake anomalies so they don't pollute runtime lag metrics.
      if (lagMs >= this.eventLoopSuspendThresholdMs()) {
        this.healthSnapshot.event_loop.last_lag_ms = lagMs;
        this.lastLagWarnAt = now;
        console.warn(
          `[proxy] Event loop suspend detected: ${lagMs}ms (ignored from histogram/p95)`,
        );
        return;
      }

      this.recordLagSample(lagMs);
      if (lagMs >= this.eventLoopWarnMs()) {
        this.maybeWarnEventLoopLag(lagMs);
      }
    }, EVENT_LOOP_SAMPLE_INTERVAL_MS);
  }

  private stopRuntimeMonitors(): void {
    if (this.healthSnapshotTimer) {
      clearInterval(this.healthSnapshotTimer);
      this.healthSnapshotTimer = null;
    }
    if (this.eventLoopLagTimer) {
      clearInterval(this.eventLoopLagTimer);
      this.eventLoopLagTimer = null;
    }
  }

  private recordLagSample(lagMs: number): void {
    this.lagSamples.push(lagMs);
    if (this.lagSamples.length > 240) this.lagSamples.shift();
    this.healthSnapshot.event_loop.last_lag_ms = lagMs;
    if (lagMs > this.healthSnapshot.event_loop.max_lag_ms) {
      this.healthSnapshot.event_loop.max_lag_ms = lagMs;
    }

    const hist = {
      lt50: 0,
      lt100: 0,
      lt250: 0,
      lt500: 0,
      gte500: 0,
    };
    for (const sample of this.lagSamples) {
      if (sample < 50) hist.lt50++;
      else if (sample < 100) hist.lt100++;
      else if (sample < 250) hist.lt250++;
      else if (sample < 500) hist.lt500++;
      else hist.gte500++;
    }
    this.healthSnapshot.event_loop.histogram = hist;

    const sorted = [...this.lagSamples].sort((a, b) => a - b);
    if (sorted.length > 0) {
      const p95Index = Math.min(
        sorted.length - 1,
        Math.floor(sorted.length * 0.95),
      );
      this.healthSnapshot.event_loop.p95_lag_ms = sorted[p95Index] ?? 0;
    }
  }

  private maybeWarnEventLoopLag(lagMs: number): void {
    const now = Date.now();
    if (now - this.lastLagWarnAt < EVENT_LOOP_WARN_THROTTLE_MS) return;
    this.lastLagWarnAt = now;

    const severity = lagMs >= this.eventLoopCriticalMs() ? "critical" : "warn";
    const maintenance = this.tokenManager.getMaintenanceState();
    console.warn(
      `[proxy] Event loop lag ${severity}: ${lagMs}ms (global active=${this.globalSemaphore.active}, pending=${this.globalSemaphore.pending}, refresh_in_progress=${maintenance.refresh_in_progress}, rescan_in_progress=${maintenance.rescan_in_progress}, refreshing_accounts=${maintenance.refreshing_accounts})`,
    );
  }

  private refreshHealthSnapshot(): void {
    const allAccounts = this.tokenManager.getAllAccounts();
    const healthy = allAccounts.filter((a) => a.health === "healthy").length;
    const total = allAccounts.length;
    const now = Date.now();
    this.healthSnapshot = {
      ...this.healthSnapshot,
      status: healthy > 0 ? "ok" : "degraded",
      uptime: Math.floor((now - this.startTime) / 1000),
      accounts: { healthy, total },
      snapshot_ts: now,
      snapshot_age_ms: 0,
      event_loop: this.healthSnapshot.event_loop,
    };
  }

  /**
   * Mark CLIProxyAPI as unhealthy for a duration — all proxy providers
   * will be skipped until the latch expires or CLIProxyAPI recovers.
   */
  private markProxyUnhealthy(durationMs = 30_000): void {
    this.cliProxyUnhealthyUntil = Date.now() + durationMs;
  }

  private isProxyHealthy(): boolean {
    // Check local latch first (fast path)
    if (Date.now() < this.cliProxyUnhealthyUntil) return false;
    // Then check watchdog state if available
    if (this.watchdog && !this.watchdog.isProxyHealthy) return false;
    return true;
  }

  private initSemaphores(): void {
    for (const [name] of this.providers) {
      const limit =
        PROVIDER_CONCURRENCY[name as Provider] ?? DEFAULT_PROVIDER_CONCURRENCY;
      this.providerSemaphores.set(name, new Semaphore(limit));
    }
  }

  private initProviders(): void {
    const pc = this.config.providers;

    // Claude/Antigravity: native Anthropic API via CLIProxyAPI
    this.providers.set("claude", new ClaudeProvider(true));
    this.providers.set("antigravity", new ClaudeProvider(true));

    // Codex: OpenAI-compat via CLIProxyAPI
    this.providers.set(
      "codex",
      new OpenAICompatProvider("codex", pc.codex?.base_url, true),
    );

    // Gemini: OpenAI-compat via CLIProxyAPI (default: Google's endpoint)
    this.providers.set("gemini", new GeminiProvider(true, pc.gemini?.base_url));

    // Kimi: OpenAI-compat direct (CLI-only for interactive, but HTTP for fleet)
    this.providers.set(
      "kimi",
      new OpenAICompatProvider(
        "kimi",
        pc.kimi?.base_url ?? "https://api.kimi.com/coding/v1",
        false,
        pc.kimi?.default_headers,
      ),
    );

    // Kimi API: OpenAI-compat direct via API key (always available, no OAuth)
    this.providers.set(
      "kimi-api",
      new OpenAICompatProvider(
        "kimi-api",
        pc["kimi-api"]?.base_url ?? "https://api.kimi.com/coding/v1",
        false,
        pc["kimi-api"]?.default_headers,
      ),
    );

    // GLM: OpenAI-compat direct to Z.AI (api/paas/v4)
    // Previously used AnthropicCompatProvider but the /api/anthropic endpoint
    // is less reliable. OpenAI-compat path is more standard and better tested.
    this.providers.set(
      "glm",
      new OpenAICompatProvider(
        "glm",
        pc.glm?.base_url ?? "https://api.z.ai/api/paas/v4",
        false,
      ),
    );

    // MiniMax: OpenAI-compat direct (not via CLIProxyAPI — it doesn't recognize lowercase aliases)
    this.providers.set(
      "minimax",
      new OpenAICompatProvider(
        "minimax",
        pc.minimax?.base_url ?? "https://api.minimax.io/v1",
        false,
      ),
    );

    // OpenRouter: OpenAI-compat via CLIProxyAPI
    this.providers.set(
      "openrouter",
      new OpenAICompatProvider(
        "openrouter",
        pc.openrouter?.base_url ?? "https://openrouter.ai/api/v1",
        true,
      ),
    );
  }

  private authenticateRequest(req: Request): boolean {
    const authHeader =
      req.headers.get("Authorization") ?? req.headers.get("x-api-key") ?? "";
    const key = authHeader.replace("Bearer ", "").trim();

    if (!key) {
      return false;
    }

    for (const allowedKey of this.config.api_keys) {
      if (allowedKey === "*") return true; // Accept all (localhost dev mode)
      if (allowedKey.includes("*")) {
        // Wildcard match: "sk-ant-*" matches "sk-ant-anything"
        const prefix = allowedKey.replace("*", "");
        if (key.startsWith(prefix)) return true;
      } else if (key === allowedKey) {
        return true;
      }
    }
    return false;
  }

  start(): void {
    const port = this.config.port;
    this.server = Bun.serve({
      port,
      hostname: "127.0.0.1",
      idleTimeout: 255, // max Bun allows — streaming responses need long idle
      fetch: (req) => this.handleRequest(req),
    });
    this.startRuntimeMonitors();
    this.swarmCoordinator.start();
    this.experimentLoops.start();
    console.log(`[proxy] Listening on http://localhost:${port}`);
  }

  private async handleRequest(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;
    const requireManagementAuth =
      process.env.ORCH_REQUIRE_MANAGEMENT_AUTH === "1";

    // Health check — no auth required (both /health and /v1/health for nexus compat)
    if ((path === "/health" || path === "/v1/health") && req.method === "GET") {
      return this.handleHealth();
    }

    if (requireManagementAuth && path === "/status" && req.method === "GET") {
      if (!this.authenticateRequest(req)) {
        return Response.json(
          {
            error: { type: "authentication_error", message: "Invalid API key" },
          },
          { status: 401 },
        );
      }
    }

    // Status endpoint
    if (path === "/status" && req.method === "GET") {
      return this.handleStatus();
    }

    if (
      requireManagementAuth &&
      path.startsWith("/v0/management/") &&
      req.method === "GET"
    ) {
      if (!this.authenticateRequest(req)) {
        return Response.json(
          {
            error: { type: "authentication_error", message: "Invalid API key" },
          },
          { status: 401 },
        );
      }
    }

    // Management usage endpoint — for fleet gateway usage-aware routing
    if (path === "/v0/management/usage" && req.method === "GET") {
      const email = url.searchParams.get("email");
      return this.handleManagementUsage(email);
    }

    // Learning router status endpoint — shows adaptive routing state
    if (path === "/v0/management/learning" && req.method === "GET") {
      return this.handleLearningStatus();
    }

    if (path === "/v0/management/routing" && req.method === "GET") {
      return this.handleRoutingStatus();
    }

    // Budget status endpoint — shows current usage vs limits
    if (path === "/v0/management/budget" && req.method === "GET") {
      return this.handleBudgetStatus();
    }

    // Auth required for all other endpoints
    if (!this.authenticateRequest(req)) {
      return new Response(
        JSON.stringify({
          error: { type: "authentication_error", message: "Invalid API key" },
        }),
        {
          status: 401,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    // Budget resume endpoint — unpark budget-stopped accounts (requires auth)
    if (path === "/v0/management/budget/resume" && req.method === "POST") {
      return this.handleBudgetResume(url);
    }

    // Circuit reset endpoint — clear all open circuits (requires auth)
    if (path === "/v0/management/circuits/reset" && req.method === "POST") {
      return this.handleCircuitReset(url);
    }

    if (path === "/v1/messages" && req.method === "POST") {
      return this.handleMessages(req);
    }

    if (path === "/v1/messages/count_tokens" && req.method === "POST") {
      return this.handleCountTokens(req);
    }

    if (path === "/v1/oracle" && req.method === "POST") {
      return this.handleOracle(req);
    }

    if (path === "/v1/models" && req.method === "GET") {
      return this.handleModels();
    }

    // Fleet dispatch endpoints
    if (path === "/v1/fleet/dispatch" && req.method === "POST") {
      return this.handleFleetDispatch(req);
    }
    if (path === "/v1/fleet/jobs" && req.method === "GET") {
      return this.handleFleetJobs();
    }
    if (path.startsWith("/v1/fleet/jobs/") && req.method === "GET") {
      const id = path.split("/v1/fleet/jobs/")[1]?.split("/")[0];
      const sub = path.split(`/v1/fleet/jobs/${id}/`)[1];
      if (!id)
        return Response.json({ error: "Missing job ID" }, { status: 400 });
      if (sub === "output") return this.handleFleetOutput(id, url);
      return this.handleFleetJobStatus(id);
    }
    if (path.startsWith("/v1/fleet/jobs/") && req.method === "POST") {
      const id = path.split("/v1/fleet/jobs/")[1]?.split("/")[0];
      const sub = path.split(`/v1/fleet/jobs/${id}/`)[1];
      if (!id)
        return Response.json({ error: "Missing job ID" }, { status: 400 });
      if (sub === "send") return this.handleFleetSend(id, req);
      if (sub === "kill") return this.handleFleetKill(id);
      if (sub === "plan") return this.handleFleetPlan(id);
      if (sub === "approve") return this.handleFleetApprove(id, req);
      return Response.json({ error: "Unknown fleet action" }, { status: 404 });
    }

    // Fractal planner endpoints
    if (path === "/v1/fleet/fractal/plan" && req.method === "POST") {
      return this.handleFractalPlan(req);
    }
    if (path.startsWith("/v1/fleet/fractal/plan/") && req.method === "GET") {
      const planId = path.split("/v1/fleet/fractal/plan/")[1]?.split("/")[0];
      if (!planId)
        return Response.json({ error: "Missing plan ID" }, { status: 400 });
      return this.handleFractalPlanGet(planId);
    }
    if (
      path.startsWith("/v1/fleet/fractal/plan/") &&
      path.endsWith("/execute") &&
      req.method === "POST"
    ) {
      const planId = path.split("/v1/fleet/fractal/plan/")[1]?.split("/")[0];
      if (!planId)
        return Response.json({ error: "Missing plan ID" }, { status: 400 });
      return this.handleFractalExecute(planId, req);
    }

    // Experiment loop endpoints
    if (path === "/v1/fleet/experiment-loops" && req.method === "POST") {
      return this.handleExperimentLoopCreate(req);
    }
    if (path === "/v1/fleet/experiment-loops" && req.method === "GET") {
      return this.handleExperimentLoopList(url);
    }
    if (
      path.startsWith("/v1/fleet/experiment-loops/") &&
      req.method === "GET"
    ) {
      const id = path.split("/v1/fleet/experiment-loops/")[1]?.split("/")[0];
      if (!id)
        return Response.json(
          { error: "Missing experiment loop ID" },
          { status: 400 },
        );
      return this.handleExperimentLoopGet(id);
    }
    if (
      path.startsWith("/v1/fleet/experiment-loops/") &&
      req.method === "DELETE"
    ) {
      const id = path.split("/v1/fleet/experiment-loops/")[1]?.split("/")[0];
      if (!id)
        return Response.json(
          { error: "Missing experiment loop ID" },
          { status: 400 },
        );
      return this.handleExperimentLoopCancel(id);
    }

    // Swarm endpoints
    if (path === "/v1/fleet/swarm" && req.method === "POST") {
      return this.handleSwarmCreate(req);
    }
    if (path === "/v1/fleet/swarms" && req.method === "GET") {
      return this.handleSwarmList(url);
    }
    if (path.startsWith("/v1/fleet/swarm/") && req.method === "GET") {
      const id = path.split("/v1/fleet/swarm/")[1]?.split("/")[0];
      if (!id)
        return Response.json({ error: "Missing swarm ID" }, { status: 400 });
      return this.handleSwarmStatus(id);
    }
    if (path.startsWith("/v1/fleet/swarm/") && req.method === "DELETE") {
      const id = path.split("/v1/fleet/swarm/")[1]?.split("/")[0];
      if (!id)
        return Response.json({ error: "Missing swarm ID" }, { status: 400 });
      return this.handleSwarmKill(id);
    }

    // Task board endpoints
    if (path === "/v1/fleet/boards" && req.method === "POST") {
      return this.handleBoardCreate(req);
    }
    if (path === "/v1/fleet/boards" && req.method === "GET") {
      return Response.json(this.taskBoard.listBoards());
    }
    if (path.startsWith("/v1/fleet/boards/") && req.method === "GET") {
      const boardId = path.split("/v1/fleet/boards/")[1]?.split("/")[0];
      if (!boardId)
        return Response.json({ error: "Missing board ID" }, { status: 400 });
      const board = this.taskBoard.getBoard(boardId);
      return board
        ? Response.json(board)
        : Response.json({ error: "Board not found" }, { status: 404 });
    }
    if (path.startsWith("/v1/fleet/boards/") && req.method === "POST") {
      const boardId = path.split("/v1/fleet/boards/")[1]?.split("/")[0];
      const sub = path.split(`/v1/fleet/boards/${boardId}/`)[1];
      if (!boardId)
        return Response.json({ error: "Missing board ID" }, { status: 400 });
      if (sub === "claim") return this.handleBoardClaim(boardId, req);
      if (sub === "complete") return this.handleBoardComplete(boardId, req);
      if (sub === "fail") return this.handleBoardFail(boardId, req);
    }

    // Fleet session endpoints (Agent Gateway session control)
    if (path === "/v1/fleet/sessions" && req.method === "GET") {
      return this.handleFleetSessions(url);
    }
    if (path.startsWith("/v1/fleet/sessions/") && req.method === "GET") {
      const id = path.split("/v1/fleet/sessions/")[1]?.split("/")[0];
      if (!id)
        return Response.json({ error: "Missing session ID" }, { status: 400 });
      return this.handleFleetSessionGet(id);
    }
    if (path.startsWith("/v1/fleet/sessions/") && req.method === "POST") {
      const id = path.split("/v1/fleet/sessions/")[1]?.split("/")[0];
      const sub = path.split(`/v1/fleet/sessions/${id}/`)[1];
      if (!id)
        return Response.json({ error: "Missing session ID" }, { status: 400 });
      if (sub === "send") return this.handleFleetSessionSend(id, req);
      if (sub === "kill") return this.handleFleetSessionKill(id);
      if (sub === "restart") return this.handleFleetSessionRestart(id);
      return Response.json(
        { error: "Unknown fleet session action" },
        { status: 404 },
      );
    }

    return new Response(
      JSON.stringify({
        error: { type: "not_found", message: `Unknown route: ${path}` },
      }),
      {
        status: 404,
        headers: { "Content-Type": "application/json" },
      },
    );
  }

  private handleHealth(): Response {
    const snapshot = {
      ...this.healthSnapshot,
      snapshot_age_ms: Math.max(
        0,
        Date.now() - this.healthSnapshot.snapshot_ts,
      ),
    };
    return Response.json(snapshot);
  }

  private handleStatus(): Response {
    const allAccounts = this.tokenManager.getAllAccounts();
    const byProvider: Record<
      string,
      {
        healthy: number;
        total: number;
        queue: { active: number; pending: number };
        accounts: object[];
      }
    > = {};

    for (const account of allAccounts) {
      if (!byProvider[account.provider]) {
        const sem = this.providerSemaphores.get(account.provider);
        byProvider[account.provider] = {
          healthy: 0,
          total: 0,
          queue: { active: sem?.active ?? 0, pending: sem?.pending ?? 0 },
          accounts: [],
        };
      }
      const p = byProvider[account.provider]!;
      p.total++;
      if (account.health === "healthy") p.healthy++;
      const now = Date.now();
      p.accounts.push({
        id: account.id,
        email: account.email,
        health: account.health,
        expiresAt: account.expiresAt?.toISOString() ?? null,
        lastUsed: account.lastUsed?.toISOString() ?? null,
        errorCount: account.errorCount,
        circuitOpen: account.circuitOpenUntil
          ? account.circuitOpenUntil > now
          : false,
        rateLimited: account.rateLimitedUntil
          ? account.rateLimitedUntil > now
          : false,
        rateLimitResetsAt:
          account.rateLimitedUntil && account.rateLimitedUntil > now
            ? new Date(account.rateLimitedUntil).toISOString()
            : null,
        rateLimitResetsIn:
          account.rateLimitedUntil && account.rateLimitedUntil > now
            ? Math.ceil((account.rateLimitedUntil - now) / 1000)
            : null,
        rateLimitHits: account.rateLimitHits,
        quotaRemaining: account.quotaRemaining,
        quotaLimit: account.quotaLimit,
        quotaResetAt: account.quotaResetAt
          ? new Date(account.quotaResetAt).toISOString()
          : null,
      });
    }

    const uptime = Math.floor((Date.now() - this.startTime) / 1000);
    const usage24h = this.usageDB.getUsageByProvider();
    const errorRates = this.usageDB.getErrorRates();

    return Response.json({
      uptime,
      health_snapshot: {
        ...this.healthSnapshot,
        snapshot_age_ms: Math.max(
          0,
          Date.now() - this.healthSnapshot.snapshot_ts,
        ),
      },
      concurrency: {
        global: {
          active: this.globalSemaphore.active,
          pending: this.globalSemaphore.pending,
          limit: GLOBAL_CONCURRENCY,
          max_pending: GLOBAL_MAX_PENDING,
          queue_timeout_ms: QUEUE_TIMEOUT_MS,
        },
      },
      providers: byProvider,
      tier_config: this.config.routing.tiers,
      degraded_routes: this.telemetry.degraded_routes,
      telemetry: this.telemetry,
      last_routing_decision: this.router.getLastRouteExplanation(),
      usage_24h: usage24h,
      error_rates_1h: errorRates,
    });
  }

  private handleBudgetStatus(): Response {
    if (!this.budgetGuard) {
      return Response.json({ enabled: false, limits: [] });
    }
    const status = this.budgetGuard.getStatus();
    return Response.json({
      enabled: !!this.config.budget?.enabled,
      limits: status,
    });
  }

  private handleLearningStatus(): Response {
    if (!this.learningRouter) {
      return Response.json({ enabled: false, state: null });
    }
    const state = this.learningRouter.getState();
    return Response.json({
      enabled: true,
      state: state
        ? {
            lastRun: new Date(state.lastRun).toISOString(),
            scores: state.scores.map((s) => ({
              provider: s.provider,
              compositeScore: s.compositeScore,
              successRateRatio: Number(s.successRate.toFixed(4)),
              successRatePercent: Number((s.successRate * 100).toFixed(1)),
              p95LatencyMs: s.p95LatencyMs,
              requestCount: s.requestCount,
              errorCount: s.errorCount,
              healthyAccounts: s.healthyAccounts,
            })),
            tierUpdates: state.tierUpdates,
            guardrailHits: state.guardrailHits ?? [],
          }
        : null,
    });
  }

  private handleRoutingStatus(): Response {
    return Response.json({
      route: this.router.getLastRouteExplanation(),
      telemetry: this.telemetry,
    });
  }

  private handleBudgetResume(url: URL): Response {
    if (!this.budgetGuard) {
      return Response.json(
        { error: "Budget guard not enabled" },
        { status: 400 },
      );
    }
    const provider = url.searchParams.get("provider") ?? undefined;
    const resumed = this.budgetGuard.resume(provider);
    return Response.json({
      resumed,
      message: `Resumed ${resumed} account(s)${provider ? ` for ${provider}` : ""}`,
    });
  }

  private async handleCountTokens(req: Request): Promise<Response> {
    let body: Partial<MessagesRequest>;
    try {
      body = (await req.json()) as Partial<MessagesRequest>;
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    if (!body || !Array.isArray(body.messages)) {
      return Response.json(
        { error: "Missing required field: messages" },
        { status: 400 },
      );
    }

    return Response.json({ input_tokens: estimateInputTokens(body) });
  }

  private handleManagementUsage(email: string | null): Response {
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const todayCutoff = todayStart.getTime();

    let totalRequests: number;
    let failureCount: number;
    let totalTokens: number;
    let tokensByHour: Record<string, number>;

    if (email) {
      // Per-account usage for fleet gateway routing
      const acctUsage = this.usageDB.getUsageByAccountEmail(
        "claude",
        email,
        todayCutoff,
      );
      totalRequests = acctUsage?.requests ?? 0;
      failureCount = acctUsage?.errors ?? 0;
      totalTokens =
        (acctUsage?.input_tokens ?? 0) + (acctUsage?.output_tokens ?? 0);
      tokensByHour = this.usageDB.getTokensByHourForAccount(
        "claude",
        email,
        todayCutoff,
      );
    } else {
      // Aggregate Claude usage
      const allUsage = this.usageDB.getUsageByProvider(todayCutoff);
      const claudeUsage = allUsage.find((u) => u.provider === "claude");
      totalRequests = claudeUsage?.requests ?? 0;
      failureCount = claudeUsage?.errors ?? 0;
      totalTokens =
        (claudeUsage?.input_tokens ?? 0) + (claudeUsage?.output_tokens ?? 0);
      tokensByHour = this.usageDB.getTokensByHour("claude", todayCutoff);
    }

    const successCount = totalRequests - failureCount;

    return Response.json({
      usage: {
        total_requests: totalRequests,
        success_count: successCount,
        failure_count: failureCount,
        total_tokens: totalTokens,
        today_tokens: totalTokens,
        tokens_by_hour: tokensByHour,
      },
    });
  }

  /**
   * Strip fields that Claude Code sends but providers reject.
   * Mutates the request in place.
   */
  private sanitizeRequest(body: MessagesRequest): void {
    // Whitelist: only pass through valid Anthropic Messages API fields.
    // Claude Code and internal retry logic can add extra fields
    // (_thinkingStripped, context_management, etc.) that upstream APIs reject
    // with "Extra inputs are not permitted".
    const ALLOWED_FIELDS = new Set([
      "model",
      "messages",
      "max_tokens",
      "temperature",
      "top_p",
      "top_k",
      "stream",
      "system",
      "stop_sequences",
      "metadata",
      "thinking",
      "tools",
      "tool_choice",
      // Anthropic beta/feature fields
      "betas",
    ]);
    const raw = body as Record<string, unknown>;
    for (const key of Object.keys(raw)) {
      if (!ALLOWED_FIELDS.has(key)) {
        delete raw[key];
      }
    }

    // Claude Code v2.1.47+ sends cache_control with a "scope" field that older
    // API versions / proxy endpoints reject ("Extra inputs are not permitted").
    // Strip "scope" from all cache_control objects in system and messages.
    const stripCacheControlScope = (obj: unknown): void => {
      if (!obj || typeof obj !== "object") return;
      if (Array.isArray(obj)) {
        for (const item of obj) stripCacheControlScope(item);
        return;
      }
      const rec = obj as Record<string, unknown>;
      if (rec.cache_control && typeof rec.cache_control === "object") {
        delete (rec.cache_control as Record<string, unknown>).scope;
      }
      // Recurse into content arrays (system messages have nested content blocks)
      if (Array.isArray(rec.content)) {
        for (const block of rec.content) stripCacheControlScope(block);
      }
    };
    if (body.system) stripCacheControlScope(body.system);
    if (body.messages) {
      for (const msg of body.messages) stripCacheControlScope(msg);
    }

    // Validate thinking budget — Anthropic requires minimum 1024 and max_tokens > budget
    if (body.thinking && typeof body.thinking === "object") {
      const thinking = body.thinking as Record<string, unknown>;
      if (
        thinking.budget_tokens &&
        typeof thinking.budget_tokens === "number"
      ) {
        if (thinking.budget_tokens < 1024) {
          thinking.budget_tokens = 1024;
        }
        // max_tokens must be greater than budget_tokens
        if (
          body.max_tokens &&
          body.max_tokens <= (thinking.budget_tokens as number)
        ) {
          body.max_tokens = (thinking.budget_tokens as number) + 1024;
        }
      }
    }

    // Normalize tool schemas before any downstream translation/provider calls.
    // Some MCP adapters emit partial JSON schema objects that break strict
    // function-calling validators on OpenAI-compatible backends.
    this.normalizeToolSchemas(body);

    // CLIProxyAPI bug workaround: its Go structs don't include the `signature`
    // field on thinking blocks, so JSON re-serialization strips signatures.
    // Strip thinking blocks from historical assistant messages to prevent
    // Anthropic rejecting the request with "Invalid signature in thinking block".
    this.stripThinkingFromHistory(body);

    // Fix empty text content blocks — Anthropic rejects with
    // "messages: text content blocks must be non-empty".
    // Can happen when non-Claude providers return empty/null text alongside
    // tool calls, and the conversation history preserves those empty blocks.
    this.fixEmptyTextBlocks(body);

    // Pre-flight: remove orphaned tool_use blocks (tool_use without matching tool_result).
    // Concurrent tool execution can leave these behind when a client interrupts mid-turn.
    this.removeOrphanedToolUseBlocks(body);

    // Final pass: ensure no messages have empty content arrays.
    // Multiple upstream passes (MCP tool_result removal, orphan tool_use removal)
    // can leave messages with content: [] which Anthropic rejects.
    this.ensureNonEmptyContent(body);
  }

  private sanitizeOutboundRequest(body: MessagesRequest): MessagesRequest {
    const outbound: MessagesRequest = { ...body };
    const metadata = body.metadata;
    if (!metadata || typeof metadata !== "object") {
      return outbound;
    }

    const safeMetadata: Record<string, string> = {};
    if (typeof metadata.user_id === "string" && metadata.user_id.length > 0) {
      safeMetadata.user_id = metadata.user_id;
    }

    if (Object.keys(safeMetadata).length > 0) {
      outbound.metadata = safeMetadata;
    } else {
      delete (outbound as Record<string, unknown>).metadata;
    }
    return outbound;
  }

  /**
   * Normalize tool schemas to strict JSON Schema object form expected by
   * OpenAI-compatible function-calling providers.
   */
  private normalizeToolSchemas(body: MessagesRequest): void {
    const raw = body as Record<string, unknown>;
    const tools = raw.tools as
      | {
          name?: unknown;
          type?: unknown;
          input_schema?: unknown;
        }[]
      | undefined;

    if (!Array.isArray(tools) || tools.length === 0) return;

    for (const tool of tools) {
      if (!tool || typeof tool !== "object") continue;
      // Built-in Anthropic tools (web_search_20250305, computer_20250124, etc.)
      // use a non-"custom" type and must NOT have input_schema.
      // Only normalize custom tools (type === "custom" or type is absent).
      if (tool.type && tool.type !== "custom") continue;
      tool.input_schema = normalizeJsonSchema(tool.input_schema);
    }
  }

  /**
   * Scan conversation history for tool_use blocks and ensure every referenced
   * tool name exists in the request's tools array. First tries to normalize
   * the reference (e.g. hyphens→underscores from non-Claude models), then
   * falls back to adding a minimal stub so the API doesn't reject the request.
   */
  private ensureToolReferences(body: MessagesRequest): {
    normalized: number;
    stubbed: number;
    degraded: boolean;
  } {
    const raw = body as Record<string, unknown>;
    const tools = raw.tools as
      | {
          name: string;
          description?: string;
          input_schema?: Record<string, unknown>;
        }[]
      | undefined;
    if (!body.messages || !Array.isArray(body.messages))
      return { normalized: 0, stubbed: 0, degraded: false };

    // Build set of known tool names
    const knownTools = new Set<string>();
    if (tools && Array.isArray(tools)) {
      for (const t of tools) {
        if (t.name) knownTools.add(t.name);
      }
    }

    // Build normalization map: lowercased + hyphens→underscores → original name
    const normalizedMap = new Map<string, string>();
    for (const name of knownTools) {
      normalizedMap.set(name.toLowerCase().replace(/-/g, "_"), name);
    }

    const resolveTool = (name: string): string | null => {
      if (knownTools.has(name)) return name;
      const normalized = normalizedMap.get(
        name.toLowerCase().replace(/-/g, "_"),
      );
      return normalized ?? null;
    };

    // Scan and fix tool references in conversation history
    let normalized = 0;
    const stillMissing: string[] = [];
    const scrubbedToolUseIds = new Set<string>();
    for (const msg of body.messages) {
      if (!Array.isArray(msg.content)) continue;
      for (const block of msg.content) {
        const b = block as Record<string, unknown>;
        if (block.type === "tool_use" && typeof b.name === "string") {
          const name = b.name as string;
          if (knownTools.has(name)) continue;

          const resolved = resolveTool(name);
          if (resolved) {
            b.name = resolved;
            normalized++;
            continue;
          }

          // Rescue mode: if an MCP tool is unavailable in this environment,
          // scrub the historical tool_use block so the live chat can continue.
          // Keep non-MCP missing tools on the legacy stub path below.
          if (name.startsWith("mcp__")) {
            if (typeof b.id === "string") scrubbedToolUseIds.add(b.id);
            delete b.id;
            delete b.name;
            delete b.input;
            b.type = "text";
            b.text = `[omitted unavailable tool call: ${name}]`;
            normalized++;
            continue;
          }

          stillMissing.push(name);
        }
      }
    }

    // Remove orphan tool_result blocks linked to scrubbed MCP tool_use blocks.
    if (scrubbedToolUseIds.size > 0) {
      for (const msg of body.messages) {
        if (!Array.isArray(msg.content)) continue;
        msg.content = msg.content.filter((block) => {
          if (block.type !== "tool_result") return true;
          const b = block as Record<string, unknown>;
          return !(
            typeof b.tool_use_id === "string" &&
            scrubbedToolUseIds.has(b.tool_use_id)
          );
        });
      }
      console.log(
        `[proxy] 🩹 Scrubbed ${scrubbedToolUseIds.size} unavailable MCP tool call(s) from history`,
      );
    }

    const dedupedStillMissing = [...new Set(stillMissing)];
    stillMissing.length = 0;
    stillMissing.push(...dedupedStillMissing);

    if (normalized > 0 || stillMissing.length > 0) {
      console.log(
        `[proxy] 🔧 Tool reference preflight: normalized=${normalized}, unresolved=${stillMissing.length}, degraded=${stillMissing.length > 3}`,
      );
    }

    // Add stubs for any still-unresolved references
    if (stillMissing.length === 0)
      return { normalized, stubbed: 0, degraded: false };

    console.log(
      `[proxy] Adding ${stillMissing.length} tool stub(s) for unresolved tool references: ${stillMissing.join(", ")}`,
    );

    if (!raw.tools || !Array.isArray(raw.tools)) {
      raw.tools = [];
    }
    const toolsArr = raw.tools as {
      name: string;
      description?: string;
      input_schema?: Record<string, unknown>;
    }[];
    for (const name of stillMissing) {
      const resolved = resolveTool(name) ?? name;
      if (toolsArr.some((t) => t.name === resolved)) continue;
      toolsArr.push({
        name: resolved,
        description: `Tool ${resolved} (stub — referenced in conversation history)`,
        input_schema: { type: "object" as const, properties: {} },
      });
    }
    return {
      normalized,
      stubbed: stillMissing.length,
      degraded: stillMissing.length > 3,
    };
  }

  /**
   * Remove thinking blocks from historical assistant messages.
   * CLIProxyAPI's Go code drops the `signature` field when re-serializing
   * thinking blocks, causing Anthropic to reject resumed conversations.
   * Stripping them preserves all text/tool content while avoiding the bug.
   */
  private stripThinkingFromHistory(body: MessagesRequest): void {
    if (!body.messages || !Array.isArray(body.messages)) return;

    let stripped = 0;
    for (const msg of body.messages) {
      if (msg.role !== "assistant") continue;
      if (!Array.isArray(msg.content)) continue;

      const original = msg.content.length;
      msg.content = msg.content.filter((block) => block.type !== "thinking");
      stripped += original - msg.content.length;
    }

    if (stripped > 0) {
      console.log(
        `[proxy] Stripped ${stripped} thinking blocks from history (CLIProxyAPI signature bug workaround)`,
      );
    }
  }

  /**
   * Fix empty text content blocks in messages.
   * Anthropic rejects requests where any text block has empty string content.
   * Non-Claude providers can produce these (e.g. Codex returning content: ""
   * alongside tool_calls). When preserved in conversation history, subsequent
   * requests to Claude fail with "text content blocks must be non-empty".
   */
  private fixEmptyTextBlocks(body: MessagesRequest): void {
    if (!body.messages || !Array.isArray(body.messages)) return;

    let fixed = 0;
    for (const msg of body.messages) {
      if (!Array.isArray(msg.content)) {
        // String content — replace empty string with space
        if (typeof msg.content === "string" && msg.content.length === 0) {
          msg.content = " ";
          fixed++;
        }
        continue;
      }

      // Array content — remove empty text blocks if there are other blocks,
      // or replace with space if it's the only block
      const hasNonTextBlocks = msg.content.some((b) => b.type !== "text");
      const originalLen = msg.content.length;

      if (hasNonTextBlocks) {
        // Remove empty text blocks — tool_use/tool_result blocks carry the message
        msg.content = msg.content.filter((block) => {
          if (block.type !== "text") return true;
          const text = (block as { type: "text"; text: string }).text;
          return text !== undefined && text !== null && text.length > 0;
        });
        fixed += originalLen - msg.content.length;
        // Guard: don't leave an empty content array
        if (msg.content.length === 0) {
          msg.content = [{ type: "text", text: " " }];
          fixed++;
        }
      } else {
        // Text-only message — replace empty text with space
        for (const block of msg.content) {
          if (block.type === "text") {
            const b = block as { type: "text"; text: string };
            if (!b.text || b.text.length === 0) {
              b.text = " ";
              fixed++;
            }
          }
        }
      }
    }

    if (fixed > 0) {
      console.log(`[proxy] Fixed ${fixed} empty text content block(s)`);
    }
  }

  /**
   * Remove orphaned tool_use blocks — tool_use blocks in assistant messages
   * that have no matching tool_result anywhere in the conversation.
   * This happens when concurrent tool execution is interrupted mid-turn.
   */
  private removeOrphanedToolUseBlocks(body: MessagesRequest): void {
    if (!body.messages || !Array.isArray(body.messages)) return;

    // Collect all tool_use ids across all assistant messages
    const allToolUseIds = new Set<string>();
    // Collect all answered tool_use ids (referenced by tool_result blocks)
    const answeredIds = new Set<string>();
    for (const msg of body.messages) {
      if (!Array.isArray(msg.content)) continue;
      for (const block of msg.content) {
        const b = block as Record<string, unknown>;
        if (b.type === "tool_use" && typeof b.id === "string") {
          allToolUseIds.add(b.id);
        }
        if (b.type === "tool_result" && typeof b.tool_use_id === "string") {
          answeredIds.add(b.tool_use_id);
        }
      }
    }

    // Remove unanswered tool_use blocks from assistant messages —
    // but skip the LAST message if it's an assistant turn (those tool_use blocks
    // are the current pending calls, not orphans).
    const lastMsg = body.messages[body.messages.length - 1];
    const skipLast = lastMsg?.role === "assistant";

    let totalRemoved = 0;
    for (let i = 0; i < body.messages.length; i++) {
      if (skipLast && i === body.messages.length - 1) continue;
      const msg = body.messages[i]!;
      if (msg.role !== "assistant" || !Array.isArray(msg.content)) continue;
      const before = msg.content.length;
      msg.content = msg.content.filter((block: Record<string, unknown>) => {
        if (block.type !== "tool_use") return true;
        return answeredIds.has(block.id as string);
      });
      totalRemoved += before - msg.content.length;
      if (msg.content.length === 0) {
        msg.content = [{ type: "text", text: " " }];
      }
    }

    if (totalRemoved > 0) {
      console.log(
        `[proxy] 🔧 Pre-flight: removed ${totalRemoved} orphaned tool_use block(s) from history`,
      );
    }

    // Rebuild tool_use ID set after removal — some IDs may have been removed above
    const remainingToolUseIds = new Set<string>();
    for (const msg of body.messages) {
      if (msg.role !== "assistant" || !Array.isArray(msg.content)) continue;
      for (const block of msg.content) {
        const b = block as Record<string, unknown>;
        if (b.type === "tool_use" && typeof b.id === "string") {
          remainingToolUseIds.add(b.id);
        }
      }
    }

    // Remove orphaned tool_result blocks — tool_result blocks in user messages
    // that reference tool_use IDs that don't exist in any assistant message.
    let orphanResults = 0;
    for (const msg of body.messages) {
      if (msg.role !== "user" || !Array.isArray(msg.content)) continue;
      const before = msg.content.length;
      msg.content = msg.content.filter((block: Record<string, unknown>) => {
        if (block.type !== "tool_result") return true;
        const toolUseId = block.tool_use_id as string;
        return (
          typeof toolUseId === "string" &&
          toolUseId.length > 0 &&
          remainingToolUseIds.has(toolUseId)
        );
      });
      orphanResults += before - msg.content.length;
      if (msg.content.length === 0) {
        msg.content = [{ type: "text", text: " " }];
      }
    }

    if (orphanResults > 0) {
      console.log(
        `[proxy] 🔧 Pre-flight: removed ${orphanResults} orphaned tool_result block(s) from history`,
      );
    }
  }

  /**
   * Ensure no messages have empty content arrays, and fix role alternation.
   * Multiple sanitization passes can strip all blocks from a message,
   * leaving content: [] which Anthropic rejects. Consecutive same-role
   * messages (after block removal) also cause 400 errors.
   */
  private ensureNonEmptyContent(body: MessagesRequest): void {
    if (!body.messages || !Array.isArray(body.messages)) return;

    // First pass: ensure no empty content arrays
    for (const msg of body.messages) {
      if (Array.isArray(msg.content) && msg.content.length === 0) {
        msg.content = [{ type: "text", text: " " }];
      }
    }

    // Second pass: merge consecutive same-role messages to fix alternation.
    // After orphan removal, two assistant or two user messages can end up
    // adjacent, which Anthropic rejects.
    let i = 0;
    while (i < body.messages.length - 1) {
      const curr = body.messages[i]!;
      const next = body.messages[i + 1]!;
      if (curr.role === next.role) {
        // Merge next into curr
        const currContent = Array.isArray(curr.content)
          ? curr.content
          : [{ type: "text" as const, text: String(curr.content ?? " ") }];
        const nextContent = Array.isArray(next.content)
          ? next.content
          : [{ type: "text" as const, text: String(next.content ?? " ") }];
        curr.content = [...currContent, ...nextContent];
        body.messages.splice(i + 1, 1);
        // Don't increment — check the new next message
      } else {
        i++;
      }
    }
  }

  /**
   * Compact conversation history when context length is exceeded.
   * Trims old messages while preserving tool_use/tool_result boundaries.
   * Keeps ~40% of messages from the most recent clean boundary point.
   */
  private compactMessages(body: MessagesRequest): boolean {
    const messages = body.messages;
    if (!messages || messages.length <= 6) return false;

    // If history is already short, force a lighter compact to still salvage the request.
    if (messages.length <= 10) {
      const keep = Math.max(4, Math.floor(messages.length * 0.6));
      const originalCount = messages.length;
      body.messages = messages.slice(messages.length - keep);
      console.log(
        `[proxy] 📦 Compact-lite conversation: ${originalCount} → ${body.messages.length} messages`,
      );
      return true;
    }

    // Target: keep roughly the last 40% of messages, minimum 10
    const targetKeep = Math.max(10, Math.floor(messages.length * 0.4));
    const searchStart = messages.length - targetKeep;

    // Scan forward from searchStart to find a clean user message boundary
    // (user message with no tool_result blocks — safe to cut before)
    let cutPoint = -1;
    for (let i = searchStart; i < messages.length - 4; i++) {
      const msg = messages[i]!;
      if (msg.role !== "user") continue;
      if (typeof msg.content === "string") {
        cutPoint = i;
        break;
      }
      if (
        Array.isArray(msg.content) &&
        !msg.content.some((b) => b.type === "tool_result")
      ) {
        cutPoint = i;
        break;
      }
    }

    if (cutPoint === -1 || cutPoint >= messages.length - 4) return false;

    const originalCount = messages.length;
    body.messages = messages.slice(cutPoint);
    console.log(
      `[proxy] 📦 Compacted conversation: ${originalCount} → ${body.messages.length} messages`,
    );
    return true;
  }

  /**
   * Auto-compact via summarization — uses a fast/cheap model to summarize
   * the conversation history, then reconstructs a minimal context that
   * preserves coding intent without losing track of what was being done.
   * Returns the summary text, or null if summarization failed.
   */
  private async autoCompact(body: MessagesRequest): Promise<boolean> {
    const messages = body.messages;
    if (!messages || messages.length < 2) return false;

    // Build a text representation of the conversation for summarization
    const conversationText = this.extractConversationText(messages);
    if (conversationText.length < 500) return false; // too short to bother

    // Find a fast provider+account for the summarization call
    const summarizeAccount = this.findSummarizationAccount();
    if (!summarizeAccount) {
      console.log(
        `[proxy] ⚠️ Auto-compact: no summarization provider available, falling back to structural compact`,
      );
      return this.structuralCompact(body);
    }

    const { account, provider, model } = summarizeAccount;

    const summarizeRequest: MessagesRequest = {
      model,
      max_tokens: 4096,
      stream: false,
      messages: [
        {
          role: "user",
          content: `You are a coding session compactor. Summarize the following conversation between a developer and an AI coding assistant into a concise context document. Preserve:

1. **Current task**: What is being worked on right now
2. **Files modified**: List of files that were created/edited and key changes
3. **Decisions made**: Important technical decisions and their rationale
4. **Current state**: Where the work left off, what's done and what remains
5. **Key code context**: Any important variable names, function signatures, or patterns being used

Be concise but complete. This summary will replace the conversation history so the assistant can continue working without losing context.

<conversation>
${conversationText.slice(0, 80000)}
</conversation>`,
        },
      ],
    };

    const abortController = new AbortController();
    const timeout = setTimeout(() => abortController.abort(), 30_000);
    try {
      console.log(
        `[proxy] 📝 Auto-compact: summarizing ${messages.length} messages via ${account.provider}/${model}`,
      );
      this.tokenManager.markUsed(account.id);
      this.tokenManager.incrementActive(account.id);
      let result;
      try {
        result = await provider.sendRequest(
          account,
          summarizeRequest,
          abortController.signal,
        );
      } finally {
        this.tokenManager.decrementActive(account.id);
      }
      clearTimeout(timeout);

      if (!result.success || !result.body) {
        console.log(
          `[proxy] ⚠️ Auto-compact summarization failed, falling back to structural compact`,
        );
        return this.structuralCompact(body);
      }

      // Extract summary text from response
      const responseBody = result.body as unknown as Record<string, unknown>;
      const content = responseBody.content as
        | Array<{ type: string; text?: string }>
        | undefined;
      const summary = content?.find((b) => b.type === "text")?.text;

      if (!summary || summary.length < 50) {
        console.log(
          `[proxy] ⚠️ Auto-compact: empty summary, falling back to structural compact`,
        );
        return this.structuralCompact(body);
      }

      // Reconstruct: keep last 2 conversation turns + inject summary as context
      const keepCount = Math.min(4, messages.length);
      const recentMessages = messages.slice(-keepCount);

      body.messages = [
        {
          role: "user",
          content: `[Auto-compacted conversation summary — the previous conversation was too long and has been summarized to continue the session]\n\n${summary}`,
        },
        {
          role: "assistant",
          content:
            "I have the context from the compacted conversation summary. I'll continue from where we left off.",
        },
        ...recentMessages,
      ];

      console.log(
        `[proxy] 📝 Auto-compact complete: ${messages.length} → ${body.messages.length} messages (summary: ${summary.length} chars)`,
      );
      return true;
    } catch (e) {
      clearTimeout(timeout);
      console.log(
        `[proxy] ⚠️ Auto-compact error: ${e instanceof Error ? e.message : String(e)}, falling back to structural compact`,
      );
      return this.structuralCompact(body);
    }
  }

  /**
   * Structural compact — no LLM call, just smart truncation of content blocks.
   * Keeps message structure intact but shrinks large tool_result/text blocks.
   * Fallback when summarization is unavailable.
   */
  private structuralCompact(body: MessagesRequest): boolean {
    const MAX_BLOCK_CHARS = 3000;
    const HEAD_CHARS = 1500;
    const TAIL_CHARS = 1200;
    let changed = false;

    for (const msg of body.messages ?? []) {
      if (!Array.isArray(msg.content)) continue;
      for (const block of msg.content) {
        if (
          block.type === "tool_result" &&
          typeof block.content === "string" &&
          block.content.length > MAX_BLOCK_CHARS
        ) {
          block.content =
            block.content.slice(0, HEAD_CHARS) +
            "\n\n[... " +
            (block.content.length - HEAD_CHARS - TAIL_CHARS) +
            " chars compacted ...]\n\n" +
            block.content.slice(-TAIL_CHARS);
          changed = true;
        }
        if (
          block.type === "text" &&
          typeof block.text === "string" &&
          block.text.length > MAX_BLOCK_CHARS
        ) {
          block.text =
            block.text.slice(0, HEAD_CHARS) +
            "\n\n[... " +
            (block.text.length - HEAD_CHARS - TAIL_CHARS) +
            " chars compacted ...]\n\n" +
            block.text.slice(-TAIL_CHARS);
          changed = true;
        }
      }
    }

    // Slice to last 6 messages as final measure
    if (body.messages && body.messages.length > 8) {
      const originalCount = body.messages.length;
      body.messages = body.messages.slice(-6);
      console.log(
        `[proxy] 📦 Structural compact: ${originalCount} → ${body.messages.length} messages`,
      );
      changed = true;
    }

    if (changed) {
      console.log(
        `[proxy] 📦 Structural compact: truncated large content blocks`,
      );
    }
    return changed;
  }

  /** Extract readable text from messages for summarization */
  private extractConversationText(
    messages: MessagesRequest["messages"],
  ): string {
    const parts: string[] = [];
    for (const msg of messages ?? []) {
      const role = msg.role.toUpperCase();
      if (typeof msg.content === "string") {
        parts.push(`${role}: ${msg.content}`);
      } else if (Array.isArray(msg.content)) {
        for (const block of msg.content) {
          if (block.type === "text" && block.text) {
            parts.push(`${role}: ${block.text}`);
          } else if (block.type === "tool_use") {
            parts.push(
              `${role} [tool_use: ${block.name}(${JSON.stringify(block.input).slice(0, 500)})]`,
            );
          } else if (block.type === "tool_result") {
            const content =
              typeof block.content === "string"
                ? block.content
                : JSON.stringify(block.content);
            parts.push(
              `${role} [tool_result: ${(content ?? "").slice(0, 1000)}]`,
            );
          }
        }
      }
    }
    return parts.join("\n\n");
  }

  /** Find a fast/cheap account+provider for the summarization call */
  private findSummarizationAccount(): {
    account: ProviderAccount;
    provider: BaseProvider;
    model: string;
  } | null {
    // Prefer: glm (free budget tier) → gemini (flash) → minimax → codex
    const candidates: Array<{ providerName: Provider; model: string }> = [
      { providerName: "glm", model: "glm-4.7-flash" },
      { providerName: "gemini", model: "gemini-2.5-flash" },
      { providerName: "minimax", model: "MiniMax-M2.5-highspeed" },
      { providerName: "codex", model: "gpt-5.1-codex-mini" },
    ];

    for (const { providerName, model } of candidates) {
      const provider = this.providers.get(providerName);
      if (!provider) continue;
      const accounts = this.tokenManager.getHealthyAccounts(providerName);
      if (accounts.length === 0) continue;
      return { account: accounts[0]!, provider, model };
    }
    return null;
  }

  private detectMode(
    body: MessagesRequest,
    req: Request,
  ): "interactive" | "fleet" {
    if (req.headers.get("x-claudemax-mode") === "fleet") return "fleet";
    if (
      body.metadata &&
      typeof body.metadata === "object" &&
      (body.metadata as Record<string, unknown>).agent_type === "fleet"
    )
      return "fleet";
    return "interactive";
  }

  private async handleMessages(
    req: Request,
    _reentryGuard = false,
  ): Promise<Response> {
    let body: MessagesRequest;
    try {
      body = (await req.json()) as MessagesRequest;
    } catch {
      return Response.json(
        {
          error: {
            type: "invalid_request_error",
            message: "Invalid JSON body",
          },
        },
        { status: 400 },
      );
    }

    if (!body.model || !body.messages) {
      return Response.json(
        {
          error: {
            type: "invalid_request_error",
            message: "Missing required fields: model, messages",
          },
        },
        { status: 400 },
      );
    }

    // Sanitize request: strip Claude Code-specific fields that providers reject
    this.sanitizeRequest(body);

    const mode = this.detectMode(body, req);
    const retryConfig = mode === "fleet" ? FLEET_RETRY : INTERACTIVE_RETRY;

    const isStreaming = body.stream === true;
    const startTime = Date.now();
    let lastError = "";
    let toolRegistryUnhealthy = false;

    const initialToolHealth = this.ensureToolReferences(body);
    if (initialToolHealth.degraded) {
      toolRegistryUnhealthy = true;
    }

    // Capture original tier BEFORE any mutations (thinking strip, etc.)
    // so routing decisions stay consistent across retries.
    const originalTier = this.router.classifyTier(body);
    const originalHadThinking = !!body.thinking;

    let lastErrorClass: ErrorClass = "unknown";
    const failedAccountIds = new Set<string>();
    const providerFailCounts = new Map<string, number>();
    const excludeProviders = new Set<string>();
    let thinkingStripped = false; // tracked locally — never set on request body
    let contextLengthFallback = false; // phase 1: tried large-context providers
    let contextCompacted = false; // phase 2: compacted conversation history
    let contextAggressivelyTruncated = false; // phase 3: aggressively truncated large content blocks
    let contextLengthFailedProviders = new Set<string>(); // providers that failed context_length
    const toolStubsAdded = new Set<string>();
    let toolConcurrencyPasses = 0; // guard: orphaned tool_use fix fires at most N times
    let emptyContentFixed = false; // guard: empty user message fix fires at most once
    let resetBonusAttempts = 0; // extra attempts granted by strategy resets (thinking strip, context fallback)
    const MAX_BONUS_ATTEMPTS = 5; // cap total bonus retries to prevent unbounded loops
    let totalAttempts = 0; // monotonic counter — never reset
    const ABSOLUTE_MAX_ATTEMPTS =
      retryConfig.maxRetries + MAX_BONUS_ATTEMPTS + 2; // 2 = context fallback + probe
    let lastResortProbed = false; // guard: last-resort probe fires at most once
    let contextFallbackBonusUsed = false; // guard: context fallback bonus increments at most once
    let floorBypassed = false; // guard: quality floor bypass fires at most once

    for (
      let attempt = 0;
      attempt < retryConfig.maxRetries + resetBonusAttempts;
      attempt++
    ) {
      const attemptStartTime = Date.now();
      totalAttempts++;
      if (totalAttempts > ABSOLUTE_MAX_ATTEMPTS) {
        console.log(
          `[proxy] 🛑 Absolute attempt cap reached (${totalAttempts}/${ABSOLUTE_MAX_ATTEMPTS}), breaking retry loop`,
        );
        break;
      }

      // Pre-flight: if CLIProxyAPI is known-unhealthy, exclude all proxy providers
      // upfront to avoid wasting retries on a broken dependency.
      // If proxy recovered (latch expired or watchdog cleared), re-include providers
      // that were excluded due to proxy health, not per-provider failures.
      if (!this.isProxyHealthy()) {
        for (const p of PROXY_PROVIDERS) excludeProviders.add(p);
      } else {
        for (const p of PROXY_PROVIDERS) {
          if (excludeProviders.has(p) && (providerFailCounts.get(p) ?? 0) < 2) {
            excludeProviders.delete(p);
          }
        }
      }

      // Backoff before retry (skip first attempt)
      if (attempt > 0) {
        const backoffMs = getBackoffMs(
          attempt - 1,
          retryConfig.backoff,
          lastErrorClass,
        );
        console.log(
          `[proxy] ⏳ backoff ${backoffMs}ms (attempt ${attempt + 1}, ${lastErrorClass})`,
        );
        await sleep(backoffMs);
      }

      // Pre-flight: proactively exclude providers where ALL accounts are rate-limited
      // This prevents wasting retry attempts on providers that will definitely fail
      for (const providerName of this.providers.keys()) {
        if (excludeProviders.has(providerName)) continue;
        const healthy = this.tokenManager.getHealthyAccounts(
          providerName as Provider,
        );
        const all = this.tokenManager
          .getAllAccounts()
          .filter((a) => a.provider === providerName);
        if (all.length > 0 && healthy.length === 0) {
          excludeProviders.add(providerName);
          console.log(
            `[proxy] ⏭️ Pre-flight skip: ${providerName} — all ${all.length} accounts rate-limited/unhealthy`,
          );
        }
      }

      // When thinking was stripped, preserve the original premium tier
      // so routing doesn't silently downgrade to standard/fast models.
      const tierOverride = thinkingStripped ? originalTier : undefined;
      const route = this.router.getNextRoute(
        body,
        failedAccountIds,
        excludeProviders,
        tierOverride,
        floorBypassed ? { bypassFloor: true } : undefined,
      );

      // Log routing intent on first attempt for observability
      if (attempt === 0) {
        const routeInfo = this.router.getLastRouteExplanation();
        if (routeInfo) {
          console.log(
            `[proxy] 🎯 ${routeInfo.explanation ?? `lane=${routeInfo.lane} task_kind=${routeInfo.task_kind} role=${routeInfo.role}`}`,
          );
        }
      }

      if (!route) {
        // All providers exhausted — return appropriate status based on last error
        if (lastErrorClass === "auth_error") {
          return Response.json(
            {
              error: {
                type: "authentication_error",
                message: `All providers returned auth errors. Last: ${lastError}`,
              },
            },
            { status: 401 },
          );
        }
        if (lastErrorClass === "model_unavailable") {
          return Response.json(
            {
              error: {
                type: "not_found_error",
                message: `Model not available on any provider. Last: ${lastError}`,
              },
            },
            { status: 404 },
          );
        }
        // If thinking was enabled but all reasoning providers exhausted,
        // strip thinking and retry with any available provider.
        // Note: original tier (premium) is preserved — classifyTier() won't
        // see thinking anymore but we override with originalTier below.
        if ((body.thinking || originalHadThinking) && !thinkingStripped) {
          console.log(
            `[proxy] ⚠️ All reasoning providers exhausted — stripping thinking and retrying (preserving tier=${originalTier})`,
          );
          delete body.thinking;
          thinkingStripped = true;
          failedAccountIds.clear();
          excludeProviders.clear();
          providerFailCounts.clear();
          if (resetBonusAttempts < MAX_BONUS_ATTEMPTS) {
            resetBonusAttempts++;
          }
          continue;
        }

        // Context length — if routing exhausted because providers were excluded
        // by the context_length handler, try compaction phases here before giving up.
        // This catches the case where retry attempts are burned and the for-loop
        // would exit at the !route check before the context_length handler fires.
        if (lastErrorClass === "context_length") {
          if (!contextCompacted) {
            console.log(
              `[proxy] ⚠️ Route exhausted with context_length — compacting conversation`,
            );
            const compacted = this.compactMessages(body);
            if (compacted) {
              this.sanitizeRequest(body);
              contextCompacted = true;
              failedAccountIds.clear();
              excludeProviders.clear();
              providerFailCounts.clear();
              contextLengthFailedProviders.clear();
              if (resetBonusAttempts < MAX_BONUS_ATTEMPTS) resetBonusAttempts++;
              continue;
            }
          }
          if (!contextAggressivelyTruncated) {
            console.log(
              `[proxy] ⚠️ Route exhausted with context_length — attempting auto-compact`,
            );
            const didCompact = await this.autoCompact(body);
            if (didCompact) {
              this.sanitizeRequest(body);
              contextAggressivelyTruncated = true;
              failedAccountIds.clear();
              excludeProviders.clear();
              providerFailCounts.clear();
              contextLengthFailedProviders.clear();
              if (resetBonusAttempts < MAX_BONUS_ATTEMPTS) resetBonusAttempts++;
              continue;
            }
          }
        }

        // Last-resort probe: before giving up with 529, check if CLIProxyAPI
        // is actually alive. If the latch/exclusions are stale, clear them and retry.
        // Guarded: fires at most once per handleMessages() call.
        if (excludeProviders.size > 0 && !lastResortProbed) {
          try {
            const probe = await fetch("http://localhost:8317/", {
              signal: AbortSignal.timeout(3000),
            });
            // Drain response body to avoid connection leak
            await probe.text().catch(() => {});
            if (probe.ok) {
              console.log(
                `[proxy] 🔄 Last-resort probe: CLIProxyAPI alive — clearing proxy-health exclusions only`,
              );
              lastResortProbed = true;
              this.cliProxyUnhealthyUntil = 0;
              // Only clear proxy-health exclusions — preserve per-provider
              // failures (auth errors, context_length, etc.)
              for (const p of PROXY_PROVIDERS) {
                if (
                  excludeProviders.has(p) &&
                  (providerFailCounts.get(p) ?? 0) < 2
                ) {
                  excludeProviders.delete(p);
                }
              }
              if (resetBonusAttempts < MAX_BONUS_ATTEMPTS) {
                resetBonusAttempts++;
              }
              continue;
            }
          } catch {
            // Probe failed — proxy is genuinely down
          }
        }

        // Auto-degrade: if the quality floor is blocking all routes and we'd 529,
        // temporarily bypass the floor to use lower-tier providers. The next request
        // will naturally route back to the correct tier when providers recover.
        if (
          !floorBypassed &&
          (originalTier === "premium" || originalTier === "standard")
        ) {
          console.log(
            `[proxy] ⚠️ Quality floor exhausted all ${originalTier} providers — auto-degrading to any available tier`,
          );
          floorBypassed = true;
          failedAccountIds.clear();
          excludeProviders.clear();
          providerFailCounts.clear();
          if (resetBonusAttempts < MAX_BONUS_ATTEMPTS) {
            resetBonusAttempts++;
          }
          continue;
        }

        return Response.json(
          {
            error: {
              type: "overloaded_error",
              message: "No healthy providers available",
            },
          },
          { status: 529 },
        );
      }

      const { account, model: translatedModel, tier } = route;
      const originalModel = body.model;
      const routedRequest = this.sanitizeOutboundRequest({
        ...body,
        model: translatedModel,
      });
      const decisionKey = `${tier}:${account.provider}:${translatedModel}`;
      this.telemetry.route_decisions[decisionKey] =
        (this.telemetry.route_decisions[decisionKey] ?? 0) + 1;
      if (translatedModel !== originalModel) {
        this.telemetry.fallback_routes++;
      }

      const provider = this.providers.get(account.provider);
      if (!provider) {
        console.error(
          `[proxy] No provider adapter for ${account.provider}, excluding`,
        );
        lastError = `No provider adapter for ${account.provider}`;
        excludeProviders.add(account.provider);
        continue;
      }

      // Resolve tier timeout — but don't start the timer until we have slots
      const timeoutMs = TIER_TIMEOUT[tier] ?? 120_000;
      const abortController = new AbortController();

      // Acquire concurrency slots FIRST — queue/provider waits should NOT
      // consume the per-tier request timeout budget
      // Queue cap: rejects with QueueFullError if > GLOBAL_MAX_PENDING waiting
      // Queue timeout: rejects with QueueTimeoutError if waiting > QUEUE_TIMEOUT_MS
      const providerSem = this.providerSemaphores.get(account.provider);
      let globalAcquired = false;
      let accountActive = false;
      let streamOwnsCleanup = false;
      try {
        await this.globalSemaphore.acquire(QUEUE_TIMEOUT_MS);
        globalAcquired = true;
      } catch (e) {
        if (e instanceof QueueFullError || e instanceof QueueTimeoutError) {
          this.telemetry.queue_rejects++;
          console.warn(`[proxy] ${e.name}: ${e.message}`);
          return Response.json(
            {
              error: {
                type: "overloaded_error",
                message: `Server overloaded — ${e.message}. Try again shortly.`,
              },
            },
            { status: 529 },
          );
        }
        throw e;
      }
      let providerAcquired = false;
      if (providerSem) {
        try {
          await providerSem.acquire(PROVIDER_SEM_TIMEOUT_MS);
          providerAcquired = true;
        } catch {
          globalAcquired = false;
          this.globalSemaphore.release();
          failedAccountIds.add(account.id);
          lastErrorClass = "timeout";
          lastError = `Provider ${account.provider} congested (semaphore timeout)`;
          console.log(
            `[proxy] ⏳ Provider semaphore timeout for ${account.provider}/${account.id}, trying next`,
          );
          continue;
        }
      }

      // START tier timeout AFTER acquiring slots — time only counts
      // actual provider work, not queue waiting
      const timeout = setTimeout(() => abortController.abort(), timeoutMs);

      try {
        console.log(
          `[proxy] → ${account.provider}/${account.id} (attempt ${attempt + 1}/${retryConfig.maxRetries}, ${mode}, tier=${tier}, timeout=${timeoutMs}ms)`,
        );
        this.tokenManager.markUsed(account.id);
        this.tokenManager.incrementActive(account.id);
        accountActive = true;

        const result = await provider.sendRequest(
          account,
          routedRequest,
          abortController.signal,
        );
        clearTimeout(timeout);

        if (result.success) {
          this.tokenManager.recordSuccess(account.id);

          // Proactive quota tracking from response headers
          if (result.rateLimitHeaders) {
            const rl = result.rateLimitHeaders;
            this.tokenManager.updateQuota(
              account.id,
              rl.remaining,
              rl.limit,
              rl.resetSeconds,
            );
          }

          // Routing indicator headers — shows what provider/model/tier was used
          const routingHeaders: Record<string, string> = {
            "x-claudemax-provider": account.provider,
            "x-claudemax-model": translatedModel,
            "x-claudemax-tier": tier,
            "x-claudemax-attempt": String(attempt + 1),
          };
          if (floorBypassed) {
            routingHeaders["x-claudemax-degraded"] =
              `floor-bypassed:${originalTier}`;
          }

          if (isStreaming && result.response && result.response.body) {
            // OpenAI-compat providers (codex, gemini, glm, minimax, kimi, openrouter)
            // already rewrite the model name inside translateStream().
            // Only Claude/Antigravity native streams need rewriteStreamModel.
            // Applying rewriteStreamModel to a translateStream output causes a
            // deadlock: both are pull-based ReadableStreams and the 500-byte buffer
            // in rewriteStreamModel prevents the inner stream's pull from completing.
            const isNativeAnthropicStream =
              account.provider === "claude" ||
              account.provider === "antigravity";
            const needsModelRewrite =
              translatedModel !== originalModel && isNativeAnthropicStream;
            const streamBody = needsModelRewrite
              ? this.rewriteStreamModel(
                  result.response.body,
                  translatedModel,
                  originalModel,
                )
              : result.response.body;

            // Transfer semaphore ownership to the stream — cleanup happens
            // when the stream finishes, not in the finally block.
            const cleanupStream = () => {
              if (accountActive) {
                this.tokenManager.decrementActive(account.id);
                accountActive = false;
              }
              if (providerSem && providerAcquired) {
                providerSem.release();
                providerAcquired = false;
              }
              if (globalAcquired) {
                this.globalSemaphore.release();
                globalAcquired = false;
              }
            };

            let usageWrappedStream: ReadableStream<Uint8Array>;
            try {
              usageWrappedStream = this.trackStreamingUsage(
                streamBody,
                {
                  provider: account.provider,
                  model: body.model,
                  accountId: account.id,
                  startedAt: attemptStartTime,
                },
                cleanupStream,
              );
            } catch (streamSetupErr) {
              cleanupStream();
              throw streamSetupErr;
            }
            streamOwnsCleanup = true;

            return new Response(usageWrappedStream, {
              status: 200,
              headers: {
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                Connection: "keep-alive",
                ...routingHeaders,
              },
            });
          }

          if (result.body) {
            // Restore original model name so Claude Code sees what it requested
            result.body.model = originalModel;

            this.usageDB.record({
              timestamp: Date.now(),
              provider: account.provider,
              model: body.model,
              account_id: account.id,
              input_tokens: result.body.usage?.input_tokens ?? 0,
              output_tokens: result.body.usage?.output_tokens ?? 0,
              latency_ms: Date.now() - attemptStartTime,
              status: "success",
              error: null,
            });

            return new Response(JSON.stringify(result.body), {
              status: 200,
              headers: {
                "Content-Type": "application/json",
                ...routingHeaders,
              },
            });
          }
        }

        // Request failed — clear timeout to avoid timer leak
        clearTimeout(timeout);
        lastError = result.error ?? "Unknown error";
        lastErrorClass = result.errorClass ?? "unknown";
        console.log(
          `[proxy] ✗ ${account.id}: ${lastErrorClass} — ${lastError.slice(0, 200)}`,
        );

        failedAccountIds.add(account.id);
        const pFails = (providerFailCounts.get(account.provider) ?? 0) + 1;
        providerFailCounts.set(account.provider, pFails);
        if (pFails >= 2) {
          excludeProviders.add(account.provider);
          this.telemetry.provider_exclusions++;
        }

        // Fast provider exclusion on timeout/connection_error for proxy providers.
        // When upstream API is having connectivity issues (TCP timeouts, TLS failures),
        // ALL accounts for that provider will fail the same way. Don't burn retries
        // waiting 180s per account — exclude the provider after the FIRST timeout
        // so the retry loop can immediately try codex/kimi/gemini instead.
        if (
          (lastErrorClass === "timeout" ||
            lastErrorClass === "connection_error") &&
          PROXY_PROVIDERS.has(account.provider) &&
          !excludeProviders.has(account.provider)
        ) {
          // Only fast-exclude if the proxy itself is up (upstream issue, not proxy down)
          const proxyLikelyUp = this.isProxyHealthy();
          if (proxyLikelyUp) {
            excludeProviders.add(account.provider);
            this.telemetry.provider_exclusions++;
            // Claude and antigravity both route to Anthropic API — if one times
            // out due to upstream issues, the other will too. Exclude both.
            const ANTHROPIC_PROVIDERS = new Set(["claude", "antigravity"]);
            if (ANTHROPIC_PROVIDERS.has(account.provider)) {
              for (const ap of ANTHROPIC_PROVIDERS) {
                if (!excludeProviders.has(ap)) {
                  excludeProviders.add(ap);
                  this.telemetry.provider_exclusions++;
                }
              }
              console.log(
                `[proxy] ⚡ Fast-excluding claude+antigravity after ${lastErrorClass} — Anthropic upstream likely degraded`,
              );
            } else {
              console.log(
                `[proxy] ⚡ Fast-excluding ${account.provider} after ${lastErrorClass} — upstream likely degraded, trying other providers`,
              );
            }
          }
        }

        // Track rate limits with reset timing
        if (lastErrorClass === "rate_limit") {
          this.tokenManager.recordRateLimit(account.id, lastError);
        }

        // auth_unavailable: CLIProxyAPI has no usable token for this provider.
        // This is systemic — park ALL accounts for the provider (not just the
        // one that failed) and exclude the provider for this request.
        if (lastErrorClass === "auth_unavailable") {
          const parkMs = 5 * 60_000;
          const parkUntil = Date.now() + parkMs;
          const allProviderAccounts = this.tokenManager
            .getAllAccounts()
            .filter((a) => a.provider === account.provider);
          for (const a of allProviderAccounts) {
            this.tokenManager.parkUntil(a.id, parkUntil);
          }
          excludeProviders.add(account.provider);
          this.telemetry.provider_exclusions++;
          console.log(
            `[proxy] 🅿️ Parked all ${allProviderAccounts.length} ${account.provider} accounts for 5m (auth_unavailable)`,
          );
        }

        // Only trip circuit breaker for per-account errors, not:
        // - connection_error: systemic (CLIProxyAPI down), not account-specific
        // - timeout: function of tier timeout config, not account health
        // - rate_limit: already handled by recordRateLimit() with self-healing
        // - auth_unavailable: handled by parking above
        // - reasoning_content errors: translation issue, not provider health
        const isTranslationError =
          lastErrorClass === "invalid_request" &&
          lastError.includes("reasoning_content");
        if (
          lastErrorClass !== "connection_error" &&
          lastErrorClass !== "timeout" &&
          lastErrorClass !== "rate_limit" &&
          lastErrorClass !== "auth_unavailable" &&
          !isTranslationError
        ) {
          this.tokenManager.recordFailure(account.id, lastError);
        }

        // Connection error from proxy provider — distinguish between:
        // 1. Proxy DOWN (ECONNREFUSED, fetch failed): exclude ALL proxy providers + latch
        // 2. Proxy GARBLED (responded with gzip/binary body): retry same provider, no latch
        // Garbled responses start with an HTTP status code (e.g. "400: ...", "429: ..."),
        // true connectivity failures do not.
        if (
          lastErrorClass === "connection_error" &&
          PROXY_PROVIDERS.has(account.provider)
        ) {
          const proxyIsDown = !/^\d{3}:/.test(lastError);
          if (proxyIsDown) {
            for (const p of PROXY_PROVIDERS) excludeProviders.add(p);
            this.markProxyUnhealthy(30_000); // 30s latch
            this.telemetry.proxy_resets++;
            console.log(
              `[proxy] CLIProxyAPI down — excluding all proxy providers (latch 30s)`,
            );
          } else {
            console.log(
              `[proxy] Garbled response from ${account.provider} (proxy alive) — retrying with different account`,
            );
          }
        }

        this.usageDB.record({
          timestamp: Date.now(),
          provider: account.provider,
          model: body.model,
          account_id: account.id,
          input_tokens: 0,
          output_tokens: 0,
          latency_ms: Date.now() - attemptStartTime,
          status: "error",
          error: lastError.slice(0, 500),
        });

        // Thinking/reasoning_content errors — provider requires reasoning_content
        // on assistant messages but thinking blocks were stripped. Strip thinking
        // from the request entirely and retry with a different provider.
        if (
          lastErrorClass === "invalid_request" &&
          lastError.includes("reasoning_content") &&
          !thinkingStripped
        ) {
          console.log(
            `[proxy] ⚠️ Provider ${account.provider} requires reasoning_content — stripping thinking and retrying`,
          );
          delete body.thinking;
          thinkingStripped = true;
          // Exclude this provider and retry — it can't handle this request
          excludeProviders.add(account.provider);
          if (resetBonusAttempts < MAX_BONUS_ATTEMPTS) {
            resetBonusAttempts++;
          }
          continue;
        }

        // Tool use concurrency errors — orphaned tool_use/tool_result blocks,
        // or role alternation violations caused by block removal.
        // Fix by scanning ALL messages and running full sanitization, then retry.
        if (
          lastErrorClass === "invalid_request" &&
          (lastError.includes("tool_use") ||
            lastError.includes("tool_result") ||
            lastError.includes("roles must alternate")) &&
          toolConcurrencyPasses < 3
        ) {
          const messages = body.messages ?? [];

          // Collect ALL tool_use ids and ALL tool_result ids
          const allToolUseIds = new Set<string>();
          const allAnsweredIds = new Set<string>();
          for (const m of messages) {
            if (!Array.isArray(m?.content)) continue;
            for (const block of m.content) {
              const b = block as Record<string, unknown>;
              if (b.type === "tool_use" && typeof b.id === "string") {
                allToolUseIds.add(b.id);
              }
              if (
                b.type === "tool_result" &&
                typeof b.tool_use_id === "string"
              ) {
                allAnsweredIds.add(b.tool_use_id);
              }
            }
          }

          let totalRemoved = 0;

          // Remove orphaned tool_use blocks (no matching tool_result)
          for (let i = 0; i < messages.length; i++) {
            const msg = messages[i];
            if (msg?.role !== "assistant" || !Array.isArray(msg.content))
              continue;

            const before = msg.content.length;
            msg.content = msg.content.filter(
              (block: Record<string, unknown>) => {
                if (block.type !== "tool_use") return true;
                return allAnsweredIds.has(block.id as string);
              },
            );
            const removed = before - msg.content.length;
            if (removed > 0) {
              console.log(
                `[proxy] 🔧 Removed ${removed} orphaned tool_use block(s) from messages[${i}]`,
              );
              totalRemoved += removed;
            }
            if (msg.content.length === 0) {
              msg.content = [{ type: "text", text: " " }];
            }
          }

          // Rebuild tool_use ID set after removal — stale IDs would let orphaned results survive
          allToolUseIds.clear();
          for (const m of messages) {
            if (m?.role !== "assistant" || !Array.isArray(m.content)) continue;
            for (const block of m.content) {
              const b = block as Record<string, unknown>;
              if (b.type === "tool_use" && typeof b.id === "string") {
                allToolUseIds.add(b.id);
              }
            }
          }

          // Remove orphaned tool_result blocks (no matching tool_use)
          for (let i = 0; i < messages.length; i++) {
            const msg = messages[i];
            if (msg?.role !== "user" || !Array.isArray(msg.content)) continue;

            const before = msg.content.length;
            msg.content = msg.content.filter(
              (block: Record<string, unknown>) => {
                if (block.type !== "tool_result") return true;
                const tid = block.tool_use_id as string;
                return !tid || allToolUseIds.has(tid);
              },
            );
            const removed = before - msg.content.length;
            if (removed > 0) {
              console.log(
                `[proxy] 🔧 Removed ${removed} orphaned tool_result block(s) from messages[${i}]`,
              );
              totalRemoved += removed;
            }
            if (msg.content.length === 0) {
              msg.content = [{ type: "text", text: " " }];
            }
          }

          // Fix role alternation — merge consecutive same-role messages
          let mergeCount = 0;
          let mi = 0;
          while (mi < body.messages.length - 1) {
            const curr = body.messages[mi]!;
            const next = body.messages[mi + 1]!;
            if (curr.role === next.role) {
              const currContent = Array.isArray(curr.content)
                ? curr.content
                : [
                    {
                      type: "text" as const,
                      text: String(curr.content ?? " "),
                    },
                  ];
              const nextContent = Array.isArray(next.content)
                ? next.content
                : [
                    {
                      type: "text" as const,
                      text: String(next.content ?? " "),
                    },
                  ];
              curr.content = [...currContent, ...nextContent];
              body.messages.splice(mi + 1, 1);
              mergeCount++;
            } else {
              mi++;
            }
          }

          if (mergeCount > 0) {
            console.log(
              `[proxy] 🔧 Merged ${mergeCount} consecutive same-role message(s)`,
            );
            totalRemoved += mergeCount;
          }

          // Ensure no empty content arrays after all the surgery
          for (const msg of body.messages) {
            if (Array.isArray(msg.content) && msg.content.length === 0) {
              msg.content = [{ type: "text", text: " " }];
            }
          }

          if (totalRemoved > 0) {
            toolConcurrencyPasses++;
            console.log(
              `[proxy] 🔧 Tool concurrency fix pass ${toolConcurrencyPasses}: removed/merged ${totalRemoved} total block(s), retrying`,
            );
            failedAccountIds.delete(account.id);
            const pFails = (providerFailCounts.get(account.provider) ?? 1) - 1;
            providerFailCounts.set(account.provider, Math.max(0, pFails));
            if (pFails < 2) excludeProviders.delete(account.provider);
            continue;
          }
        }

        // Empty content errors — user messages with empty content blocks.
        // Fix by adding placeholder content or removing empty blocks, then retry.
        if (
          lastErrorClass === "invalid_request" &&
          lastError.includes("non-empty content") &&
          !emptyContentFixed
        ) {
          emptyContentFixed = true;
          const messages = body.messages ?? [];
          let fixed = 0;
          for (const msg of messages) {
            if (msg.role !== "user") continue;
            if (typeof msg.content === "string" && msg.content.trim() === "") {
              msg.content = "(continue)";
              fixed++;
            } else if (Array.isArray(msg.content)) {
              // Remove empty text blocks, add placeholder if nothing remains
              msg.content = msg.content.filter(
                (block: Record<string, unknown>) => {
                  if (
                    block.type === "text" &&
                    (!block.text || (block.text as string).trim() === "")
                  ) {
                    return false;
                  }
                  return true;
                },
              );
              if (msg.content.length === 0) {
                msg.content = [{ type: "text", text: "(continue)" }];
                fixed++;
              }
            }
          }
          if (fixed > 0) {
            console.log(
              `[proxy] 🔧 Fixed ${fixed} empty user message(s), retrying`,
            );
            failedAccountIds.delete(account.id);
            const pFails = (providerFailCounts.get(account.provider) ?? 1) - 1;
            providerFailCounts.set(account.provider, Math.max(0, pFails));
            if (pFails < 2) excludeProviders.delete(account.provider);
            continue;
          }
        }

        // Tool reference errors are self-healing — extract missing tool, add stub, retry
        if (
          lastErrorClass === "invalid_request" &&
          (lastError.includes("not found in available tools") ||
            lastError.includes("is not defined"))
        ) {
          const toolMatch = lastError.match(
            /Tool(?:\s+reference)?\s+'([^']+)'/,
          );
          if (toolMatch) {
            const missingTool = toolMatch[1]!;
            const raw = body as Record<string, unknown>;
            if (!raw.tools || !Array.isArray(raw.tools)) raw.tools = [];
            const toolsArr = raw.tools as {
              name: string;
              description?: string;
              input_schema?: Record<string, unknown>;
            }[];

            // Rebuild resolver from current tool set (including any prior stubs)
            const knownTools = new Set<string>(toolsArr.map((t) => t.name));
            const normalizedMap = new Map<string, string>();
            for (const name of knownTools) {
              normalizedMap.set(name.toLowerCase().replace(/-/g, "_"), name);
            }
            const resolvedTool = knownTools.has(missingTool)
              ? missingTool
              : (normalizedMap.get(
                  missingTool.toLowerCase().replace(/-/g, "_"),
                ) ?? missingTool);

            // Normalize existing history references before retrying.
            // Without this, we can keep retrying with the same missing hyphenated
            // tool name even after adding an underscore-normalized stub.
            for (const msg of body.messages ?? []) {
              if (!Array.isArray(msg.content)) continue;
              for (const block of msg.content) {
                const b = block as Record<string, unknown>;
                if (block.type === "tool_use" && typeof b.name === "string") {
                  const current = b.name as string;
                  const currentNorm = current.toLowerCase().replace(/-/g, "_");
                  const missingNorm = missingTool
                    .toLowerCase()
                    .replace(/-/g, "_");
                  if (current === missingTool || currentNorm === missingNorm) {
                    b.name = resolvedTool;
                  }
                }
              }
            }

            // Cap auto-stubs at 5 per request to prevent unbounded growth
            if (toolStubsAdded.size >= 5) {
              console.log(
                `[proxy] ⚠️ Tool stub cap reached (${toolStubsAdded.size}), stopping self-healing for '${missingTool}'`,
              );
              toolRegistryUnhealthy = true;
              // Fall through to invalid_request handler below
            } else if (!toolsArr.some((t) => t.name === resolvedTool)) {
              toolsArr.push({
                name: resolvedTool,
                description: `Tool ${resolvedTool} (auto-stub from error recovery)`,
                input_schema: { type: "object", properties: {} },
              });
              toolStubsAdded.add(resolvedTool);
              console.log(
                `[proxy] 🔧 Auto-added tool stub for '${missingTool}' as '${resolvedTool}' (${toolStubsAdded.size}/5), retrying`,
              );
              // Don't count this as a provider failure — it's a request fixup
              failedAccountIds.delete(account.id);
              const pFails =
                (providerFailCounts.get(account.provider) ?? 1) - 1;
              providerFailCounts.set(account.provider, Math.max(0, pFails));
              if (pFails < 2) excludeProviders.delete(account.provider);
              continue;
            } else {
              console.log(
                `[proxy] 🔧 Tool '${resolvedTool}' already available for missing reference '${missingTool}', retrying`,
              );
              // Don't count this as a provider failure — it's a request fixup
              failedAccountIds.delete(account.id);
              const pFails =
                (providerFailCounts.get(account.provider) ?? 1) - 1;
              providerFailCounts.set(account.provider, Math.max(0, pFails));
              if (pFails < 2) excludeProviders.delete(account.provider);
              continue;
            }
          }
        }

        // Invalid request errors are request format issues — stop retrying entirely
        if (lastErrorClass === "invalid_request") {
          if (toolRegistryUnhealthy) {
            return Response.json(
              {
                error: {
                  type: "invalid_request_error",
                  message: lastError,
                  reason: "environment_tool_registry_unhealthy",
                  recoverable: true,
                  suggested_action: "retry_in_safe_mode_main_thread",
                },
              },
              { status: 400 },
            );
          }
          return Response.json(
            {
              error: {
                type: "invalid_request_error",
                message: lastError,
              },
            },
            { status: 400 },
          );
        }

        // Context length fallback chain:
        // Phase 1: route to large-context providers (Gemini 1-2M)
        // Phase 2: compact conversation (drop older messages)
        // Phase 3: auto-compact via LLM summarization
        if (lastErrorClass === "context_length") {
          // Track which providers already failed with context_length
          // to avoid retrying them in the large-context phase
          contextLengthFailedProviders.add(account.provider);

          if (!contextLengthFallback) {
            console.log(
              `[proxy] ⚠️ Context length exceeded on ${account.provider} — falling back to large-context providers`,
            );
            contextLengthFallback = true;
            failedAccountIds.clear();
            excludeProviders.clear();
            providerFailCounts.clear();
            // Only allow large-context providers, excluding any that already failed
            for (const [name] of this.providers) {
              if (
                !LARGE_CONTEXT_PROVIDERS.has(name) ||
                contextLengthFailedProviders.has(name)
              ) {
                excludeProviders.add(name);
              }
            }
            if (
              !contextFallbackBonusUsed &&
              resetBonusAttempts < MAX_BONUS_ATTEMPTS
            ) {
              resetBonusAttempts++;
              contextFallbackBonusUsed = true;
            }
            continue;
          }
          if (!contextCompacted) {
            console.log(
              `[proxy] ⚠️ Large-context providers also failed — compacting conversation history`,
            );
            const compacted = this.compactMessages(body);
            if (compacted) {
              // Re-sanitize after compaction (tool refs, empty blocks, etc.)
              this.sanitizeRequest(body);
              contextCompacted = true;
              failedAccountIds.clear();
              excludeProviders.clear();
              providerFailCounts.clear();
              continue;
            }
          }
          // Phase 4: auto-compact via LLM summarization (or structural fallback)
          if (!contextAggressivelyTruncated) {
            console.log(
              `[proxy] ⚠️ Compaction insufficient — attempting auto-compact summarization`,
            );
            const didCompact = await this.autoCompact(body);
            if (didCompact) {
              this.sanitizeRequest(body);
              contextAggressivelyTruncated = true;
              failedAccountIds.clear();
              excludeProviders.clear();
              providerFailCounts.clear();
              contextLengthFailedProviders.clear();
              if (resetBonusAttempts < MAX_BONUS_ATTEMPTS) {
                resetBonusAttempts++;
              }
              continue;
            }
          }
          // All fallback options exhausted
          return Response.json(
            {
              error: {
                type: "invalid_request_error",
                message: `Context too long for all providers (including after compaction and auto-compact). ${lastError}`,
              },
            },
            { status: 400 },
          );
        }

        // Non-retryable for this account/provider — exclude and try others
        if (!provider.isRetryable(lastErrorClass)) {
          excludeProviders.add(account.provider);
        }
      } catch (e) {
        clearTimeout(timeout);
        lastError = e instanceof Error ? e.message : String(e);
        // Classify caught exceptions — connection errors are systemic
        const isConnError =
          lastError.includes("ECONNREFUSED") ||
          lastError.includes("ECONNRESET") ||
          lastError.includes("fetch failed") ||
          lastError.includes("Unable to connect");
        lastErrorClass = isConnError ? "connection_error" : "unknown";
        console.error(
          `[proxy] Exception on attempt ${attempt + 1} (${lastErrorClass}):`,
          lastError,
        );
        failedAccountIds.add(account.id);
        const pFails2 = (providerFailCounts.get(account.provider) ?? 0) + 1;
        providerFailCounts.set(account.provider, pFails2);
        if (pFails2 >= 2) {
          excludeProviders.add(account.provider);
          this.telemetry.provider_exclusions++;
        }
        // Don't trip circuit breaker for systemic connection errors
        if (!isConnError) {
          this.tokenManager.recordFailure(account.id, lastError);
        }
        // Connection error from proxy provider → exclude all + set latch
        if (isConnError && PROXY_PROVIDERS.has(account.provider)) {
          for (const p of PROXY_PROVIDERS) excludeProviders.add(p);
          this.markProxyUnhealthy(30_000);
          this.telemetry.proxy_resets++;
        }
      } finally {
        if (!streamOwnsCleanup) {
          if (accountActive) {
            this.tokenManager.decrementActive(account.id);
            accountActive = false;
          }
          if (providerSem && providerAcquired) {
            providerSem.release();
            providerAcquired = false;
          }
          if (globalAcquired) {
            this.globalSemaphore.release();
            globalAcquired = false;
          }
        }
      }
    }

    // All retries exhausted — last-chance context_length recovery before giving up.
    // This fires when the for-loop exhausted all attempts with context_length errors
    // but never reached the in-loop context_length handler (e.g. all attempts burned
    // on provider rotation).
    if (lastErrorClass === "context_length") {
      if (!contextAggressivelyTruncated) {
        console.log(
          `[proxy] ⚠️ Retries exhausted with context_length — final auto-compact attempt`,
        );
        const didCompact = await this.autoCompact(body);
        if (didCompact) {
          this.sanitizeRequest(body);
          contextAggressivelyTruncated = true;
          // Re-enter the retry loop with one bonus attempt
          // by recursing through handleMessages with the compacted body
          // (simpler than re-entering the for loop)
          const compactedReq = new Request(req.url, {
            method: "POST",
            headers: req.headers,
            body: JSON.stringify(body),
          });
          if (_reentryGuard) {
            console.log(
              `[proxy] 🛑 Re-entry guard: already in recursive handleMessages, skipping`,
            );
          } else {
            console.log(
              `[proxy] 🔄 Re-entering handleMessages with auto-compacted body`,
            );
            return this.handleMessages(compactedReq, true);
          }
        }
      }
    }

    // All retries exhausted — pick appropriate status code
    if (lastErrorClass === "auth_error") {
      return Response.json(
        {
          error: {
            type: "authentication_error",
            message: `All providers returned auth errors. Last: ${lastError}`,
          },
        },
        { status: 401 },
      );
    }
    if (lastErrorClass === "model_unavailable") {
      return Response.json(
        {
          error: {
            type: "not_found_error",
            message: `Model not available on any provider. Last: ${lastError}`,
          },
        },
        { status: 404 },
      );
    }
    if (lastErrorClass === "context_length") {
      return Response.json(
        {
          error: {
            type: "invalid_request_error",
            message: `Context too long for all providers after auto-compact. ${lastError}`,
          },
        },
        { status: 400 },
      );
    }
    return Response.json(
      {
        error: {
          type: "overloaded_error",
          message: `All providers failed after ${retryConfig.maxRetries} attempts (${mode} mode). Last error: ${lastError}`,
        },
      },
      { status: 529 },
    );
  }

  private async handleOracle(req: Request): Promise<Response> {
    let body: { question: string; context?: string; model?: string };
    try {
      body = (await req.json()) as {
        question: string;
        context?: string;
        model?: string;
      };
    } catch {
      return Response.json(
        {
          error: {
            type: "invalid_request_error",
            message: "Invalid JSON body",
          },
        },
        { status: 400 },
      );
    }

    if (!body.question) {
      return Response.json(
        {
          error: {
            type: "invalid_request_error",
            message: "Missing required field: question",
          },
        },
        { status: 400 },
      );
    }

    const ORACLE_MODELS = new Set([
      "gpt-5.4",
      "gpt-5.3-codex",
      "gpt-5.2-codex",
      "gpt-5.2",
      "gpt-5.1-codex",
    ]);
    const oracleModel =
      body.model && ORACLE_MODELS.has(body.model) ? body.model : "gpt-5.4";
    const oracleRequest: MessagesRequest = {
      model: oracleModel,
      messages: [{ role: "user", content: body.question }],
      max_tokens: 8192,
      stream: false,
      ...(body.context ? { system: body.context } : {}),
    };

    // Get a codex account directly — one attempt, no retry loop
    const codexAccounts = this.tokenManager.getHealthyAccounts("codex");
    const codexAccount = codexAccounts[0];

    if (!codexAccount) {
      return Response.json(
        {
          error: {
            type: "overloaded_error",
            message: "No healthy codex accounts available",
          },
        },
        { status: 529 },
      );
    }

    const provider = this.providers.get("codex");
    if (!provider) {
      return Response.json(
        {
          error: {
            type: "api_error",
            message: "Codex provider not initialized",
          },
        },
        { status: 500 },
      );
    }

    const abortController = new AbortController();
    const timeout = setTimeout(() => abortController.abort(), 120_000);
    const startTime = Date.now();

    // Acquire concurrency slots — oracle calls must respect the same limits
    const providerSem = this.providerSemaphores.get("codex");
    let globalAcquired = false;
    let providerAcquired = false;
    try {
      await this.globalSemaphore.acquire(QUEUE_TIMEOUT_MS);
      globalAcquired = true;
    } catch (e) {
      clearTimeout(timeout);
      if (e instanceof QueueFullError || e instanceof QueueTimeoutError) {
        return Response.json(
          {
            error: {
              type: "overloaded_error",
              message: `Server overloaded — ${e.message}. Try again shortly.`,
            },
          },
          { status: 529 },
        );
      }
      throw e;
    }
    if (providerSem) {
      try {
        await providerSem.acquire(PROVIDER_SEM_TIMEOUT_MS);
        providerAcquired = true;
      } catch {
        this.globalSemaphore.release();
        clearTimeout(timeout);
        return Response.json(
          {
            error: {
              type: "overloaded_error",
              message: "Codex provider congested, try again shortly.",
            },
          },
          { status: 529 },
        );
      }
    }

    try {
      console.log(
        `[proxy] oracle → codex/${codexAccount.id} (model=${oracleModel})`,
      );
      this.tokenManager.markUsed(codexAccount.id);
      this.tokenManager.incrementActive(codexAccount.id);
      const result = await provider.sendRequest(
        codexAccount,
        oracleRequest,
        abortController.signal,
      );
      clearTimeout(timeout);

      if (result.success && result.body) {
        this.tokenManager.recordSuccess(codexAccount.id);
        this.usageDB.record({
          timestamp: Date.now(),
          provider: "codex",
          model: oracleModel,
          account_id: codexAccount.id,
          input_tokens: result.body.usage?.input_tokens ?? 0,
          output_tokens: result.body.usage?.output_tokens ?? 0,
          latency_ms: Date.now() - startTime,
          status: "success",
          error: null,
        });
        return new Response(JSON.stringify(result.body), {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "x-claudemax-provider": "codex",
            "x-claudemax-model": oracleModel,
            "x-claudemax-tier": "standard",
            "x-claudemax-attempt": "1",
          },
        });
      }

      const errMsg = result.error ?? "Unknown error";
      this.tokenManager.recordFailure(codexAccount.id, errMsg);
      this.usageDB.record({
        timestamp: Date.now(),
        provider: "codex",
        model: oracleModel,
        account_id: codexAccount.id,
        input_tokens: 0,
        output_tokens: 0,
        latency_ms: Date.now() - startTime,
        status: "error",
        error: errMsg.slice(0, 500),
      });
      return Response.json(
        { error: { type: "api_error", message: errMsg } },
        { status: 502 },
      );
    } catch (e) {
      clearTimeout(timeout);
      const errMsg = e instanceof Error ? e.message : String(e);
      console.error(`[proxy] oracle error:`, errMsg);
      this.usageDB.record({
        timestamp: Date.now(),
        provider: "codex",
        model: oracleModel,
        account_id: codexAccount.id,
        input_tokens: 0,
        output_tokens: 0,
        latency_ms: Date.now() - startTime,
        status: "error",
        error: errMsg.slice(0, 500),
      });
      return Response.json(
        { error: { type: "api_error", message: errMsg } },
        { status: 502 },
      );
    } finally {
      this.tokenManager.decrementActive(codexAccount.id);
      if (providerSem && providerAcquired) providerSem.release();
      if (globalAcquired) this.globalSemaphore.release();
    }
  }

  private handleCircuitReset(url: URL): Response {
    const provider = url.searchParams.get("provider") ?? undefined;
    let cleared = 0;

    for (const account of this.tokenManager.getAllAccounts()) {
      if (provider && account.provider !== provider) continue;
      if (this.tokenManager.resetCircuit(account.id)) cleared++;
    }

    // Also clear the proxy health latch
    this.cliProxyUnhealthyUntil = 0;

    return Response.json({
      cleared,
      message: `Reset ${cleared} circuit breaker(s)${provider ? ` for ${provider}` : ""}`,
    });
  }

  private handleModels(): Response {
    const models = this.router.getAvailableModels();
    const data = models.map((m) => ({
      id: m.model,
      object: "model",
      created: 0,
      owned_by: m.provider,
    }));

    return Response.json({ object: "list", data });
  }

  // --- Fleet endpoints ---

  private async handleFleetDispatch(req: Request): Promise<Response> {
    let body: FleetDispatchRequest;
    try {
      body = (await req.json()) as FleetDispatchRequest;
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    if (!body.cli || !body.prompt) {
      return Response.json(
        { error: "Missing required fields: cli, prompt" },
        { status: 400 },
      );
    }

    if (!["codex", "kimi", "claude", "gemini"].includes(body.cli)) {
      return Response.json(
        {
          error: `Invalid CLI: ${body.cli}. Must be codex, kimi, claude, or gemini`,
        },
        { status: 400 },
      );
    }

    if (
      body.sandbox !== undefined &&
      !["workspace-write", "read-only", "danger-full-access"].includes(
        body.sandbox,
      )
    ) {
      return Response.json(
        {
          error:
            "Invalid sandbox. Must be one of: workspace-write, read-only, danger-full-access",
        },
        { status: 400 },
      );
    }

    if (
      body.reasoning_effort !== undefined &&
      !["low", "medium", "high", "xhigh"].includes(body.reasoning_effort)
    ) {
      return Response.json(
        {
          error:
            "Invalid reasoning_effort. Must be one of: low, medium, high, xhigh",
        },
        { status: 400 },
      );
    }

    if (body.task_kind === "sandbox_ops") {
      return this.handleSandboxOpsFleetDispatch(body);
    }

    // Auto-route review tasks to Codex to consume separate review quota
    if (body.task_kind === "review" && body.cli !== "codex") {
      console.log(
        `[fleet] Review task_kind detected — routing to codex (separate review quota)`,
      );
      body.cli = "codex";
    }

    // Fractal Fleet: classify task complexity and reroute to cheaper agents when safe
    const fractalEnabled = this.config.fractal_fleet?.enabled !== false;
    if (fractalEnabled && body.metadata?.["no_reroute"] !== true) {
      // Build availability context from live infrastructure
      const availability: FleetAvailability = {
        exhaustedProviders: new Set(
          (
            ["codex", "kimi", "claude", "gemini", "glm", "minimax"] as const
          ).filter((p) => this.router.isProviderExhausted(p)),
        ),
        budgetStoppedProviders: new Set(
          this.budgetGuard
            ? this.budgetGuard
                .getStatus()
                .filter((s) => s.status === "stopped")
                .map((s) => s.provider)
            : [],
        ),
        budgetAlertProviders: new Set(
          this.budgetGuard
            ? this.budgetGuard
                .getStatus()
                .filter((s) => s.status === "alert")
                .map((s) => s.provider)
            : [],
        ),
        providerScores: new Map(
          this.learningRouter
            ?.getState()
            ?.scores.map(
              (s) => [s.provider, s.compositeScore] as [string, number],
            ) ?? [],
        ),
      };
      const classification = classifyTask(body, availability);
      if (classification.rerouted) {
        console.log(
          `[fleet] 🧩 Fractal reroute: ${classification.original_cli}→${classification.recommended_cli} ` +
            `(complexity=${classification.complexity}, score=${classification.score}, ` +
            `reasons: ${classification.reasons.join(", ")})`,
        );
        applyClassification(body, classification);
      } else {
        console.log(
          `[fleet] 🧩 Fractal: keeping ${body.cli} (complexity=${classification.complexity}, score=${classification.score})`,
        );
      }

      // Auto-decompose: intercept high-complexity tasks and fractal-plan them
      const autoDecompose = this.config.fractal_fleet?.auto_decompose !== false;
      const minComplexity =
        this.config.fractal_fleet?.auto_decompose_min_complexity ?? "complex";
      const complexityRank = {
        trivial: 0,
        standard: 1,
        complex: 2,
        deep: 3,
      } as const;
      const meetsThreshold =
        complexityRank[classification.complexity] >=
        complexityRank[minComplexity];

      if (
        autoDecompose &&
        meetsThreshold &&
        body.metadata?.["no_decompose"] !== true &&
        this.fractalPlanner
      ) {
        console.log(
          `[fleet] 🌳 Auto-decompose: complexity=${classification.complexity} ≥ ${minComplexity}, ` +
            `decomposing "${body.prompt.slice(0, 80)}..."`,
        );
        try {
          const plan = await this.fractalPlanner.plan({
            objective: body.prompt,
            cwd: body.cwd ?? process.cwd(),
            auto_execute: true,
            metadata: {
              ...body.metadata,
              auto_decomposed: true,
              original_cli: body.cli,
            },
          });
          return Response.json({
            auto_decomposed: true,
            plan_id: plan.plan_id,
            swarm_id: plan.swarm_id,
            status: plan.status,
            stats: plan.stats,
            leaf_tasks: plan.leaf_tasks.map((t) => ({
              id: t.id,
              description: t.description.slice(0, 120),
              complexity: t.complexity,
              assigned_cli: t.assigned_cli,
              assigned_model: t.assigned_model,
            })),
          });
        } catch (decomposeErr) {
          console.error(
            `[fleet] 🌳 Auto-decompose failed, falling back to single dispatch:`,
            (decomposeErr as Error).message,
          );
          // Fall through to normal dispatch
        }
      }
    }

    // Wire TaskClassifier into single dispatch — cost-optimize even non-fractal dispatches.
    // Only runs when the fractal classification block above did NOT fire (either fractal
    // is disabled, or no_reroute was set). Avoids double-classifying the same request.
    if (
      !body.metadata?.["fractal_rerouted"] &&
      (!fractalEnabled || body.metadata?.["no_reroute"] === true)
    ) {
      try {
        const availability: FleetAvailability = {
          exhaustedProviders: new Set(
            (
              ["codex", "kimi", "claude", "gemini", "glm", "minimax"] as const
            ).filter((p) => this.router.isProviderExhausted(p)),
          ),
          budgetStoppedProviders: new Set(
            this.budgetGuard
              ? this.budgetGuard
                  .getStatus()
                  .filter((s) => s.status === "stopped")
                  .map((s) => s.provider)
              : [],
          ),
          budgetAlertProviders: new Set(
            this.budgetGuard
              ? this.budgetGuard
                  .getStatus()
                  .filter((s) => s.status === "alert")
                  .map((s) => s.provider)
              : [],
          ),
          providerScores: new Map(
            this.learningRouter
              ?.getState()
              ?.scores.map(
                (s) => [s.provider, s.compositeScore] as [string, number],
              ) ?? [],
          ),
        };
        const classification = classifyTask(body, availability);
        if (classification.rerouted) {
          console.log(
            `[fleet] Single dispatch reroute: ${classification.original_cli} -> ${classification.recommended_cli} (complexity=${classification.complexity})`,
          );
          applyClassification(body, classification);
        }
      } catch (classifyErr) {
        // Non-blocking — if classification fails, proceed with original request
        console.warn(
          "[fleet] TaskClassifier failed (non-blocking):",
          classifyErr,
        );
      }
    }

    try {
      const job = await this.fleet.dispatch(body);
      const fractalInfo = body.metadata?.["fractal_rerouted"]
        ? {
            fractal_rerouted: true,
            fractal_original_cli: body.metadata["fractal_original_cli"],
            fractal_complexity: body.metadata["fractal_complexity"],
          }
        : undefined;
      return Response.json({
        id: job.id,
        cli: job.cli,
        status: job.status,
        model: job.model,
        tmux_session: job.tmuxSession,
        attach_command: `tmux attach -t ${job.tmuxSession}`,
        ...fractalInfo,
      });
    } catch (e) {
      return Response.json({ error: (e as Error).message }, { status: 500 });
    }
  }

  private async handleSandboxOpsFleetDispatch(
    body: FleetDispatchRequest,
  ): Promise<Response> {
    const image =
      typeof body.metadata?.sandbox_image === "string"
        ? body.metadata.sandbox_image
        : "opensandbox/code-interpreter:v1.0.1";

    const priority =
      body.metadata?.priority === "high" ||
      body.metadata?.priority === "low" ||
      body.metadata?.priority === "normal"
        ? (body.metadata.priority as "high" | "normal" | "low")
        : "normal";

    const intent: Agent GatewaySandboxIntent = {
      image,
      command: body.prompt,
      priority,
      policyProfile: "agent-gateway_controlled",
      metadata: {
        source: "fleet_dispatch",
        cli: body.cli,
        task_kind: body.task_kind,
        ...(body.metadata ?? {}),
      },
    };

    try {
      const created = await this.agent-gatewaySandbox.create(intent);
      return Response.json({
        id: created.id,
        sandbox_id: created.sandboxId,
        status: created.status,
        mode: "sandbox_ops",
      });
    } catch (e) {
      return Response.json(
        { error: e instanceof Error ? e.message : String(e) },
        { status: 400 },
      );
    }
  }

  private handleFleetJobs(): Response {
    const jobs = this.fleet.listJobs().map((j) => ({
      id: j.id,
      cli: j.cli,
      status: j.status,
      model: j.model,
      cwd: j.cwd,
      created_at: j.createdAt,
      started_at: j.startedAt,
      completed_at: j.completedAt,
    }));
    return Response.json({ jobs });
  }

  private handleFleetJobStatus(id: string): Response {
    const job = this.fleet.refreshJobStatus(id);
    if (!job) {
      return Response.json({ error: "Job not found" }, { status: 404 });
    }
    return Response.json({
      id: job.id,
      cli: job.cli,
      status: job.status,
      model: job.model,
      cwd: job.cwd,
      created_at: job.createdAt,
      started_at: job.startedAt,
      completed_at: job.completedAt,
      error: job.error,
      tmux_session: job.tmuxSession,
      attach_command: `tmux attach -t ${job.tmuxSession}`,
    });
  }

  private handleFleetOutput(id: string, url: URL): Response {
    const requestedLines = parseInt(url.searchParams.get("lines") ?? "50", 10);
    const lines = Number.isFinite(requestedLines)
      ? Math.max(1, Math.min(1000, requestedLines))
      : 50;
    const output = this.fleet.captureOutput(id, lines);
    if (output === null) {
      return Response.json(
        { error: "Job not found or no output" },
        { status: 404 },
      );
    }
    return Response.json({ id, output });
  }

  private async handleFleetSend(id: string, req: Request): Promise<Response> {
    let body: { message: string };
    try {
      body = (await req.json()) as { message: string };
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    if (!body.message) {
      return Response.json({ error: "Missing message field" }, { status: 400 });
    }

    const sent = this.fleet.sendMessage(id, body.message);
    if (!sent) {
      return Response.json(
        { error: "Job not running or not found" },
        { status: 404 },
      );
    }
    return Response.json({ ok: true, id });
  }

  private handleFleetKill(id: string): Response {
    const killed = this.fleet.killJob(id);
    if (!killed) {
      return Response.json(
        { error: "Job not found or not running" },
        { status: 404 },
      );
    }
    return Response.json({ ok: true, id, status: "killed" });
  }

  private handleFleetPlan(id: string): Response {
    const plan = this.fleet.getPlan(id);
    if (plan === null) {
      return Response.json(
        { error: "No plan found for this job" },
        { status: 404 },
      );
    }
    const job = this.fleet.refreshJobStatus(id);
    return Response.json({
      id,
      status: job?.status ?? "unknown",
      plan,
    });
  }

  private async handleFleetApprove(
    id: string,
    req: Request,
  ): Promise<Response> {
    try {
      const body =
        req.method === "POST"
          ? ((await req.json().catch(() => ({}))) as Record<string, unknown>)
          : {};
      const approvedBy = (body.approved_by as string) ?? "user";
      const job = await this.fleet.approveAndExecute(id, approvedBy);
      return Response.json({
        ok: true,
        id,
        status: job.status,
        message: `Plan approved by ${approvedBy}, execution started`,
      });
    } catch (e) {
      return Response.json(
        { error: e instanceof Error ? e.message : String(e) },
        { status: 400 },
      );
    }
  }

  // --- Task board handlers ---

  private async handleBoardCreate(req: Request): Promise<Response> {
    const body = (await req.json()) as {
      tasks: Array<{ id: string; description: string }>;
    };
    if (!body.tasks?.length) {
      return Response.json({ error: "tasks array required" }, { status: 400 });
    }
    const board = this.taskBoard.createBoard(body.tasks);
    return Response.json(board, { status: 201 });
  }

  private async handleBoardClaim(
    boardId: string,
    req: Request,
  ): Promise<Response> {
    const body = (await req.json()) as { agent_id: string };
    if (!body.agent_id) {
      return Response.json({ error: "agent_id required" }, { status: 400 });
    }
    const result = this.taskBoard.claimNext(boardId, body.agent_id);
    if (result === "board_not_found") {
      return Response.json({ error: "Board not found" }, { status: 404 });
    }
    return result
      ? Response.json(result)
      : Response.json({ error: "No open tasks" }, { status: 404 });
  }

  private async handleBoardComplete(
    boardId: string,
    req: Request,
  ): Promise<Response> {
    const body = (await req.json()) as { task_id: string; result: string };
    if (!body.task_id || !body.result) {
      return Response.json(
        { error: "task_id and result required" },
        { status: 400 },
      );
    }
    const task = this.taskBoard.complete(boardId, body.task_id, body.result);
    return task
      ? Response.json(task)
      : Response.json(
          { error: "Task not found or not claimed" },
          { status: 404 },
        );
  }

  private async handleBoardFail(
    boardId: string,
    req: Request,
  ): Promise<Response> {
    const body = (await req.json()) as { task_id: string; error: string };
    if (!body.task_id || !body.error) {
      return Response.json(
        { error: "task_id and error required" },
        { status: 400 },
      );
    }
    const task = this.taskBoard.fail(boardId, body.task_id, body.error);
    return task
      ? Response.json(task)
      : Response.json(
          { error: "Task not found or not claimed" },
          { status: 404 },
        );
  }

  private handleFleetSessions(url: URL): Response {
    const host = url.searchParams.get("host") ?? "all";
    const includeWarp = url.searchParams.get("include_warp") !== "false";
    const source =
      (url.searchParams.get("source") as
        | "tmux"
        | "warp"
        | "unknown"
        | "all"
        | null) ?? "all";
    const sessions = this.fleet.listSessions({
      host,
      include_warp: includeWarp,
      source,
    });
    return Response.json({ sessions });
  }

  private handleFleetSessionGet(id: string): Response {
    const session = this.fleet.getSession(id);
    if (!session) {
      return Response.json({ error: "Session not found" }, { status: 404 });
    }
    return Response.json({ session });
  }

  private async handleFleetSessionSend(
    id: string,
    req: Request,
  ): Promise<Response> {
    let body: { message: string; enter?: boolean };
    try {
      body = (await req.json()) as { message: string; enter?: boolean };
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    if (!body.message) {
      return Response.json({ error: "Missing message field" }, { status: 400 });
    }

    const result = this.fleet.sendSessionMessage(
      id,
      body.message,
      body.enter ?? true,
    );
    if (!result.ok) {
      return Response.json(
        { error: result.error ?? "Session not found or unavailable" },
        { status: 404 },
      );
    }

    return Response.json(result);
  }

  private handleFleetSessionKill(id: string): Response {
    const result = this.fleet.killSessionById(id);
    if (!result.ok) {
      return Response.json(
        { error: result.error ?? "Session not found or unavailable" },
        { status: 404 },
      );
    }
    return Response.json(result);
  }

  private handleFleetSessionRestart(id: string): Response {
    const result = this.fleet.restartSessionById(id);
    if (!result.ok) {
      return Response.json(
        { error: result.error ?? "Session restart failed" },
        { status: 404 },
      );
    }
    return Response.json(result);
  }

  // --- Fractal planner endpoints ---

  private async handleFractalPlan(req: Request): Promise<Response> {
    if (this.config.fractal_fleet?.enabled === false) {
      return Response.json(
        { error: "Fractal fleet is disabled" },
        { status: 403 },
      );
    }

    let body: FractalPlanRequest;
    try {
      body = (await req.json()) as FractalPlanRequest;
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    if (!body.objective) {
      return Response.json(
        { error: "Missing required field: objective" },
        { status: 400 },
      );
    }

    try {
      const plan = await this.fractalPlanner.plan(body);
      console.log(
        `[fractal] 🌳 Plan ${plan.plan_id}: ${plan.stats.total_tasks} tasks, ` +
          `${plan.stats.leaf_tasks} leaves, ` +
          `est. ${plan.stats.estimated_cost_reduction_pct}% cost reduction`,
      );
      return Response.json(plan);
    } catch (e) {
      console.error("[fractal] Plan failed:", (e as Error).message);
      return Response.json({ error: (e as Error).message }, { status: 500 });
    }
  }

  private handleFractalPlanGet(planId: string): Response {
    const plan = this.fractalPlanner.getPlan(planId);
    if (!plan) {
      return Response.json({ error: "Plan not found" }, { status: 404 });
    }
    return Response.json(plan);
  }

  private async handleFractalExecute(
    planId: string,
    req: Request,
  ): Promise<Response> {
    try {
      let body: {
        cwd?: string;
        max_concurrency?: number;
        fail_fast?: boolean;
      } = {};
      try {
        body = (await req.json()) as typeof body;
      } catch {
        /* empty body ok */
      }

      const swarmId = await this.fractalPlanner.execute(planId, {
        objective: "", // not needed for execute
        cwd: body.cwd,
        max_concurrency: body.max_concurrency,
        fail_fast: body.fail_fast,
      });

      console.log(`[fractal] 🚀 Executing plan ${planId} → swarm ${swarmId}`);
      return Response.json({
        plan_id: planId,
        swarm_id: swarmId,
        status: "executing",
      });
    } catch (e) {
      return Response.json({ error: (e as Error).message }, { status: 500 });
    }
  }

  // --- Experiment loop endpoints ---

  private async handleExperimentLoopCreate(req: Request): Promise<Response> {
    let body: import("./fleet/types.ts").ExperimentLoopRequest;
    try {
      body =
        (await req.json()) as import("./fleet/types.ts").ExperimentLoopRequest;
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    try {
      const loop = await this.experimentLoops.create(body);
      return Response.json({
        id: loop.id,
        status: loop.status,
        status_url: `/v1/fleet/experiment-loops/${loop.id}`,
        created_at: loop.created_at,
        artifact_root: loop.artifact_root,
        manifest_path: loop.manifest_path,
      });
    } catch (e) {
      return Response.json({ error: (e as Error).message }, { status: 400 });
    }
  }

  private handleExperimentLoopGet(id: string): Response {
    const loop = this.experimentLoops.getLoop(id);
    if (!loop) {
      return Response.json(
        { error: "Experiment loop not found" },
        { status: 404 },
      );
    }
    return Response.json(loop);
  }

  private handleExperimentLoopList(url: URL): Response {
    const status = url.searchParams.get("status") as
      | import("./fleet/types.ts").ExperimentLoopStatus
      | null;
    const loops = this.experimentLoops
      .listLoops(status ?? undefined)
      .map((loop) => ({
        id: loop.id,
        status: loop.status,
        created_at: loop.created_at,
        completed_at: loop.completed_at,
        rounds: loop.rounds.length,
        winner_candidate_id: loop.winner_candidate_id,
        error: loop.error,
      }));
    return Response.json({ experiment_loops: loops });
  }

  private handleExperimentLoopCancel(id: string): Response {
    const ok = this.experimentLoops.cancelLoop(id);
    if (!ok) {
      return Response.json(
        { error: "Experiment loop not found or not running" },
        { status: 404 },
      );
    }
    return Response.json({ ok: true, id, status: "cancelled" });
  }

  // --- Swarm endpoints ---

  private async handleSwarmCreate(req: Request): Promise<Response> {
    let body: FleetSwarmRequest;
    try {
      body = (await req.json()) as FleetSwarmRequest;
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    if (!body.tasks || !Array.isArray(body.tasks) || body.tasks.length === 0) {
      return Response.json(
        { error: "Missing required field: tasks (non-empty array)" },
        { status: 400 },
      );
    }

    try {
      const swarm = await this.swarmCoordinator.create(body);
      const taskSummary: Record<string, string> = {};
      for (const [id, task] of Object.entries(swarm.tasks)) {
        taskSummary[id] = task.status;
      }
      return Response.json({
        id: swarm.id,
        status: swarm.status,
        tasks: taskSummary,
        status_url: `/v1/fleet/swarm/${swarm.id}`,
        created_at: swarm.created_at,
      });
    } catch (e) {
      return Response.json({ error: (e as Error).message }, { status: 400 });
    }
  }

  private handleSwarmStatus(id: string): Response {
    const swarm = this.swarmCoordinator.getSwarm(id);
    if (!swarm) {
      return Response.json({ error: "Swarm not found" }, { status: 404 });
    }
    return Response.json(swarm);
  }

  private handleSwarmList(url: URL): Response {
    const status = url.searchParams.get("status") as
      | "pending"
      | "running"
      | "completed"
      | "failed"
      | null;
    const swarms = this.swarmCoordinator
      .listSwarms(status ?? undefined)
      .map((s) => ({
        id: s.id,
        status: s.status,
        created_at: s.created_at,
        completed_at: s.completed_at,
        task_count: Object.keys(s.tasks).length,
        error: s.error,
      }));
    return Response.json({ swarms });
  }

  private handleSwarmKill(id: string): Response {
    const killed = this.swarmCoordinator.killSwarm(id);
    if (!killed) {
      return Response.json(
        { error: "Swarm not found or not running" },
        { status: 404 },
      );
    }
    return Response.json({ ok: true, id, status: "killed" });
  }

  private trackStreamingUsage(
    body: ReadableStream<Uint8Array>,
    usageCtx: {
      provider: string;
      model: string;
      accountId: string;
      startedAt: number;
    },
    onComplete?: () => void,
  ): ReadableStream<Uint8Array> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let inputTokens = 0;
    let outputTokens = 0;

    // Idempotent finish — called from done, error, AND cancel paths.
    // Prevents double usage recording and double semaphore release.
    let finished = false;
    const finishOnce = () => {
      if (finished) return;
      finished = true;
      this.usageDB.record({
        timestamp: Date.now(),
        provider: usageCtx.provider,
        model: usageCtx.model,
        account_id: usageCtx.accountId,
        input_tokens: inputTokens,
        output_tokens: outputTokens,
        latency_ms: Date.now() - usageCtx.startedAt,
        status: "success",
        error: null,
        ...(inputTokens === 0 && outputTokens === 0
          ? { tokens_reported: false }
          : {}),
      });
      onComplete?.();
    };

    const encoder = new TextEncoder();
    return new ReadableStream({
      async pull(controller) {
        let done: boolean;
        let value: Uint8Array | undefined;
        try {
          ({ done, value } = await reader.read());
        } catch (e) {
          // Mid-stream connection drop (ECONNRESET, socket hang up, etc.)
          // Emit an SSE error event so Claude Code sees a clean termination
          // instead of hanging on a dead stream.
          const msg = e instanceof Error ? e.message : String(e);
          console.error(
            `[proxy] ⚠️ Upstream stream error during trackStreamingUsage: ${msg}`,
          );
          const errorEvent = `event: error\ndata: {"type":"error","error":{"type":"api_error","message":"Upstream connection lost: ${msg.replace(/"/g, '\\"')}"}}\n\n`;
          controller.enqueue(encoder.encode(errorEvent));
          finishOnce();
          controller.close();
          return;
        }
        if (done) {
          finishOnce();
          controller.close();
          return;
        }

        const text = decoder.decode(value, { stream: true });
        buffer += text;

        let split = buffer.split("\n\n");
        buffer = split.pop() ?? "";
        for (const block of split) {
          const dataLine = block
            .split("\n")
            .find((line) => line.startsWith("data: "));
          if (!dataLine) continue;
          // Fast path: skip JSON parse for content deltas (majority of events)
          const raw = dataLine.slice(6);
          if (
            raw.includes('"content_block_delta"') ||
            raw.includes('"content_block_start"') ||
            raw.includes('"content_block_stop"') ||
            raw.includes('"ping"')
          )
            continue;
          try {
            const parsed = JSON.parse(raw);
            if (
              parsed?.type === "message_start" &&
              typeof parsed?.message?.usage?.input_tokens === "number"
            ) {
              inputTokens = parsed.message.usage.input_tokens;
            }
            if (
              parsed?.type === "message_delta" &&
              typeof parsed?.usage?.output_tokens === "number"
            ) {
              outputTokens = parsed.usage.output_tokens;
            }
          } catch {
            // ignore malformed chunk
          }
        }

        // Pass through original bytes — avoid re-encode to prevent
        // multi-byte UTF-8 corruption across chunk boundaries
        controller.enqueue(value);
      },
      cancel() {
        finishOnce();
        reader.cancel();
      },
    });
  }

  private rewriteStreamModel(
    body: ReadableStream<Uint8Array>,
    fromModel: string,
    toModel: string,
  ): ReadableStream<Uint8Array> {
    const reader = body.getReader();
    const encoder = new TextEncoder();
    const decoder = new TextDecoder();
    // Buffer first ~500 bytes to handle model name split across chunk boundaries
    let rewritten = false;
    let pendingChunks: Uint8Array[] = [];
    let pendingLen = 0;
    const BUFFER_THRESHOLD = 500;

    return new ReadableStream({
      async pull(controller) {
        let done: boolean;
        let value: Uint8Array | undefined;
        try {
          ({ done, value } = await reader.read());
        } catch (e) {
          // Mid-stream connection drop (ECONNRESET, socket hang up, etc.)
          const msg = e instanceof Error ? e.message : String(e);
          console.error(
            `[proxy] ⚠️ Upstream stream error during rewriteStreamModel: ${msg}`,
          );
          for (const chunk of pendingChunks) controller.enqueue(chunk);
          pendingChunks = [];
          const errorEvent = `event: error\ndata: {"type":"error","error":{"type":"api_error","message":"Upstream connection lost: ${msg.replace(/"/g, '\\"')}"}}\n\n`;
          controller.enqueue(encoder.encode(errorEvent));
          controller.close();
          return;
        }
        if (done || !value) {
          if (!rewritten && pendingChunks.length > 0) {
            const combined = new Uint8Array(pendingLen);
            let offset = 0;
            for (const chunk of pendingChunks) {
              combined.set(chunk, offset);
              offset += chunk.byteLength;
            }
            pendingChunks = [];
            pendingLen = 0;
            const text = decoder.decode(combined, { stream: false });
            if (text.includes(fromModel)) {
              controller.enqueue(
                encoder.encode(text.replaceAll(fromModel, toModel)),
              );
            } else {
              controller.enqueue(combined);
            }
          } else {
            for (const chunk of pendingChunks) controller.enqueue(chunk);
            pendingChunks = [];
          }
          controller.close();
          return;
        }
        if (!rewritten) {
          pendingChunks.push(value);
          pendingLen += value.byteLength;
          if (pendingLen >= BUFFER_THRESHOLD || done) {
            // Decode accumulated buffer and attempt rewrite
            const combined = new Uint8Array(pendingLen);
            let offset = 0;
            for (const chunk of pendingChunks) {
              combined.set(chunk, offset);
              offset += chunk.byteLength;
            }
            pendingChunks = [];
            pendingLen = 0;
            // Decode as a complete unit (stream: false) so no bytes are held
            // back by the TextDecoder. Model names are ASCII so splitting is
            // not a concern, but stale decoder state would corrupt subsequent
            // raw-byte pass-through chunks (line below the rewritten guard).
            const text = decoder.decode(combined, { stream: false });
            if (text.includes(fromModel)) {
              const fixed = text.replaceAll(fromModel, toModel);
              controller.enqueue(encoder.encode(fixed));
            } else {
              // No match in buffer — pass original bytes to avoid re-encode corruption
              controller.enqueue(combined);
            }
            rewritten = true;
          }
          return;
        }
        controller.enqueue(value);
      },
      cancel() {
        reader.cancel();
      },
    });
  }

  stop(): void {
    this.stopRuntimeMonitors();
    this.server?.stop();
  }
}
