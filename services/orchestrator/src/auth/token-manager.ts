import { EventEmitter } from "events";
import { homedir } from "os";
import { join, basename } from "path";
import type {
  Config,
  Provider,
  ProviderAccount,
  ProviderConfig,
  TokenFile,
  TokenHealth,
  TokenEvent,
  Tier,
} from "../types.ts";

export function getTokenScanPatterns(
  provider: Provider,
  provConfig: ProviderConfig,
): string[] {
  const configured = Array.isArray(provConfig.pattern)
    ? provConfig.pattern
    : provConfig.pattern
      ? [provConfig.pattern]
      : [];
  const patterns = [...configured];
  if (provider === "gemini" && !patterns.includes("gemini-*.json")) {
    patterns.push("gemini-*.json");
  }
  return [...new Set(patterns.filter(Boolean))];
}

export class TokenManager extends EventEmitter {
  private accounts: Map<string, ProviderAccount> = new Map();
  private config: Config;
  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private refreshTimer: ReturnType<typeof setInterval> | null = null;
  private rateLimitTimers: Map<string, ReturnType<typeof setTimeout>> =
    new Map();
  private activeRequests: Map<string, number> = new Map();
  private refreshingAccounts = new Set<string>();
  private refreshLoopInProgress = false;
  private rescanLoopInProgress = false;
  private refreshBackoffUntil = new Map<string, number>();
  private logThrottle = new Map<string, number>();
  private expiredNotified = new Set<string>();
  // During rescan, _scanTarget collects new accounts while this.accounts
  // remains readable by concurrent getHealthyAccounts()/getAllAccounts() calls.
  private _scanTarget: Map<string, ProviderAccount> | null = null;
  // Snapshot of accounts at rescan start; held until atomic swap so readers
  // always see a consistent, non-empty map even if an await lands after
  // _scanTarget is cleared but before this.accounts is replaced.
  private _rescanSnapshot: Map<string, ProviderAccount> | null = null;

  constructor(config: Config) {
    super();
    this.config = config;
  }

  async init(): Promise<void> {
    await this.scanAllTokens();
    this.startHealthChecks();
    this.startRefreshLoop();
    this.startRescanLoop();
  }

  updateConfig(config: Config): void {
    this.config = config;
  }

  private shouldLog(key: string, minIntervalMs: number): boolean {
    const now = Date.now();
    const nextAllowed = this.logThrottle.get(key) ?? 0;
    if (now < nextAllowed) return false;
    this.logThrottle.set(key, now + minIntervalMs);
    return true;
  }

  private logThrottled(
    key: string,
    message: string,
    minIntervalMs = 60_000,
    level: "log" | "warn" | "error" = "log",
  ): void {
    if (!this.shouldLog(key, minIntervalMs)) return;
    if (level === "warn") console.warn(message);
    else if (level === "error") console.error(message);
    else console.log(message);
  }

  /** Returns the map that scanAllTokens/loadTokenFile should write to. */
  private get scanMap(): Map<string, ProviderAccount> {
    return this._scanTarget ?? this.accounts;
  }

  private async scanAllTokens(): Promise<void> {
    for (const [providerName, provConfig] of Object.entries(
      this.config.providers,
    )) {
      const provider = providerName as Provider;
      const addApiKeyAccount = () => {
        if (!provConfig.api_key || provConfig.api_key.length === 0) return;
        const id = `${providerName}-apikey`;
        this.scanMap.set(id, {
          id,
          provider,
          tier: provConfig.tier,
          filename: "",
          accessToken: provConfig.api_key,
          health: "healthy",
          expiresAt: null,
          lastUsed: null,
          lastRefresh: null,
          errorCount: 0,
          lastError: null,
          disabled: false,
          circuitFailures: [],
          circuitOpenUntil: null,
          rateLimitedUntil: null,
          rateLimitHits: 0,
          lastRateLimitAt: null,
          quotaRemaining: null,
          quotaLimit: null,
          quotaResetAt: null,
        });
      };

      if (!provConfig.token_dir || !provConfig.pattern) {
        // API key providers (glm, minimax, openrouter) and any provider without token scanning
        addApiKeyAccount();
        continue;
      }

      const tokenDir = provConfig.token_dir;
      const patterns = getTokenScanPatterns(provider, provConfig);
      let loadedTokenFiles = 0;
      const seenFiles = new Set<string>();

      for (const pattern of patterns) {
        const glob = new Bun.Glob(pattern);
        try {
          for await (const file of glob.scan({ cwd: tokenDir })) {
            const fullPath = join(tokenDir, file);
            if (seenFiles.has(fullPath)) continue;
            seenFiles.add(fullPath);
            await this.loadTokenFile(fullPath, provider, provConfig.tier);
            loadedTokenFiles++;
          }
        } catch (e) {
          console.error(
            `[token-manager] Error scanning ${tokenDir}/${pattern}:`,
            e,
          );
        }
      }

      // Hybrid providers should still work with API key auth when no token files are present.
      if (loadedTokenFiles === 0) addApiKeyAccount();
    }

    // Deduplicate accounts with same email+provider (different filename encodings)
    // For codex: personal/team/free are separate org accounts with distinct quotas — never dedup them
    const seen = new Map<string, string>(); // "provider:email[:orgId]" → account id
    const dupes: string[] = [];
    const target = this.scanMap;
    for (const [id, account] of target) {
      if (!account.email) continue;
      let key = `${account.provider}:${account.email}`;
      // Codex filenames encode orgId: codex-{orgId}-{email}-{type}.json
      // Different orgIds = different accounts, must not be deduplicated
      if (account.provider === "codex") {
        const orgMatch = account.filename.match(/^codex-([a-f0-9]+)-/);
        if (orgMatch) key += `:${orgMatch[1]}`;
      }
      // Claude/antigravity: personal vs team are separate workspaces with distinct quotas
      if (account.provider === "claude" || account.provider === "antigravity") {
        const typeMatch = account.filename.match(
          /-(personal|team|free)\.json$/,
        );
        if (typeMatch) key += `:${typeMatch[1]}`;
      }
      const existing = seen.get(key);
      if (existing) {
        // Keep the one with later expiry, or the first one if no expiry
        const existingAccount = target.get(existing)!;
        const existingExpiry = existingAccount.expiresAt?.getTime() ?? 0;
        const currentExpiry = account.expiresAt?.getTime() ?? 0;
        if (currentExpiry > existingExpiry) {
          dupes.push(existing);
          seen.set(key, id);
        } else {
          dupes.push(id);
        }
      } else {
        seen.set(key, id);
      }
    }
    for (const dupeId of dupes) {
      target.delete(dupeId);
      console.log(`[token-manager] Deduplicated: removed ${dupeId}`);
    }
  }

  private async loadTokenFile(
    path: string,
    provider: Provider,
    tier: Tier,
  ): Promise<void> {
    try {
      const raw = await Bun.file(path).text();
      const token = JSON.parse(raw) as TokenFile;
      const filename = basename(path);
      const id = `${provider}-${filename}`;

      if (token.disabled) {
        console.log(`[token-manager] Skipping disabled token: ${id}`);
        return;
      }

      // Check email exclusion list (e.g., to permanently block certain accounts)
      const email =
        "email" in token ? (token as { email?: string }).email : undefined;
      if (email) {
        const lower = email.toLowerCase();
        // Global exclusion — blocks this email across ALL providers
        if (
          this.config.excluded_emails?.length &&
          this.config.excluded_emails.some((e) => e.toLowerCase() === lower)
        ) {
          console.log(
            `[token-manager] Skipping excluded email: ${id} (${email})`,
          );
          return;
        }
        // Per-provider exclusion — blocks this email for specific providers only
        // e.g. exclude user@example.com from claude but keep their gemini/codex/antigravity
        const providerExclusions =
          this.config.excluded_provider_emails?.[provider];
        if (
          providerExclusions?.length &&
          providerExclusions.some((e) => e.toLowerCase() === lower)
        ) {
          console.log(
            `[token-manager] Skipping ${provider}-excluded email: ${id} (${email})`,
          );
          return;
        }
      }

      const account = this.tokenToAccount(id, provider, tier, filename, token);
      this.scanMap.set(id, account);

      this.emit("token_event", {
        type: account.health === "healthy" ? "token_healthy" : "token_degraded",
        account,
        ...(account.health !== "healthy" ? { reason: "initial scan" } : {}),
      } as TokenEvent);
    } catch (e) {
      console.error(`[token-manager] Failed to load ${path}:`, e);
    }
  }

  private tokenToAccount(
    id: string,
    provider: Provider,
    tier: Tier,
    filename: string,
    token: TokenFile,
  ): ProviderAccount {
    let accessToken = "";
    let expiresAt: Date | null = null;
    let email: string | undefined;
    let lastRefresh: Date | null = null;

    switch (token.type) {
      case "claude":
        accessToken = token.access_token;
        expiresAt = new Date(token.expired);
        email = token.email;
        lastRefresh = new Date(token.last_refresh);
        break;
      case "antigravity":
        accessToken = token.access_token;
        expiresAt = new Date(token.expired);
        email = token.email;
        break;
      case "codex":
        accessToken = token.access_token;
        expiresAt = new Date(token.expired);
        email = token.email;
        lastRefresh = new Date(token.last_refresh);
        break;
      case "gemini":
        accessToken = token.token.access_token;
        expiresAt = new Date(token.token.expiry);
        email = token.email;
        break;
      case "kimi":
        accessToken = token.access_token;
        expiresAt = new Date(token.expired);
        lastRefresh = new Date(token.last_refresh);
        break;
    }

    const health = this.assessHealth(expiresAt, token.disabled);

    return {
      id,
      provider,
      tier,
      filename,
      email,
      accessToken,
      health,
      expiresAt,
      lastUsed: null,
      lastRefresh,
      errorCount: 0,
      lastError: null,
      disabled: token.disabled,
      circuitFailures: [],
      circuitOpenUntil: null,
      rateLimitedUntil: null,
      rateLimitHits: 0,
      lastRateLimitAt: null,
      quotaRemaining: null,
      quotaLimit: null,
      quotaResetAt: null,
    };
  }

  private assessHealth(expiresAt: Date | null, disabled: boolean): TokenHealth {
    if (disabled) return "dead";
    if (!expiresAt) return "healthy"; // API key tokens don't expire
    const now = Date.now();
    const expiryMs = expiresAt.getTime();
    if (expiryMs < now) return "expired";
    // Degraded if expiring within lead_time
    const leadTime = this.config.health.token_refresh.lead_time * 1000;
    if (expiryMs - now < leadTime) return "degraded";
    return "healthy";
  }

  getHealthyAccounts(provider?: Provider): ProviderAccount[] {
    const now = Date.now();
    const accounts: ProviderAccount[] = [];
    // If a rescan is in progress, read from the pre-rescan snapshot so callers
    // never see an empty or partially-populated map during the async scan or
    // the synchronous window between _scanTarget=null and this.accounts=next.
    const source = this._rescanSnapshot ?? this.accounts;

    for (const account of source.values()) {
      if (provider && account.provider !== provider) continue;
      if (account.disabled) continue;
      if (account.health === "dead" || account.health === "expired") continue;
      // Skip accounts expiring within 2 minutes — too risky, may fail mid-request.
      // Degraded tokens (within lead_time) are still routable but sorted after healthy.
      if (account.expiresAt) {
        const timeToExpiry = account.expiresAt.getTime() - now;
        if (timeToExpiry > 0 && timeToExpiry < 2 * 60 * 1000) continue;
      }
      // Check circuit breaker
      if (account.circuitOpenUntil && now < account.circuitOpenUntil) continue;
      // Check rate limit window
      if (account.rateLimitedUntil && now < account.rateLimitedUntil) continue;
      // If circuit was open but cooldown passed, reset it
      if (account.circuitOpenUntil && now >= account.circuitOpenUntil) {
        account.circuitOpenUntil = null;
        account.circuitFailures = [];
        this.emit("token_event", {
          type: "circuit_close",
          account,
        } as TokenEvent);
      }
      accounts.push(account);
    }

    // Sort: healthy first, then degraded.
    // Within same health: least active connections first, then least recently used.
    return accounts.sort((a, b) => {
      if (a.health !== b.health) {
        return a.health === "healthy" ? -1 : 1;
      }
      // Prefer accounts with more remaining quota (from response headers)
      const aQuota = a.quotaRemaining ?? Infinity;
      const bQuota = b.quotaRemaining ?? Infinity;
      if (aQuota !== bQuota && aQuota !== Infinity && bQuota !== Infinity) {
        return bQuota - aQuota; // higher remaining = better
      }
      // Prefer accounts with fewer active requests (least-connections)
      const aActive = this.activeRequests.get(a.id) ?? 0;
      const bActive = this.activeRequests.get(b.id) ?? 0;
      if (aActive !== bActive) return aActive - bActive;
      // Tiebreak: least recently used
      const aTime = a.lastUsed?.getTime() ?? 0;
      const bTime = b.lastUsed?.getTime() ?? 0;
      return aTime - bTime;
    });
  }

  /**
   * Returns true if a provider has at least one truly healthy (non-degraded) account
   * with no open circuit breaker. Used by the router to skip dead providers entirely.
   */
  hasHealthyProvider(provider: Provider): boolean {
    const now = Date.now();
    for (const account of this.accounts.values()) {
      if (account.provider !== provider) continue;
      if (account.disabled) continue;
      if (account.health !== "healthy") continue;
      if (account.circuitOpenUntil && now < account.circuitOpenUntil) continue;
      return true;
    }
    return false;
  }

  getAllAccounts(): ProviderAccount[] {
    return Array.from((this._rescanSnapshot ?? this.accounts).values());
  }

  getAccount(id: string): ProviderAccount | undefined {
    return this.accounts.get(id);
  }

  markUsed(id: string): void {
    const account = this.accounts.get(id);
    if (account) account.lastUsed = new Date();
  }

  incrementActive(id: string): void {
    this.activeRequests.set(id, (this.activeRequests.get(id) ?? 0) + 1);
  }

  decrementActive(id: string): void {
    const current = this.activeRequests.get(id) ?? 0;
    if (current > 0) this.activeRequests.set(id, current - 1);
  }

  getActiveCount(id: string): number {
    return this.activeRequests.get(id) ?? 0;
  }

  recordFailure(id: string, error: string): void {
    const account = this.accounts.get(id);
    if (!account) return;

    account.errorCount++;
    account.lastError = error;
    account.circuitFailures.push({ timestamp: Date.now() });

    // Clean old failures outside the window, cap at 20 to prevent unbounded growth
    const window = 60_000; // 60 seconds
    const cutoff = Date.now() - window;
    account.circuitFailures = account.circuitFailures.filter(
      (f) => f.timestamp > cutoff,
    );
    if (account.circuitFailures.length > 20) {
      account.circuitFailures = account.circuitFailures.slice(-20);
    }

    // Check circuit breaker threshold
    const threshold = this.config.health.circuit_breaker.threshold;
    if (account.circuitFailures.length >= threshold) {
      const cooldown = this.config.health.circuit_breaker.cooldown * 1000;
      account.circuitOpenUntil = Date.now() + cooldown;
      console.log(
        `[token-manager] Circuit OPEN for ${id} — ${threshold} failures in 60s, cooldown ${cooldown / 1000}s`,
      );
      this.emit("token_event", { type: "circuit_open", account } as TokenEvent);
    }
  }

  resetCircuit(id: string): boolean {
    const account = this.accounts.get(id);
    if (!account || !account.circuitOpenUntil) return false;
    account.circuitOpenUntil = null;
    account.circuitFailures = [];
    this.emit("token_event", { type: "circuit_close", account } as TokenEvent);
    return true;
  }

  recordRateLimit(id: string, errorBody: string): void {
    const account = this.accounts.get(id);
    if (!account) return;

    account.rateLimitHits++;
    account.lastRateLimitAt = Date.now();

    // Parse reset timing from error response
    const resetSeconds = this.parseResetSeconds(errorBody);
    let newLimitUntil: number;
    if (resetSeconds > 0) {
      newLimitUntil = Date.now() + resetSeconds * 1000;
    } else {
      const backoffMinutes = Math.min(
        5 * Math.min(account.rateLimitHits, 4),
        20,
      );
      newLimitUntil = Date.now() + backoffMinutes * 60_000;
    }

    // Preserve existing rateLimitedUntil if it's further in the future
    // (e.g., budget parking sets midnight — don't shorten it with a 5min backoff)
    if (account.rateLimitedUntil && account.rateLimitedUntil > newLimitUntil) {
      console.log(
        `[token-manager] Rate limit on ${id}: keeping existing limit until ${new Date(account.rateLimitedUntil).toISOString()} (new would be ${new Date(newLimitUntil).toISOString()})`,
      );
    } else {
      account.rateLimitedUntil = newLimitUntil;
      if (resetSeconds > 0) {
        console.log(
          `[token-manager] Rate limit on ${id}: resets in ${resetSeconds}s (at ${new Date(account.rateLimitedUntil).toISOString()})`,
        );
      } else {
        const backoffMinutes = Math.min(
          5 * Math.min(account.rateLimitHits, 4),
          20,
        );
        console.log(
          `[token-manager] Rate limit on ${id}: no reset info, backing off ${backoffMinutes}m (hit #${account.rateLimitHits})`,
        );
      }
    }

    // Clear any prior recovery timer for this account
    const prevTimer = this.rateLimitTimers.get(id);
    if (prevTimer) clearTimeout(prevTimer);

    // Schedule recovery probe
    const resetMs =
      (account.rateLimitedUntil ?? Date.now() + 300_000) - Date.now();
    const timer = setTimeout(() => {
      this.rateLimitTimers.delete(id);
      // Re-fetch account in case rescanTokens() replaced the object
      const current = this.accounts.get(id);
      if (current?.rateLimitedUntil && Date.now() >= current.rateLimitedUntil) {
        current.rateLimitedUntil = null;
        console.log(
          `[token-manager] Rate limit window passed for ${id}, re-enabling`,
        );
        this.emit("token_event", {
          type: "rate_limit_clear",
          account: current,
        } as TokenEvent);
      }
    }, resetMs + 5000); // 5s grace period after reset
    this.rateLimitTimers.set(id, timer);
  }

  /**
   * Park an account until a specific timestamp (e.g., midnight for budget enforcement).
   * Uses the same rateLimitedUntil mechanism as rate limit handling.
   */
  parkUntil(id: string, until: number): void {
    const account = this.accounts.get(id);
    if (!account) return;
    account.rateLimitedUntil = until;

    const prev = this.rateLimitTimers.get(id);
    if (prev) clearTimeout(prev);

    const resetMs = until - Date.now();
    if (resetMs <= 0) return;

    const timer = setTimeout(() => {
      this.rateLimitTimers.delete(id);
      const current = this.accounts.get(id);
      if (current?.rateLimitedUntil && Date.now() >= current.rateLimitedUntil) {
        current.rateLimitedUntil = null;
        console.log(
          `[token-manager] Budget park expired for ${id}, re-enabling`,
        );
        this.emit("token_event", {
          type: "budget_unpark",
          account: current,
        } as TokenEvent);
      }
    }, resetMs + 5000);
    this.rateLimitTimers.set(id, timer);
  }

  /**
   * Immediately unpark a budget-parked account.
   * Clears rateLimitedUntil AND the associated recovery timer.
   */
  unpark(id: string): boolean {
    const account = this.accounts.get(id);
    if (!account) return false;
    if (!account.rateLimitedUntil) return false;

    account.rateLimitedUntil = null;

    const timer = this.rateLimitTimers.get(id);
    if (timer) {
      clearTimeout(timer);
      this.rateLimitTimers.delete(id);
    }

    this.emit("token_event", {
      type: "budget_unpark",
      account,
    } as TokenEvent);

    return true;
  }

  private parseResetSeconds(errorBody: string): number {
    try {
      // Strip "429: " or "503: " status prefix from proxy error format
      const jsonBody = errorBody.replace(/^\d{3}:\s*/, "");
      const parsed = JSON.parse(jsonBody);
      const err = parsed?.error ?? parsed;

      // CLIProxyAPI format: { error: { reset_seconds: 15 } }
      if (err?.reset_seconds) return Number(err.reset_seconds);

      // Parse reset timing from error message — covers multiple formats:
      // Anthropic: "Please retry after X seconds"
      // Gemini/Antigravity via CLIProxyAPI: "quota will reset after Xs"
      const msg = err?.message ?? "";
      const resetMatch = msg.match(/(?:retry|reset) after (\d+)\s*s/i);
      if (resetMatch) return Number(resetMatch[1]);

      // Gemini format: { quotaResetDelay: "97h21m51.455920606s" }
      if (err?.quotaResetDelay) {
        const d = err.quotaResetDelay;
        if (typeof d !== "string") return 0;
        const hours = d.match(/(\d+)h/)?.[1] ?? 0;
        const mins = d.match(/(\d+)m/)?.[1] ?? 0;
        const secs = d.match(/([\d.]+)s/)?.[1] ?? 0;
        return (
          Number(hours) * 3600 + Number(mins) * 60 + Math.ceil(Number(secs))
        );
      }

      // Gemini: quotaResetTimeStamp as ISO date
      if (err?.quotaResetTimeStamp) {
        const resetTime = new Date(err.quotaResetTimeStamp).getTime();
        return Math.max(0, Math.ceil((resetTime - Date.now()) / 1000));
      }
    } catch {
      // Not JSON — try regex on raw body
      const match = errorBody.match(
        /(?:reset|retry)\s*(?:after|in|seconds)[:\s]*(\d+)\s*s?/i,
      );
      if (match) return Number(match[1]);
    }
    return 0;
  }

  recordSuccess(id: string): void {
    const account = this.accounts.get(id);
    if (!account) return;
    account.errorCount = 0;
    account.lastError = null;
    // Clear rate limit on success
    if (account.rateLimitedUntil) {
      account.rateLimitedUntil = null;
      account.rateLimitHits = 0;
    }
  }

  /**
   * Update quota tracking from response rate limit headers.
   * Parks the account proactively when remaining quota drops below threshold.
   */
  updateQuota(
    id: string,
    remaining: number | null,
    limit: number | null,
    resetSeconds: number | null,
  ): void {
    const account = this.accounts.get(id);
    if (!account) return;

    if (remaining !== null) account.quotaRemaining = remaining;
    if (limit !== null) account.quotaLimit = limit;
    if (resetSeconds !== null && resetSeconds > 0) {
      account.quotaResetAt = Date.now() + resetSeconds * 1000;
    }

    // Proactive parking: if remaining drops to 0, park until reset
    if (remaining !== null && remaining <= 0) {
      const resetMs = resetSeconds ? resetSeconds * 1000 : 300_000;
      account.rateLimitedUntil = Date.now() + resetMs;
      console.log(
        `[token-manager] ⚠️ Quota exhausted on ${id}: remaining=${remaining}, parking for ${resetSeconds ?? 300}s`,
      );
      this.emit("token_event", {
        type: "token_degraded",
        account,
        reason: "quota_exhausted_proactive",
      } as TokenEvent);
    }
  }

  private startHealthChecks(): void {
    const interval = this.config.health.check_interval * 1000;
    this.healthCheckTimer = setInterval(() => this.runHealthChecks(), interval);
    // Run once on start
    this.runHealthChecks();
  }

  private runHealthChecks(): void {
    for (const account of this.accounts.values()) {
      const prevHealth = account.health;
      account.health = this.assessHealth(account.expiresAt, account.disabled);

      if (prevHealth !== account.health) {
        console.log(
          `[token-manager] ${account.id}: ${prevHealth} → ${account.health}`,
        );
        if (account.health === "expired") {
          if (!this.expiredNotified.has(account.id)) {
            this.expiredNotified.add(account.id);
            this.emit("token_event", {
              type: "token_expired",
              account,
            } as TokenEvent);
          }
        } else if (account.health === "degraded") {
          this.expiredNotified.delete(account.id);
          this.emit("token_event", {
            type: "token_degraded",
            account,
            reason: "approaching expiry",
          } as TokenEvent);
        } else if (account.health === "healthy") {
          this.expiredNotified.delete(account.id);
        }
      } else if (account.health !== "expired") {
        this.expiredNotified.delete(account.id);
      }
    }
  }

  private rescanTimer: ReturnType<typeof setInterval> | null = null;

  private startRefreshLoop(): void {
    const interval = this.config.health.token_refresh.interval * 1000;
    this.refreshTimer = setInterval(() => this.runRefreshTick(), interval);
    // Run once shortly after startup to recover expired tokens
    setTimeout(() => this.runRefreshTick(), 5_000);
  }

  /**
   * Periodic disk rescan — picks up tokens refreshed externally by CLIProxyAPI's
   * own auto-refresh, new logins, and removed/disabled files.
   * Runs every 5 minutes independently of the refresh loop.
   */
  private startRescanLoop(): void {
    const RESCAN_INTERVAL =
      this.config.health.token_rescan?.interval_ms ?? 5 * 60 * 1000;
    this.rescanTimer = setInterval(async () => {
      if (this.rescanLoopInProgress || this.refreshLoopInProgress) return;
      this.rescanLoopInProgress = true;
      const prevCount = this.accounts.size;
      try {
        await this.rescanTokens();
        const newCount = this.accounts.size;
        if (newCount !== prevCount) {
          const healthy = [...this.accounts.values()].filter(
            (a) => a.health === "healthy",
          ).length;
          console.log(
            `[token-manager] Rescan: ${prevCount} → ${newCount} accounts (${healthy} healthy)`,
          );
        }
      } finally {
        this.rescanLoopInProgress = false;
      }
    }, RESCAN_INTERVAL);
  }

  private async runRefreshTick(): Promise<void> {
    if (this.refreshLoopInProgress || this.rescanLoopInProgress) return;
    this.refreshLoopInProgress = true;
    try {
      await this.refreshExpiring();
    } finally {
      this.refreshLoopInProgress = false;
    }
  }

  // Providers whose OAuth tokens cannot be auto-refreshed by CLIProxyAPI.
  // Polling for these just wastes event-loop time — they need manual re-auth.
  private static NON_REFRESHABLE_PROVIDERS = new Set(["kimi", "antigravity"]);

  private async refreshExpiring(): Promise<void> {
    const now = Date.now();
    const maxParallel = Math.max(
      4,
      this.config.health.token_refresh.max_parallel ?? 4,
    );

    // Collect tokens that need active refresh via cliproxyapi:
    //   1. Expired tokens (up to 72h old) — recover from missed refreshes
    //   2. Degraded tokens (within lead_time of expiry) — proactive refresh
    //      before they expire and cause request failures
    const maxExpiredAge = 72 * 60 * 60 * 1000;
    const leadTime = this.config.health.token_refresh.lead_time * 1000;
    const toRefresh: ProviderAccount[] = [];
    const degradedForReload: ProviderAccount[] = [];
    for (const account of this.accounts.values()) {
      if (!account.expiresAt) continue;
      if (account.disabled) continue;
      const timeToExpiry = account.expiresAt.getTime() - now;
      // Skip providers whose OAuth tokens cannot be auto-refreshed.
      if (TokenManager.NON_REFRESHABLE_PROVIDERS.has(account.provider))
        continue;
      const backoffUntil = this.refreshBackoffUntil.get(account.id) ?? 0;
      if (backoffUntil > now) continue;

      // Skip tokens that have failed too many consecutive refreshes.
      // These likely need manual re-auth, not automated retry.
      const MAX_CONSECUTIVE_FAILURES = 5;
      const consecutiveFailures = this.refreshFailures.get(account.id) ?? 0;
      if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
        // Set a long cooldown to avoid wasting event loop time
        const nonRefreshableCooldown =
          this.config.health.token_refresh.non_refreshable_cooldown_ms ??
          3_600_000;
        if (
          !this.refreshBackoffUntil.has(account.id) ||
          (this.refreshBackoffUntil.get(account.id) ?? 0) < now
        ) {
          this.refreshBackoffUntil.set(
            account.id,
            now + nonRefreshableCooldown,
          );
          this.logThrottled(
            `refresh-exhausted:${account.id}`,
            `[token-manager] ⛔ Token ${account.id} exhausted ${MAX_CONSECUTIVE_FAILURES} refresh attempts — backing off ${Math.round(nonRefreshableCooldown / 60000)}min (manual re-auth needed)`,
            nonRefreshableCooldown,
          );
        }
        continue;
      }

      if (timeToExpiry < 0 && -timeToExpiry < maxExpiredAge) {
        // Expired — needs active refresh
        toRefresh.push(account);
      } else if (timeToExpiry > 0 && timeToExpiry < leadTime) {
        // Degraded — try disk reload first, then active refresh if still degraded
        degradedForReload.push(account);
      }
    }

    // Disk reload for degraded tokens — picks up CLIProxyAPI's own auto-refresh
    if (degradedForReload.length > 0) {
      await Promise.allSettled(
        degradedForReload.map((account) =>
          this.reloadTokenFile(account).catch(() => {}),
        ),
      );
      // After reload, check if still degraded — if so, actively refresh
      for (const account of degradedForReload) {
        if (!account.expiresAt) continue;
        const timeToExpiry = account.expiresAt.getTime() - Date.now();
        if (timeToExpiry > 0 && timeToExpiry < leadTime) {
          toRefresh.push(account);
        }
      }
    }

    if (toRefresh.length > 0) {
      this.logThrottled(
        "refresh-batch",
        `[token-manager] Refreshing ${toRefresh.length} expired token(s)`,
        15_000,
      );
      const queue = toRefresh.filter(
        (account) => !this.refreshingAccounts.has(account.id),
      );
      let cursor = 0;
      const workers = Array.from({
        length: Math.min(maxParallel, queue.length),
      }).map(async () => {
        while (cursor < queue.length) {
          const index = cursor++;
          const account = queue[index];
          if (!account) continue;
          this.refreshingAccounts.add(account.id);
          try {
            await this.refreshToken(account);
          } finally {
            this.refreshingAccounts.delete(account.id);
          }
        }
      });
      await Promise.allSettled(workers);
    }
  }

  // Track consecutive refresh failures per account for notification throttling
  private refreshFailures: Map<string, number> = new Map();

  private async refreshToken(account: ProviderAccount): Promise<void> {
    this.logThrottled(
      `refresh-start:${account.id}`,
      `[token-manager] Refreshing token for ${account.id}...`,
      60_000,
    );
    try {
      // Gemini uses OAuth2 refresh_token flow directly — cliproxyapi doesn't handle it
      if (account.provider === "gemini") {
        await this.refreshGeminiToken(account);
        this.refreshFailures.delete(account.id);
        this.refreshBackoffUntil.delete(account.id);
        return;
      }

      // Phase 1: Try disk reload (CLIProxyAPI may have already refreshed)
      const prevExpiry = account.expiresAt?.getTime() ?? 0;
      await this.reloadTokenFile(account);
      const afterReload =
        this.accounts.get(account.id)?.expiresAt?.getTime() ?? 0;

      if (afterReload > prevExpiry) {
        console.log(
          `[token-manager] Token ${account.id} refreshed via disk reload`,
        );
        this.refreshFailures.delete(account.id);
        this.refreshBackoffUntil.delete(account.id);
        return;
      }

      // Phase 2: Quick re-check — CLIProxyAPI refreshes tokens on its own timer.
      // ONLY poll if this is the FIRST failure for this token. Subsequent failures
      // mean CLIProxyAPI can't refresh it — sleeping just blocks the event loop.
      let refreshed = false;
      const priorFailures = this.refreshFailures.get(account.id) ?? 0;
      const MAX_RETRIES = priorFailures === 0 ? 2 : 0;
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        // Short waits: 2s, 5s — just enough to catch an in-flight refresh
        const waitMs = attempt === 1 ? 2_000 : 5_000;
        await Bun.sleep(waitMs);

        await this.reloadTokenFile(account);
        const afterRefresh =
          this.accounts.get(account.id)?.expiresAt?.getTime() ?? 0;

        if (afterRefresh > prevExpiry) {
          console.log(
            `[token-manager] Token ${account.id} refreshed via disk reload (attempt ${attempt})`,
          );
          this.refreshFailures.delete(account.id);
          this.refreshBackoffUntil.delete(account.id);
          refreshed = true;
          break;
        }
      }

      if (!refreshed) {
        const failures = (this.refreshFailures.get(account.id) ?? 0) + 1;
        this.refreshFailures.set(account.id, failures);
        const retryCooldownMs =
          this.config.health.token_refresh.retry_cooldown_ms ?? 120_000;
        this.refreshBackoffUntil.set(account.id, Date.now() + retryCooldownMs);

        this.logThrottled(
          `refresh-fail:${account.id}`,
          `[token-manager] ⚠️ Token ${account.id} still expired after ${MAX_RETRIES} refresh attempts (total failures: ${failures})`,
          60_000,
          "warn",
        );

        if (account.provider === "kimi") {
          const nonRefreshableCooldownMs =
            this.config.health.token_refresh.non_refreshable_cooldown_ms ??
            900_000;
          this.refreshBackoffUntil.set(
            account.id,
            Date.now() + nonRefreshableCooldownMs,
          );
          this.logThrottled(
            `refresh-kimi:${account.id}`,
            `[token-manager] Kimi token refresh is not automatic — run: cliproxyapi --kimi-login`,
            10 * 60_000,
            "warn",
          );
        }

        // Notify on first failure and every 3rd failure after (avoid spam)
        if (failures === 1 || failures % 3 === 0) {
          const safeId = account.id.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
          Bun.spawn([
            "osascript",
            "-e",
            `display notification "Token refresh failed: ${safeId} (${failures}x)" with title "ClaudeMax" subtitle "May need manual re-auth"`,
          ]);
        }
      }
    } catch (e) {
      console.error(`[token-manager] Refresh error for ${account.id}:`, e);
      const retryCooldownMs =
        this.config.health.token_refresh.retry_cooldown_ms ?? 120_000;
      this.refreshBackoffUntil.set(account.id, Date.now() + retryCooldownMs);
    }
  }

  private async refreshGeminiToken(account: ProviderAccount): Promise<void> {
    const provConfig = this.config.providers[account.provider];
    if (!provConfig?.token_dir) return;
    const fullPath = join(provConfig.token_dir, account.filename);
    const raw = await Bun.file(fullPath).text();
    const data = JSON.parse(raw);
    const t = data.token;
    if (
      !t?.refresh_token ||
      !t?.client_id ||
      !t?.client_secret ||
      !t?.token_uri
    ) {
      console.error(
        `[token-manager] Gemini token ${account.id} missing refresh credentials`,
      );
      return;
    }

    const params = new URLSearchParams({
      client_id: t.client_id,
      client_secret: t.client_secret,
      refresh_token: t.refresh_token,
      grant_type: "refresh_token",
    });

    const tokenUri = String(t.token_uri || "").trim();
    let tokenHost = "";
    try {
      const parsed = new URL(tokenUri);
      tokenHost = parsed.hostname;
      if (parsed.protocol !== "https:") {
        console.error(
          `[token-manager] Gemini token ${account.id} has non-HTTPS token_uri`,
        );
        return;
      }
    } catch {
      console.error(
        `[token-manager] Gemini token ${account.id} has invalid token_uri`,
      );
      return;
    }

    const allowedHosts = new Set([
      "oauth2.googleapis.com",
      "accounts.google.com",
      "www.googleapis.com",
    ]);
    if (!allowedHosts.has(tokenHost)) {
      console.error(
        `[token-manager] Gemini token ${account.id} token_uri host is not allowlisted (${tokenHost})`,
      );
      return;
    }

    let result: { access_token: string; expires_in: number };

    try {
      const resp = await fetch(tokenUri, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: params.toString(),
        signal: AbortSignal.timeout(15_000),
      });

      if (!resp.ok) {
        const body = await resp.text();
        throw new Error(`HTTP ${resp.status}: ${body.slice(0, 200)}`);
      }
      result = (await resp.json()) as typeof result;
    } catch (fetchErr) {
      const errMsg =
        fetchErr instanceof Error ? fetchErr.message : String(fetchErr);
      const errLower = errMsg.toLowerCase();
      const isTlsError =
        errLower.includes("certificate") ||
        errLower.includes("tls") ||
        errLower.includes("ssl") ||
        errLower.includes("cert");

      if (!isTlsError) {
        console.error(
          `[token-manager] Gemini refresh failed for ${account.id}: ${errMsg}`,
        );
        return;
      }

      // TLS error — fall back to curl (Bun native fetch has intermittent cert issues)
      console.warn(
        `[token-manager] Gemini fetch TLS error for ${account.id}, falling back to curl`,
      );
      const proc = Bun.spawn(
        [
          "curl",
          "-sf",
          "-m",
          "15",
          "-X",
          "POST",
          "-H",
          "Content-Type: application/x-www-form-urlencoded",
          "-d",
          params.toString(),
          tokenUri,
        ],
        { stdout: "pipe", stderr: "pipe" },
      );
      const stdout = await new Response(proc.stdout).text();
      const exitCode = await proc.exited;
      if (exitCode !== 0) {
        const stderr = await new Response(proc.stderr).text();
        console.error(
          `[token-manager] Gemini curl refresh failed for ${account.id}: exit=${exitCode} ${stderr.slice(0, 200)}`,
        );
        return;
      }
      result = JSON.parse(stdout) as typeof result;
    }

    t.access_token = result.access_token;
    t.expiry = new Date(Date.now() + result.expires_in * 1000).toISOString();
    await Bun.write(fullPath, JSON.stringify(data, null, 2));
    await this.reloadTokenFile(account);
    console.log(
      `[token-manager] Gemini token ${account.id} refreshed via OAuth2 (expires ${t.expiry})`,
    );
  }

  private async reloadTokenFile(account: ProviderAccount): Promise<void> {
    const provConfig = this.config.providers[account.provider];
    if (!provConfig?.token_dir) return;
    const fullPath = join(provConfig.token_dir, account.filename);
    const raw = await Bun.file(fullPath).text();
    const token = JSON.parse(raw) as TokenFile;
    const updated = this.tokenToAccount(
      account.id,
      account.provider,
      account.tier,
      account.filename,
      token,
    );
    // Preserve runtime state
    updated.lastUsed = account.lastUsed;
    updated.errorCount = account.errorCount;
    updated.circuitFailures = account.circuitFailures;
    updated.circuitOpenUntil = account.circuitOpenUntil;
    updated.rateLimitedUntil = account.rateLimitedUntil;
    updated.rateLimitHits = account.rateLimitHits;
    updated.lastRateLimitAt = account.lastRateLimitAt;
    updated.quotaRemaining = account.quotaRemaining;
    updated.quotaLimit = account.quotaLimit;
    updated.quotaResetAt = account.quotaResetAt;
    this.accounts.set(account.id, updated);
    this.logThrottled(
      `refresh-success:${account.id}`,
      `[token-manager] Refreshed ${account.id} successfully`,
      30_000,
    );
    this.emit("token_event", {
      type: "token_refreshed",
      account: updated,
    } as TokenEvent);
  }

  async rescanTokens(): Promise<void> {
    // Preserve runtime throttling state to avoid bypassing active rate-limit windows
    // during a token rescan.
    const rateLimitState = new Map<
      string,
      { until: number | null; hits: number; lastAt: number | null }
    >();
    for (const [id, account] of this.accounts.entries()) {
      rateLimitState.set(id, {
        until: account.rateLimitedUntil,
        hits: account.rateLimitHits,
        lastAt: account.lastRateLimitAt,
      });
    }

    // Clear all rate limit timers before rescan
    for (const timer of this.rateLimitTimers.values()) clearTimeout(timer);
    this.rateLimitTimers.clear();

    // Scan into a separate map via _scanTarget. this.accounts remains
    // readable by concurrent getHealthyAccounts()/getAllAccounts() calls
    // throughout the entire async scan — no empty-map race window.
    // _rescanSnapshot pins the pre-scan view so getHealthyAccounts() returns
    // a consistent non-empty result even after _scanTarget is cleared but
    // before this.accounts is replaced with nextAccounts.
    const prevAccounts = this.accounts;
    this._rescanSnapshot = prevAccounts;
    const nextAccounts = new Map<string, ProviderAccount>();
    this._scanTarget = nextAccounts;
    await this.scanAllTokens();
    this._scanTarget = null;

    if (nextAccounts.size === 0 && prevAccounts.size > 0) {
      console.warn(
        `[token-manager] Rescan returned 0 accounts — keeping previous ${prevAccounts.size} accounts`,
      );
      // Don't swap — keep prevAccounts; clear snapshot so readers see live map.
      this._rescanSnapshot = null;
    } else {
      // Restore active rate-limit state BEFORE swapping, so there's no window
      // where a freshly scanned account is visible without its throttling state.
      const now = Date.now();
      for (const [id, state] of rateLimitState.entries()) {
        const account = nextAccounts.get(id);
        if (!account) continue;
        account.rateLimitHits = state.hits;
        account.lastRateLimitAt = state.lastAt;
        if (state.until && state.until > now) {
          account.rateLimitedUntil = state.until;
          const resetMs = state.until - now;
          const timer = setTimeout(() => {
            this.rateLimitTimers.delete(id);
            const current = this.accounts.get(id);
            if (
              current?.rateLimitedUntil &&
              Date.now() >= current.rateLimitedUntil
            ) {
              current.rateLimitedUntil = null;
              this.emit("token_event", {
                type: "rate_limit_clear",
                account: current,
              } as TokenEvent);
            }
          }, resetMs + 5000);
          this.rateLimitTimers.set(id, timer);
        }
      }
      // Restore additional runtime state (circuit breaker, errors, quota, usage)
      // from prevAccounts which still holds the live state during the scan.
      for (const [id, prev] of prevAccounts.entries()) {
        const account = nextAccounts.get(id);
        if (!account) continue;
        account.health = prev.health;
        account.lastUsed = prev.lastUsed;
        account.errorCount = prev.errorCount;
        account.lastError = prev.lastError;
        account.circuitFailures = prev.circuitFailures;
        account.circuitOpenUntil = prev.circuitOpenUntil;
        account.quotaRemaining = prev.quotaRemaining;
        account.quotaLimit = prev.quotaLimit;
        account.quotaResetAt = prev.quotaResetAt;
      }
      // Keep expired-notification dedupe state only for still-present accounts.
      for (const id of [...this.expiredNotified]) {
        if (!nextAccounts.has(id)) this.expiredNotified.delete(id);
      }
      // Atomic swap — all accounts are populated and runtime state restored.
      // Clear snapshot first so getHealthyAccounts() switches to nextAccounts
      // in the same synchronous turn (no interleave possible after this point).
      this._rescanSnapshot = null;
      this.accounts = nextAccounts;
    }
  }

  getMaintenanceState(): {
    refresh_in_progress: boolean;
    rescan_in_progress: boolean;
    refreshing_accounts: number;
    refresh_backoff_accounts: number;
  } {
    return {
      refresh_in_progress: this.refreshLoopInProgress,
      rescan_in_progress: this.rescanLoopInProgress,
      refreshing_accounts: this.refreshingAccounts.size,
      refresh_backoff_accounts: this.refreshBackoffUntil.size,
    };
  }

  shutdown(): void {
    if (this.healthCheckTimer) clearInterval(this.healthCheckTimer);
    if (this.refreshTimer) clearInterval(this.refreshTimer);
    if (this.rescanTimer) clearInterval(this.rescanTimer);
    for (const timer of this.rateLimitTimers.values()) clearTimeout(timer);
    this.rateLimitTimers.clear();
    this.logThrottle.clear();
    this.expiredNotified.clear();
    this.refreshBackoffUntil.clear();
  }
}
