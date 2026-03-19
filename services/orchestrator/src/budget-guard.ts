import type { Config, BudgetLimit } from "./types.ts";
import type { TokenManager } from "./auth/token-manager.ts";
import type { UsageDB } from "./db/usage.ts";

export class BudgetGuard {
  private config: Config;
  private tokenManager: TokenManager;
  private usageDB: UsageDB;
  private timer: ReturnType<typeof setInterval> | null = null;
  private alertedToday: Map<string, string> = new Map(); // key → date
  private blockedToday: Map<string, string> = new Map(); // key → date
  private parkedAccounts: Set<string> = new Set();

  constructor(config: Config, tokenManager: TokenManager, usageDB: UsageDB) {
    this.config = config;
    this.tokenManager = tokenManager;
    this.usageDB = usageDB;
  }

  updateConfig(config: Config): void {
    const wasEnabled = this.config.budget?.enabled;
    const oldInterval = this.config.budget?.check_interval;
    this.config = config;

    const nowEnabled = config.budget?.enabled;
    const newInterval = config.budget?.check_interval;

    // Restart timer if enable state or interval changed
    if (wasEnabled !== nowEnabled || oldInterval !== newInterval) {
      this.stop();
      if (nowEnabled) {
        this.start();
      } else {
        console.log("[budget] Budget enforcement disabled via hot-reload");
      }
    }
  }

  start(): void {
    if (this.timer) return; // Already running — prevent duplicate loops
    const budget = this.config.budget;
    if (!budget?.enabled || !budget.limits?.length) {
      console.log("[budget] Budget enforcement disabled");
      return;
    }

    const interval = (budget.check_interval ?? 60) * 1000;
    console.log(
      `[budget] Enabled — ${budget.limits.length} limit(s), checking every ${interval / 1000}s`,
    );

    this.check();
    this.timer = setInterval(() => this.check(), interval);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  getStatus(): {
    key: string;
    provider: string;
    email?: string;
    tokens_used: number;
    daily_limit: number;
    pct: number;
    alert_at: number;
    stop_at: number;
    status: "ok" | "alert" | "stopped";
    accounts_parked: string[];
  }[] {
    const budget = this.config.budget;
    if (!budget?.enabled) return [];

    const dailyUsage = this.usageDB.getTodayTokensByAccount();
    const results: ReturnType<BudgetGuard["getStatus"]> = [];

    for (const limit of budget.limits) {
      const alertAt = limit.alert_at ?? 0.8;
      const stopAt = limit.stop_at ?? 0.95;
      const key = limit.email
        ? `${limit.provider}:${limit.email}`
        : limit.provider;

      const accounts = this.tokenManager.getAllAccounts().filter((a) => {
        if (a.provider !== limit.provider) return false;
        if (limit.email && a.email !== limit.email) return false;
        return true;
      });

      let totalTokens = 0;
      for (const account of accounts) {
        totalTokens += dailyUsage.get(account.id) ?? 0;
      }

      const pct =
        limit.daily_tokens > 0
          ? Math.round((totalTokens / limit.daily_tokens) * 100)
          : 0;

      const parked = accounts
        .filter((a) => this.parkedAccounts.has(a.id))
        .map((a) => a.id);

      let status: "ok" | "alert" | "stopped" = "ok";
      if (totalTokens >= Math.floor(limit.daily_tokens * stopAt)) {
        status = "stopped";
      } else if (totalTokens >= Math.floor(limit.daily_tokens * alertAt)) {
        status = "alert";
      }

      results.push({
        key,
        provider: limit.provider,
        email: limit.email,
        tokens_used: totalTokens,
        daily_limit: limit.daily_tokens,
        pct,
        alert_at: alertAt,
        stop_at: stopAt,
        status,
        accounts_parked: parked,
      });
    }

    return results;
  }

  /**
   * Unpark all budget-parked accounts for a specific provider (or all).
   * Used for manual resume via management API.
   */
  resume(provider?: string): number {
    let resumed = 0;
    for (const accountId of [...this.parkedAccounts]) {
      const account = this.tokenManager.getAccount(accountId);
      if (!account) {
        this.parkedAccounts.delete(accountId);
        continue;
      }
      if (provider && account.provider !== provider) continue;

      this.tokenManager.unpark(accountId);
      this.parkedAccounts.delete(accountId);
      resumed++;
      console.log(`[budget] Manually resumed ${accountId}`);
    }

    // Clear block state so enforcement won't immediately re-park
    if (provider) {
      for (const [key] of this.blockedToday) {
        if (key === provider || key.startsWith(`${provider}:`)) {
          this.blockedToday.delete(key);
          this.alertedToday.delete(key);
        }
      }
    } else {
      this.blockedToday.clear();
      this.alertedToday.clear();
    }

    return resumed;
  }

  private check(): void {
    const budget = this.config.budget;
    if (!budget?.enabled) return;

    const now = new Date();
    const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
    const dailyUsage = this.usageDB.getTodayTokensByAccount();

    // Day rollover: clear all state and unpark all budget-parked accounts
    let dayRolledOver = false;
    for (const [key, day] of this.blockedToday) {
      if (day !== today) {
        this.blockedToday.delete(key);
        this.alertedToday.delete(key);
        dayRolledOver = true;
      }
    }
    for (const [key, day] of this.alertedToday) {
      if (day !== today) this.alertedToday.delete(key);
    }

    if (dayRolledOver || this.parkedAccounts.size > 0) {
      // Unpark all budget-parked accounts — usage counters reset at midnight
      for (const accountId of [...this.parkedAccounts]) {
        const account = this.tokenManager.getAccount(accountId);
        if (!account) {
          this.parkedAccounts.delete(accountId);
          continue;
        }
        // Unpark if it's a new day OR the park expiry has passed
        if (dayRolledOver || (account.rateLimitedUntil && Date.now() >= account.rateLimitedUntil)) {
          this.tokenManager.unpark(accountId);
          this.parkedAccounts.delete(accountId);
        }
      }
    }

    for (const limit of budget.limits) {
      this.checkLimit(limit, dailyUsage, today);
    }
  }

  private checkLimit(
    limit: BudgetLimit,
    dailyUsage: Map<string, number>,
    today: string,
  ): void {
    const alertAt = limit.alert_at ?? 0.8;
    const stopAt = limit.stop_at ?? 0.95;
    const alertThreshold = Math.floor(limit.daily_tokens * alertAt);
    const stopThreshold = Math.floor(limit.daily_tokens * stopAt);

    const accounts = this.tokenManager.getAllAccounts().filter((a) => {
      if (a.provider !== limit.provider) return false;
      if (limit.email && a.email !== limit.email) return false;
      return true;
    });

    if (accounts.length === 0) return;

    let totalTokens = 0;
    for (const account of accounts) {
      totalTokens += dailyUsage.get(account.id) ?? 0;
    }

    const key = limit.email
      ? `${limit.provider}:${limit.email}`
      : limit.provider;
    const pct = Math.round((totalTokens / limit.daily_tokens) * 100);

    // Check stop threshold
    if (totalTokens >= stopThreshold && this.blockedToday.get(key) !== today) {
      this.blockedToday.set(key, today);
      console.log(
        `[budget] ⛔ STOP ${key}: ${totalTokens.toLocaleString()} / ${limit.daily_tokens.toLocaleString()} tokens (${pct}%)`,
      );

      const midnight = new Date();
      midnight.setHours(24, 0, 0, 0);
      const parkUntil = midnight.getTime();

      let parked = 0;
      for (const account of accounts) {
        if (account.disabled) continue;
        this.tokenManager.parkUntil(account.id, parkUntil);
        this.parkedAccounts.add(account.id);
        parked++;
      }

      this.notify(
        "Budget Stop",
        `${key}: ${pct}% (${totalTokens.toLocaleString()} tokens) — ${parked} account(s) parked until midnight`,
      );
      return;
    }

    // Check alert threshold
    if (totalTokens >= alertThreshold && this.alertedToday.get(key) !== today) {
      this.alertedToday.set(key, today);
      console.log(
        `[budget] ⚠️ ALERT ${key}: ${totalTokens.toLocaleString()} / ${limit.daily_tokens.toLocaleString()} tokens (${pct}%)`,
      );
      this.notify(
        "Budget Alert",
        `${key} at ${pct}%: ${totalTokens.toLocaleString()} / ${limit.daily_tokens.toLocaleString()} tokens`,
      );
    }
  }

  private notify(title: string, message: string): void {
    try {
      // Escape AppleScript string literals — backslashes and double quotes
      const safeTitle = title.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
      const safeMsg = message.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
      Bun.spawn([
        "osascript",
        "-e",
        `display notification "${safeMsg}" with title "${safeTitle}" sound name "Glass"`,
      ]);
    } catch {
      // Notification failure is non-critical
    }
  }
}
