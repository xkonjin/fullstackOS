import { loadConfig, watchConfig, setConfig } from "./config.ts";
import { TokenManager } from "./auth/token-manager.ts";
import { TaskRouter } from "./router/task-router.ts";
import { LearningRouter } from "./router/learning-router.ts";
import { UsageDB } from "./db/usage.ts";
import { WatchdogService } from "./watchdog.ts";
import { BudgetGuard } from "./budget-guard.ts";
import type { Config, TokenEvent } from "./types.ts";
import type { ProxyServer } from "./proxy.ts";
import { homedir } from "os";
import { join } from "path";

const PROVIDER_CANARIES: Partial<Record<string, string>> = {
  glm: "glm-4.7",
  minimax: "MiniMax-M2.5-highspeed",
  // kimi: skipped — routes through CLIProxyAPI OAuth (not direct API key),
  // so canary probe fails when kimi tokens are degraded even though CLIProxyAPI
  // handles auth correctly. Kimi health is verified by CLIProxyAPI, not here.
};

const CANARY_TIMEOUT_MS: Partial<Record<string, number>> = {
  minimax: 60_000,
};

const DEFAULT_CANARY_TIMEOUT_MS = 30_000;
const CANARY_MAX_ATTEMPTS = 2;
const STARTUP_STATE_PATH = join(
  homedir(),
  ".claudemax",
  "orchestrator-startup-state.json",
);

function isTimeoutError(error: unknown): boolean {
  if (!error) return false;
  const msg = error instanceof Error ? error.message : String(error);
  const lowered = msg.toLowerCase();
  return (
    lowered.includes("timed out") ||
    lowered.includes("timeout") ||
    lowered.includes("aborterror") ||
    lowered.includes("request aborted")
  );
}

export async function runProviderCanaries(
  config: Config,
): Promise<Set<string>> {
  const disabled = new Set<string>();
  const apiKey = config.api_keys[0] ?? "your-proxy-key";

  for (const [provider, model] of Object.entries(PROVIDER_CANARIES)) {
    const providerConfig = config.providers[provider];
    if (!providerConfig) continue;

    const timeoutMs = CANARY_TIMEOUT_MS[provider] ?? DEFAULT_CANARY_TIMEOUT_MS;
    let passed = false;
    let timedOut = false;
    let finalError: string | null = null;

    for (let attempt = 1; attempt <= CANARY_MAX_ATTEMPTS; attempt++) {
      try {
        const resp = await fetch(
          `http://127.0.0.1:${config.port}/v1/messages`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${apiKey}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              model,
              max_tokens: 32,
              messages: [
                {
                  role: "user",
                  content: `Return exactly ${provider}-canary-ok`,
                },
              ],
            }),
            signal: AbortSignal.timeout(timeoutMs),
          },
        );

        const routedProvider = resp.headers.get("x-claudemax-provider") ?? "";
        if (!resp.ok || routedProvider !== provider) {
          disabled.add(provider);
          // Drain response body to avoid connection leak
          await resp.text().catch(() => {});
          console.warn(
            `[canary] DISABLE ${provider}: status=${resp.status} routed=${routedProvider || "none"}`,
          );
          break;
        }

        // Drain response body to avoid connection leak
        await resp.text().catch(() => {});
        console.log(`[canary] PASS ${provider} via ${model}`);
        passed = true;
        break;
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        finalError = msg;
        if (isTimeoutError(e)) {
          timedOut = true;
          if (attempt < CANARY_MAX_ATTEMPTS) {
            console.warn(
              `[canary] TIMEOUT ${provider}: attempt ${attempt}/${CANARY_MAX_ATTEMPTS} (${msg})`,
            );
            continue;
          }
        } else {
          disabled.add(provider);
          console.warn(`[canary] DISABLE ${provider}: ${msg}`);
          break;
        }
      }
    }

    if (!passed && timedOut && !disabled.has(provider)) {
      console.warn(
        `[canary] SOFT-FAIL ${provider}: timeout after ${CANARY_MAX_ATTEMPTS} attempts (${finalError ?? "timeout"}) — keeping provider enabled`,
      );
    }
  }

  return disabled;
}

export function removeProvidersFromTiers(
  config: Config,
  providers: Set<string>,
): void {
  if (providers.size === 0) return;
  for (const tier of Object.keys(config.routing.tiers)) {
    const key = tier as keyof typeof config.routing.tiers;
    config.routing.tiers[key] = config.routing.tiers[key].filter(
      (p) => !providers.has(p),
    );
  }
}

export async function applyProviderCanaryGate(config: Config): Promise<void> {
  const disabled = await runProviderCanaries(config);
  if (disabled.size === 0) return;
  removeProvidersFromTiers(config, disabled);
  const list = [...disabled].join(", ");
  console.warn(
    `[canary] Active gate removed provider(s) from routing tiers: ${list}`,
  );
}

export async function waitForProxyReady(port: number): Promise<void> {
  for (let i = 0; i < 20; i++) {
    try {
      const resp = await fetch(`http://127.0.0.1:${port}/health`, {
        signal: AbortSignal.timeout(1_000),
      });
      if (resp.ok) return;
    } catch {
      // retry
    }
    await new Promise((r) => setTimeout(r, 250));
  }
}

export async function bootstrapProviderCanaryGate(
  config: Config,
  tokenManager: TokenManager,
  router: TaskRouter,
  proxy: ProxyServer,
  budgetGuard: BudgetGuard,
  learningRouter: LearningRouter,
): Promise<void> {
  await waitForProxyReady(config.port);
  await applyProviderCanaryGate(config);
  tokenManager.updateConfig(config);
  router.updateConfig(config);
  proxy.updateConfig(config);
  budgetGuard.updateConfig(config);
  learningRouter.updateConfig(config);
}

async function runStartupImportPreflight(): Promise<void> {
  const criticalImports = [
    "./proxy.ts",
    "./fleet/experiment-loop.ts",
    "./fleet/swarm.ts",
    "./auth/token-manager.ts",
  ];
  const failed: Array<{ spec: string; reason: string }> = [];

  for (const spec of criticalImports) {
    try {
      await import(spec);
    } catch (error) {
      failed.push({
        spec,
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }

  if (failed.length === 0) return;

  console.error("[startup] Import preflight failed:");
  for (const item of failed) {
    console.error(`[startup]   ${item.spec}: ${item.reason}`);
  }
  console.error(
    "[startup] Fix module resolution before restart loops: verify LaunchAgent entrypoint and repo checkout consistency.",
  );
  throw new Error("startup import preflight failed");
}

function getListeningPid(port: number): number | null {
  try {
    const lsof = Bun.spawnSync([
      "/usr/sbin/lsof",
      "-ti",
      `:${port}`,
      "-sTCP:LISTEN",
    ]);
    const text = Buffer.from(lsof.stdout as unknown as ArrayBuffer)
      .toString()
      .trim();
    const pid = parseInt(text.split("\n")[0] ?? "", 10);
    return pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

async function checkPortAndResolve(port: number): Promise<void> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 3000);
    const resp = await fetch(`http://127.0.0.1:${port}/health`, {
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (resp.ok) {
      // Healthy instance already running — sleep until it dies, then let launchd restart us
      const existingPid = await getListeningPid(port);
      console.log(
        `[startup] Port ${port} already held by healthy orchestrator (PID ${existingPid ?? "?"}) — waiting for it to exit`,
      );
      if (existingPid) {
        // Poll until the existing process exits (max 2 min to avoid PID-reuse deadlock)
        const maxWaitMs = 120_000;
        const waitStart = Date.now();
        while (Date.now() - waitStart < maxWaitMs) {
          try {
            process.kill(existingPid, 0); // signal 0 = check if alive
          } catch {
            console.log(`[startup] PID ${existingPid} exited — restarting`);
            break;
          }
          await Bun.sleep(5000);
        }
        return; // Fall through to normal startup
      }
      // Can't find PID — just exit and let launchd retry
      process.exit(0);
    }

    // Port is held but /health is not OK — stale process, kill it
    console.warn(
      `[startup] Port ${port} held by unhealthy process (status ${resp.status}) — taking over`,
    );
  } catch (err: unknown) {
    // Connection refused or timeout — port may be held by a non-HTTP process
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes("abort") || msg.includes("timeout")) {
      console.warn(
        `[startup] Port ${port} held by unresponsive process — taking over`,
      );
    } else {
      // ECONNREFUSED — port is free, proceed normally
      return;
    }
  }

  // Kill the stale process holding the port
  try {
    const lsof = Bun.spawnSync([
      "/usr/sbin/lsof",
      "-ti",
      `:${port}`,
      "-sTCP:LISTEN",
    ]);
    const pids = Buffer.from(lsof.stdout as unknown as ArrayBuffer)
      .toString()
      .trim()
      .split("\n")
      .filter(Boolean);
    for (const pid of pids) {
      const pidNum = parseInt(pid, 10);
      if (pidNum > 0 && pidNum !== process.pid) {
        console.warn(
          `[startup] Killing stale process PID ${pidNum} on port ${port}`,
        );
        process.kill(pidNum, "SIGTERM");
      }
    }
    // Wait briefly for port to free
    await Bun.sleep(1000);
  } catch {
    console.warn(`[startup] Failed to kill stale process on port ${port}`);
  }
}

async function logStartupStability(): Promise<void> {
  const now = Date.now();
  try {
    const file = Bun.file(STARTUP_STATE_PATH);
    let previousTs = 0;
    if (await file.exists()) {
      const prev = (await file.json()) as { ts?: number; pid?: number };
      previousTs = prev.ts ?? 0;
    }
    if (previousTs > 0) {
      const deltaSec = Math.round((now - previousTs) / 1000);
      if (deltaSec < 300) {
        console.warn(
          `[startup] Restart-storm guard: previous boot ${deltaSec}s ago`,
        );
      }
    }
  } catch (error) {
    console.warn("[startup] Failed to read startup-state:", error);
  }

  try {
    await Bun.write(
      STARTUP_STATE_PATH,
      JSON.stringify({ ts: now, pid: process.pid }, null, 2),
    );
  } catch (error) {
    console.warn("[startup] Failed to persist startup-state:", error);
  }
}

async function main() {
  console.log("=== ClaudeMax Orchestrator ===");
  console.log(`[startup] PID ${process.pid} | ${new Date().toISOString()}`);
  await logStartupStability();
  await runStartupImportPreflight();

  // 1. Load config
  const config = await loadConfig();
  setConfig(config);
  console.log(`[startup] Config loaded — port: ${config.port}`);

  // Warn if using default/weak API key
  if (config.api_keys?.includes("your-proxy-key")) {
    console.warn(
      `[startup] ⚠️  Using default API key "your-proxy-key" — change this in config.yaml or set CLIPROXYAPI_API_KEY`,
    );
  }

  // 2. Initialize token manager
  const tokenManager = new TokenManager(config);
  await tokenManager.init();

  const allAccounts = tokenManager.getAllAccounts();
  const healthy = allAccounts.filter((a) => a.health === "healthy");
  const degraded = allAccounts.filter((a) => a.health === "degraded");
  const expired = allAccounts.filter((a) => a.health === "expired");

  console.log(
    `[startup] Accounts: ${healthy.length} healthy, ${degraded.length} degraded, ${expired.length} expired (${allAccounts.length} total)`,
  );

  // Log per-provider summary
  const byProvider = new Map<string, number>();
  for (const a of healthy) {
    byProvider.set(a.provider, (byProvider.get(a.provider) ?? 0) + 1);
  }
  for (const [provider, count] of byProvider) {
    console.log(`[startup]   ${provider}: ${count} healthy`);
  }

  // Listen for token events (deduplicate circuit open spam)
  const circuitOpenSeen = new Set<string>();
  tokenManager.on("token_event", (event: TokenEvent) => {
    if (event.type === "circuit_open") {
      if (!circuitOpenSeen.has(event.account.id)) {
        circuitOpenSeen.add(event.account.id);
        console.warn(`[circuit] OPEN: ${event.account.id}`);
        // Auto-clear after cooldown so we log again if it re-opens
        setTimeout(() => circuitOpenSeen.delete(event.account.id), 300_000);
      }
    } else if (event.type === "circuit_close") {
      circuitOpenSeen.delete(event.account.id);
      console.log(`[circuit] CLOSED: ${event.account.id}`);
    } else if (event.type === "token_expired") {
      console.warn(`[token] EXPIRED: ${event.account.id}`);
      const safeId = event.account.id
        .replace(/\\/g, "\\\\")
        .replace(/"/g, '\\"');
      Bun.spawn([
        "osascript",
        "-e",
        `display notification "Token expired: ${safeId}" with title "ClaudeMax" subtitle "Manual re-auth needed"`,
      ]);
    }
  });

  // 3. Initialize usage DB
  const usageDB = new UsageDB(config.database.path);
  console.log(`[startup] Usage DB: ${config.database.path}`);

  // 4. Initialize router
  const router = new TaskRouter(config, tokenManager, usageDB);

  // 5. Start proxy server
  // Allow PORT env override for running test instances on different ports
  if (process.env.PORT) {
    config.port = parseInt(process.env.PORT, 10);
    console.log(`[startup] PORT override: ${config.port}`);
  }

  const budgetGuard = new BudgetGuard(config, tokenManager, usageDB);

  // 6. Create learning router — closes the feedback loop
  const learningRouter = new LearningRouter(config, usageDB, tokenManager);

  // Check if port is already in use — defer to healthy instance, kill stale one
  await checkPortAndResolve(config.port);

  const { ProxyServer } = await import("./proxy.ts");
  const proxy = new ProxyServer(
    config,
    tokenManager,
    router,
    usageDB,
    budgetGuard,
    learningRouter,
  );
  proxy.start();
  console.log(`[startup] Ready — http://127.0.0.1:${config.port}`);

  // Pre-warm DNS cache and TLS connections for upstream providers.
  // Eliminates 40-100ms cold-start latency on first request per host.
  const preconnectHosts = [
    "http://localhost:8317", // CLIProxyAPI (most traffic)
    "https://api.kimi.com", // Kimi API (high DNS latency: ~572ms)
    "https://api.minimax.io", // MiniMax (high DNS latency: ~442ms)
    "https://api.z.ai", // GLM
  ];
  for (const host of preconnectHosts) {
    try {
      fetch.preconnect(host);
    } catch {
      // preconnect is best-effort
    }
  }

  // 2b. Probe CLIProxyAPI health (non-blocking — don't delay health endpoint)
  fetch("http://localhost:8317/", { signal: AbortSignal.timeout(15_000) })
    .then((probe) =>
      console.log(`[startup] CLIProxyAPI: healthy (status ${probe.status})`),
    )
    .catch(() =>
      console.warn(
        `[startup] CLIProxyAPI: UNREACHABLE — degraded mode (API-key providers still work)`,
      ),
    );

  // 7. Start budget enforcement
  budgetGuard.start();

  // 8. Start learning router
  learningRouter.start();

  // 8.5 Provider reliability canary gate (non-blocking — runs after proxy is ready)
  bootstrapProviderCanaryGate(
    config,
    tokenManager,
    router,
    proxy,
    budgetGuard,
    learningRouter,
  ).catch((e) => console.error("[canary] Gate failed:", e));

  // 9. Watch config for hot-reload
  watchConfig((newConfig) => {
    setConfig(newConfig);
    tokenManager.updateConfig(newConfig);
    router.updateConfig(newConfig);
    proxy.updateConfig(newConfig);
    budgetGuard.updateConfig(newConfig);
    learningRouter.updateConfig(newConfig);
    console.log("[hot-reload] Config reloaded");
  });

  // 10. Start watchdog — auto-restarts CLIProxyAPI if it goes down
  // Watchdog clears proxy-provider circuits on recovery to prevent 300s dead-time
  const watchdog = new WatchdogService();
  watchdog.setTokenManager(tokenManager);
  watchdog.start();
  proxy.setWatchdog(watchdog);

  // Graceful shutdown
  process.on("SIGINT", () =>
    shutdown(
      proxy,
      tokenManager,
      usageDB,
      watchdog,
      budgetGuard,
      learningRouter,
    ),
  );
  process.on("SIGTERM", () =>
    shutdown(
      proxy,
      tokenManager,
      usageDB,
      watchdog,
      budgetGuard,
      learningRouter,
    ),
  );
}

function shutdown(
  proxy: ProxyServer,
  tokenManager: TokenManager,
  usageDB: UsageDB,
  watchdog: WatchdogService,
  budgetGuard: BudgetGuard,
  learningRouter: LearningRouter,
) {
  console.log("\n[shutdown] Graceful shutdown...");
  learningRouter.stop();
  budgetGuard.stop();
  watchdog.stop();
  proxy.stop();
  tokenManager.shutdown();
  usageDB.close();
  process.exit(0);
}

// Prevent single-request crashes (e.g. stack overflow) from killing the process
process.on("uncaughtException", (err) => {
  console.error(
    "[uncaughtException]",
    err.message,
    err.stack?.split("\n").slice(0, 5).join("\n"),
  );
});
process.on("unhandledRejection", (reason) => {
  console.error("[unhandledRejection]", reason);
});

if (import.meta.main) {
  main().catch((e) => {
    console.error("[fatal]", e);
    process.exit(1);
  });
}
