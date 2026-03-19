/**
 * WatchdogService — self-healing for orchestrator dependencies.
 *
 * Periodically probes CLIProxyAPI and auto-restarts it if down.
 * On recovery: clears circuit breakers for proxy-dependent providers,
 * preventing the 300s dead-time after CLIProxyAPI flaps.
 *
 * Exposes `isProxyHealthy` for the proxy server to skip proxy providers
 * when CLIProxyAPI is known-bad (avoids N×M retry stampede).
 */

import type { TokenManager } from "./auth/token-manager.ts";

const PROBE_INTERVAL_MS = 30_000; // check every 30s
const RESTART_COOLDOWN_MS = 120_000; // don't restart more than once per 2min
const PROBE_TIMEOUT_MS = 5_000;
const CLIPROXYAPI_URL = "http://localhost:8317/";

// Providers that route through CLIProxyAPI — circuit breakers for these
// should be cleared when CLIProxyAPI recovers (they're systemic, not per-account)
const PROXY_PROVIDERS = new Set(["claude", "antigravity", "codex", "gemini", "openrouter"]);

export class WatchdogService {
  private timer: ReturnType<typeof setInterval> | null = null;
  private lastRestartAt = 0;
  private consecutiveFailures = 0;
  private tokenManager: TokenManager | null = null;
  private _proxyHealthy = true;
  private _lastProbeAt = 0;

  /**
   * Attach TokenManager so watchdog can clear circuits on recovery.
   * Must be called before start() for full functionality.
   */
  setTokenManager(tm: TokenManager): void {
    this.tokenManager = tm;
  }

  /** Whether CLIProxyAPI was healthy at last probe */
  get isProxyHealthy(): boolean {
    return this._proxyHealthy;
  }

  /** Timestamp of last successful probe (0 if never) */
  get lastHealthyAt(): number {
    return this._lastProbeAt;
  }

  start(): void {
    console.log(
      `[watchdog] Started — probing CLIProxyAPI every ${PROBE_INTERVAL_MS / 1000}s`,
    );
    this.timer = setInterval(() => this.check(), PROBE_INTERVAL_MS);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async check(): Promise<void> {
    try {
      const resp = await fetch(CLIPROXYAPI_URL, {
        signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
      });
      // Drain response body to avoid connection leak
      await resp.text().catch(() => {});
      if (resp.ok) {
        const wasUnhealthy = !this._proxyHealthy;
        this._proxyHealthy = true;
        this._lastProbeAt = Date.now();

        if (this.consecutiveFailures > 0) {
          console.log(
            `[watchdog] CLIProxyAPI recovered after ${this.consecutiveFailures} failures`,
          );
          // Clear circuits for all proxy-dependent providers — these were
          // tripped by a systemic proxy issue, not per-account problems
          if (wasUnhealthy || this.consecutiveFailures >= 2) {
            this.clearProxyCircuits();
          }
        }
        this.consecutiveFailures = 0;
        return;
      }
      this.consecutiveFailures++;
      this._proxyHealthy = false;
      console.warn(
        `[watchdog] CLIProxyAPI returned ${resp.status} (failure #${this.consecutiveFailures})`,
      );
    } catch {
      this.consecutiveFailures++;
      this._proxyHealthy = false;
      console.warn(
        `[watchdog] CLIProxyAPI unreachable (failure #${this.consecutiveFailures})`,
      );
    }

    // Auto-restart after 4 consecutive failures (120s of sustained downtime)
    // Threshold raised to avoid conflicts with external monitor scripts
    if (this.consecutiveFailures >= 4) {
      await this.tryRestart();
    }
  }

  /**
   * Clear circuit breakers for all proxy-dependent provider accounts.
   * Called when CLIProxyAPI recovers — circuits opened during the outage
   * are systemic (not per-account), so they should be cleared immediately
   * instead of waiting for the 300s cooldown.
   */
  private clearProxyCircuits(): void {
    if (!this.tokenManager) return;

    let cleared = 0;
    for (const account of this.tokenManager.getAllAccounts()) {
      if (!PROXY_PROVIDERS.has(account.provider)) continue;
      if (!account.circuitOpenUntil) continue;
      // Clear all open circuits for proxy providers — they were tripped by
      // systemic proxy failure. The circuitOpenUntil timestamp tells us when
      // the circuit would auto-close; if it's within the next 10 minutes,
      // it was opened recently enough to be proxy-related.
      const timeUntilClose = account.circuitOpenUntil - Date.now();
      if (timeUntilClose > 600_000) continue; // very long cooldown — likely not proxy-related

      if (this.tokenManager?.resetCircuit(account.id)) cleared++;
    }
    if (cleared > 0) {
      console.log(
        `[watchdog] Cleared ${cleared} proxy-provider circuit breakers after CLIProxyAPI recovery`,
      );
    }
  }

  /**
   * Ensure the CLIProxyAPI launchd plist includes the -config flag.
   * After `brew upgrade`, the plist is regenerated from the formula
   * template which omits -config, causing v6.8.39+ to fail to start.
   */
  private async ensureConfigFlag(): Promise<void> {
    const configPath = "/opt/homebrew/etc/cliproxyapi.conf";
    const script = `
import plistlib, glob, os
fixed = 0
# Fix Cellar plists (brew services source)
for p in glob.glob("/opt/homebrew/Cellar/cliproxyapi/*/homebrew.mxcl.cliproxyapi.plist"):
    with open(p,"rb") as f: d=plistlib.load(f)
    a=d.get("ProgramArguments",[])
    if "-config" not in a:
        a.extend(["-config","${configPath}"])
        d["ProgramArguments"]=a
        with open(p,"wb") as f: plistlib.dump(d,f)
        fixed+=1
# Fix active LaunchAgent plist
la=os.path.expanduser("~/Library/LaunchAgents/homebrew.mxcl.cliproxyapi.plist")
if os.path.exists(la):
    with open(la,"rb") as f: d=plistlib.load(f)
    a=d.get("ProgramArguments",[])
    if "-config" not in a:
        a.extend(["-config","${configPath}"])
        d["ProgramArguments"]=a
        with open(la,"wb") as f: plistlib.dump(d,f)
        fixed+=1
# Ad-hoc sign if needed
b="/opt/homebrew/opt/cliproxyapi/bin/cliproxyapi"
if os.path.exists(b):
    import subprocess
    r=subprocess.run(["codesign","-dv",b],capture_output=True,text=True)
    if "not signed" in r.stderr:
        subprocess.run(["codesign","--force","--sign","-",b],capture_output=True)
        fixed+=1
print(fixed)
`;
    try {
      const proc = Bun.spawn(["python3", "-c", script], {
        stdout: "pipe",
        stderr: "pipe",
      });
      const exitCode = await proc.exited;
      const out = (await new Response(proc.stdout).text()).trim();
      if (exitCode === 0 && out !== "0") {
        console.log(
          `[watchdog] Fixed ${out} plist/binary issue(s) for CLIProxyAPI`,
        );
      }
    } catch (e) {
      console.warn(
        `[watchdog] ensureConfigFlag failed: ${(e as Error).message}`,
      );
    }
  }

  private async tryRestart(): Promise<void> {
    const now = Date.now();
    if (now - this.lastRestartAt < RESTART_COOLDOWN_MS) {
      console.log(
        `[watchdog] Restart cooldown — last restart ${Math.round((now - this.lastRestartAt) / 1000)}s ago`,
      );
      return;
    }

    this.lastRestartAt = now;
    console.warn(`[watchdog] Auto-restarting CLIProxyAPI...`);

    try {
      // Step 1: Ensure -config flag in plist (survives brew upgrades)
      await this.ensureConfigFlag();

      // Step 2: Kill process — launchd KeepAlive restarts it
      const killProc = Bun.spawn(["pkill", "-x", "cliproxyapi"], {
        stdout: "pipe",
        stderr: "pipe",
      });
      await killProc.exited;

      // Give launchd time to restart
      await new Promise((r) => setTimeout(r, 5000));

      // Step 3: If launchd didn't restart, force plist reload
      const checkProc = Bun.spawn(["pgrep", "-x", "cliproxyapi"], {
        stdout: "pipe",
      });
      const checkExit = await checkProc.exited;
      if (checkExit !== 0) {
        console.warn(
          `[watchdog] KeepAlive didn't restart — reloading plist`,
        );
        const plist = `${process.env.HOME}/Library/LaunchAgents/homebrew.mxcl.cliproxyapi.plist`;
        await Bun.spawn(["launchctl", "unload", plist], {
          stdout: "pipe",
          stderr: "pipe",
        }).exited;
        await new Promise((r) => setTimeout(r, 1000));
        await Bun.spawn(["launchctl", "load", plist], {
          stdout: "pipe",
          stderr: "pipe",
        }).exited;
        await new Promise((r) => setTimeout(r, 5000));
      }

      // Step 4: Verify recovery
      try {
        const verify = await fetch(CLIPROXYAPI_URL, {
          signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
        });
        // Drain response body to avoid connection leak
        await verify.text().catch(() => {});
        if (verify.ok) {
          console.log(
            `[watchdog] CLIProxyAPI verified healthy after restart`,
          );
          this._proxyHealthy = true;
          this._lastProbeAt = Date.now();
          this.consecutiveFailures = 0;
          this.clearProxyCircuits();
        } else {
          console.warn(
            `[watchdog] CLIProxyAPI restarted but returned ${verify.status}`,
          );
        }
      } catch {
        console.warn(
          `[watchdog] CLIProxyAPI restarted but still unreachable`,
        );
      }
    } catch (e) {
      console.error(
        `[watchdog] Failed to restart CLIProxyAPI: ${(e as Error).message}`,
      );
    }
  }
}
