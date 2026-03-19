import { homedir, hostname } from "os";
import { join } from "path";
import {
  mkdirSync,
  writeFileSync,
  readdirSync,
  readFileSync,
  unlinkSync,
  existsSync,
} from "fs";
import { resolve } from "path";
import { execFileSync } from "child_process";
import type {
  AgentCLI,
  FleetJob,
  JobStatus,
  FleetDispatchRequest,
  ClaudeSession,
  RemoteHostConfig,
  SessionListOptions,
  SessionActionResponse,
} from "./types.ts";
import {
  getSessionName,
  isTmuxAvailable,
  createSession,
  capturePane,
  killSession,
  isSessionActive,
  sendKeys,
  listClaudeSessions,
  listClaudeProcessSessions,
  isProcessSession,
  resolveProcessPid,
  sendToProcessSession,
  killProcessByPid,
  restartProcessByPid,
  parseSessionId,
  resolveTmuxSessionName,
} from "./tmux.ts";

const JOBS_DIR = join(homedir(), ".claudemax", "fleet-jobs");

const DEFAULT_REMOTE_HOSTS: RemoteHostConfig[] = [
  {
    label: "macmini",
    host: process.env.FLEET_MACMINI_HOST ?? "10.0.1.100",
    user: process.env.FLEET_MACMINI_USER ?? "admin",
    port: process.env.FLEET_MACMINI_PORT
      ? Number(process.env.FLEET_MACMINI_PORT)
      : undefined,
  },
  {
    label: "mbp",
    host: process.env.FLEET_MBP_HOST ?? "",
    user: process.env.FLEET_MBP_USER ?? "user",
    port: process.env.FLEET_MBP_PORT
      ? Number(process.env.FLEET_MBP_PORT)
      : undefined,
  },
];

function detectLocalHostLabel(): "mbp" | "macmini" {
  const h = hostname().toLowerCase();
  if (h.includes("mini") || h.includes("server")) return "macmini";
  return "mbp";
}

// Default models per CLI
const DEFAULT_MODELS: Record<AgentCLI, string> = {
  codex: "gpt-5.4",
  kimi: "kimi-code/kimi-for-coding",
  claude: "claude-sonnet-4-6",
  gemini: "gemini-2.5-pro",
};

export class FleetDispatcher {
  constructor() {
    mkdirSync(JOBS_DIR, { recursive: true });
  }

  async dispatch(req: FleetDispatchRequest): Promise<FleetJob> {
    if (!isTmuxAvailable()) {
      throw new Error("tmux is required for fleet dispatch");
    }

    // Health gate: claude fleet jobs route through orchestrator (:8318)
    // Fail fast if orchestrator is unreachable rather than creating a hanging tmux session
    if (req.cli === "claude") {
      try {
        const healthResp = await fetch("http://localhost:8318/health", {
          signal: AbortSignal.timeout(3000),
        });
        // Drain response body to avoid connection leak
        await healthResp.text().catch(() => {});
        if (!healthResp.ok) {
          throw new Error(`Orchestrator returned ${healthResp.status}`);
        }
      } catch (e) {
        throw new Error(
          `Fleet health gate failed: orchestrator (:8318) unreachable — ${e instanceof Error ? e.message : String(e)}. Fix orchestrator before dispatching claude fleet jobs.`,
        );
      }
    }

    const id = this.generateId();
    const model = this.safeCliArg(
      req.model ?? DEFAULT_MODELS[req.cli],
      "model",
    );
    let cwd = this.safeCliArg(req.cwd ?? process.cwd(), "cwd");
    const isolate = req.metadata?.["isolate"] === true;
    let worktreeDir: string | null = null;

    if (isolate) {
      worktreeDir = this.createWorktree(id, cwd);
      if (worktreeDir) {
        cwd = worktreeDir;
      } else {
        console.warn(
          `[fleet] Worktree isolation requested but failed for ${id}, using original cwd`,
        );
      }
    }

    const sessionName = getSessionName(id);
    const logFile = join(JOBS_DIR, `${id}.log`);
    const promptFile = join(JOBS_DIR, `${id}.prompt`);

    const planGate = req.metadata?.["plan_gate"] === true;
    const planFile = join(JOBS_DIR, `${id}.plan.md`);
    const verifyCommand = req.metadata?.["verify_command"] as
      | string
      | undefined;

    // Build effective prompt with optional plan-gate and self-verify wrappers
    let effectivePrompt = req.prompt;

    if (planGate) {
      effectivePrompt = [
        "You are in PLANNING mode. Do NOT execute any changes yet.",
        "Analyze the following objective and produce a detailed implementation plan.",
        "Output your plan as markdown with:",
        "- ## Summary (1-2 sentences)",
        "- ## Files to modify (list each file and what changes)",
        "- ## Steps (numbered, specific actions)",
        "- ## Risks (what could go wrong)",
        "- ## Verification (how to confirm it worked)",
        "",
        "OBJECTIVE:",
        req.prompt,
      ].join("\n");
    } else if (verifyCommand) {
      // Self-verify: append verification instructions to the prompt
      effectivePrompt = [
        req.prompt,
        "",
        "IMPORTANT — SELF-VERIFICATION REQUIRED:",
        `After completing all changes, run this verification command: \`${verifyCommand}\``,
        "If the verification fails, fix the issues and re-run until it passes.",
        "Do NOT report completion until verification passes.",
        "Include the verification output in your final response.",
      ].join("\n");
    }

    // Write prompt to file (avoids shell escaping issues)
    writeFileSync(promptFile, effectivePrompt);

    const job: FleetJob = {
      id,
      cli: req.cli,
      prompt: req.prompt, // Store original prompt for execution phase
      model,
      cwd,
      status: planGate ? "planning" : "pending",
      tmuxSession: sessionName,
      logFile,
      createdAt: new Date().toISOString(),
      startedAt: null,
      completedAt: null,
      error: null,
      options: {
        oneShot: req.one_shot ?? true,
        thinking: req.thinking,
        sandbox: this.normalizeSandbox(req.sandbox),
        reasoningEffort: this.normalizeReasoningEffort(req.reasoning_effort),
        worktreeDir: worktreeDir ?? undefined,
        planGate,
        planFile: planGate ? planFile : undefined,
        verifyCommand: verifyCommand ?? undefined,
        metadata: req.metadata ?? undefined,
      },
    };

    // Build the shell command based on CLI type
    const shellCmd = this.buildCommand(job, promptFile);

    const result = createSession({ sessionName, shellCmd, cwd });
    if (!result.success) {
      job.status = "failed";
      job.error = result.error ?? "Failed to create tmux session";
      this.saveJob(job);
      throw new Error(job.error);
    }

    job.status = planGate ? "planning" : "running";
    job.startedAt = new Date().toISOString();
    this.saveJob(job);

    console.log(
      `[fleet] Dispatched ${req.cli} agent ${id} → ${sessionName}${planGate ? " (plan-gate)" : ""}`,
    );
    return job;
  }

  /**
   * Create an isolated git worktree for a fleet job.
   * Returns the worktree path, or null if git is not available or cwd is not a git repo.
   */
  private createWorktree(jobId: string, cwd: string): string | null {
    try {
      // Find the git root for the working directory
      const gitRoot = execFileSync("git", ["rev-parse", "--show-toplevel"], {
        cwd,
        encoding: "utf-8",
        timeout: 5000,
      }).trim();

      if (!gitRoot) return null;

      // Create worktree directory under ~/.worktrees/{repo-name}/{job-id}
      const repoName = gitRoot.split("/").pop() ?? "unknown";
      const worktreeDir = join(
        homedir(),
        ".worktrees",
        repoName,
        `fleet-${jobId}`,
      );

      execFileSync(
        "git",
        ["worktree", "add", "--detach", worktreeDir, "HEAD"],
        {
          cwd: gitRoot,
          timeout: 10000,
          stdio: "pipe",
        },
      );

      console.log(`[fleet] Created worktree for ${jobId}: ${worktreeDir}`);
      return worktreeDir;
    } catch (e) {
      console.warn(
        `[fleet] Worktree creation failed for ${jobId}: ${e instanceof Error ? e.message : String(e)}`,
      );
      return null;
    }
  }

  /**
   * Remove a worktree created for a fleet job.
   */
  removeWorktree(jobId: string, cwd: string): void {
    try {
      const gitRoot = execFileSync("git", ["rev-parse", "--show-toplevel"], {
        cwd,
        encoding: "utf-8",
        timeout: 5000,
      }).trim();

      if (!gitRoot) return;

      const repoName = gitRoot.split("/").pop() ?? "unknown";
      const worktreeDir = join(
        homedir(),
        ".worktrees",
        repoName,
        `fleet-${jobId}`,
      );

      execFileSync("git", ["worktree", "remove", "--force", worktreeDir], {
        cwd: gitRoot,
        timeout: 10000,
        stdio: "pipe",
      });
      console.log(`[fleet] Removed worktree for ${jobId}`);
    } catch (e: unknown) {
      console.warn(
        "[dispatcher] worktree remove failed:",
        (e as Error).message,
      );
    }
  }

  /**
   * Write a shell script for the job and return the tmux command to run it.
   * This avoids nested quoting hell with script/bash -c.
   */
  private buildCommand(job: FleetJob, promptFile: string): string {
    const logFile = job.logFile;
    const scriptFile = join(JOBS_DIR, `${job.id}.sh`);
    // Source user profile for PATH, then run the CLI
    const mcpProfile = job.options.metadata?.["mcp_profile"] as
      | string
      | undefined;
    const REPO_ROOT = resolve(__dirname, "..", "..", "..", "..");

    let script = [
      "#!/bin/bash",
      "# Auto-generated by claudemax fleet dispatcher",
      'export PATH="$HOME/.local/bin:$HOME/.npm-global/bin:$PATH"',
      "",
    ].join("\n");

    switch (job.cli) {
      case "codex": {
        if (mcpProfile) {
          const profileToml = join(
            REPO_ROOT,
            "generated",
            `mcp-${mcpProfile}.toml`,
          );
          if (existsSync(profileToml)) {
            script += `export CODEX_MCP_CONFIG="${profileToml}"\n`;
          }
        }
        if (job.options.oneShot) {
          script += [
            `codex exec \\`,
            `  -m "${job.model}" \\`,
            `  -c 'model_reasoning_effort="${job.options.reasoningEffort}"' \\`,
            `  -c 'check_for_update_on_startup=false' \\`,
            `  -s "${job.options.sandbox ?? "danger-full-access"}" \\`,
            `  --skip-git-repo-check \\`,
            `  --ephemeral \\`,
            `  - < "${promptFile}"`,
          ].join("\n");
        } else {
          script += [
            `codex \\`,
            `  -c 'model="${job.model}"' \\`,
            `  -c 'model_reasoning_effort="${job.options.reasoningEffort}"' \\`,
            `  -c 'skip_update_check=true' \\`,
            `  -a never \\`,
            `  -s "${job.options.sandbox ?? "danger-full-access"}"`,
          ].join("\n");
        }
        break;
      }

      case "kimi": {
        const thinkingFlag =
          job.options.thinking === true
            ? "--thinking"
            : job.options.thinking === false
              ? "--no-thinking"
              : "";
        script += [
          `kimi \\`,
          `  --yolo \\`,
          `  -m "${job.model}" \\`,
          `  -w "${job.cwd}" \\`,
          thinkingFlag ? `  ${thinkingFlag} \\` : "",
          `  -p "$(cat '${promptFile}')"`,
        ]
          .filter(Boolean)
          .join("\n");
        break;
      }

      case "claude": {
        // Route fleet claude agents through orchestrator for resilient retry
        script += `export ANTHROPIC_BASE_URL=http://localhost:8318\n`;
        script += `export ANTHROPIC_API_KEY=your-proxy-key\n`;
        if (mcpProfile) {
          const profileJson = join(
            REPO_ROOT,
            "generated",
            `mcp-${mcpProfile}.json`,
          );
          if (existsSync(profileJson)) {
            script += `export CLAUDE_MCP_CONFIG="${profileJson}"\n`;
          }
        }
        if (job.options.oneShot) {
          script += `claude --print --model "${job.model}" --dangerously-skip-permissions "$(cat '${promptFile}')"`;
        } else {
          script += `claude --model "${job.model}" --dangerously-skip-permissions`;
        }
        break;
      }

      case "gemini": {
        if (job.options.oneShot) {
          script += [
            `gemini \\`,
            `  --model "${job.model}" \\`,
            `  --yolo \\`,
            `  --prompt "$(cat '${promptFile}')"`,
          ].join("\n");
        } else {
          script += `gemini --model "${job.model}" --approval-mode auto_edit`;
        }
        break;
      }
    }

    // Capture exit code and keep session alive so tmux can read output
    script += [
      "",
      "EXIT_CODE=$?",
      `echo ""`,
      `echo "[fleet] Job ${job.id} completed — exit code $EXIT_CODE — $(date -u +%Y-%m-%dT%H:%M:%SZ)"`,
      `sleep 86400`,
    ].join("\n");

    writeFileSync(scriptFile, script, { mode: 0o755 });

    // Run the script directly in tmux (no script(1) wrapper — use tmux capture-pane for output)
    return `bash "${scriptFile}" 2>&1 | tee "${logFile}"`;
  }

  /**
   * Read the log file for a completed/running job.
   */
  readLogFile(id: string): string | null {
    try {
      return readFileSync(join(JOBS_DIR, `${id}.log`), "utf-8");
    } catch {
      return null;
    }
  }

  getJob(id: string): FleetJob | null {
    try {
      const data = readFileSync(join(JOBS_DIR, `${id}.json`), "utf-8");
      return JSON.parse(data) as FleetJob;
    } catch {
      return null;
    }
  }

  refreshJobStatus(id: string): FleetJob | null {
    const job = this.getJob(id);
    if (!job) return null;

    if (job.status === "running" || job.status === "planning") {
      // Check log file for completion marker (written by the generated script)
      // This is more reliable than isSessionActive since `sleep 86400` keeps pane alive
      const log = this.readLogFile(id);
      if (log && log.includes(`[fleet] Job ${id} completed`)) {
        const exitMatch = log.match(/exit code (\d+)/);
        const exitCode = exitMatch?.[1] ? parseInt(exitMatch[1], 10) : 0;

        if (
          job.options.planGate &&
          job.status === "planning" &&
          exitCode === 0
        ) {
          // Plan phase completed — save output as plan and wait for approval
          const planContent =
            log.split(`[fleet] Job ${id} completed`)[0]?.trim() ?? "";
          if (job.options.planFile) {
            writeFileSync(job.options.planFile, planContent);
          }
          job.status = "awaiting_approval";
          this.saveJob(job);
          console.log(`[fleet] 📋 Job ${id}: plan ready, awaiting approval`);
        } else {
          job.status = exitCode === 0 ? "completed" : "failed";
          job.completedAt = new Date().toISOString();
          if (exitCode !== 0) job.error = `exit code ${exitCode}`;
          this.saveJob(job);
        }
      } else if (!isSessionActive(job.tmuxSession)) {
        // Session died without writing completion marker — treat as unknown termination
        job.status = "failed";
        job.completedAt = new Date().toISOString();
        job.error =
          "failed_unknown_termination: session terminated without completion marker";
        this.saveJob(job);
        console.log(
          `[fleet] ⚠️ Job ${id}: tmux session dead without marker — marking as failed`,
        );
      }

      // Clean up worktree if job used isolation and terminal state reached
      if (
        job.status !== "running" &&
        job.status !== "planning" &&
        job.status !== "awaiting_approval" &&
        job.options.worktreeDir
      ) {
        this.removeWorktree(job.id, job.options.worktreeDir);
      }
    }
    return job;
  }

  /**
   * Get the plan text for a plan-gate job.
   */
  getPlan(id: string): string | null {
    const job = this.getJob(id);
    if (!job?.options.planFile) return null;
    try {
      return readFileSync(job.options.planFile, "utf-8");
    } catch {
      // Plan file may not exist yet
      return this.readLogFile(id);
    }
  }

  /**
   * Approve a plan-gate job and launch the execution phase.
   * The original prompt is used for execution (not the plan-wrapped prompt).
   */
  async approveAndExecute(id: string, approvedBy?: string): Promise<FleetJob> {
    const job = this.getJob(id);
    if (!job) throw new Error(`Job ${id} not found`);
    if (job.status !== "awaiting_approval") {
      throw new Error(`Job ${id} is ${job.status}, not awaiting_approval`);
    }

    // Read the plan to include as context for the execution phase
    const plan = this.getPlan(id);
    const executionPrompt = plan
      ? [
          "You previously created this implementation plan:",
          "",
          plan,
          "",
          "Now EXECUTE this plan. Implement all the changes described above.",
          "Follow the plan exactly. Verify each step as you go.",
        ].join("\n")
      : job.prompt;

    // Kill the old planning session (it's sleeping after completion)
    killSession(job.tmuxSession);

    // Write new prompt for execution phase
    const promptFile = join(JOBS_DIR, `${id}.prompt`);
    writeFileSync(promptFile, executionPrompt);

    // Update job state
    job.options.approvedBy = approvedBy ?? "user";
    job.options.approvedAt = new Date().toISOString();
    job.status = "running";
    job.logFile = join(JOBS_DIR, `${id}.exec.log`);

    // Build and launch execution phase
    const shellCmd = this.buildCommand(
      { ...job, options: { ...job.options, oneShot: true } },
      promptFile,
    );
    const result = createSession({
      sessionName: job.tmuxSession,
      shellCmd,
      cwd: job.cwd,
    });

    if (!result.success) {
      job.status = "failed";
      job.error = result.error ?? "Failed to create execution session";
      this.saveJob(job);
      throw new Error(job.error);
    }

    this.saveJob(job);
    console.log(
      `[fleet] ▶️ Job ${id}: plan approved by ${job.options.approvedBy}, executing`,
    );
    return job;
  }

  /**
   * Mark jobs that have been running for too long as stale failures.
   * Called periodically or on listJobs to clean up stuck state.
   */
  private reapStaleJobs(): void {
    const MAX_RUNNING_MS = 4 * 60 * 60 * 1000; // 4 hours
    const now = Date.now();
    try {
      const files = readdirSync(JOBS_DIR).filter((f) => f.endsWith(".json"));
      for (const f of files) {
        try {
          const job = JSON.parse(
            readFileSync(join(JOBS_DIR, f), "utf-8"),
          ) as FleetJob;
          if (job.status !== "running" || !job.startedAt) continue;
          const elapsed = now - new Date(job.startedAt).getTime();
          if (elapsed > MAX_RUNNING_MS) {
            job.status = "failed";
            job.completedAt = new Date().toISOString();
            job.error = `Stale: running for ${Math.round(elapsed / 60000)}min without completion`;
            this.saveJob(job);
            console.log(
              `[fleet] 🕐 Reaped stale job ${job.id} (running ${Math.round(elapsed / 60000)}min)`,
            );
          }
        } catch {
          /* skip corrupt files */
        }
      }
    } catch {
      /* JOBS_DIR might not exist yet */
    }
  }

  captureOutput(id: string, lines?: number): string | null {
    const job = this.getJob(id);
    if (!job) return null;
    // Prefer log file (captured via tee), fall back to tmux pane
    const logOutput = this.readLogFile(id);
    if (logOutput && logOutput.trim().length > 0) {
      if (lines) {
        return logOutput.split("\n").slice(-lines).join("\n");
      }
      return logOutput;
    }
    return capturePane(job.tmuxSession, lines);
  }

  sendMessage(id: string, message: string): boolean {
    const job = this.getJob(id);
    if (!job || job.status !== "running") return false;
    return sendKeys(job.tmuxSession, message, true);
  }

  killJob(id: string): boolean {
    const job = this.getJob(id);
    if (!job) return false;

    const killed = killSession(job.tmuxSession);
    if (killed) {
      job.status = "killed";
      job.completedAt = new Date().toISOString();
      this.saveJob(job);
    }
    return killed;
  }

  listJobs(): FleetJob[] {
    this.reapStaleJobs();
    try {
      return readdirSync(JOBS_DIR)
        .filter((f) => f.endsWith(".json"))
        .map((f) => {
          try {
            return JSON.parse(
              readFileSync(join(JOBS_DIR, f), "utf-8"),
            ) as FleetJob;
          } catch {
            return null;
          }
        })
        .filter((j): j is FleetJob => j !== null)
        .sort(
          (a, b) =>
            new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
        );
    } catch {
      return [];
    }
  }

  listActiveJobs(): FleetJob[] {
    return this.listJobs()
      .filter((j) => j.status === "running")
      .map((j) => this.refreshJobStatus(j.id)!)
      .filter((j) => j.status === "running");
  }

  cleanOldJobs(days = 7): number {
    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    let cleaned = 0;
    for (const job of this.listJobs()) {
      if (
        job.status !== "running" &&
        new Date(job.createdAt).getTime() < cutoff
      ) {
        try {
          unlinkSync(join(JOBS_DIR, `${job.id}.json`));
          unlinkSync(join(JOBS_DIR, `${job.id}.prompt`));
          unlinkSync(join(JOBS_DIR, `${job.id}.log`));
          unlinkSync(join(JOBS_DIR, `${job.id}.sh`));
        } catch {}
        cleaned++;
      }
    }
    return cleaned;
  }

  listSessions(options: SessionListOptions = {}): ClaudeSession[] {
    const localHostLabel = detectLocalHostLabel();
    const targets: Array<{ hostLabel: string; remote?: RemoteHostConfig }> = [];

    const wantsLocal =
      !options.host ||
      options.host === "all" ||
      options.host === "local" ||
      options.host === localHostLabel;

    if (wantsLocal) {
      targets.push({ hostLabel: localHostLabel });
    }

    for (const remote of DEFAULT_REMOTE_HOSTS) {
      if (!remote.host) continue;
      const includeRemote =
        options.host === "all" || options.host === remote.label;
      if (!includeRemote) continue;
      targets.push({ hostLabel: remote.label, remote });
    }

    const sessionsById = new Map<string, ClaudeSession>();

    for (const target of targets) {
      const tmuxSessions = listClaudeSessions(target.hostLabel, target.remote);
      const procSessions = listClaudeProcessSessions(
        target.hostLabel,
        target.remote,
      );
      const combined = [
        ...tmuxSessions,
        ...procSessions.filter(
          (p) =>
            !tmuxSessions.some((t) => t.pane_pid && t.pane_pid === p.pane_pid),
        ),
      ];

      for (const session of combined) {
        const existing = sessionsById.get(session.id);
        if (!existing) {
          sessionsById.set(session.id, session);
          continue;
        }

        const existingIsLocal = existing.host === "local";
        const currentIsLocal = session.host === "local";

        if (!existingIsLocal && currentIsLocal) {
          sessionsById.set(session.id, session);
        }
      }
    }

    let sessions = [...sessionsById.values()];

    if (options.source && options.source !== "all") {
      sessions = sessions.filter(
        (session) => session.source === options.source,
      );
    }

    sessions.sort((a, b) => (b.last_activity ?? 0) - (a.last_activity ?? 0));
    return sessions;
  }

  getSession(sessionId: string): ClaudeSession | null {
    const parsed = parseSessionId(sessionId);
    if (!parsed) return null;
    return (
      this.listSessions({ host: parsed.hostLabel }).find(
        (s) => s.id === sessionId,
      ) ?? null
    );
  }

  sendSessionMessage(
    sessionId: string,
    message: string,
    enter = true,
  ): SessionActionResponse {
    const parsed = parseSessionId(sessionId);
    if (!parsed) {
      return {
        ok: false,
        id: sessionId,
        action: "send",
        error: "Invalid session id",
      };
    }

    const remote = this.resolveRemote(parsed.hostLabel);

    if (isProcessSession(parsed.shortId)) {
      const pid = resolveProcessPid(parsed.shortId);
      if (!pid) {
        return {
          ok: false,
          id: sessionId,
          action: "send",
          error: "Invalid process session id",
        };
      }
      const ok = sendToProcessSession(pid, message, enter, remote);
      return ok
        ? { ok: true, id: sessionId, action: "send" }
        : {
            ok: false,
            id: sessionId,
            action: "send",
            error:
              "Process session not writable (tty unavailable or process exited)",
          };
    }

    const tmuxSession = resolveTmuxSessionName(parsed.shortId);
    const ok = sendKeys(tmuxSession, message, enter, remote);

    return ok
      ? { ok: true, id: sessionId, action: "send" }
      : {
          ok: false,
          id: sessionId,
          action: "send",
          error: "Session not found or unavailable",
        };
  }

  killSessionById(sessionId: string): SessionActionResponse {
    const parsed = parseSessionId(sessionId);
    if (!parsed) {
      return {
        ok: false,
        id: sessionId,
        action: "kill",
        error: "Invalid session id",
      };
    }

    const remote = this.resolveRemote(parsed.hostLabel);

    if (isProcessSession(parsed.shortId)) {
      const pid = resolveProcessPid(parsed.shortId);
      if (!pid) {
        return {
          ok: false,
          id: sessionId,
          action: "kill",
          error: "Invalid process session id",
        };
      }
      const ok = killProcessByPid(pid, remote);
      return ok
        ? { ok: true, id: sessionId, action: "kill" }
        : {
            ok: false,
            id: sessionId,
            action: "kill",
            error: "Process session not killable",
          };
    }

    const tmuxSession = resolveTmuxSessionName(parsed.shortId);
    const ok = killSession(tmuxSession, remote);

    return ok
      ? { ok: true, id: sessionId, action: "kill" }
      : {
          ok: false,
          id: sessionId,
          action: "kill",
          error: "Session not found or unavailable",
        };
  }

  restartSessionById(sessionId: string): SessionActionResponse {
    const parsed = parseSessionId(sessionId);
    if (!parsed) {
      return {
        ok: false,
        id: sessionId,
        action: "restart",
        error: "Invalid session id",
      };
    }

    const remote = this.resolveRemote(parsed.hostLabel);
    if (isProcessSession(parsed.shortId)) {
      const pid = resolveProcessPid(parsed.shortId);
      if (!pid) {
        return {
          ok: false,
          id: sessionId,
          action: "restart",
          error: "Invalid process session id",
        };
      }
      const result = restartProcessByPid(pid, remote);
      return result.ok
        ? { ok: true, id: sessionId, action: "restart" }
        : {
            ok: false,
            id: sessionId,
            action: "restart",
            error: result.note ?? "Process session restart failed",
          };
    }

    const killed = this.killSessionById(sessionId);
    if (!killed.ok) {
      return {
        ok: false,
        id: sessionId,
        action: "restart",
        error: killed.error,
      };
    }
    return { ok: true, id: sessionId, action: "restart" };
  }

  private resolveRemote(hostLabel: string): RemoteHostConfig | undefined {
    const localHostLabel = detectLocalHostLabel();
    if (hostLabel === localHostLabel || hostLabel === "local") return undefined;
    const remote = DEFAULT_REMOTE_HOSTS.find((h) => h.label === hostLabel);
    if (!remote || !remote.host) return undefined;
    return remote;
  }

  private saveJob(job: FleetJob): void {
    writeFileSync(
      join(JOBS_DIR, `${job.id}.json`),
      JSON.stringify(job, null, 2),
    );
  }

  private safeCliArg(value: string, name: string): string {
    if (/[$`"'\n\r\\]/.test(value)) {
      throw new Error(`Invalid ${name}: contains unsafe shell characters`);
    }
    return value;
  }

  private normalizeSandbox(
    value: FleetDispatchRequest["sandbox"],
  ): "workspace-write" | "read-only" | "danger-full-access" {
    const sandbox = value ?? "danger-full-access";
    if (
      sandbox !== "workspace-write" &&
      sandbox !== "read-only" &&
      sandbox !== "danger-full-access"
    ) {
      throw new Error(
        `Invalid sandbox: ${String(value)}. Must be workspace-write, read-only, or danger-full-access`,
      );
    }
    return sandbox;
  }

  private normalizeReasoningEffort(
    value: FleetDispatchRequest["reasoning_effort"],
  ): "low" | "medium" | "high" | "xhigh" {
    const effort = value ?? "high";
    if (
      effort !== "low" &&
      effort !== "medium" &&
      effort !== "high" &&
      effort !== "xhigh"
    ) {
      throw new Error(
        `Invalid reasoning_effort: ${String(value)}. Must be low, medium, high, or xhigh`,
      );
    }
    return effort;
  }

  private generateId(): string {
    return crypto.randomUUID().replace(/-/g, "").slice(0, 8);
  }
}
