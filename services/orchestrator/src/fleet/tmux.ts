import { execSync, spawnSync } from "child_process";
import type { ClaudeSession, RemoteHostConfig } from "./types.ts";

const TMUX_PREFIX = "fleet";
const CLAUDE_SESSION_ID_PREFIX = "claude";

function shSingleQuote(input: string): string {
  return `'${input.replace(/'/g, "'\\''")}'`;
}

function tmuxExec(cmd: string, remote?: RemoteHostConfig): string {
  if (!remote) {
    return execSync(cmd, {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
      maxBuffer: 50 * 1024 * 1024,
    }).toString();
  }
  const portArg = remote.port ? ` -p ${remote.port}` : "";
  const sshCmd = `ssh -o BatchMode=yes -o ConnectTimeout=3${portArg} ${remote.user}@${remote.host} ${shSingleQuote(cmd)}`;
  return execSync(sshCmd, {
    encoding: "utf-8",
    stdio: ["pipe", "pipe", "pipe"],
    maxBuffer: 50 * 1024 * 1024,
  }).toString();
}

export function getSessionName(jobId: string): string {
  return `${TMUX_PREFIX}-${jobId}`;
}

export function isTmuxAvailable(remote?: RemoteHostConfig): boolean {
  try {
    tmuxExec("which tmux", remote);
    return true;
  } catch {
    return false;
  }
}

export function sessionExists(
  sessionName: string,
  remote?: RemoteHostConfig,
): boolean {
  try {
    tmuxExec(
      `tmux has-session -t ${shSingleQuote(sessionName)} 2>/dev/null`,
      remote,
    );
    return true;
  } catch {
    return false;
  }
}

export function createSession(options: {
  sessionName: string;
  shellCmd: string;
  cwd: string;
}): { success: boolean; error?: string } {
  try {
    execSync(
      `tmux new-session -d -s ${shSingleQuote(options.sessionName)} -c ${shSingleQuote(options.cwd)} ${shSingleQuote(options.shellCmd)}`,
      { stdio: "pipe", cwd: options.cwd },
    );
    return { success: true };
  } catch (err) {
    return { success: false, error: (err as Error).message };
  }
}

export function sendKeys(
  sessionName: string,
  text: string,
  enter = false,
  remote?: RemoteHostConfig,
): boolean {
  if (!sessionExists(sessionName, remote)) return false;
  try {
    const escaped = text.replace(/'/g, "'\\''");
    tmuxExec(
      `tmux send-keys -t ${shSingleQuote(sessionName)} '${escaped}'`,
      remote,
    );
    if (enter) {
      spawnSync("sleep", ["0.3"]);
      tmuxExec(`tmux send-keys -t ${shSingleQuote(sessionName)} Enter`, remote);
    }
    return true;
  } catch {
    return false;
  }
}

export function capturePane(
  sessionName: string,
  lines?: number,
  remote?: RemoteHostConfig,
): string | null {
  if (!sessionExists(sessionName, remote)) return null;
  try {
    const output = tmuxExec(
      `tmux capture-pane -t ${shSingleQuote(sessionName)} -p -S -`,
      remote,
    );
    if (lines) {
      const allLines = output.split("\n");
      return allLines.slice(-lines).join("\n");
    }
    return output;
  } catch {
    return null;
  }
}

export function killSession(
  sessionName: string,
  remote?: RemoteHostConfig,
): boolean {
  if (!sessionExists(sessionName, remote)) return false;
  try {
    tmuxExec(`tmux kill-session -t ${shSingleQuote(sessionName)}`, remote);
    return true;
  } catch {
    return false;
  }
}

export function isSessionActive(
  sessionName: string,
  remote?: RemoteHostConfig,
): boolean {
  if (!sessionExists(sessionName, remote)) return false;
  try {
    const pid = tmuxExec(
      `tmux list-panes -t ${shSingleQuote(sessionName)} -F "#{pane_pid}"`,
      remote,
    ).trim();
    if (!pid) return false;
    const pidNum = parseInt(pid, 10);
    if (!Number.isFinite(pidNum) || pidNum <= 0) return false;
    if (remote) {
      // For remote hosts, check via kill -0 on the remote machine
      tmuxExec(`kill -0 ${pidNum} 2>/dev/null`, remote);
      return true;
    }
    process.kill(pidNum, 0);
    return true;
  } catch {
    return false;
  }
}

export function listFleetSessions(): string[] {
  try {
    const output = tmuxExec(
      `tmux list-sessions -F "#{session_name}" 2>/dev/null`,
    );
    return output
      .trim()
      .split("\n")
      .filter((line) => line.startsWith(TMUX_PREFIX));
  } catch {
    return [];
  }
}

export function listClaudeSessions(
  hostLabel: string,
  remote?: RemoteHostConfig,
): ClaudeSession[] {
  try {
    const format =
      "#{session_name}\t#{pane_pid}\t#{pane_current_command}\t#{pane_current_path}\t#{pane_last_activity}";
    const output = tmuxExec(
      `tmux list-panes -a -F ${shSingleQuote(format)} 2>/dev/null`,
      remote,
    ).trim();
    if (!output) return [];

    const host = remote ? remote.host : "local";
    const sessions: ClaudeSession[] = [];

    for (const line of output.split("\n")) {
      const [sessionName, panePidRaw, commandRaw, cwdRaw, lastActivityRaw] =
        line.split("\t");
      const command = (commandRaw || "").trim();
      if (!command || !command.toLowerCase().includes("claude")) continue;

      const shortId = `${CLAUDE_SESSION_ID_PREFIX}-${sessionName}`;
      const id = `${hostLabel}:${shortId}`;

      sessions.push({
        id,
        short_id: shortId,
        host,
        host_label: hostLabel,
        tmux_session: sessionName || null,
        source: "tmux",
        cwd: cwdRaw || null,
        command,
        last_activity: lastActivityRaw ? Number(lastActivityRaw) : null,
        pane_pid: panePidRaw ? Number(panePidRaw) : null,
        tty: null,
        warp_metadata: {
          terminal: "warp",
          workspace: cwdRaw || undefined,
        },
      });
    }

    return sessions;
  } catch {
    return [];
  }
}

export function listClaudeProcessSessions(
  hostLabel: string,
  remote?: RemoteHostConfig,
): ClaudeSession[] {
  try {
    const host = remote ? remote.host : "local";
    const cmd = "ps -axo pid=,tty=,etime=,command= | grep -i '[c]laude'";
    const output = tmuxExec(cmd, remote).trim();
    if (!output) return [];

    const sessions: ClaudeSession[] = [];
    for (const line of output.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const match = trimmed.match(/^(\d+)\s+(\S+)\s+(\S+)\s+(.+)$/);
      if (!match) continue;

      const pid = Number(match[1] ?? 0);
      const ttyRaw = (match[2] ?? "").trim();
      const etime = match[3] ?? "";
      const command = (match[4] ?? "").trim();
      if (!command || !command.toLowerCase().includes("claude")) continue;

      const shortId = `proc-${pid}`;
      const id = `${hostLabel}:${shortId}`;
      const tty = ttyRaw && ttyRaw !== "??" ? `/dev/${ttyRaw}` : null;

      sessions.push({
        id,
        short_id: shortId,
        host,
        host_label: hostLabel,
        tmux_session: null,
        source: "warp",
        cwd: null,
        command,
        last_activity: null,
        pane_pid: pid,
        tty,
        warp_metadata: {
          terminal: "warp",
          workspace: etime,
        },
      });
    }

    return sessions;
  } catch {
    return [];
  }
}

export function isProcessSession(shortId: string): boolean {
  return shortId.startsWith("proc-");
}

export function resolveProcessPid(shortId: string): number | null {
  if (!isProcessSession(shortId)) return null;
  const pid = Number(shortId.slice("proc-".length));
  return Number.isFinite(pid) && pid > 0 ? pid : null;
}

export function sendToProcessSession(
  pid: number,
  message: string,
  enter = true,
  remote?: RemoteHostConfig,
): boolean {
  try {
    const escaped = message.replace(/'/g, `'\\''`);
    const ttyCmd = `ps -p ${pid} -o tty= | tr -d ' '`;
    const ttyRaw = tmuxExec(ttyCmd, remote).trim();
    if (!ttyRaw || ttyRaw === "??") return false;
    const ttyPath = `/dev/${ttyRaw}`;
    const suffix = enter ? "\\r" : "";
    tmuxExec(
      `if kill -0 ${pid} 2>/dev/null; then printf '%s${suffix}' '${escaped}' > ${shSingleQuote(ttyPath)}; else exit 1; fi`,
      remote,
    );
    return true;
  } catch {
    return false;
  }
}

export function killProcessByPid(
  pid: number,
  remote?: RemoteHostConfig,
): boolean {
  try {
    tmuxExec(`kill -TERM ${pid} 2>/dev/null || true`, remote);
    return true;
  } catch {
    return false;
  }
}

export function restartProcessByPid(
  pid: number,
  remote?: RemoteHostConfig,
): { ok: boolean; note?: string } {
  const killed = killProcessByPid(pid, remote);
  if (!killed) return { ok: false };
  return {
    ok: true,
    note: "Process restart done as terminate path for Warp sessions; relaunch may occur via Warp profile",
  };
}

export function parseSessionId(
  sessionId: string,
): { hostLabel: string; shortId: string } | null {
  const idx = sessionId.indexOf(":");
  if (idx <= 0) return null;
  const hostLabel = sessionId.slice(0, idx);
  const shortId = sessionId.slice(idx + 1);
  if (!hostLabel || !shortId) return null;
  return { hostLabel, shortId };
}

export function resolveTmuxSessionName(shortId: string): string {
  if (shortId.startsWith(`${CLAUDE_SESSION_ID_PREFIX}-`)) {
    return shortId.slice(`${CLAUDE_SESSION_ID_PREFIX}-`.length);
  }
  return shortId;
}
