#!/usr/bin/env python3
"""Symphony-inspired orchestrator for fullstackOS.

Long-running daemon that:
- Polls issue tracker on fixed cadence
- Maintains orchestrator state for dispatch/retry/reconciliation
- Creates per-issue workspaces
- Launches coding agents

Per Symphony spec: https://github.com/openai/symphony/blob/main/SPEC.md

Usage:
    from fleet.pipeline.symphony_orchestrator import SymphonyOrchestrator

    orchestrator = SymphonyOrchestrator(
        workflow_path="path/to/WORKFLOW.md",
        workspace_root="/tmp/symphony_workspaces",
    )
    orchestrator.run()  # Starts polling loop
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import subprocess
import threading
import time
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from fleet.pipeline.issue_tracker import Issue, IssueTrackerClient
from fleet.pipeline.workflow_loader import load_workflow, merge_with_defaults
from fleet.pipeline.workspace_manager import WorkspaceManager

log = logging.getLogger("symphony.orchestrator")


# --- Runtime State Data Structures ---


@dataclass
class RunAttempt:
    """Run attempt entity per Symphony spec."""

    issue_id: str
    issue_identifier: str
    attempt: int  # 0 for first run, 1+ for retries
    workspace_path: Path
    started_at: datetime
    status: str = "pending"  # pending, running, success, failed, stopped
    error: str | None = None


@dataclass
class RetryEntry:
    """Retry schedule entry."""

    issue_id: str
    identifier: str
    attempt: int
    due_at_ms: int  # Monotonic clock timestamp
    error: str | None = None


@dataclass
class LiveSession:
    """Agent session metadata."""

    session_id: str
    thread_id: str
    turn_id: str
    workspace_path: Path
    started_at: datetime
    last_event: str = "started"
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    turn_count: int = 0


class OrchestratorError(Exception):
    """Orchestrator error."""

    pass


class SymphonyOrchestrator:
    """Main orchestrator per Symphony spec.

    Implements the Orchestration State Machine.
    """

    def __init__(
        self,
        workflow_path: str | Path,
        workspace_root: str | Path | None = None,
        state_db: str | Path | None = None,
    ):
        """Initialize orchestrator.

        Args:
            workflow_path: Path to WORKFLOW.md
            workspace_root: Override workspace root
            state_db: Path to SQLite state database
        """
        self.workflow_path = Path(workflow_path)

        # Load workflow
        self.workflow = load_workflow(workflow_path)
        self.config = self._resolve_env_config(
            merge_with_defaults(self.workflow["config"])
        )
        self.prompt_template = self.workflow["prompt_template"]

        # Resolve workspace root from config or use default
        ws_root = workspace_root or self.config.get("workspace", {}).get("root")
        if not ws_root:
            ws_root = Path(tempfile.gettempdir()) / "symphony_workspaces"

        # Setup components
        self.workspace_manager = WorkspaceManager(
            root=ws_root,
            hooks=self.config.get("hooks", {}),
            timeout_ms=self.config.get("hooks", {}).get("timeout_ms", 60000),
        )

        # Setup issue tracker
        tracker_config = self.config.get("tracker", {})
        self.issue_tracker = IssueTrackerClient(
            endpoint=tracker_config.get("endpoint", "https://api.linear.app/graphql"),
            api_key=tracker_config.get("api_key"),
            project_slug=tracker_config.get("project_slug"),
            active_states=tracker_config.get("active_states", ["Todo", "In Progress"]),
            terminal_states=tracker_config.get("terminal_states", ["Closed", "Done"]),
        )

        # Configurable limits
        self.poll_interval_ms = self.config.get("polling", {}).get("interval_ms", 30000)
        self.max_concurrent_agents = self.config.get("agent", {}).get(
            "max_concurrent_agents", 10
        )
        self.max_retry_backoff_ms = self.config.get("agent", {}).get(
            "max_retry_backoff_ms", 300000
        )

        # State database
        self.state_db = (
            Path(state_db)
            if state_db
            else Path(tempfile.gettempdir()) / "symphony_state.db"
        )
        self._init_state_db()

        # Runtime state
        self._running = {}  # issue_id -> RunAttempt
        self._claimed = set()  # issue_ids reserved/running/retrying
        self._retry_queue: list[RetryEntry] = []
        self._completed = set()  # issue_ids completed (bookkeeping)
        self._live_sessions: dict[str, LiveSession] = {}
        self._agent_processes: dict[str, subprocess.Popen] = {}

        # Token accounting
        self._codex_totals = {"input": 0, "output": 0, "total": 0, "runtime_seconds": 0}

        # Control flags
        self._shutdown = threading.Event()
        self._tick_lock = threading.Lock()

        # Agent command from config
        codex_config = self.config.get("codex", {})
        self.agent_command = codex_config.get("command", "codex app-server")
        self.agent_timeout_ms = codex_config.get("return_timeout_ms", 3600000)

    @staticmethod
    def _resolve_env_config(value: Any) -> Any:
        """Resolve $VAR or ${VAR} recursively for config trees."""
        if isinstance(value, dict):
            return {
                k: SymphonyOrchestrator._resolve_env_config(v) for k, v in value.items()
            }
        if isinstance(value, list):
            return [SymphonyOrchestrator._resolve_env_config(v) for v in value]
        if isinstance(value, str):
            if value.startswith("${") and value.endswith("}") and len(value) > 3:
                return os.environ.get(value[2:-1], "")
            if value.startswith("$") and len(value) > 1 and " " not in value:
                return os.environ.get(value[1:], "")
        return value

    def _init_state_db(self) -> None:
        """Initialize state database."""
        conn = sqlite3.connect(self.state_db, timeout=10)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL,
                issue_identifier TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                workspace_path TEXT NOT NULL,
                started_at TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                UNIQUE(issue_id, attempt)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                thread_id TEXT,
                turn_id TEXT,
                workspace_path TEXT,
                started_at TEXT,
                last_event TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                total_tokens INTEGER,
                turn_count INTEGER
            )
        """)
        conn.commit()
        conn.close()

    def _save_run(self, run: RunAttempt) -> None:
        """Save run to state DB."""
        conn = sqlite3.connect(self.state_db, timeout=10)
        conn.execute(
            """
            INSERT OR REPLACE INTO runs
            (issue_id, issue_identifier, attempt, workspace_path, started_at, status, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                run.issue_id,
                run.issue_identifier,
                run.attempt,
                str(run.workspace_path),
                run.started_at.isoformat(),
                run.status,
                run.error,
            ),
        )
        conn.commit()
        conn.close()

    def _load_runs_for_issue(self, issue_id: str) -> list[RunAttempt]:
        """Load all runs for an issue."""
        conn = sqlite3.connect(self.state_db, timeout=10)
        rows = conn.execute(
            """
            SELECT issue_id, issue_identifier, attempt, workspace_path, started_at, status, error
            FROM runs WHERE issue_id = ? ORDER BY attempt DESC
        """,
            (issue_id,),
        ).fetchall()
        conn.close()

        runs = []
        for row in rows:
            runs.append(
                RunAttempt(
                    issue_id=row[0],
                    issue_identifier=row[1],
                    attempt=row[2],
                    workspace_path=Path(row[3]),
                    started_at=datetime.fromisoformat(row[4]),
                    status=row[5],
                    error=row[6],
                )
            )
        return runs

    def run(self) -> None:
        """Main run loop - polls and dispatches."""
        log.info(
            f"Starting Symphony orchestrator (poll_interval={self.poll_interval_ms}ms)"
        )

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Cleanup terminal workspaces on startup
        self._cleanup_terminal_workspaces()

        while not self._shutdown.is_set():
            try:
                self._poll_tick()
            except Exception as e:
                log.error(f"Poll tick error: {e}")

            # Wait for next tick or shutdown
            self._shutdown.wait(timeout=self.poll_interval_ms / 1000)

        log.info("Orchestrator shutdown complete")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signal."""
        log.info(f"Received signal {signum}, initiating shutdown...")
        self._shutdown.set()

    def _poll_tick(self) -> None:
        """Single poll tick - fetch issues and dispatch."""
        with self._tick_lock:
            # 1. Process retry queue
            self._process_retry_queue()

            # 2. Reconcile active runs
            self._reconcile_active_runs()

            # 3. Fetch candidate issues
            if len(self._running) < self.max_concurrent_agents:
                candidates = self._fetch_candidate_issues()
                for issue in candidates:
                    if len(self._running) >= self.max_concurrent_agents:
                        break
                    self._dispatch_issue(issue)

    def _process_retry_queue(self) -> None:
        """Process pending retries."""
        now_ms = int(time.time() * 1000)

        # Filter due entries
        due = [r for r in self._retry_queue if r.due_at_ms <= now_ms]
        self._retry_queue = [r for r in self._retry_queue if r.due_at_ms > now_ms]

        for retry in due:
            if len(self._running) >= self.max_concurrent_agents:
                # Re-queue if at capacity
                retry.due_at_ms = now_ms + 5000  # Try again in 5s
                self._retry_queue.append(retry)
                continue

            log.info(f"Retrying issue {retry.identifier} (attempt {retry.attempt})")

            # Re-dispatch
            issue = self.issue_tracker.get_issue_by_id(retry.issue_id)
            if issue and self.issue_tracker.is_active(issue.state):
                self._dispatch_issue(issue, attempt=retry.attempt, error=retry.error)
            else:
                log.warning(
                    f"Issue {retry.issue_id} not eligible for retry; releasing claim"
                )
                self._claimed.discard(retry.issue_id)

    def _reconcile_active_runs(self) -> None:
        """Reconcile active runs with issue tracker state."""
        # Check each running issue's current state
        for issue_id, run in list(self._running.items()):
            issue = self.issue_tracker.get_issue_by_id(issue_id)

            if not issue:
                # Issue deleted - stop the run
                log.info(f"Issue {issue_id} no longer exists, stopping run")
                self._stop_run(issue_id, "issue_deleted")
                continue

            if self.issue_tracker.is_terminal(issue.state):
                # Issue moved to terminal state - stop the run
                log.info(f"Issue {issue_id} moved to terminal state: {issue.state}")
                self._stop_run(
                    issue_id,
                    f"state_became_terminal:{issue.state}",
                    cleanup_workspace=True,
                )
                continue

            if not self.issue_tracker.is_active(issue.state):
                # Issue moved out of active set - stop without cleanup
                log.info(f"Issue {issue_id} moved to non-active state: {issue.state}")
                self._stop_run(
                    issue_id,
                    f"state_became_non_active:{issue.state}",
                    cleanup_workspace=False,
                )
                continue

    def _fetch_candidate_issues(self) -> list[Issue]:
        """Fetch eligible issues for dispatch."""
        all_active = self.issue_tracker.get_active_issues()
        candidates = []

        for issue in all_active:
            # Skip if already claimed or running
            if issue.id in self._claimed:
                continue

            # Check if blocked
            if issue.blocked_by and issue.state.strip().lower() == "todo":
                blocked = False
                for blocker in issue.blocked_by:
                    if not self.issue_tracker.is_terminal(blocker.state):
                        blocked = True
                        break
                if blocked:
                    continue

            candidates.append(issue)

        # Sort by priority (lower = higher priority)
        candidates.sort(
            key=lambda i: (
                i.priority is None,
                i.priority if i.priority is not None else 999,
                i.created_at.isoformat() if i.created_at else "",
                i.identifier,
            )
        )

        return candidates

    def _dispatch_issue(
        self, issue: Issue, attempt: int = 0, error: str | None = None
    ) -> None:
        """Dispatch an issue to an agent.

        Args:
            issue: Issue to dispatch
            attempt: Attempt number (0 = first)
            error: Previous error if retry
        """
        if issue.id in self._running:
            log.warning(f"Issue {issue.id} already running")
            return

        # Mark as claimed
        self._claimed.add(issue.id)

        # Get workspace
        try:
            workspace = self.workspace_manager.get_workspace(issue.identifier)
        except Exception as e:
            log.error(f"Workspace setup failed for {issue.identifier}: {e}")
            self._schedule_retry(issue, attempt, f"workspace_setup_failed:{e}")
            return

        # Run before_run hook
        if not self.workspace_manager.before_run(workspace.path):
            log.error(f"before_run hook failed for {issue.identifier}")
            self._schedule_retry(issue, attempt, "hook_failed:before_run")
            return

        # Build prompt
        prompt = self._build_prompt(issue)

        # Create run record
        run = RunAttempt(
            issue_id=issue.id,
            issue_identifier=issue.identifier,
            attempt=attempt,
            workspace_path=workspace.path,
            started_at=datetime.now(),
            status="running",
        )
        self._running[issue.id] = run
        self._save_run(run)

        # Launch agent (async)
        thread = threading.Thread(
            target=self._run_agent,
            args=(run, prompt, workspace.path),
            daemon=True,
        )
        thread.start()

        log.info(
            f"Dispatched {issue.identifier} (attempt {attempt}) to workspace {workspace.path}"
        )

    def _build_prompt(self, issue: Issue) -> str:
        """Build agent prompt from template + issue data."""
        # Render template with issue data
        prompt = self.prompt_template

        # Simple variable substitution
        prompt = prompt.replace("{{issue_id}}", issue.id)
        prompt = prompt.replace("{{issue_identifier}}", issue.identifier)
        prompt = prompt.replace("{{issue_title}}", issue.title)
        prompt = prompt.replace("{{issue_description}}", issue.description or "")
        prompt = prompt.replace(
            "{{issue_priority}}",
            "" if issue.priority is None else str(issue.priority),
        )
        prompt = prompt.replace("{{issue_state}}", issue.state)
        prompt = prompt.replace("{{issue_url}}", issue.url or "")

        return prompt

    def _run_agent(self, run: RunAttempt, prompt: str, workspace: Path) -> None:
        """Run the coding agent in a workspace."""
        session_id = f"{run.issue_identifier}-{run.attempt}"

        try:
            # Build agent command
            cmd = ["bash", "-lc", self.agent_command]

            # Launch agent subprocess
            proc = subprocess.Popen(
                cmd,
                cwd=workspace,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self._agent_processes[run.issue_id] = proc

            # Create session
            session = LiveSession(
                session_id=session_id,
                thread_id=session_id,
                turn_id="0",
                workspace_path=workspace,
                started_at=datetime.now(),
            )
            self._live_sessions[session_id] = session

            # Send prompt
            stdout, stderr = proc.communicate(
                input=prompt, timeout=self.agent_timeout_ms / 1000
            )

            if proc.returncode == 0:
                run.status = "success"
                log.info(f"Agent completed successfully for {run.issue_identifier}")
            else:
                run.status = "failed"
                run.error = stderr[:500] if stderr else "unknown error"
                log.error(f"Agent failed for {run.issue_identifier}: {run.error}")

        except subprocess.TimeoutExpired:
            run.status = "failed"
            run.error = "timeout"
            log.error(f"Agent timed out for {run.issue_identifier}")

            # Kill subprocess
            try:
                proc.kill()
            except Exception:
                pass

        except Exception as e:
            run.status = "failed"
            run.error = str(e)[:500]
            log.error(f"Agent error for {run.issue_identifier}: {e}")

        finally:
            self._agent_processes.pop(run.issue_id, None)
            # Update state
            self._running.pop(run.issue_id, None)
            self._completed.add(run.issue_id)
            self._save_run(run)

            # Run after_run hook
            success = run.status == "success"
            self.workspace_manager.after_run(workspace, success)

            # Schedule retry if failed
            if run.status == "failed":
                self._schedule_retry_from_run(run)
            else:
                self._claimed.discard(run.issue_id)

    def _schedule_retry(self, issue: Issue, attempt: int, error: str) -> None:
        """Schedule a retry with exponential backoff."""
        # Deduplicate any existing retry for the same issue.
        self._retry_queue = [r for r in self._retry_queue if r.issue_id != issue.id]

        # Exponential backoff: 10s, 20s, 40s, 80s...
        delay_ms = min(10000 * (2 ** max(attempt, 0)), self.max_retry_backoff_ms)
        due_at_ms = int(time.time() * 1000) + delay_ms

        retry = RetryEntry(
            issue_id=issue.id,
            identifier=issue.identifier,
            attempt=attempt + 1,
            due_at_ms=due_at_ms,
            error=error,
        )
        self._retry_queue.append(retry)

        log.info(
            f"Scheduled retry for {issue.identifier} at {due_at_ms} (delay={delay_ms}ms)"
        )

    def _schedule_retry_from_run(self, run: RunAttempt) -> None:
        """Schedule retry from failed run."""
        # Get issue to rebuild
        issue = self.issue_tracker.get_issue_by_id(run.issue_id)
        if issue and self.issue_tracker.is_active(issue.state):
            self._schedule_retry(issue, run.attempt, run.error or "unknown")
            return
        self._claimed.discard(run.issue_id)

    def _stop_run(
        self, issue_id: str, reason: str, cleanup_workspace: bool = False
    ) -> None:
        """Stop an active run."""
        run = self._running.get(issue_id)
        if not run:
            return

        proc = self._agent_processes.get(issue_id)
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        run.status = "stopped"
        run.error = reason

        self._running.pop(issue_id, None)
        self._completed.add(issue_id)
        self._claimed.discard(issue_id)
        self._save_run(run)

        if cleanup_workspace:
            try:
                self.workspace_manager.remove_workspace(run.issue_identifier)
            except Exception as e:
                log.warning(f"Failed workspace cleanup for {run.issue_identifier}: {e}")

        log.info(f"Stopped run for {issue_id}: {reason}")

    def _cleanup_terminal_workspaces(self) -> None:
        """Clean up workspaces for terminal issues on startup."""
        try:
            terminal_issues = {
                issue.identifier for issue in self.issue_tracker.get_terminal_issues()
            }
            cleaned = self.workspace_manager.cleanup_terminal_workspaces(
                terminal_issues
            )
            log.info(f"Cleaned up {cleaned} terminal workspaces on startup")
        except Exception as e:
            log.warning(f"Terminal workspace cleanup skipped due to tracker error: {e}")

    def get_status(self) -> dict[str, Any]:
        """Get orchestrator status for observability."""
        return {
            "workflow": str(self.workflow_path),
            "poll_interval_ms": self.poll_interval_ms,
            "max_concurrent_agents": self.max_concurrent_agents,
            "running": len(self._running),
            "claimed": len(self._claimed),
            "retry_queue": len(self._retry_queue),
            "completed": len(self._completed),
            "live_sessions": len(self._live_sessions),
            "codex_totals": self._codex_totals,
        }


def main():
    """CLI entry point."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Symphony Orchestrator")
    parser.add_argument("workflow", help="Path to WORKFLOW.md")
    parser.add_argument("--workspace-root", help="Override workspace root")
    parser.add_argument("--state-db", help="State database path")
    parser.add_argument("--once", action="store_true", help="Run single poll tick only")

    args = parser.parse_args()

    orchestrator = SymphonyOrchestrator(
        workflow_path=args.workflow,
        workspace_root=args.workspace_root,
        state_db=args.state_db,
    )

    if args.once:
        orchestrator._poll_tick()
        print(json.dumps(orchestrator.get_status(), indent=2))
    else:
        orchestrator.run()


if __name__ == "__main__":
    main()
