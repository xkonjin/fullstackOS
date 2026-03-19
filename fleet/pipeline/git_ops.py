from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import time
from pathlib import Path

log = logging.getLogger("aifleet.git_ops")

FIXED_WORKTREES = ["code", "tests", "ui", "uitest"]

# Worktrees assigned for longer than this are considered stale.
STALE_WORKTREE_TIMEOUT_SECONDS = int(os.environ.get("STALE_WORKTREE_TIMEOUT_SECONDS", str(4 * 3600)))

# Max overflow worktrees allowed at once (backpressure)
MAX_OVERFLOW_WORKTREES = int(os.environ.get("MAX_OVERFLOW_WORKTREES", "4"))

# Lock timeout for worktree acquisition (seconds)
ACQUIRE_LOCK_TIMEOUT_SECONDS = 30


def _now_ts() -> int:
    return int(time.time())


def _run_git(
    args: list[str], cwd: str | None = None, timeout: int = 60
) -> tuple[bool, str, str]:
    """Run git command. Returns (success, stdout, stderr)."""
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", f"Git command timed out after {timeout}s"
    except Exception as e:
        return False, "", str(e)


def init_pipeline_tables(conn: sqlite3.Connection) -> None:
    """Create all pipeline-related tables and indexes if they don't exist.

    Tables created: pipeline_runs, pipeline_stages, worktree_state,
    cycle_summaries, project_config, pipeline_skills, cost_log, budgets,
    notifications, run_history.  Safe to call multiple times (idempotent).
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL UNIQUE,
            pipeline_type TEXT NOT NULL,
            project_repo TEXT NOT NULL,
            title TEXT NOT NULL,
            raw_input TEXT,
            structured_objective TEXT,
            spec_json TEXT,
            plan_json TEXT,
            config_json TEXT DEFAULT '{}',
            status TEXT DEFAULT 'created',
            current_stage TEXT,
            created_at INTEGER NOT NULL,
            started_at INTEGER,
            completed_at INTEGER,
            total_cost_usd REAL DEFAULT 0.0,
            cycle_count INTEGER DEFAULT 0,
            error TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_stages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            stage_order INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            agent_via TEXT,
            agent_model TEXT,
            worktree_name TEXT,
            run_id TEXT,
            cycle INTEGER DEFAULT 1,
            input_json TEXT,
            output_json TEXT,
            cost_usd REAL DEFAULT 0.0,
            started_at INTEGER,
            completed_at INTEGER,
            error TEXT,
            human_input TEXT,
            UNIQUE(pipeline_id, stage_name, cycle)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS worktree_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worktree_name TEXT NOT NULL UNIQUE,
            worktree_path TEXT NOT NULL,
            branch_name TEXT NOT NULL,
            pipeline_id TEXT,
            stage_name TEXT,
            status TEXT DEFAULT 'free',
            created_at INTEGER NOT NULL,
            last_used_at INTEGER,
            pr_url TEXT,
            pr_number INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS cycle_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            cycle INTEGER NOT NULL,
            summary TEXT NOT NULL,
            files_changed TEXT,
            test_results TEXT,
            errors_encountered TEXT,
            fixes_applied TEXT,
            created_at INTEGER NOT NULL,
            UNIQUE(pipeline_id, stage_name, cycle)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS project_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_repo TEXT NOT NULL UNIQUE,
            pipeline_type_defaults TEXT DEFAULT '{}',
            human_gates TEXT DEFAULT '["spec","review"]',
            deploy_target TEXT DEFAULT 'merge',
            deploy_config TEXT DEFAULT '{}',
            worktree_config TEXT DEFAULT '{}',
            notification_channel TEXT DEFAULT 'telegram',
            max_ralph_cycles INTEGER DEFAULT 5,
            created_at INTEGER NOT NULL,
            updated_at INTEGER
        )
    """)

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_id ON pipeline_runs(pipeline_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pipeline_stages_pipeline_id ON pipeline_stages(pipeline_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pipeline_stages_status ON pipeline_stages(status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pipeline_stages_advance ON pipeline_stages(pipeline_id, status, stage_order)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_worktree_state_status ON worktree_state(status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_worktree_state_pipeline_id ON worktree_state(pipeline_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cycle_summaries_pipeline_id ON cycle_summaries(pipeline_id)"
    )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            skill_name TEXT NOT NULL,
            skill_path TEXT NOT NULL,
            injected_at INTEGER,
            UNIQUE(pipeline_id, stage_name, skill_name),
            FOREIGN KEY (pipeline_id) REFERENCES pipeline_runs(pipeline_id)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pipeline_skills_pipeline_id ON pipeline_skills(pipeline_id)"
    )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS cost_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            timestamp INTEGER,
            provider TEXT,
            model TEXT,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            estimated_cost_usd REAL DEFAULT 0.0,
            tier TEXT,
            via TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cost_log_run_id ON cost_log(run_id)")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS budgets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope TEXT NOT NULL,
            scope_id TEXT,
            budget_usd REAL NOT NULL,
            spent_usd REAL DEFAULT 0.0,
            period TEXT DEFAULT 'monthly',
            started_at INTEGER NOT NULL,
            expires_at INTEGER,
            alert_thresholds TEXT DEFAULT '[0.7, 0.8, 0.9, 1.0]'
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            severity TEXT DEFAULT 'info',
            task_id TEXT,
            run_id TEXT,
            pipeline_id TEXT,
            channel TEXT,
            created_at INTEGER NOT NULL,
            delivered_at INTEGER,
            delivery_status TEXT DEFAULT 'pending'
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_notifications_event_type ON notifications(event_type)"
    )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            ts INTEGER NOT NULL,
            status TEXT NOT NULL,
            objective TEXT,
            repo TEXT,
            via TEXT,
            tier TEXT,
            model TEXT,
            duration_seconds REAL,
            cost_usd REAL DEFAULT 0.0,
            notes TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_history_ts ON run_history(ts)")

    conn.commit()


def init_worktrees(repo_root: str, conn: sqlite3.Connection) -> list[str]:
    """Create 4 fixed worktrees if they don't exist, register in DB. Returns list of worktree names."""
    created = []
    worktrees_dir = Path(repo_root) / ".worktrees"
    worktrees_dir.mkdir(parents=True, exist_ok=True)

    for name in FIXED_WORKTREES:
        worktree_path = worktrees_dir / name
        branch_name = f"worktree/{name}"

        existing = conn.execute(
            "SELECT worktree_name, worktree_path, branch_name FROM worktree_state WHERE worktree_name = ?",
            (name,),
        ).fetchone()

        if existing:
            existing_path = str(
                existing["worktree_path"]
                if not isinstance(existing, tuple)
                else existing[1]
            )
            existing_branch = str(
                existing["branch_name"]
                if not isinstance(existing, tuple)
                else existing[2]
            )
            expected_path = str(worktree_path)
            expected_branch = branch_name

            # Heal stale fixed worktree records from other repos, then continue ensuring git worktree exists.
            if existing_path != expected_path or existing_branch != expected_branch:
                conn.execute(
                    """
                    UPDATE worktree_state
                    SET worktree_path = ?, branch_name = ?, status = 'free', pipeline_id = NULL, stage_name = NULL
                    WHERE worktree_name = ?
                    """,
                    (expected_path, expected_branch, name),
                )
                conn.commit()
                log.warning(
                    "Healed worktree registration for %s: path %s -> %s, branch %s -> %s",
                    name,
                    existing_path,
                    expected_path,
                    existing_branch,
                    expected_branch,
                )

            # Do not continue: still ensure git worktree exists on disk/metadata for this repo root.

        success, stdout, stderr = _run_git(
            ["worktree", "list", "--porcelain"], cwd=repo_root
        )
        if success and str(worktree_path) in stdout and worktree_path.exists():
            pass
        else:
            success, stdout, stderr = _run_git(
                ["show-ref", "--verify", f"refs/heads/{branch_name}"], cwd=repo_root
            )
            branch_exists = success

            if branch_exists:
                success, stdout, stderr = _run_git(
                    ["worktree", "add", str(worktree_path), branch_name], cwd=repo_root
                )
            else:
                success, stdout, stderr = _run_git(
                    ["worktree", "add", "-b", branch_name, str(worktree_path), "main"],
                    cwd=repo_root,
                )

            if not success:
                continue

        conn.execute(
            """
            INSERT OR IGNORE INTO worktree_state
            (worktree_name, worktree_path, branch_name, status, created_at)
            VALUES (?, ?, ?, 'free', ?)
        """,
            (name, str(worktree_path), branch_name, _now_ts()),
        )
        conn.commit()
        created.append(name)

    return created


def acquire_worktree(
    conn: sqlite3.Connection, pipeline_id: str, stage_name: str, preferred: str = ""
) -> dict | None:
    """Claim a free worktree atomically. Creates overflow if all busy. Returns worktree dict or None."""
    conn.row_factory = sqlite3.Row

    candidates = [preferred] if preferred else []
    candidates.extend(n for n in FIXED_WORKTREES if n != preferred)

    for name in candidates:
        row_meta = conn.execute(
            "SELECT worktree_path, branch_name FROM worktree_state WHERE worktree_name = ?",
            (name,),
        ).fetchone()

        if row_meta:
            wt_path = str(
                row_meta["worktree_path"]
                if not isinstance(row_meta, tuple)
                else row_meta[0]
            )
            wt_branch = str(
                row_meta["branch_name"]
                if not isinstance(row_meta, tuple)
                else row_meta[1]
            )

            if wt_path and not Path(wt_path).exists():
                wt_path_obj = Path(wt_path)
                repo_root = str(wt_path_obj.parent.parent)

                ok, _, _ = _run_git(
                    ["worktree", "add", wt_path, wt_branch], cwd=repo_root
                )
                if not ok:
                    ok, _, _ = _run_git(
                        ["worktree", "add", "-b", wt_branch, wt_path, "main"],
                        cwd=repo_root,
                    )

                if ok:
                    log.warning(
                        "Auto-healed missing worktree path for %s at %s", name, wt_path
                    )
                else:
                    log.warning(
                        "Worktree %s path is missing and auto-heal failed: %s",
                        name,
                        wt_path,
                    )
                    continue

        # Use BEGIN IMMEDIATE for write-lock to prevent concurrent claims.
        # If caller already opened a transaction, avoid nested BEGIN crash.
        started_tx = False
        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")
            started_tx = True
        try:
            result = conn.execute(
                """
                UPDATE worktree_state
                SET status = 'assigned', pipeline_id = ?, stage_name = ?, last_used_at = ?
                WHERE worktree_name = ? AND status = 'free'
            """,
                (pipeline_id, stage_name, _now_ts(), name),
            )
            if result.rowcount > 0:
                row = conn.execute(
                    "SELECT * FROM worktree_state WHERE worktree_name = ?", (name,)
                ).fetchone()
                if started_tx:
                    conn.commit()
                log.info(
                    "Acquired worktree %s for pipeline %s stage %s",
                    name,
                    pipeline_id,
                    stage_name,
                )
                return dict(row)
            # No rows matched — rollback the IMMEDIATE lock so others can proceed
            if started_tx:
                conn.rollback()
        except Exception:
            if started_tx and conn.in_transaction:
                conn.rollback()
            raise

    # All fixed worktrees busy — try to reclaim a stale one before creating overflow
    stale_row = _reclaim_stale_worktree(conn, pipeline_id, stage_name)
    if stale_row:
        return stale_row

    # Backpressure: check overflow count before creating more
    overflow_count_row = conn.execute(
        "SELECT COUNT(*) as cnt FROM worktree_state WHERE worktree_name LIKE 'overflow_%'"
    ).fetchone()
    overflow_count = overflow_count_row["cnt"] if overflow_count_row else 0

    if overflow_count >= MAX_OVERFLOW_WORKTREES:
        log.warning(
            "Backpressure: %d overflow worktrees active (max %d) — rejecting acquire for pipeline %s",
            overflow_count,
            MAX_OVERFLOW_WORKTREES,
            pipeline_id,
        )
        return None

    # Create overflow worktree
    overflow_name = f"overflow_{os.urandom(4).hex()}"
    existing_row = conn.execute(
        "SELECT worktree_path FROM worktree_state LIMIT 1"
    ).fetchone()

    if not existing_row:
        log.error("No worktrees registered in DB — cannot create overflow")
        return None

    base_path = Path(existing_row["worktree_path"]).parent
    overflow_path = base_path / overflow_name
    branch_name = f"worktree/{overflow_name}"

    repo_root = base_path.parent
    success, stdout, stderr = _run_git(
        ["worktree", "add", "-b", branch_name, str(overflow_path), "main"],
        cwd=str(repo_root),
    )

    if not success:
        log.error("Failed to create overflow worktree %s: %s", overflow_name, stderr)
        return None

    try:
        conn.execute(
            """
            INSERT INTO worktree_state
            (worktree_name, worktree_path, branch_name, pipeline_id, stage_name, status, created_at, last_used_at)
            VALUES (?, ?, ?, ?, ?, 'assigned', ?, ?)
        """,
            (
                overflow_name,
                str(overflow_path),
                branch_name,
                pipeline_id,
                stage_name,
                _now_ts(),
                _now_ts(),
            ),
        )
        conn.commit()
    except Exception as e:
        # DB insert failed but git worktree exists — clean up
        log.error(
            "Failed to register overflow worktree %s in DB: %s — cleaning up",
            overflow_name,
            e,
        )
        _run_git(
            ["worktree", "remove", str(overflow_path), "--force"], cwd=str(repo_root)
        )
        _run_git(["branch", "-D", branch_name], cwd=str(repo_root))
        return None

    row = conn.execute(
        "SELECT * FROM worktree_state WHERE worktree_name = ?", (overflow_name,)
    ).fetchone()

    log.info(
        "Created overflow worktree %s for pipeline %s stage %s",
        overflow_name,
        pipeline_id,
        stage_name,
    )
    return dict(row) if row else None


def _reclaim_stale_worktree(
    conn: sqlite3.Connection, pipeline_id: str, stage_name: str
) -> dict | None:
    """Reclaim a stale fixed worktree, skipping stale entries with missing paths."""
    cutoff = _now_ts() - STALE_WORKTREE_TIMEOUT_SECONDS

    stale_rows = conn.execute(
        """
        SELECT worktree_name, worktree_path, branch_name
        FROM worktree_state
        WHERE status = 'assigned' AND last_used_at < ? AND worktree_name IN ({})
        ORDER BY last_used_at ASC
        """.format(",".join("?" * len(FIXED_WORKTREES))),
        (cutoff, *FIXED_WORKTREES),
    ).fetchall()

    for stale in stale_rows:
        wt_name = stale["worktree_name"] if not isinstance(stale, tuple) else stale[0]
        wt_path = str(
            stale["worktree_path"] if not isinstance(stale, tuple) else stale[1]
        )
        wt_branch = str(
            stale["branch_name"] if not isinstance(stale, tuple) else stale[2]
        )

        if wt_path and not Path(wt_path).exists():
            wt_path_obj = Path(wt_path)
            repo_root = str(wt_path_obj.parent.parent)
            ok, _, _ = _run_git(["worktree", "add", wt_path, wt_branch], cwd=repo_root)
            if not ok:
                ok, _, _ = _run_git(
                    ["worktree", "add", "-b", wt_branch, wt_path, "main"], cwd=repo_root
                )
            if not ok:
                log.warning(
                    "Skipping stale reclaim for %s because path is missing and heal failed: %s",
                    wt_name,
                    wt_path,
                )
                continue

        started_tx = False
        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")
            started_tx = True
        try:
            result = conn.execute(
                """
                UPDATE worktree_state
                SET status = 'assigned', pipeline_id = ?, stage_name = ?, last_used_at = ?
                WHERE worktree_name = ?
                """,
                (pipeline_id, stage_name, _now_ts(), wt_name),
            )
            if result.rowcount > 0:
                row = conn.execute(
                    "SELECT * FROM worktree_state WHERE worktree_name = ?",
                    (wt_name,),
                ).fetchone()
                if started_tx:
                    conn.commit()
                if row:
                    log.warning(
                        "Reclaimed stale worktree %s (was assigned >%ds)",
                        wt_name,
                        STALE_WORKTREE_TIMEOUT_SECONDS,
                    )
                    return dict(row)
            # No rows matched — release the IMMEDIATE lock
            if started_tx:
                conn.rollback()
        except Exception:
            if started_tx and conn.in_transaction:
                conn.rollback()
            raise

    return None


def release_worktree(conn: sqlite3.Connection, worktree_name: str) -> None:
    """Return worktree to pool (set status='free', clear pipeline_id/stage_name).
    Also resets any dirty git state in the worktree."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT worktree_path FROM worktree_state WHERE worktree_name = ?",
        (worktree_name,),
    ).fetchone()

    if not row:
        return

    if row["worktree_path"]:
        wt_path = row["worktree_path"]
        if not Path(wt_path).exists():
            log.warning(
                "Worktree path %s does not exist — marking free anyway", wt_path
            )
        else:
            # Best-effort cleanup of dirty git state
            _run_git(["rebase", "--abort"], cwd=wt_path)
            _run_git(["checkout", "--", "."], cwd=wt_path)
            success, _, stderr = _run_git(["checkout", "main"], cwd=wt_path)
            if not success:
                log.warning(
                    "Failed to reset worktree %s to main: %s", worktree_name, stderr
                )

    conn.execute(
        """
        UPDATE worktree_state
        SET status = 'free', pipeline_id = NULL, stage_name = NULL
        WHERE worktree_name = ?
    """,
        (worktree_name,),
    )
    conn.commit()
    log.info("Released worktree %s", worktree_name)


def requeue_stale_worktrees(conn: sqlite3.Connection) -> int:
    """Release worktrees that have been assigned for longer than the stale timeout.
    Returns count of worktrees released."""
    conn.row_factory = sqlite3.Row
    cutoff = _now_ts() - STALE_WORKTREE_TIMEOUT_SECONDS

    stale_rows = conn.execute(
        """
        SELECT worktree_name, pipeline_id, stage_name FROM worktree_state
        WHERE status = 'assigned' AND last_used_at < ?
    """,
        (cutoff,),
    ).fetchall()

    count = 0
    for row in stale_rows:
        log.warning(
            "Releasing stale worktree %s (pipeline %s, stage %s)",
            row["worktree_name"],
            row["pipeline_id"],
            row["stage_name"],
        )
        release_worktree(conn, row["worktree_name"])
        count += 1

    return count


def create_feature_branch(worktree_path: str, pipeline_id: str, stage: str) -> str:
    """Create and checkout branch feat/{pipeline_id}/{stage} in worktree.
    Returns branch name. Raises RuntimeError on failure."""
    branch_name = f"feat/{pipeline_id}/{stage}"

    # Reset any dirty state (uncommitted changes, interrupted rebase)
    _run_git(["rebase", "--abort"], cwd=worktree_path)
    _run_git(["checkout", "--", "."], cwd=worktree_path)

    success, _, stderr = _run_git(["fetch", "origin", "main"], cwd=worktree_path)
    if not success:
        raise RuntimeError(f"Failed to fetch origin/main: {stderr}")

    success, _, stderr = _run_git(["checkout", "main"], cwd=worktree_path)
    if not success:
        raise RuntimeError(f"Failed to checkout main: {stderr}")

    success, _, stderr = _run_git(
        ["pull", "origin", "main", "--ff-only"], cwd=worktree_path
    )
    if not success:
        # Pull may fail if diverged — try reset instead
        success, _, stderr = _run_git(
            ["reset", "--hard", "origin/main"], cwd=worktree_path
        )
        if not success:
            raise RuntimeError(f"Failed to sync with main: {stderr}")

    # Delete old branch if exists (from prior cycle)
    _run_git(["branch", "-D", branch_name], cwd=worktree_path)

    success, _, stderr = _run_git(["checkout", "-b", branch_name], cwd=worktree_path)
    if not success:
        raise RuntimeError(f"Failed to create branch {branch_name}: {stderr}")

    log.info("Created feature branch %s in %s", branch_name, worktree_path)
    return branch_name


def sync_with_main(worktree_path: str) -> tuple[bool, str]:
    """Fetch origin and rebase onto main. Returns (success, error_msg)."""
    success, stdout, stderr = _run_git(["fetch", "origin", "main"], cwd=worktree_path)
    if not success:
        return False, f"Failed to fetch origin/main: {stderr}"

    success, stdout, stderr = _run_git(["rebase", "origin/main"], cwd=worktree_path)
    if not success:
        _run_git(["rebase", "--abort"], cwd=worktree_path)
        return False, f"Rebase conflict: {stderr}"

    return True, ""


def create_pr(
    worktree_path: str, title: str, body: str
) -> tuple[str | None, int | None]:
    """Run gh pr create. Returns (pr_url, pr_number) or (None, None) on failure."""
    # Push the current branch first
    success, stdout, stderr = _run_git(
        ["push", "-u", "origin", "HEAD"], cwd=worktree_path
    )
    if not success:
        log.error("Failed to push branch for PR: %s", stderr)
        return None, None

    try:
        result = subprocess.run(
            ["gh", "pr", "create", "--title", title, "--body", body],
            cwd=worktree_path,
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            log.error("gh pr create failed: %s", result.stderr)
            return None, None

        pr_url = result.stdout.strip()

        pr_info_result = subprocess.run(
            ["gh", "pr", "view", "--json", "number"],
            cwd=worktree_path,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if pr_info_result.returncode == 0:
            pr_data = json.loads(pr_info_result.stdout)
            pr_number = pr_data.get("number")
            log.info("Created PR #%s: %s", pr_number, pr_url)
            return pr_url, pr_number

        return pr_url, None

    except subprocess.TimeoutExpired:
        log.error("gh pr create timed out")
        return None, None
    except Exception as e:
        log.error("gh pr create error: %s", e)
        return None, None


def merge_pr(pr_number: int) -> tuple[bool, str]:
    """Run gh pr merge --squash --delete-branch. Returns (success, error_msg)."""
    try:
        result = subprocess.run(
            ["gh", "pr", "merge", str(pr_number), "--squash", "--delete-branch"],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            log.error("gh pr merge failed for #%s: %s", pr_number, result.stderr)
            return False, result.stderr

        log.info("Merged PR #%s", pr_number)
        return True, ""

    except subprocess.TimeoutExpired:
        log.error("gh pr merge timed out for #%s", pr_number)
        return False, "merge timed out"
    except Exception as e:
        log.error("gh pr merge error for #%s: %s", pr_number, e)
        return False, str(e)


def cleanup_orphaned_worktrees(conn: sqlite3.Connection, repo_root: str) -> int:
    """Remove overflow worktrees whose pipelines are completed/failed/cancelled.
    Also removes git worktrees from disk. Returns count cleaned."""
    conn.row_factory = sqlite3.Row

    orphans = conn.execute("""
        SELECT w.worktree_name, w.worktree_path, w.branch_name, w.pipeline_id
        FROM worktree_state w
        LEFT JOIN pipeline_runs p ON p.pipeline_id = w.pipeline_id
        WHERE w.worktree_name LIKE 'overflow_%'
          AND (w.pipeline_id IS NULL OR p.status IN ('completed','failed','cancelled') OR p.pipeline_id IS NULL)
    """).fetchall()

    cleaned = 0
    for row in orphans:
        wt_name = row["worktree_name"]
        wt_path = row["worktree_path"]
        branch = row["branch_name"]

        # Remove git worktree from disk (best effort)
        _run_git(["worktree", "remove", wt_path, "--force"], cwd=repo_root)
        _run_git(["branch", "-D", branch], cwd=repo_root)

        conn.execute("DELETE FROM worktree_state WHERE worktree_name = ?", (wt_name,))
        cleaned += 1

    conn.commit()
    if cleaned > 0:
        log.info("Cleaned %d orphaned overflow worktrees", cleaned)

    return cleaned


def heartbeat_worktree(conn: sqlite3.Connection, worktree_name: str) -> None:
    """Update last_used_at for an assigned worktree to prevent stale reclaim.
    Non-fatal best-effort update."""
    try:
        conn.execute(
            "UPDATE worktree_state SET last_used_at = ? WHERE worktree_name = ? AND status = 'assigned'",
            (_now_ts(), worktree_name),
        )
        conn.commit()
    except Exception as e:
        log.debug("heartbeat_worktree failed for %s: %s", worktree_name, e)


def scan_worktree_drift(conn: sqlite3.Connection) -> list[dict]:
    """Detect DB worktree records that drift from on-disk state.

    Returns a list of drift items with fields:
      - worktree_name
      - path
      - status
      - issue: one of {missing_path, dirty_worktree, invalid_branch}
      - details

    This is read-only (no mutation). Callers decide remediation policy.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT worktree_name, worktree_path, branch_name, status, pipeline_id FROM worktree_state"
    ).fetchall()

    drift: list[dict] = []
    for row in rows:
        name = row["worktree_name"]
        path = row["worktree_path"]
        branch = row["branch_name"]
        status = row["status"]

        p = Path(path)
        if not p.exists():
            drift.append(
                {
                    "worktree_name": name,
                    "path": path,
                    "status": status,
                    "issue": "missing_path",
                    "details": "worktree path does not exist on disk",
                }
            )
            continue

        ok, stdout, stderr = _run_git(["status", "--porcelain"], cwd=path, timeout=15)
        if ok and stdout.strip():
            drift.append(
                {
                    "worktree_name": name,
                    "path": path,
                    "status": status,
                    "issue": "dirty_worktree",
                    "details": "uncommitted changes present",
                }
            )

        ok, stdout, stderr = _run_git(
            ["rev-parse", "--abbrev-ref", "HEAD"], cwd=path, timeout=15
        )
        head = stdout.strip() if ok else ""
        if ok and branch and head and head != branch:
            drift.append(
                {
                    "worktree_name": name,
                    "path": path,
                    "status": status,
                    "issue": "invalid_branch",
                    "details": f"db branch={branch}, head={head}",
                }
            )

    return drift


def sync_worktree_records(conn: sqlite3.Connection, repo_root: str) -> dict:
    """Reconcile worktree_state with `git worktree list --porcelain`.

    Strategy:
      - Parse current git worktrees from repo_root.
      - For DB records whose path no longer exists in git worktree list:
        - if status=free -> mark as free with NULL pipeline/stage (preserve record)
        - if status=assigned -> flag conflict in return payload (no force reassignment)
      - For discovered git worktrees not in DB under `.worktrees/<name>` pattern:
        - add as free records with inferred branch name

    Returns summary dict with counts and conflicts.
    """
    summary = {
        "db_records": 0,
        "git_worktrees": 0,
        "missing_in_git": 0,
        "added_from_git": 0,
        "healed_free": 0,
        "conflicts": [],
    }

    ok, stdout, stderr = _run_git(
        ["worktree", "list", "--porcelain"], cwd=repo_root, timeout=30
    )
    if not ok:
        return {**summary, "error": f"git worktree list failed: {stderr}"}

    git_paths: set[str] = set()
    git_meta: dict[str, dict] = {}
    current: dict[str, str] = {}
    for line in stdout.splitlines():
        if line.startswith("worktree "):
            if current.get("worktree"):
                wp = current["worktree"]
                git_paths.add(wp)
                git_meta[wp] = dict(current)
            current = {"worktree": line.split(" ", 1)[1].strip()}
        elif line.startswith("branch "):
            current["branch"] = line.split(" ", 1)[1].strip().replace("refs/heads/", "")
    if current.get("worktree"):
        wp = current["worktree"]
        git_paths.add(wp)
        git_meta[wp] = dict(current)

    summary["git_worktrees"] = len(git_paths)

    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT worktree_name, worktree_path, status, pipeline_id, stage_name, branch_name FROM worktree_state"
    ).fetchall()
    summary["db_records"] = len(rows)

    for row in rows:
        wt_path = row["worktree_path"]
        if wt_path in git_paths:
            continue
        summary["missing_in_git"] += 1
        if row["status"] == "free":
            conn.execute(
                "UPDATE worktree_state SET pipeline_id = NULL, stage_name = NULL, status = 'free' WHERE worktree_name = ?",
                (row["worktree_name"],),
            )
            summary["healed_free"] += 1
        else:
            summary["conflicts"].append(
                {
                    "worktree_name": row["worktree_name"],
                    "path": wt_path,
                    "status": row["status"],
                    "pipeline_id": row["pipeline_id"],
                    "stage_name": row["stage_name"],
                    "reason": "assigned worktree missing from git list",
                }
            )

    # Add git worktrees that look like managed `.worktrees/<name>` but missing in DB
    for wt_path in git_paths:
        p = Path(wt_path)
        try:
            rel = p.relative_to(Path(repo_root) / ".worktrees")
        except Exception:
            continue
        if len(rel.parts) != 1:
            continue
        name = rel.parts[0]
        exists = conn.execute(
            "SELECT 1 FROM worktree_state WHERE worktree_name = ?", (name,)
        ).fetchone()
        if exists:
            continue
        branch = (git_meta.get(wt_path, {}) or {}).get("branch", f"worktree/{name}")
        conn.execute(
            """
            INSERT INTO worktree_state(worktree_name, worktree_path, branch_name, status, created_at)
            VALUES(?,?,?,?,?)
            """,
            (name, wt_path, branch, "free", _now_ts()),
        )
        summary["added_from_git"] += 1

    conn.commit()
    return summary


def auto_create_pr(
    repo_path: str, base_branch: str = "main"
) -> tuple[str | None, int | None]:
    """Auto-create PR from current branch with AI-generated title/body from commit messages."""
    success, branch, _ = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_path)
    if not success or not branch.strip() or branch.strip() == base_branch:
        log.info("auto_create_pr: not on a feature branch (branch=%s)", branch.strip())
        return None, None

    branch = branch.strip()

    success, log_output, _ = _run_git(
        ["log", f"{base_branch}..HEAD", "--oneline", "--no-decorate"],
        cwd=repo_path,
    )
    if not success or not log_output.strip():
        log.info("auto_create_pr: no commits ahead of %s", base_branch)
        return None, None

    commits = log_output.strip().splitlines()

    if "/" in branch:
        title = branch.split("/", 1)[-1].replace("-", " ").replace("_", " ").title()
    else:
        title = commits[0].split(" ", 1)[-1] if commits else branch

    body_lines = ["## Changes\n"]
    for commit in commits[:20]:
        body_lines.append(f"- {commit}")
    if len(commits) > 20:
        body_lines.append(f"\n... and {len(commits) - 20} more commits")

    body = "\n".join(body_lines)
    return create_pr(repo_path, title, body)
