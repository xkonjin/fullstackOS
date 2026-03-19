from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import subprocess
import time

from pipeline.git_ops import release_worktree
from pipeline.quality_gates import GateVerdict, QualityGate
from pipeline.stages import (
    PIPELINE_STAGES,
    get_project_config,
    is_human_gate,
    run_stage,
)
from pipeline.telegram import (
    DEFAULT_GATE_TIMEOUT_SECONDS,
    queue_approval,
    resolve_approval,
    send_gate_notification,
    send_pipeline_notification,
)


def _compute_unblocked_tasks(tasks: list[dict], completed: set[str]) -> list[str]:
    runnable: list[str] = []
    for task in tasks:
        task_id = str(task.get("task_id", "")).strip()
        if not task_id or task_id in completed:
            continue

        deps_raw = task.get("depends_on", [])
        if isinstance(deps_raw, str):
            deps_iterable = [deps_raw]
        elif isinstance(deps_raw, (list, tuple, set)):
            deps_iterable = deps_raw
        else:
            deps_iterable = []

        deps = [str(d).strip() for d in deps_iterable if str(d).strip()]
        if all(dep in completed for dep in deps):
            runnable.append(task_id)
    return sorted(runnable)


def _prepare_wave_stage_input(
    stage_name: str, stage: dict, result_output: dict | None
) -> str | None:
    if stage_name != "plan" or not isinstance(result_output, dict):
        return None

    tasks = result_output.get("tasks")
    if not isinstance(tasks, list):
        return None

    completed: set[str] = set()
    first_wave = _compute_unblocked_tasks(tasks, completed)

    stage_input = {
        "swarm_state": {
            "enabled": True,
            "tasks_total": len(tasks),
            "completed_task_ids": sorted(completed),
            "current_wave": first_wave,
            "wave_index": 0,
        }
    }
    return json.dumps(stage_input)


def _load_stage_input_json(
    conn: sqlite3.Connection, pipeline_id: str, stage_name: str, cycle: int
) -> dict:
    row = conn.execute(
        "SELECT input_json FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?",
        (pipeline_id, stage_name, cycle),
    ).fetchone()
    if not row:
        return {}

    raw = row[0] if isinstance(row, tuple) else row["input_json"]
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _seed_implement_wave_state_from_plan(
    conn: sqlite3.Connection, pipeline_id: str, cycle: int
) -> None:
    existing = _load_stage_input_json(conn, pipeline_id, "implement", cycle)
    if existing.get("swarm_state"):
        return

    row = conn.execute(
        "SELECT output_json FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = 'plan' ORDER BY cycle DESC LIMIT 1",
        (pipeline_id,),
    ).fetchone()
    if not row or not row[0]:
        return

    try:
        plan_output = json.loads(row[0])
    except Exception:
        return

    if not isinstance(plan_output, dict):
        return
    tasks = plan_output.get("tasks")
    if not isinstance(tasks, list):
        return

    initial_wave = _compute_unblocked_tasks(tasks, set())
    seeded = {
        "swarm_state": {
            "enabled": True,
            "tasks_total": len(tasks),
            "completed_task_ids": [],
            "current_wave": initial_wave,
            "wave_index": 0,
        }
    }
    conn.execute(
        "UPDATE pipeline_stages SET input_json = ? WHERE pipeline_id = ? AND stage_name = 'implement' AND cycle = ?",
        (json.dumps(seeded), pipeline_id, cycle),
    )
    conn.commit()


def _validate_wave_completion(
    conn: sqlite3.Connection,
    pipeline_id: str,
    cycle: int,
    result_output: dict | None,
    strict: bool,
) -> tuple[bool, str]:
    if not strict or not isinstance(result_output, dict):
        return True, ""

    stage_input = _load_stage_input_json(conn, pipeline_id, "implement", cycle)
    swarm = stage_input.get("swarm_state") if isinstance(stage_input, dict) else None
    if not isinstance(swarm, dict):
        return True, ""

    current_wave = set(
        str(x).strip() for x in swarm.get("current_wave", []) if str(x).strip()
    )
    if not current_wave:
        return True, ""

    completed_now = result_output.get("completed_task_ids")
    if not isinstance(completed_now, list):
        return (
            False,
            "wave completion invalid: implement output missing completed_task_ids list",
        )

    completed_set = set(str(x).strip() for x in completed_now if str(x).strip())
    if not completed_set:
        return False, "wave completion invalid: completed_task_ids list is empty"

    invalid = sorted(completed_set - current_wave)
    if invalid:
        return False, f"wave completion invalid: tasks outside current_wave {invalid}"

    return True, ""


def _advance_wave_stage_input(
    conn: sqlite3.Connection,
    pipeline_id: str,
    stage_name: str,
    cycle: int,
    result_output: dict | None,
) -> None:
    if stage_name != "implement" or not isinstance(result_output, dict):
        return

    completed_now = result_output.get("completed_task_ids")
    if not isinstance(completed_now, list):
        return

    plan_row = conn.execute(
        "SELECT output_json FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = 'plan' ORDER BY cycle DESC LIMIT 1",
        (pipeline_id,),
    ).fetchone()
    if not plan_row or not plan_row[0]:
        return

    try:
        plan_output = json.loads(plan_row[0])
    except Exception:
        return

    tasks = plan_output.get("tasks") if isinstance(plan_output, dict) else None
    if not isinstance(tasks, list):
        return

    stage_row = conn.execute(
        "SELECT input_json FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?",
        (pipeline_id, stage_name, cycle),
    ).fetchone()

    existing: dict = {}
    if stage_row and stage_row[0]:
        try:
            existing = json.loads(stage_row[0])
        except Exception:
            existing = {}

    swarm = existing.get("swarm_state") if isinstance(existing, dict) else {}
    if not isinstance(swarm, dict):
        swarm = {}

    completed = set(
        str(x).strip() for x in swarm.get("completed_task_ids", []) if str(x).strip()
    )
    completed.update(str(x).strip() for x in completed_now if str(x).strip())

    current_wave = _compute_unblocked_tasks(tasks, completed)
    done = len(completed) >= len(
        [t for t in tasks if isinstance(t, dict) and str(t.get("task_id", "")).strip()]
    )

    updated = {
        "swarm_state": {
            "enabled": True,
            "tasks_total": len(tasks),
            "completed_task_ids": sorted(completed),
            "current_wave": current_wave,
            "wave_index": int(swarm.get("wave_index", 0)) + 1,
            "done": done,
        }
    }

    conn.execute(
        "UPDATE pipeline_stages SET input_json = ? WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?",
        (json.dumps(updated), pipeline_id, stage_name, cycle),
    )
    conn.commit()


def _mark_wave_done_if_missing(
    conn: sqlite3.Connection, pipeline_id: str, stage_name: str, cycle: int
) -> None:
    if stage_name != "implement":
        return

    row = conn.execute(
        "SELECT input_json FROM pipeline_stages WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?",
        (pipeline_id, stage_name, cycle),
    ).fetchone()
    if not row or not row[0]:
        return

    try:
        payload = json.loads(row[0])
    except Exception:
        return

    if not isinstance(payload, dict):
        return
    swarm = payload.get("swarm_state")
    if not isinstance(swarm, dict):
        return

    if swarm.get("current_wave"):
        return

    if swarm.get("done") is True:
        return

    swarm["done"] = True
    payload["swarm_state"] = swarm
    conn.execute(
        "UPDATE pipeline_stages SET input_json = ? WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?",
        (json.dumps(payload), pipeline_id, stage_name, cycle),
    )
    conn.commit()


_STAGE_GATE_MAP: dict[str, str] = {
    "plan": "strategize_to_scaffold",
    "implement": "scaffold_to_build",
    "test": "build_to_harden",
    "review": "harden_to_launch",
}


def _run_quality_gate(
    conn: sqlite3.Connection,
    pipeline_id: str,
    stage_name: str,
    stage_output: dict,
    cfg: dict,
) -> None:
    """Run an advisory quality gate after stage completion. Logs but never blocks."""
    gate_name = _STAGE_GATE_MAP.get(stage_name)
    if not gate_name:
        return

    evidence: list[dict] = []
    if isinstance(stage_output, dict):
        for key, val in stage_output.items():
            if isinstance(val, dict):
                evidence.append(val)
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        evidence.append(item)

    context = {"pipeline_id": pipeline_id, "stage_name": stage_name, "retry_count": 0}

    try:
        gate = QualityGate(conn, cfg)
        result = gate.evaluate(gate_name, evidence, context)

        if result.verdict == GateVerdict.FAIL:
            log.warning(
                "Quality gate %s FAILED (advisory) for pipeline %s: score=%.2f failures=%s",
                gate_name,
                pipeline_id,
                result.score,
                result.failures,
            )
        elif result.verdict == GateVerdict.WARN:
            log.info(
                "Quality gate %s WARN for pipeline %s: score=%.2f warnings=%s",
                gate_name,
                pipeline_id,
                result.score,
                result.warnings,
            )

        gate._record_gate_result(pipeline_id, result)
    except Exception as exc:
        log.debug("Quality gate %s failed (non-blocking): %s", gate_name, exc)


log = logging.getLogger("aifleet.engine")

# Max age (seconds) before a running/waiting_human pipeline is marked stale.
PIPELINE_STALE_TTL = 24 * 3600  # 24 hours


def _now_ts() -> int:
    return int(time.time())


def gc_stale_pipelines(
    conn: sqlite3.Connection,
    ttl: int = PIPELINE_STALE_TTL,
    exclude_id: str | None = None,
) -> dict:
    """Mark pipelines stuck in running/created for > ttl seconds as failed.

    Also fails any waiting_human stages whose pipeline is being cleaned up.
    Returns {cleaned_pipelines: int, cleaned_stages: int}.
    """
    cutoff = _now_ts() - ttl
    params: list = [cutoff]
    exclude_clause = ""
    if exclude_id:
        exclude_clause = " AND pipeline_id != ?"
        params.append(exclude_id)
    stale_rows = conn.execute(
        f"""
        SELECT pipeline_id, status, started_at, created_at FROM pipeline_runs
        WHERE status IN ('running', 'created')
          AND COALESCE(started_at, created_at) < ?
          {exclude_clause}
    """,
        params,
    ).fetchall()

    cleaned_pipelines = 0
    cleaned_stages = 0

    # Bulk-cancel stages for all stale pipelines in one query
    stale_ids = [row[0] for row in stale_rows]
    if stale_ids:
        placeholders = ",".join("?" * len(stale_ids))
        result = conn.execute(
            f"""
            UPDATE pipeline_stages
            SET status = 'cancelled', error = 'parent pipeline marked stale'
            WHERE pipeline_id IN ({placeholders}) AND status IN ('pending', 'running')
        """,
            stale_ids,
        )
        cleaned_stages = result.rowcount

    for row in stale_rows:
        pid = row[0]
        age_ts = row[2] or row[3]
        age_hours = round((_now_ts() - age_ts) / 3600, 1) if age_ts else 0

        cur = conn.execute(
            """
            UPDATE pipeline_runs
            SET status = 'failed', error = ?, completed_at = ?
            WHERE pipeline_id = ? AND status IN ('running', 'created')
        """,
            (f"stale pipeline gc: no progress for {age_hours}h", _now_ts(), pid),
        )
        if cur.rowcount == 0:
            continue
        cleaned_pipelines += 1

    if cleaned_pipelines:
        conn.commit()
        log.info(
            "gc_stale_pipelines: cleaned %d pipelines, %d stages (ttl=%ds)",
            cleaned_pipelines,
            cleaned_stages,
            ttl,
        )

    return {"cleaned_pipelines": cleaned_pipelines, "cleaned_stages": cleaned_stages}


def init_checkpoint_table(conn: sqlite3.Connection) -> None:
    """Initialize pipeline_checkpoints table for tracking worktree state."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            cycle INTEGER NOT NULL DEFAULT 1,
            commit_hash TEXT,
            diff_stat TEXT,
            snapshot_at INTEGER NOT NULL,
            FOREIGN KEY (pipeline_id) REFERENCES pipeline_runs(pipeline_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_checkpoint_pipeline
        ON pipeline_checkpoints(pipeline_id, snapshot_at DESC)
    """)
    conn.commit()


def snapshot_checkpoint(
    conn: sqlite3.Connection, pipeline_id: str, worktree_path: str
) -> dict:
    """Snapshot current worktree state at a checkpoint. Returns {ok, commit_hash, error}."""
    if not os.path.exists(worktree_path):
        return {"ok": False, "error": "worktree path does not exist"}

    try:
        # Get current commit hash
        commit_result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=worktree_path,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if commit_result.returncode != 0:
            return {
                "ok": False,
                "error": f"git rev-parse failed: {commit_result.stderr}",
            }

        commit_hash = commit_result.stdout.strip()

        # Get diff stat
        diff_result = subprocess.run(
            ["git", "diff", "--stat"],
            cwd=worktree_path,
            capture_output=True,
            text=True,
            timeout=5,
        )
        diff_stat = diff_result.stdout.strip() if diff_result.returncode == 0 else ""

        # Get current stage from pipeline
        conn.row_factory = sqlite3.Row
        stage_row = conn.execute(
            """
            SELECT stage_name, cycle FROM pipeline_stages
            WHERE pipeline_id = ? AND status = 'completed'
            ORDER BY stage_order DESC, cycle DESC
            LIMIT 1
        """,
            (pipeline_id,),
        ).fetchone()

        stage_name = stage_row["stage_name"] if stage_row else "unknown"
        cycle = stage_row["cycle"] if stage_row else 1

        # Insert checkpoint
        conn.execute(
            """
            INSERT INTO pipeline_checkpoints
            (pipeline_id, stage_name, cycle, commit_hash, diff_stat, snapshot_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (pipeline_id, stage_name, cycle, commit_hash, diff_stat, _now_ts()),
        )

        conn.commit()

        log.info(
            "Checkpoint saved for pipeline %s at stage %s cycle %d: %s",
            pipeline_id,
            stage_name,
            cycle,
            commit_hash[:8],
        )

        return {
            "ok": True,
            "commit_hash": commit_hash,
            "stage_name": stage_name,
            "cycle": cycle,
        }

    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "git command timed out"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def create_pipeline(
    conn: sqlite3.Connection,
    pipeline_type: str,
    project_repo: str,
    title: str,
    raw_input: str,
    config: dict | None = None,
) -> str:
    """Create a new pipeline run. Returns pipeline_id."""
    if pipeline_type not in PIPELINE_STAGES:
        raise ValueError(
            f"Invalid pipeline_type: {pipeline_type}. Must be one of {list(PIPELINE_STAGES.keys())}"
        )

    pipeline_id = "pip_" + os.urandom(4).hex()
    config_json = json.dumps(config or {})
    now = _now_ts()

    conn.execute(
        """
        INSERT INTO pipeline_runs
        (pipeline_id, pipeline_type, project_repo, title, raw_input, config_json, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 'created', ?)
    """,
        (pipeline_id, pipeline_type, project_repo, title, raw_input, config_json, now),
    )

    stages = PIPELINE_STAGES[pipeline_type]
    for idx, stage_name in enumerate(stages):
        conn.execute(
            """
            INSERT INTO pipeline_stages
            (pipeline_id, stage_name, stage_order, cycle, status)
            VALUES (?, ?, ?, 1, 'pending')
        """,
            (pipeline_id, stage_name, idx),
        )

    conn.commit()
    return pipeline_id


def advance_pipeline(conn: sqlite3.Connection, pipeline_id: str, cfg: dict) -> dict:
    """Advance pipeline to next stage. Returns {status, stage, result, done, ralph_loop}."""
    conn.row_factory = sqlite3.Row

    pipeline_row = conn.execute(
        "SELECT * FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()

    # Opportunistic GC AFTER reading current pipeline — exclude self to avoid self-GC
    try:
        gc_stale_pipelines(conn, exclude_id=pipeline_id)
    except Exception as exc:
        log.debug("gc_stale_pipelines failed (non-blocking): %s", exc)

    if not pipeline_row:
        return {"status": "not_found", "done": True, "error": "pipeline not found"}

    pipeline = dict(pipeline_row)
    status = pipeline["status"]

    if status not in ("created", "running"):
        return {"status": status, "done": True}

    if status == "created":
        conn.execute(
            "UPDATE pipeline_runs SET status = 'running', started_at = ? WHERE pipeline_id = ?",
            (_now_ts(), pipeline_id),
        )
        # Commit deferred to stage execution block below
        pipeline["status"] = "running"

    stage_row = conn.execute(
        """
        SELECT * FROM pipeline_stages
        WHERE pipeline_id = ? AND status IN ('pending', 'waiting_human')
        ORDER BY stage_order ASC
        LIMIT 1
    """,
        (pipeline_id,),
    ).fetchone()

    if not stage_row:
        conn.execute(
            "UPDATE pipeline_runs SET status = 'completed', completed_at = ? WHERE pipeline_id = ?",
            (_now_ts(), pipeline_id),
        )
        conn.commit()
        return {"status": "completed", "done": True}

    stage = dict(stage_row)
    stage_name = stage["stage_name"]

    if stage["status"] == "waiting_human":
        # Check for gate timeout
        gate_started = stage.get("started_at") or 0
        gate_timeout = DEFAULT_GATE_TIMEOUT_SECONDS
        if (
            gate_started
            and gate_timeout > 0
            and (_now_ts() - gate_started) > gate_timeout
        ):
            log.warning(
                "Gate timeout for pipeline %s stage %s after %ds",
                pipeline_id,
                stage_name,
                _now_ts() - gate_started,
            )
            result = conn.execute(
                """
                UPDATE pipeline_stages
                SET status = 'failed', error = 'gate timeout', completed_at = ?
                WHERE pipeline_id = ? AND stage_name = ? AND cycle = ? AND status = 'waiting_human'
            """,
                (_now_ts(), pipeline_id, stage_name, stage["cycle"]),
            )
            if result.rowcount == 0:
                # Race: stage was approved/rejected concurrently
                return {"status": "waiting_human", "stage": stage_name, "done": False}
            conn.execute(
                """
                UPDATE pipeline_runs
                SET status = 'failed', error = ?
                WHERE pipeline_id = ?
            """,
                (f"Gate timeout at stage {stage_name}", pipeline_id),
            )
            conn.commit()

            ok, err = send_pipeline_notification(
                pipeline_id, pipeline["title"], "gate_timeout", f"Stage: {stage_name}"
            )
            if not ok:
                log.error(
                    "Failed to send gate timeout notification for pipeline %s: %s",
                    pipeline_id,
                    err,
                )

            return {
                "status": "failed",
                "stage": stage_name,
                "error": "gate timeout",
                "done": True,
            }

        return {
            "status": "waiting_human",
            "stage": stage_name,
            "done": False,
        }

    if is_human_gate(
        conn, pipeline["project_repo"], stage_name, pipeline=pipeline
    ) and not stage.get("human_input"):
        conn.execute(
            """
            UPDATE pipeline_stages
            SET status = 'waiting_human', started_at = ?
            WHERE pipeline_id = ? AND stage_name = ? AND cycle = ? AND status = 'pending'
        """,
            (_now_ts(), pipeline_id, stage_name, stage["cycle"]),
        )
        conn.commit()

        ok, err, _msg_id = send_gate_notification(
            pipeline_id, pipeline["title"], stage_name, pipeline["project_repo"]
        )
        if not ok:
            log.error(
                "Gate notification failed for pipeline %s stage %s: %s",
                pipeline_id,
                stage_name,
                err,
            )

        # Track in approval queue for monitoring/reminders
        try:
            queue_approval(conn, pipeline_id, stage_name)
        except Exception as exc:
            log.debug("Approval queue tracking failed (non-blocking): %s", exc)

        return {
            "status": "waiting_human",
            "stage": stage_name,
            "notification_sent": ok,
            "done": False,
        }

    if stage_name == "implement":
        _seed_implement_wave_state_from_plan(conn, pipeline_id, stage["cycle"])

    conn.execute(
        """
        UPDATE pipeline_stages
        SET status = 'running', started_at = ?
        WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
    """,
        (_now_ts(), pipeline_id, stage_name, stage["cycle"]),
    )

    conn.execute(
        "UPDATE pipeline_runs SET current_stage = ? WHERE pipeline_id = ?",
        (stage_name, pipeline_id),
    )
    conn.commit()

    stage_cfg = dict(cfg or {})
    fleetmax_cfg = (
        stage_cfg.get("fleetmax") if isinstance(stage_cfg.get("fleetmax"), dict) else {}
    )
    if "swarm_waves_enabled" not in stage_cfg and isinstance(fleetmax_cfg, dict):
        stage_cfg["swarm_waves_enabled"] = bool(
            fleetmax_cfg.get("swarm_waves_enabled", False)
        )
    if "swarm_waves_strict" not in stage_cfg and isinstance(fleetmax_cfg, dict):
        stage_cfg["swarm_waves_strict"] = bool(
            fleetmax_cfg.get("swarm_waves_strict", False)
        )
    pipeline_config_json = pipeline.get("config_json")
    if pipeline_config_json:
        try:
            pipeline_cfg = json.loads(pipeline_config_json)
            if isinstance(pipeline_cfg, dict):
                stage_cfg.update(pipeline_cfg)
        except Exception:
            pass

    try:
        result = run_stage(stage_name, pipeline, conn, stage_cfg)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.execute(
            """
            UPDATE pipeline_stages
            SET status = 'failed', error = ?, completed_at = ?
            WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
        """,
            (str(e), _now_ts(), pipeline_id, stage_name, stage["cycle"]),
        )

        conn.execute(
            """
            UPDATE pipeline_runs
            SET status = 'failed', error = ?
            WHERE pipeline_id = ?
        """,
            (str(e), pipeline_id),
        )
        conn.commit()

        return {
            "status": "failed",
            "stage": stage_name,
            "error": str(e),
            "done": True,
        }

    if result.get("waiting_human"):
        ok, err, _msg_id = send_gate_notification(
            pipeline_id, pipeline["title"], stage_name, pipeline["project_repo"]
        )
        if not ok:
            log.error(
                "Gate notification failed for pipeline %s stage %s: %s",
                pipeline_id,
                stage_name,
                err,
            )

        return {
            "status": "waiting_human",
            "stage": stage_name,
            "notification_sent": ok,
            "done": False,
        }

    if result["ok"]:
        if stage_name == "implement":
            strict_wave = bool(stage_cfg.get("swarm_waves_strict", False))
            wave_ok, wave_error = _validate_wave_completion(
                conn,
                pipeline_id,
                stage["cycle"],
                result.get("output"),
                strict=strict_wave,
            )
            if not wave_ok:
                conn.execute(
                    """
                    UPDATE pipeline_stages
                    SET status = 'failed', error = ?, completed_at = ?
                    WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
                """,
                    (wave_error, _now_ts(), pipeline_id, stage_name, stage["cycle"]),
                )
                conn.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = 'failed', error = ?
                    WHERE pipeline_id = ?
                """,
                    (wave_error, pipeline_id),
                )
                conn.commit()
                return {
                    "status": "failed",
                    "stage": stage_name,
                    "error": wave_error,
                    "done": True,
                }

        output_json = json.dumps(result.get("output", {}))
        cost_usd = result.get("cost_usd", 0.0)
        stage_input_json = _prepare_wave_stage_input(
            stage_name, stage, result.get("output")
        )

        if stage_input_json is not None:
            conn.execute(
                """
                UPDATE pipeline_stages
                SET status = 'completed', output_json = ?, input_json = ?, cost_usd = ?, completed_at = ?
                WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
            """,
                (
                    output_json,
                    stage_input_json,
                    cost_usd,
                    _now_ts(),
                    pipeline_id,
                    stage_name,
                    stage["cycle"],
                ),
            )
        else:
            conn.execute(
                """
                UPDATE pipeline_stages
                SET status = 'completed', output_json = ?, cost_usd = ?, completed_at = ?
                WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
            """,
                (
                    output_json,
                    cost_usd,
                    _now_ts(),
                    pipeline_id,
                    stage_name,
                    stage["cycle"],
                ),
            )

        conn.commit()

        _advance_wave_stage_input(
            conn, pipeline_id, stage_name, stage["cycle"], result.get("output")
        )
        _mark_wave_done_if_missing(conn, pipeline_id, stage_name, stage["cycle"])

        # Quality gate check (advisory)
        _run_quality_gate(conn, pipeline_id, stage_name, result.get("output", {}), cfg)

        # Snapshot checkpoint after stage completion
        worktree_rows = conn.execute(
            "SELECT worktree_path FROM worktree_state WHERE pipeline_id = ?",
            (pipeline_id,),
        ).fetchall()
        if worktree_rows and worktree_rows[0]["worktree_path"]:
            snapshot_checkpoint(conn, pipeline_id, worktree_rows[0]["worktree_path"])

        # Handle intake routing
        if stage_name == "intake":
            routing = result.get("output", {}).get("routing", "")
            if routing == "skip_to_research":
                conn.execute(
                    """
                    UPDATE pipeline_stages
                    SET status = 'skipped'
                    WHERE pipeline_id = ? AND stage_name = 'refine' AND status = 'pending'
                """,
                    (pipeline_id,),
                )
                conn.commit()

        proj_cfg = get_project_config(conn, pipeline["project_repo"])
        max_ralph_cycles = proj_cfg.get("max_ralph_cycles", 5)

        # Re-read cycle_count to ensure we have latest value
        current_cycle_row = conn.execute(
            "SELECT cycle_count FROM pipeline_runs WHERE pipeline_id = ?",
            (pipeline_id,),
        ).fetchone()
        current_cycle = (
            (current_cycle_row["cycle_count"] or 0) if current_cycle_row else 0
        )

        if stage_name == "test" and _test_has_failures(result.get("output", {})):
            if current_cycle < max_ralph_cycles:
                new_cycle = _ralph_loop_reset(conn, pipeline_id, "fix", ["fix", "test"])
                return {
                    "status": "running",
                    "stage": "fix",
                    "ralph_loop": True,
                    "cycle": new_cycle,
                    "done": False,
                }

        # Adversarial stage found critical/high issues → trigger ralph loop
        if stage_name == "adversarial":
            output = result.get("output", {})
            issues = output.get("issues", []) if isinstance(output, dict) else []
            if not isinstance(issues, list):
                issues = []
            has_critical = any(
                isinstance(i, dict)
                and str(i.get("severity", "")).lower() in ("critical", "high")
                for i in issues
            )
            if has_critical and current_cycle < max_ralph_cycles:
                # Route through ralph → test → adversarial loop
                new_cycle = _ralph_loop_reset(
                    conn, pipeline_id, "ralph", ["ralph", "test", "adversarial"]
                )
                return {
                    "status": "running",
                    "stage": "ralph",
                    "ralph_loop": True,
                    "cycle": new_cycle,
                    "done": False,
                }

        # RALPH stage has remaining issues or failing tests → loop back
        if stage_name == "ralph":
            output = result.get("output", {})
            remaining = (
                output.get("remaining_issues", []) if isinstance(output, dict) else []
            )
            if not isinstance(remaining, list):
                remaining = []
            tests_failing_raw = (
                output.get("tests_failing") if isinstance(output, dict) else 0
            )
            try:
                tests_failing = (
                    int(float(tests_failing_raw))
                    if tests_failing_raw is not None
                    else 0
                )
            except (TypeError, ValueError):
                tests_failing = 0
            if (remaining or tests_failing > 0) and current_cycle < max_ralph_cycles:
                new_cycle = _ralph_loop_reset(
                    conn, pipeline_id, "ralph", ["ralph", "test"]
                )
                return {
                    "status": "running",
                    "stage": "ralph",
                    "ralph_loop": True,
                    "cycle": new_cycle,
                    "done": False,
                }

        if stage_name == "review":
            output = result.get("output", {})
            needs_fix = False
            if isinstance(output, dict):
                verdict = str(output.get("verdict", "APPROVE")).upper()
                issues = output.get("issues", [])
                has_issues = isinstance(issues, list) and len(issues) > 0
                needs_fix = bool(
                    output.get("needs_fix")
                    or verdict == "REQUEST_CHANGES"
                    or has_issues
                )
            if needs_fix and current_cycle < max_ralph_cycles:
                new_cycle = _ralph_loop_reset(
                    conn,
                    pipeline_id,
                    "implement",
                    ["implement", "test", "fix", "review"],
                )
                return {
                    "status": "running",
                    "stage": "implement",
                    "ralph_loop": True,
                    "cycle": new_cycle,
                    "done": False,
                }

        return {
            "status": "running",
            "stage": stage_name,
            "result": result,
            "done": False,
        }
    else:
        error = result.get("error", "stage execution failed")
        conn.execute(
            """
            UPDATE pipeline_stages
            SET status = 'failed', error = ?, completed_at = ?
            WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
        """,
            (error, _now_ts(), pipeline_id, stage_name, stage["cycle"]),
        )

        conn.execute(
            """
            UPDATE pipeline_runs
            SET status = 'failed', error = ?
            WHERE pipeline_id = ?
        """,
            (error, pipeline_id),
        )
        conn.commit()

        return {
            "status": "failed",
            "stage": stage_name,
            "error": error,
            "done": True,
        }


def approve_gate(
    conn: sqlite3.Connection, pipeline_id: str, human_input: str = ""
) -> dict:
    """Unblock a pipeline waiting at a human gate. Returns {ok, stage, error}."""
    conn.row_factory = sqlite3.Row

    # Verify pipeline exists and is in a valid state
    pipeline_row = conn.execute(
        "SELECT status FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()

    if not pipeline_row:
        return {"ok": False, "error": "pipeline not found"}

    if pipeline_row["status"] not in ("created", "running"):
        return {"ok": False, "error": f"pipeline already {pipeline_row['status']}"}

    stage_row = conn.execute(
        """
        SELECT * FROM pipeline_stages
        WHERE pipeline_id = ? AND status = 'waiting_human'
        ORDER BY stage_order ASC
        LIMIT 1
    """,
        (pipeline_id,),
    ).fetchone()

    if not stage_row:
        return {"ok": False, "error": "no stage waiting for approval"}

    stage = dict(stage_row)
    stage_name = stage["stage_name"]

    result = conn.execute(
        """
        UPDATE pipeline_stages
        SET human_input = ?, status = 'pending'
        WHERE pipeline_id = ? AND stage_name = ? AND cycle = ? AND status = 'waiting_human'
    """,
        (human_input or "approved", pipeline_id, stage_name, stage["cycle"]),
    )

    if result.rowcount == 0:
        return {
            "ok": False,
            "error": "stage no longer waiting (concurrent approval/rejection)",
        }

    conn.commit()

    # Resolve in approval queue if tracked
    try:
        resolve_approval(conn, pipeline_id, f"approved: {human_input or ''}")
    except Exception as exc:
        log.debug("Approval queue resolution failed (non-blocking): %s", exc)

    log.info("Gate approved for pipeline %s stage %s", pipeline_id, stage_name)
    return {"ok": True, "stage": stage_name}


def reject_gate(conn: sqlite3.Connection, pipeline_id: str, reason: str = "") -> dict:
    """Reject a pipeline at a human gate, cancelling it. Returns {ok, stage, error}."""
    conn.row_factory = sqlite3.Row

    pipeline_row = conn.execute(
        "SELECT status, title FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()

    if not pipeline_row:
        return {"ok": False, "error": "pipeline not found"}

    if pipeline_row["status"] not in ("created", "running"):
        return {"ok": False, "error": f"pipeline already {pipeline_row['status']}"}

    stage_row = conn.execute(
        """
        SELECT * FROM pipeline_stages
        WHERE pipeline_id = ? AND status = 'waiting_human'
        ORDER BY stage_order ASC
        LIMIT 1
    """,
        (pipeline_id,),
    ).fetchone()

    if not stage_row:
        return {"ok": False, "error": "no stage waiting for rejection"}

    stage = dict(stage_row)
    stage_name = stage["stage_name"]
    reject_msg = reason or "rejected by human"

    conn.execute(
        """
        UPDATE pipeline_stages
        SET status = 'failed', error = ?, completed_at = ?
        WHERE pipeline_id = ? AND stage_name = ? AND cycle = ? AND status = 'waiting_human'
    """,
        (reject_msg, _now_ts(), pipeline_id, stage_name, stage["cycle"]),
    )

    conn.execute(
        """
        UPDATE pipeline_runs
        SET status = 'failed', error = ?, completed_at = ?
        WHERE pipeline_id = ?
    """,
        (f"Rejected at gate {stage_name}: {reject_msg}", _now_ts(), pipeline_id),
    )

    # Skip remaining stages
    conn.execute(
        """
        UPDATE pipeline_stages
        SET status = 'skipped'
        WHERE pipeline_id = ? AND status = 'pending'
    """,
        (pipeline_id,),
    )

    # Release any assigned worktrees
    worktree_rows = conn.execute(
        "SELECT worktree_name FROM worktree_state WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchall()

    for row in worktree_rows:
        release_worktree(conn, row["worktree_name"])

    conn.commit()

    send_pipeline_notification(
        pipeline_id,
        pipeline_row["title"],
        "failed",
        f"Rejected at {stage_name}: {reject_msg}",
    )
    log.info(
        "Gate rejected for pipeline %s stage %s: %s",
        pipeline_id,
        stage_name,
        reject_msg,
    )

    return {"ok": True, "stage": stage_name}


def cancel_pipeline(conn: sqlite3.Connection, pipeline_id: str) -> dict:
    """Cancel a pipeline, clean up worktrees. Returns {ok, error}."""
    conn.row_factory = sqlite3.Row

    pipeline_row = conn.execute(
        "SELECT * FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()

    if not pipeline_row:
        return {"ok": False, "error": "pipeline not found"}

    pipeline = dict(pipeline_row)

    if pipeline["status"] in ("completed", "cancelled", "failed"):
        return {"ok": False, "error": f"pipeline already {pipeline['status']}"}

    conn.execute(
        "UPDATE pipeline_runs SET status = 'cancelled', completed_at = ? WHERE pipeline_id = ?",
        (_now_ts(), pipeline_id),
    )

    conn.execute(
        """
        UPDATE pipeline_stages
        SET status = 'skipped'
        WHERE pipeline_id = ? AND status IN ('pending', 'running', 'waiting_human')
    """,
        (pipeline_id,),
    )

    worktree_rows = conn.execute(
        "SELECT worktree_name FROM worktree_state WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchall()

    for row in worktree_rows:
        release_worktree(conn, row["worktree_name"])

    conn.commit()

    return {"ok": True}


def get_pipeline(conn: sqlite3.Connection, pipeline_id: str) -> dict | None:
    """Get full pipeline state including stages."""
    conn.row_factory = sqlite3.Row

    pipeline_row = conn.execute(
        "SELECT * FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()

    if not pipeline_row:
        return None

    pipeline = dict(pipeline_row)

    stage_rows = conn.execute(
        """
        SELECT * FROM pipeline_stages
        WHERE pipeline_id = ?
        ORDER BY stage_order ASC, cycle ASC
    """,
        (pipeline_id,),
    ).fetchall()

    pipeline["stages"] = [dict(row) for row in stage_rows]

    return pipeline


def list_pipelines(
    conn: sqlite3.Connection, status: str = "", limit: int = 20
) -> list[dict]:
    """List recent pipelines, optionally filtered by status."""
    conn.row_factory = sqlite3.Row

    if status:
        rows = conn.execute(
            """
            SELECT * FROM pipeline_runs
            WHERE status = ?
            ORDER BY created_at DESC
            LIMIT ?
        """,
            (status, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT * FROM pipeline_runs
            ORDER BY created_at DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()

    return [dict(row) for row in rows]


def run_pipeline_to_completion(
    conn: sqlite3.Connection,
    pipeline_id: str,
    cfg: dict,
    timeout_seconds: int = 7200,
) -> dict:
    """Run pipeline stages sequentially until done or blocked at human gate.
    Times out after timeout_seconds (default 2hr). Returns final pipeline state."""
    max_iterations = 100
    iterations = 0
    last_stage = None
    stuck_count = 0
    wall_clock_start = time.time()

    # Fetch pipeline title once for notifications
    conn.row_factory = sqlite3.Row
    title_row = conn.execute(
        "SELECT title FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()
    pipeline_title = title_row["title"] if title_row else pipeline_id

    while iterations < max_iterations:
        # Wall-clock timeout check
        elapsed = time.time() - wall_clock_start
        if elapsed > timeout_seconds:
            error_msg = (
                f"pipeline timeout after {int(elapsed)}s (limit {timeout_seconds}s)"
            )
            conn.execute(
                "UPDATE pipeline_runs SET status = 'failed', error = ? WHERE pipeline_id = ?",
                (error_msg, pipeline_id),
            )
            conn.commit()
            send_pipeline_notification(pipeline_id, pipeline_title, "failed", error_msg)
            log.warning("Pipeline %s timed out after %ds", pipeline_id, int(elapsed))
            return get_pipeline(conn, pipeline_id) or {"error": error_msg, "done": True}
        result = advance_pipeline(conn, pipeline_id, cfg)

        if result.get("done"):
            status = result.get("status", "unknown")
            if status == "completed":
                send_pipeline_notification(pipeline_id, pipeline_title, "completed")
            elif status == "failed":
                send_pipeline_notification(
                    pipeline_id, pipeline_title, "failed", result.get("error", "")
                )
            # Post-pipeline self-reflection
            if cfg.get("reflection", {}).get("enabled", True):
                try:
                    from coordinator.reflect import post_run_reflect

                    pipeline_data = get_pipeline(conn, pipeline_id)
                    post_run_reflect(
                        conn=conn,
                        cfg=cfg,
                        run_id=pipeline_id,
                        repo=pipeline_data.get("repo", "") if pipeline_data else "",
                        objective=pipeline_title,
                        status=status,
                        via="pipeline",
                        tier="pipeline",
                        model="mixed",
                        duration=time.time() - wall_clock_start,
                        cost=0.0,
                        output_path="",
                        pipeline_id=pipeline_id,
                    )
                except Exception as exc:
                    log.warning("Post-pipeline reflection failed: %s", exc)
            return get_pipeline(conn, pipeline_id) or result

        if result.get("status") == "waiting_human":
            return get_pipeline(conn, pipeline_id) or result

        current_stage = result.get("stage")
        is_ralph_loop = result.get("ralph_loop", False)
        if current_stage == last_stage and not is_ralph_loop:
            stuck_count += 1
            if stuck_count > 10:
                error_msg = (
                    f"stuck on stage {current_stage} for {stuck_count} iterations"
                )
                conn.execute(
                    "UPDATE pipeline_runs SET status = 'failed', error = ? WHERE pipeline_id = ?",
                    (error_msg, pipeline_id),
                )
                conn.commit()
                send_pipeline_notification(
                    pipeline_id, pipeline_title, "failed", error_msg
                )
                return {"error": error_msg, "done": True}
        else:
            stuck_count = 0
        last_stage = current_stage

        iterations += 1
        time.sleep(1.0)

    error_msg = "max iterations exceeded"
    conn.execute(
        "UPDATE pipeline_runs SET status = 'failed', error = ? WHERE pipeline_id = ?",
        (error_msg, pipeline_id),
    )
    conn.commit()
    send_pipeline_notification(pipeline_id, pipeline_title, "failed", error_msg)
    return {"error": error_msg, "done": True}


def _ralph_loop_reset(
    conn: sqlite3.Connection, pipeline_id: str, from_stage: str, stages: list[str]
) -> int:
    """Reset specified stages for a new RALPH cycle. Returns new cycle number.

    Stages are inserted in the order given by ``stages`` (the desired execution
    order). ``from_stage`` must be the first element of ``stages`` and is used
    only for logging/clarity. Stage_order values are assigned sequentially
    starting from a high offset (1000 + cycle * 100) to ensure they sort after
    the original pipeline stages when queried by ``stage_order ASC``.

    Thread safety: SQLite serializes writes at the database level — concurrent
    callers will queue behind the write lock (using busy_timeout). The MAX →
    INSERT → UPDATE sequence is atomic within a single commit because Python's
    sqlite3 module groups DML statements into an implicit transaction.
    """
    conn.row_factory = sqlite3.Row

    max_cycle_row = conn.execute(
        """
        SELECT MAX(cycle) as max_cycle FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name IN ({})
    """.format(",".join("?" * len(stages))),
        (pipeline_id, *stages),
    ).fetchone()

    current_max_cycle = (
        max_cycle_row["max_cycle"]
        if max_cycle_row and max_cycle_row["max_cycle"]
        else 1
    )
    new_cycle = current_max_cycle + 1

    # Use high stage_order offset so RALPH cycle stages sort after original stages
    base_order = 1000 + new_cycle * 100

    for offset, stage_name in enumerate(stages):
        conn.execute(
            """
            INSERT INTO pipeline_stages
            (pipeline_id, stage_name, stage_order, cycle, status)
            VALUES (?, ?, ?, ?, 'pending')
        """,
            (pipeline_id, stage_name, base_order + offset, new_cycle),
        )

    conn.execute(
        "UPDATE pipeline_runs SET cycle_count = ? WHERE pipeline_id = ?",
        (new_cycle, pipeline_id),
    )
    conn.commit()

    return new_cycle


def get_pipeline_checkpoint(conn: sqlite3.Connection, pipeline_id: str) -> dict | None:
    """Get a snapshot of the pipeline's progress for resumption.
    Returns checkpoint dict with completed stages, current position, and accumulated cost."""
    conn.row_factory = sqlite3.Row

    pipeline_row = conn.execute(
        "SELECT * FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()

    if not pipeline_row:
        return None

    pipeline = dict(pipeline_row)

    stage_rows = conn.execute(
        """
        SELECT stage_name, status, cycle, cost_usd, completed_at
        FROM pipeline_stages
        WHERE pipeline_id = ?
        ORDER BY stage_order ASC, cycle ASC
    """,
        (pipeline_id,),
    ).fetchall()

    completed = []
    current = None
    pending = []

    for row in stage_rows:
        s = dict(row)
        if s["status"] == "completed":
            completed.append(s)
        elif s["status"] in ("pending", "waiting_human", "running"):
            if current is None:
                current = s
            else:
                pending.append(s)
        elif s["status"] == "failed":
            current = s
            break

    total_cost = sum(s.get("cost_usd", 0) or 0 for s in completed)

    return {
        "pipeline_id": pipeline_id,
        "status": pipeline["status"],
        "completed_stages": [s["stage_name"] for s in completed],
        "completed_count": len(completed),
        "current_stage": current["stage_name"] if current else None,
        "current_status": current["status"] if current else None,
        "pending_count": len(pending),
        "cycle_count": pipeline.get("cycle_count", 0),
        "total_cost_usd": round(total_cost, 4),
        "created_at": pipeline["created_at"],
        "elapsed_seconds": _now_ts() - pipeline["created_at"],
    }


def resume_pipeline(conn: sqlite3.Connection, pipeline_id: str, cfg: dict) -> dict:
    """Resume a pipeline from its last checkpoint.
    Handles stuck/failed pipelines by resetting the current stage to pending.
    Attempts to restore worktree to checkpoint commit if available."""
    conn.row_factory = sqlite3.Row

    pipeline_row = conn.execute(
        "SELECT * FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()

    if not pipeline_row:
        return {"ok": False, "error": "pipeline not found"}

    pipeline = dict(pipeline_row)

    if pipeline["status"] in ("completed", "cancelled"):
        return {"ok": False, "error": f"pipeline already {pipeline['status']}"}

    # Look up latest checkpoint
    checkpoint_row = conn.execute(
        """
        SELECT * FROM pipeline_checkpoints
        WHERE pipeline_id = ?
        ORDER BY snapshot_at DESC
        LIMIT 1
    """,
        (pipeline_id,),
    ).fetchone()

    checkpoint_info = None
    if checkpoint_row:
        checkpoint = dict(checkpoint_row)
        checkpoint_info = {
            "stage_name": checkpoint["stage_name"],
            "cycle": checkpoint["cycle"],
            "commit_hash": checkpoint["commit_hash"],
            "snapshot_at": checkpoint["snapshot_at"],
        }

        # Attempt to restore worktree to checkpoint commit
        worktree_row = conn.execute(
            "SELECT worktree_path FROM worktree_state WHERE pipeline_id = ?",
            (pipeline_id,),
        ).fetchone()

        if worktree_row and checkpoint["commit_hash"]:
            worktree_path = worktree_row["worktree_path"]
            if os.path.exists(worktree_path):
                try:
                    checkout_result = subprocess.run(
                        ["git", "checkout", checkpoint["commit_hash"]],
                        cwd=worktree_path,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    if checkout_result.returncode == 0:
                        log.info(
                            "Restored worktree to checkpoint commit %s for pipeline %s",
                            checkpoint["commit_hash"][:8],
                            pipeline_id,
                        )
                        checkpoint_info["restored"] = True
                    else:
                        log.warning(
                            "Failed to restore worktree for pipeline %s: %s",
                            pipeline_id,
                            checkout_result.stderr,
                        )
                        checkpoint_info["restored"] = False
                        checkpoint_info["restore_error"] = checkout_result.stderr
                except Exception as e:
                    log.warning(
                        "Exception restoring worktree for pipeline %s: %s",
                        pipeline_id,
                        str(e),
                    )
                    checkpoint_info["restored"] = False
                    checkpoint_info["restore_error"] = str(e)

    # If pipeline is failed, reset it to running
    if pipeline["status"] == "failed":
        conn.execute(
            "UPDATE pipeline_runs SET status = 'running', error = NULL WHERE pipeline_id = ?",
            (pipeline_id,),
        )

    # Find the first failed or running stage and reset to pending
    stuck_row = conn.execute(
        """
        SELECT stage_name, cycle FROM pipeline_stages
        WHERE pipeline_id = ? AND status IN ('failed', 'running')
        ORDER BY stage_order ASC
        LIMIT 1
    """,
        (pipeline_id,),
    ).fetchone()

    if stuck_row:
        conn.execute(
            """
            UPDATE pipeline_stages
            SET status = 'pending', error = NULL, started_at = NULL, completed_at = NULL
            WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
        """,
            (pipeline_id, stuck_row["stage_name"], stuck_row["cycle"]),
        )
        log.info(
            "Reset stage %s cycle %d to pending for pipeline %s",
            stuck_row["stage_name"],
            stuck_row["cycle"],
            pipeline_id,
        )

    conn.commit()

    # Now advance
    result = advance_pipeline(conn, pipeline_id, cfg)
    return {"ok": True, "advance_result": result, "checkpoint": checkpoint_info}


def skip_stage(conn: sqlite3.Connection, pipeline_id: str, stage_name: str) -> dict:
    """Mark a failed stage as skipped so pipeline can continue past it.
    Returns {ok, error}."""
    conn.row_factory = sqlite3.Row

    # Find the failed stage
    stage_row = conn.execute(
        """
        SELECT * FROM pipeline_stages
        WHERE pipeline_id = ? AND stage_name = ? AND status = 'failed'
        ORDER BY cycle DESC
        LIMIT 1
    """,
        (pipeline_id, stage_name),
    ).fetchone()

    if not stage_row:
        return {"ok": False, "error": f"no failed stage named {stage_name} found"}

    # Mark as skipped
    conn.execute(
        """
        UPDATE pipeline_stages
        SET status = 'skipped'
        WHERE pipeline_id = ? AND stage_name = ? AND cycle = ?
    """,
        (pipeline_id, stage_name, stage_row["cycle"]),
    )

    # If pipeline is failed, reset to running
    conn.execute(
        """
        UPDATE pipeline_runs
        SET status = 'running', error = NULL
        WHERE pipeline_id = ? AND status = 'failed'
    """,
        (pipeline_id,),
    )

    conn.commit()

    log.info("Skipped failed stage %s for pipeline %s", stage_name, pipeline_id)
    return {"ok": True}


def get_pipeline_progress(conn: sqlite3.Connection, pipeline_id: str) -> dict:
    """Get pipeline progress summary.
    Returns {total_stages, completed, failed, pending, waiting_human, current_stage, pct_complete}."""
    conn.row_factory = sqlite3.Row

    pipeline_row = conn.execute(
        "SELECT current_stage FROM pipeline_runs WHERE pipeline_id = ?", (pipeline_id,)
    ).fetchone()

    if not pipeline_row:
        return {"error": "pipeline not found"}

    stage_rows = conn.execute(
        """
        SELECT status FROM pipeline_stages
        WHERE pipeline_id = ?
    """,
        (pipeline_id,),
    ).fetchall()

    if not stage_rows:
        return {"error": "no stages found"}

    total = len(stage_rows)
    completed = sum(1 for row in stage_rows if row["status"] == "completed")
    failed = sum(1 for row in stage_rows if row["status"] == "failed")
    pending = sum(1 for row in stage_rows if row["status"] == "pending")
    waiting_human = sum(1 for row in stage_rows if row["status"] == "waiting_human")

    pct_complete = round((completed / total * 100), 1) if total > 0 else 0

    return {
        "total_stages": total,
        "completed": completed,
        "failed": failed,
        "pending": pending,
        "waiting_human": waiting_human,
        "current_stage": pipeline_row["current_stage"],
        "pct_complete": pct_complete,
    }


def _test_has_failures(stage_output: dict) -> bool:
    """Check if test stage output indicates failures."""
    if not stage_output:
        return False

    tests_failed_raw = stage_output.get("tests_failed", 0)
    try:
        tests_failed = (
            int(float(tests_failed_raw)) if tests_failed_raw is not None else 0
        )
    except (TypeError, ValueError):
        tests_failed = 0
    if tests_failed > 0:
        return True

    failures = stage_output.get("failures", [])
    if isinstance(failures, list) and len(failures) > 0:
        return True

    raw = stage_output.get("raw", "")
    if not isinstance(raw, str):
        raw = str(raw)
    match = re.search(r"\b(\d+)\s+(failed|failure)", raw.lower())
    if match and int(match.group(1)) > 0:
        return True
    if re.search(r"(FAIL|ERROR)[:\s]", raw, re.IGNORECASE):
        return True

    return False
