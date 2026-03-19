"""Pipeline Observability — stage timing, cost aggregation, and telemetry.

Provides structured telemetry for pipeline runs without external dependencies.
All data is derived from the pipeline_stages SQLite table.
"""

from __future__ import annotations

import json
import logging
import sqlite3

log = logging.getLogger("aifleet.observability")


def collect_pipeline_metrics(
    conn: sqlite3.Connection,
    pipeline_id: str,
) -> dict:
    """Collect aggregated metrics for a pipeline run.

    Returns:
        {
            "pipeline_id": str,
            "total_stages": int,
            "completed": int,
            "failed": int,
            "pending": int,
            "total_cost_usd": float,
            "total_duration_seconds": float,
            "stage_metrics": [...],
            "cost_by_stage_type": {...},
            "slowest_stage": str | None,
        }
    """
    rows = conn.execute(
        """SELECT stage_name, status, cost_usd, started_at, completed_at, output_json
           FROM pipeline_stages
           WHERE pipeline_id = ?
           ORDER BY stage_order ASC""",
        (pipeline_id,),
    ).fetchall()

    stage_metrics: list[dict] = []
    total_cost = 0.0
    total_duration = 0.0
    completed = 0
    failed = 0
    pending = 0
    cost_by_type: dict[str, float] = {}
    slowest_stage: str | None = None
    slowest_duration = 0.0

    for row in rows:
        if isinstance(row, tuple):
            name, status, cost, started, finished, output_json = row
        else:
            name = row["stage_name"]
            status = row["status"]
            cost = row["cost_usd"]
            started = row["started_at"]
            finished = row["completed_at"]
            output_json = row["output_json"]

        cost_val = float(cost or 0)
        total_cost += cost_val

        duration = 0.0
        if started and finished:
            duration = max(0, float(finished) - float(started))
            total_duration += duration

        if status == "completed":
            completed += 1
        elif status == "failed":
            failed += 1
        else:
            pending += 1

        cost_by_type[name] = cost_by_type.get(name, 0) + cost_val

        if duration > slowest_duration:
            slowest_duration = duration
            slowest_stage = name

        # Extract summary from output if available
        output_summary = _extract_output_summary(output_json)

        stage_metrics.append({
            "stage": name,
            "status": status,
            "cost_usd": round(cost_val, 6),
            "duration_seconds": round(duration, 2),
            "output_summary": output_summary,
        })

    return {
        "pipeline_id": pipeline_id,
        "total_stages": len(rows),
        "completed": completed,
        "failed": failed,
        "pending": pending,
        "total_cost_usd": round(total_cost, 4),
        "total_duration_seconds": round(total_duration, 1),
        "stage_metrics": stage_metrics,
        "cost_by_stage_type": {k: round(v, 6) for k, v in cost_by_type.items()},
        "slowest_stage": slowest_stage,
        "slowest_duration_seconds": round(slowest_duration, 2),
    }


def record_stage_timing(
    conn: sqlite3.Connection,
    pipeline_id: str,
    stage_name: str,
    started_at: float | None = None,
    completed_at: float | None = None,
) -> None:
    """Record or update timing for a pipeline stage."""
    updates = []
    params: list = []

    if started_at is not None:
        updates.append("started_at = ?")
        params.append(int(started_at))
    if completed_at is not None:
        updates.append("completed_at = ?")
        params.append(int(completed_at))

    if not updates:
        return

    params.extend([pipeline_id, stage_name])
    conn.execute(
        f"UPDATE pipeline_stages SET {', '.join(updates)} WHERE pipeline_id = ? AND stage_name = ?",
        params,
    )
    conn.commit()


def log_stage_result(
    stage_name: str,
    result: dict,
    duration_seconds: float = 0.0,
) -> None:
    """Log a structured stage result for observability."""
    ok = result.get("ok", False)
    cost = result.get("cost_usd", 0.0)
    error = result.get("error")

    if ok:
        log.info(
            "[pipeline] stage=%s status=ok cost=$%.4f duration=%.1fs",
            stage_name, cost, duration_seconds,
        )
    else:
        log.warning(
            "[pipeline] stage=%s status=failed error=%s cost=$%.4f duration=%.1fs",
            stage_name, error, cost, duration_seconds,
        )


def format_pipeline_summary(metrics: dict) -> str:
    """Format pipeline metrics as a human-readable summary string."""
    lines = [
        f"Pipeline {metrics['pipeline_id']}:",
        f"  Stages: {metrics['completed']}/{metrics['total_stages']} completed, "
        f"{metrics['failed']} failed, {metrics['pending']} pending",
        f"  Cost: ${metrics['total_cost_usd']:.4f}",
        f"  Duration: {metrics['total_duration_seconds']:.0f}s",
    ]
    if metrics.get("slowest_stage"):
        lines.append(
            f"  Slowest: {metrics['slowest_stage']} ({metrics['slowest_duration_seconds']:.0f}s)"
        )
    if metrics.get("failed") and metrics.get("stage_metrics"):
        failed_names = [
            s["stage"] for s in metrics["stage_metrics"] if s["status"] == "failed"
        ]
        if failed_names:
            lines.append(f"  Failed: {', '.join(failed_names)}")
    return "\n".join(lines)


def _extract_output_summary(output_json: str | None) -> dict | None:
    """Extract a compact summary from a stage's output_json."""
    if not output_json:
        return None
    try:
        output = json.loads(output_json)
    except Exception:
        return None

    if not isinstance(output, dict):
        return None

    summary = output.get("summary")
    if isinstance(summary, dict):
        return summary

    # For task-producing stages, count tasks
    tasks = output.get("tasks")
    if isinstance(tasks, list):
        return {"task_count": len(tasks)}

    # For query-producing stages, count queries
    queries = output.get("queries")
    if isinstance(queries, list):
        return {"query_count": len(queries)}

    return None
