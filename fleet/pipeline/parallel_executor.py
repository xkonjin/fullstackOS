"""Parallel stage execution — Loop 15

Executes independent pipeline stages concurrently.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import sqlite3
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set

log = logging.getLogger("aifleet.parallel")


class StageDependency:
    """Defines dependencies between stages."""

    # Define which stages can run in parallel
    INDEPENDENT_STAGES: Set[str] = {"research", "intake", "refine"}

    # Stages that must run sequentially
    SEQUENTIAL_STAGES: List[str] = [
        "spec",
        "plan",
        "issues",
        "implement",
        "test",
        "fix",
        "review",
    ]

    @classmethod
    def can_run_parallel(cls, stage1: str, stage2: str) -> bool:
        """Check if two stages can run in parallel."""
        # Both must be in independent set
        return stage1 in cls.INDEPENDENT_STAGES and stage2 in cls.INDEPENDENT_STAGES

    @classmethod
    def get_parallel_groups(cls, stages: List[str]) -> List[List[str]]:
        """Group stages into parallelizable batches."""
        groups = []
        current_group = []

        for stage in stages:
            if stage in cls.INDEPENDENT_STAGES:
                current_group.append(stage)
            else:
                # Flush current group if any
                if current_group:
                    groups.append(current_group)
                    current_group = []
                # Add sequential stage as its own group
                groups.append([stage])

        if current_group:
            groups.append(current_group)

        return groups


@dataclass
class ParallelStageResult:
    """Result from a parallel stage execution."""

    stage_name: str
    success: bool
    output: Any
    error: Optional[str] = None
    duration_seconds: float = 0.0


class StageTimeoutError(Exception):
    """Raised when a parallel stage exceeds its timeout."""

    pass


class ParallelExecutor:
    """Executes pipeline stages in parallel where possible."""

    def __init__(self, max_workers: int = 3, stage_timeout: Optional[float] = 600.0):
        self.max_workers = max_workers
        self.stage_timeout = stage_timeout  # seconds; None = no timeout
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

    def __enter__(self):
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None

    def execute_parallel(
        self, stages: List[str], stage_fn: Callable[[str], Any]
    ) -> Dict[str, ParallelStageResult]:
        """Execute stages in parallel where possible.

        Args:
            stages: List of stage names to execute
            stage_fn: Function to execute for each stage

        Returns:
            Dict mapping stage names to their results
        """
        if not self._executor:
            raise RuntimeError("Executor not initialized, use as context manager")

        # Group stages into parallel batches
        groups = StageDependency.get_parallel_groups(stages)
        results: Dict[str, ParallelStageResult] = {}

        for group in groups:
            if len(group) == 1:
                # Sequential execution
                stage = group[0]
                try:
                    import time

                    start = time.time()
                    output = stage_fn(stage)
                    duration = time.time() - start
                    results[stage] = ParallelStageResult(
                        stage_name=stage,
                        success=True,
                        output=output,
                        duration_seconds=duration,
                    )
                except Exception as e:
                    results[stage] = ParallelStageResult(
                        stage_name=stage, success=False, output=None, error=str(e)
                    )
            else:
                # Parallel execution -- wrap each stage_fn call to capture
                # wall-clock duration so callers get observability parity
                # with the sequential path.
                import time as _time

                def _timed_stage(stage_name: str):
                    t0 = _time.monotonic()
                    result = stage_fn(stage_name)
                    return result, _time.monotonic() - t0

                futures = {
                    self._executor.submit(_timed_stage, stage): (
                        _time.monotonic(),
                        stage,
                    )
                    for stage in group
                }

                done, not_done = concurrent.futures.wait(
                    futures.keys(),
                    timeout=self.stage_timeout,
                )

                # Cancel any futures that didn't finish in time
                for future in not_done:
                    future.cancel()
                    submit_time, stage = futures[future]
                    elapsed = _time.monotonic() - submit_time
                    log.warning(
                        "Stage %s timed out after %.1fs (limit=%.1fs)",
                        stage,
                        elapsed,
                        self.stage_timeout or 0,
                    )
                    results[stage] = ParallelStageResult(
                        stage_name=stage,
                        success=False,
                        output=None,
                        error=f"Stage timed out after {elapsed:.1f}s",
                        duration_seconds=elapsed,
                    )

                for future in done:
                    _submit_time, stage = futures[future]
                    try:
                        output, elapsed = future.result()
                        results[stage] = ParallelStageResult(
                            stage_name=stage,
                            success=True,
                            output=output,
                            duration_seconds=elapsed,
                        )
                    except Exception as e:
                        elapsed = _time.monotonic() - _submit_time
                        results[stage] = ParallelStageResult(
                            stage_name=stage,
                            success=False,
                            output=None,
                            error=str(e),
                            duration_seconds=elapsed,
                        )

                # Log parallel group summary for observability
                group_results = {s: results[s] for s in group if s in results}
                ok_count = sum(1 for r in group_results.values() if r.success)
                durations = {
                    s: f"{r.duration_seconds:.2f}s"
                    for s, r in group_results.items()
                    if r.duration_seconds > 0
                }
                log.info(
                    "Parallel group completed: %d/%d ok, durations=%s",
                    ok_count,
                    len(group),
                    durations,
                )

        return results

    async def execute_parallel_async(
        self, stages: List[str], stage_fn: Callable[[str], asyncio.Future]
    ) -> Dict[str, ParallelStageResult]:
        """Async version of parallel execution."""
        groups = StageDependency.get_parallel_groups(stages)
        results: Dict[str, ParallelStageResult] = {}

        for group in groups:
            if len(group) == 1:
                stage = group[0]
                try:
                    import time

                    start = time.time()
                    output = await stage_fn(stage)
                    duration = time.time() - start
                    results[stage] = ParallelStageResult(
                        stage_name=stage,
                        success=True,
                        output=output,
                        duration_seconds=duration,
                    )
                except Exception as e:
                    results[stage] = ParallelStageResult(
                        stage_name=stage, success=False, output=None, error=str(e)
                    )
            else:
                # Parallel async execution -- timed wrapper for observability
                import time as _time

                async def _timed_async(stage_name: str):
                    t0 = _time.monotonic()
                    res = await stage_fn(stage_name)
                    return res, _time.monotonic() - t0

                tasks = [_timed_async(stage) for stage in group]
                completed = await asyncio.gather(*tasks, return_exceptions=True)

                for stage, result in zip(group, completed):
                    if isinstance(result, Exception):
                        results[stage] = ParallelStageResult(
                            stage_name=stage,
                            success=False,
                            output=None,
                            error=str(result),
                        )
                    else:
                        output, elapsed = result
                        results[stage] = ParallelStageResult(
                            stage_name=stage,
                            success=True,
                            output=output,
                            duration_seconds=elapsed,
                        )

        return results


def execute_stages_parallel(
    stages: List[str],
    stage_fn: Callable[[str], Any],
    max_workers: int = 3,
    stage_timeout: Optional[float] = 600.0,
) -> Dict[str, ParallelStageResult]:
    """Convenience function for parallel stage execution.

    Usage:
        results = execute_stages_parallel(
            ['research', 'intake', 'refine'],
            lambda stage: run_stage(stage, pipeline, conn, cfg),
            stage_timeout=300.0,
        )
    """
    with ParallelExecutor(
        max_workers=max_workers, stage_timeout=stage_timeout
    ) as executor:
        return executor.execute_parallel(stages, stage_fn)


class StageScheduler:
    """Schedules stages for optimal parallel execution."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def estimate_stage_duration(self, stage_name: str) -> float:
        """Estimate how long a stage will take based on history."""
        row = self.conn.execute(
            """
            SELECT AVG(duration_seconds) FROM run_history
            WHERE objective LIKE ? AND duration_seconds IS NOT NULL
        """,
            (f"%{stage_name}%",),
        ).fetchone()

        # Default estimates
        defaults = {
            "intake": 30,
            "refine": 60,
            "research": 120,
            "spec": 180,
            "plan": 120,
            "implement": 300,
            "test": 120,
            "fix": 180,
            "review": 120,
        }

        if row and row[0]:
            return max(row[0], defaults.get(stage_name, 60))
        return defaults.get(stage_name, 60)

    def optimize_schedule(self, stages: List[str]) -> List[List[str]]:
        """Optimize stage ordering for parallel execution.

        Returns groups of stages that can run in parallel.
        """
        # Get parallel groups
        groups = StageDependency.get_parallel_groups(stages)

        # Within independent groups, order by estimated duration (longest first)
        # This helps minimize total execution time
        for i, group in enumerate(groups):
            if len(group) > 1:
                groups[i] = sorted(
                    group, key=lambda s: self.estimate_stage_duration(s), reverse=True
                )

        return groups
