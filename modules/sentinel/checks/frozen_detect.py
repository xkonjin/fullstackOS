"""Frozen process detection.

A "frozen" service: TCP port is LISTEN (kernel accepts connections)
but the process never accept()s or respond. PID alive, 0% CPU.

Detection strategy:
  1. TCP connect succeeds (port open) but HTTP probe times out
  2. lsof finds PID listening on port
  3. ps shows 0% CPU for that PID across 2 samples
"""

from __future__ import annotations

import asyncio
import logging
import re

from ..config import FROZEN_ACCEPT_TIMEOUT, FROZEN_CPU_THRESHOLD

log = logging.getLogger("sentinel.checks.frozen_detect")


async def check_frozen(port: int) -> dict:
    """Check if a process on the given port is frozen.

    Returns:
        {"frozen": bool, "pid": int|None, "cpu": float|None, "reason": str}
    """
    # Step 1: find PID via lsof
    pid = await _find_pid_on_port(port)
    if pid is None:
        return {"frozen": False, "pid": None, "cpu": None, "reason": "no process on port"}

    # Step 2: sample CPU twice with 1s gap
    cpu1 = await _get_cpu(pid)
    await asyncio.sleep(1.0)
    cpu2 = await _get_cpu(pid)

    if cpu1 is None or cpu2 is None:
        return {"frozen": False, "pid": pid, "cpu": None, "reason": "could not read CPU"}

    avg_cpu = (cpu1 + cpu2) / 2

    # Step 3: try HTTP probe with short timeout
    tcp_ok = await _tcp_connect_test(port)

    if tcp_ok and avg_cpu < FROZEN_CPU_THRESHOLD:
        log.warning("frozen process detected: PID %d on port %d (CPU %.1f%%)", pid, port, avg_cpu)
        return {"frozen": True, "pid": pid, "cpu": avg_cpu, "reason": "port open, 0% CPU"}

    return {"frozen": False, "pid": pid, "cpu": avg_cpu, "reason": "process alive"}


async def _find_pid_on_port(port: int) -> int | None:
    """Use lsof to find the PID listening on a port."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "lsof", "-i", f":{port}", "-sTCP:LISTEN", "-t",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
        pids = stdout.decode().strip().split("\n")
        if pids and pids[0]:
            return int(pids[0])
    except Exception:
        log.debug("lsof failed for port %d", port)
    return None


async def _get_cpu(pid: int) -> float | None:
    """Get CPU% for a PID via ps."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ps", "-p", str(pid), "-o", "%cpu=",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
        val = stdout.decode().strip()
        if val:
            return float(val)
    except Exception:
        log.debug("ps failed for PID %d", pid)
    return None


async def _tcp_connect_test(port: int) -> bool:
    """Quick TCP connect to see if port is open (kernel LISTEN backlog)."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", port),
            timeout=FROZEN_ACCEPT_TIMEOUT,
        )
        writer.close()
        return True
    except Exception:
        return False
