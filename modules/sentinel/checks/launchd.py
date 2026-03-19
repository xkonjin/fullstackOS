"""LaunchAgent health checks via launchctl."""

from __future__ import annotations

import asyncio
import logging
import re

from ..config import LAUNCHD_LABELS

log = logging.getLogger("sentinel.checks.launchd")


async def check_launchd_agents() -> dict[str, dict]:
    """Check status of all monitored LaunchAgents.

    Returns {label: {status, pid, exit_code}}.
    """
    results: dict[str, dict] = {}

    for label in LAUNCHD_LABELS:
        results[label] = await _check_agent(label)

    return results


async def _check_agent(label: str) -> dict:
    """Check a single LaunchAgent via launchctl list."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "launchctl", "list", label,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=5)

        if proc.returncode != 0:
            # Not loaded
            return {"status": "not_loaded", "pid": None, "exit_code": None, "label": label}

        output = stdout.decode()
        info = _parse_launchctl_output(output)
        info["label"] = label

        pid = info.get("pid")
        exit_code = info.get("exit_code")

        if pid and pid > 0:
            info["status"] = "running"
        elif exit_code is not None and exit_code != 0:
            info["status"] = "error"
        elif exit_code == 0:
            info["status"] = "stopped"
        else:
            info["status"] = "unknown"

        return info

    except asyncio.TimeoutError:
        return {"status": "timeout", "pid": None, "exit_code": None, "label": label}
    except Exception:
        log.debug("launchctl check failed for %s", label)
        return {"status": "error", "pid": None, "exit_code": None, "label": label}


def _parse_launchctl_output(output: str) -> dict:
    """Parse launchctl list <label> output into a dict."""
    info: dict = {"pid": None, "exit_code": None}

    for line in output.strip().split("\n"):
        line = line.strip()
        if "=" not in line and "\t" not in line:
            continue

        # Format: "key" = value; or PID\tExitCode\tLabel
        if "=" in line:
            key, _, val = line.partition("=")
            key = key.strip().strip('"').strip()
            val = val.strip().rstrip(";").strip('"').strip()

            if key.lower() == "pid":
                try:
                    info["pid"] = int(val)
                except ValueError:
                    pass
            elif key.lower() in ("lastexitstatus", "exit_code"):
                try:
                    info["exit_code"] = int(val)
                except ValueError:
                    pass

    # Also try tab-separated format (launchctl list output)
    lines = output.strip().split("\n")
    if lines:
        parts = lines[0].split("\t")
        if len(parts) >= 3:
            try:
                pid_str = parts[0].strip()
                if pid_str != "-":
                    info["pid"] = int(pid_str)
                exit_str = parts[1].strip()
                if exit_str != "-":
                    info["exit_code"] = int(exit_str)
            except ValueError:
                pass

    return info
