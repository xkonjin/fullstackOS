"""Harness Validator — Python bridge to the Sandbox Harness API.

Provides lightweight validation by executing commands in sandboxed
environments via the harness service (default: http://127.0.0.1:18450).

Used by pipeline stages to verify implementations before merge.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

log = logging.getLogger("aifleet.harness_validator")

DEFAULT_HARNESS_URL = "http://127.0.0.1:18450"
DEFAULT_TIMEOUT = 30


def get_harness_url() -> str:
    return os.environ.get("SANDBOX_HARNESS_URL", DEFAULT_HARNESS_URL)


def is_harness_available() -> bool:
    """Check if the sandbox harness service is reachable."""
    try:
        req = urllib.request.Request(
            f"{get_harness_url()}/health",
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception:
        return False


def create_validation_job(
    command: str,
    env: dict[str, str] | None = None,
    timeout_seconds: int = 120,
    policy_profile: str = "standard",
) -> dict:
    """Create a sandbox job to validate an implementation.

    Returns:
        {"ok": bool, "job_id": str, "status": str, "error": str | None}
    """
    harness_url = get_harness_url()
    token = os.environ.get("SANDBOX_HARNESS_TOKEN", "")

    payload = {
        "command": command,
        "env": env or {},
        "timeoutSeconds": timeout_seconds,
        "policyProfile": policy_profile,
        "policy": {
            "maxRuntimeSeconds": timeout_seconds,
            "allowNetwork": False,
        },
    }

    headers = {
        "Content-Type": "application/json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        req = urllib.request.Request(
            f"{harness_url}/v1/jobs",
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT) as resp:
            body = json.loads(resp.read())
            return {
                "ok": True,
                "job_id": body.get("id", ""),
                "status": body.get("status", "unknown"),
                "error": None,
            }
    except urllib.error.HTTPError as exc:
        return {"ok": False, "job_id": "", "status": "error", "error": f"HTTP {exc.code}: {exc.reason}"}
    except Exception as exc:
        return {"ok": False, "job_id": "", "status": "error", "error": str(exc)}


def get_job_status(job_id: str) -> dict:
    """Poll a sandbox job for its current status.

    Returns:
        {"ok": bool, "status": str, "error": str | None}
    """
    harness_url = get_harness_url()
    token = os.environ.get("SANDBOX_HARNESS_TOKEN", "")

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        req = urllib.request.Request(
            f"{harness_url}/v1/jobs/{job_id}",
            headers=headers,
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            return {
                "ok": True,
                "status": body.get("status", "unknown"),
                "error": None,
            }
    except Exception as exc:
        return {"ok": False, "status": "error", "error": str(exc)}


def validate_command(
    command: str,
    env: dict[str, str] | None = None,
    timeout_seconds: int = 120,
) -> dict:
    """Create a validation job and return the result.

    If harness is unavailable, returns a skip result rather than failing.

    Returns:
        {"ok": bool, "validated": bool, "job_id": str, "status": str,
         "skipped": bool, "error": str | None}
    """
    if not is_harness_available():
        log.info("Sandbox harness not available — skipping validation")
        return {
            "ok": True,
            "validated": False,
            "job_id": "",
            "status": "skipped",
            "skipped": True,
            "error": None,
        }

    result = create_validation_job(command, env, timeout_seconds)
    return {
        "ok": result["ok"],
        "validated": result["ok"],
        "job_id": result.get("job_id", ""),
        "status": result.get("status", ""),
        "skipped": False,
        "error": result.get("error"),
    }
