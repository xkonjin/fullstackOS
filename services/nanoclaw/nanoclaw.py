#!/usr/bin/env python3
"""nanoclaw — Kimi-powered self-healing daemon for claudemax infrastructure.

Replaces 9 independent monitoring components with a single daemon.
Level 3 healing: detect, restart, refresh tokens, clean logs.
Does NOT modify configs (Level 4 excluded).
Does NOT touch clauded (direct Anthropic) — only claudemax stack.
"""

import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from refreshers import TokenRefresher
from reasoning import KimiReasoner

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

STATE_DIR = Path.home() / ".nanoclaw"
STATE_FILE = STATE_DIR / "state.json"
LOG_DIR = STATE_DIR / "logs"
PID_FILE = STATE_DIR / "nanoclaw.pid"

HEALTH_INTERVAL = 30        # seconds between health probes
TOKEN_INTERVAL = 300        # seconds between token refresh checks
HOUSEKEEP_INTERVAL = 3600   # seconds between log/disk cleanup
PROVIDER_TEST_INTERVAL = 300  # seconds between provider auth tests

MAX_RESTARTS_PER_HOUR = 3
MAX_REFRESH_ATTEMPTS = 3
KIMI_DAILY_TOKEN_CAP = 50_000
ALERT_COOLDOWN = 1800       # 30 min per issue type
LOG_MAX_AGE_DAYS = 3
LOG_MAX_SIZE_GB = 10
DISK_MIN_FREE_GB = 20

PROXY_URL = "http://127.0.0.1:8317"
ORCHESTRATOR_URL = "http://127.0.0.1:8318"
FLEET_GW_URL = "http://127.0.0.1:4105"
AUTH_DIR = Path.home() / ".cli-proxy-api"

TELEGRAM_BOT_TOKEN = os.environ.get("NANOCLAW_TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("NANOCLAW_TELEGRAM_CHAT_ID", "YOUR_USER_ID")

SERVICES = {
    "cliproxyapi": {
        "label": "homebrew.mxcl.cliproxyapi",
        "url": f"{PROXY_URL}/",
        "method": "GET",
    },
    "orchestrator": {
        "label": "com.claudemax.orchestrator",
        "url": f"{ORCHESTRATOR_URL}/health",
        "method": "GET",
        "timeout": 15,  # Orchestrator needs more time during startup
        "post_restart_cooldown": 60,  # Wait 60s after restart before re-probing
    },
    "fleet-gateway": {
        "label": "com.ai.fleet.gateway",
        "host": "127.0.0.1",
        "port": 4105,
        "tcp_only": True,
    },
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("nanoclaw")
log.setLevel(logging.INFO)

_fmt = logging.Formatter("[%(asctime)s] %(levelname)-5s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_fh = logging.FileHandler(LOG_DIR / "nanoclaw.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)

# ---------------------------------------------------------------------------
# State Management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return _default_state()


def save_state(state: dict):
    # Cap incidents to prevent unbounded growth
    if len(state.get("incidents", [])) > 100:
        state["incidents"] = state["incidents"][-100:]

    tmp = STATE_FILE.with_suffix(f".{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(state, indent=2, default=str))
        os.chmod(tmp, 0o600)
        tmp.rename(STATE_FILE)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def _default_state() -> dict:
    return {
        "last_check": None,
        "services": {},
        "tokens": {},
        "kimi_usage": {"tokens_today": 0, "calls_today": 0, "date": _today()},
        "incidents": [],
        "alert_cooldowns": {},
    }


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

import urllib.request
import urllib.error
import socket


def http_get(url: str, headers: dict = None, timeout: int = 10) -> tuple:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, socket.timeout, OSError) as e:
        return 0, str(e)


def http_post(url: str, body, headers: dict = None,
              timeout: int = 15, content_type: str = "application/json") -> tuple:
    hdrs = headers or {}
    hdrs["Content-Type"] = content_type
    if isinstance(body, dict):
        data = json.dumps(body).encode()
    else:
        data = body.encode() if isinstance(body, str) else body
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, socket.timeout, OSError) as e:
        return 0, str(e)


def tcp_check(host: str, port: int, timeout: int = 5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, OSError):
        return False

# ---------------------------------------------------------------------------
# Alert
# ---------------------------------------------------------------------------

def send_alert(state: dict, issue_type: str, message: str):
    cooldowns = state.setdefault("alert_cooldowns", {})
    last = cooldowns.get(issue_type, 0)
    now = time.time()
    if now - last < ALERT_COOLDOWN:
        log.debug(f"Alert suppressed (cooldown): {issue_type}")
        return
    cooldowns[issue_type] = now

    log.warning(f"ALERT [{issue_type}]: {message}")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            http_post(url, {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": f"🔧 nanoclaw [{issue_type}]\n{message}",
                "parse_mode": "Markdown",
            }, timeout=10)
        except Exception as e:
            log.error(f"Telegram alert failed: {e}")

# ---------------------------------------------------------------------------
# Health Probes
# ---------------------------------------------------------------------------

async def probe_services(state: dict) -> list[dict]:
    """Probe all services. Returns list of failures."""
    failures = []
    for name, svc in SERVICES.items():
        svc_state = state["services"].setdefault(name, {
            "status": "unknown", "last_restart": None,
            "restart_count_1h": 0, "restart_timestamps": [],
        })

        # Skip if in post-restart cooldown
        cooldown = svc.get("post_restart_cooldown", 30)
        last_restart = svc_state.get("last_restart")
        if last_restart:
            try:
                lr_dt = datetime.fromisoformat(last_restart.replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - lr_dt).total_seconds() < cooldown:
                    continue  # Still in cooldown, skip this probe
            except (ValueError, TypeError):
                pass

        if svc.get("tcp_only"):
            ok = tcp_check(svc["host"], svc["port"])
            if ok:
                svc_state["status"] = "healthy"
            else:
                svc_state["status"] = "down"
                failures.append({"service": name, "error": "TCP connect failed"})
        else:
            probe_timeout = svc.get("timeout", 10)
            code, body = http_get(svc["url"], timeout=probe_timeout)
            if 200 <= code < 300:
                svc_state["status"] = "healthy"
                # Extra: check orchestrator account health
                if name == "orchestrator":
                    try:
                        data = json.loads(body)
                        healthy = data.get("accounts", {}).get("healthy", 0)
                        total = data.get("accounts", {}).get("total", 0)
                        if total > 0 and healthy < total * 0.5:
                            failures.append({
                                "service": name,
                                "error": f"Low healthy accounts: {healthy}/{total}",
                                "severity": "warning",
                            })
                    except json.JSONDecodeError:
                        pass
            else:
                svc_state["status"] = "down"
                failures.append({"service": name, "error": f"HTTP {code}: {body[:200]}"})

    state["last_check"] = _now_iso()
    return failures


async def probe_providers(state: dict) -> list[dict]:
    """Test actual model calls through proxy to detect auth failures."""
    failures = []
    test_models = {
        "claude": "claude-haiku-4-5-20251001",
        "gemini": "gemini-2.5-flash-lite",
        "codex": "gpt-5.1",
        # kimi: OAuth token can't be refreshed (geo-blocked) but kimi-k2
        # routes through moonshot API key provider which works fine.
        # Don't probe it — the OAuth expiry is expected and harmless.
    }
    for provider, model in test_models.items():
        code, body = http_post(
            f"{PROXY_URL}/v1/chat/completions",
            {"model": model, "max_tokens": 5, "messages": [{"role": "user", "content": "ok"}]},
            headers={"Authorization": "Bearer your-proxy-key"},
            timeout=30,
        )
        if code == 200:
            try:
                data = json.loads(body)
                if data.get("choices", [{}])[0].get("finish_reason") in ("stop", "length"):
                    continue
            except json.JSONDecodeError:
                pass
        failures.append({"provider": provider, "model": model, "code": code, "error": body[:200]})
    return failures

# ---------------------------------------------------------------------------
# Healing Actions
# ---------------------------------------------------------------------------

def restart_service(state: dict, name: str) -> bool:
    svc = SERVICES.get(name)
    if not svc:
        return False

    svc_state = state["services"].setdefault(name, {
        "status": "unknown", "last_restart": None,
        "restart_count_1h": 0, "restart_timestamps": [],
    })

    # Rate limit: max 3 restarts per hour
    now = time.time()
    timestamps = svc_state.get("restart_timestamps", [])
    timestamps = [t for t in timestamps if now - t < 3600]
    if len(timestamps) >= MAX_RESTARTS_PER_HOUR:
        send_alert(state, f"restart_limit_{name}",
                    f"Service `{name}` hit restart limit ({MAX_RESTARTS_PER_HOUR}/hr). Manual intervention needed.")
        return False

    label = svc["label"]
    log.info(f"Restarting service: {name} ({label})")

    try:
        # kickstart -k sends SIGTERM then restarts
        result = subprocess.run(
            ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/{label}"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            timestamps.append(now)
            svc_state["restart_timestamps"] = timestamps
            svc_state["last_restart"] = _now_iso()
            svc_state["restart_count_1h"] = len(timestamps)
            log.info(f"Restarted {name} successfully")
            return True
        else:
            log.error(f"Restart failed for {name}: {result.stderr}")
            return False
    except Exception as e:
        log.error(f"Restart exception for {name}: {e}")
        return False


def clean_logs(state: dict) -> dict:
    """Clean old/oversized logs. Returns summary."""
    results = {"deleted_files": 0, "freed_bytes": 0}
    log_dir = AUTH_DIR / "logs"
    if not log_dir.exists():
        return results

    cutoff = time.time() - (LOG_MAX_AGE_DAYS * 86400)
    for f in log_dir.iterdir():
        if f.is_file() and f.stat().st_mtime < cutoff:
            size = f.stat().st_size
            try:
                f.unlink()
                results["deleted_files"] += 1
                results["freed_bytes"] += size
            except OSError:
                pass

    # Also clean nanoclaw's own logs if >50MB
    own_log = LOG_DIR / "nanoclaw.log"
    if own_log.exists() and own_log.stat().st_size > 50_000_000:
        # Truncate to last 10MB
        data = own_log.read_bytes()
        own_log.write_bytes(data[-10_000_000:])
        results["freed_bytes"] += len(data) - 10_000_000

    return results


def check_disk_space() -> float:
    """Returns free disk space in GB."""
    st = os.statvfs("/")
    return (st.f_bavail * st.f_frsize) / (1024 ** 3)

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------

class Nanoclaw:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.state = load_state()
        self.refresher = TokenRefresher(AUTH_DIR, dry_run=dry_run)
        self.reasoner = KimiReasoner(PROXY_URL, dry_run=dry_run)
        self.running = True
        self._health_tick = 0
        self._token_tick = 0
        self._housekeep_tick = 0
        self._provider_tick = 0

    async def run(self):
        log.info(f"nanoclaw starting (dry_run={self.dry_run})")
        write_pid()

        # Initial health check
        await self._health_cycle()
        await self._token_cycle()

        while self.running:
            try:
                await asyncio.sleep(1)
                self._health_tick += 1
                self._token_tick += 1
                self._housekeep_tick += 1
                self._provider_tick += 1

                if self._health_tick >= HEALTH_INTERVAL:
                    self._health_tick = 0
                    await self._health_cycle()

                if self._token_tick >= TOKEN_INTERVAL:
                    self._token_tick = 0
                    await self._token_cycle()

                if self._provider_tick >= PROVIDER_TEST_INTERVAL:
                    self._provider_tick = 0
                    await self._provider_cycle()

                if self._housekeep_tick >= HOUSEKEEP_INTERVAL:
                    self._housekeep_tick = 0
                    await self._housekeep_cycle()

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

        log.info("nanoclaw shutting down")
        save_state(self.state)

    async def _health_cycle(self):
        failures = await probe_services(self.state)
        if not failures:
            save_state(self.state)
            return

        for f in failures:
            svc = f.get("service")
            severity = f.get("severity", "critical")
            error = f.get("error", "unknown")

            if severity == "warning":
                log.warning(f"Health warning: {svc} — {error}")
                continue

            log.error(f"Health failure: {svc} — {error}")

            if self.dry_run:
                log.info(f"[DRY RUN] Would restart {svc}")
                continue

            # Direct action: restart the service
            restarted = restart_service(self.state, svc)
            if restarted:
                # Don't re-probe immediately — the post_restart_cooldown
                # in the next health cycle will handle verification.
                # Probing too soon causes restart loops.
                log.info(f"Restart issued for {svc}, will verify in next cycle")
            else:
                send_alert(self.state, f"restart_failed_{svc}",
                           f"Cannot restart `{svc}`: {error}")

        save_state(self.state)

    async def _token_cycle(self):
        refreshed, failures = self.refresher.check_and_refresh_all()

        for provider, email, msg in refreshed:
            log.info(f"Token refreshed: {provider}/{email} — {msg}")
            self.state["tokens"].setdefault(f"{provider}-{email}", {})["last_refresh"] = _now_iso()

        for provider, email, error, attempts in failures:
            log.error(f"Token refresh failed: {provider}/{email} — {error} (attempt {attempts})")
            key = f"{provider}-{email}"
            tok_state = self.state["tokens"].setdefault(key, {})
            tok_state["refresh_failures"] = attempts

            if attempts >= MAX_REFRESH_ATTEMPTS:
                send_alert(self.state, f"token_expired_{key}",
                           f"Token for `{provider}/{email}` failed {attempts} refresh attempts. Manual re-auth needed.")
            elif attempts == 1:
                # First failure — maybe transient, try Kimi reasoning on next failure
                pass

        save_state(self.state)

    async def _provider_cycle(self):
        failures = await probe_providers(self.state)
        if not failures:
            return

        for f in failures:
            provider = f["provider"]
            error = f.get("error", "")
            log.warning(f"Provider probe failed: {provider} ({f['model']}) — HTTP {f['code']}: {error[:100]}")

            if "auth_unavailable" in error or f["code"] in (401, 403):
                # Token issue — trigger immediate refresh
                log.info(f"Auth unavailable for {provider}, triggering token refresh")
                self.refresher.refresh_provider(provider)

        save_state(self.state)

    async def _housekeep_cycle(self):
        # Log cleanup
        result = clean_logs(self.state)
        if result["deleted_files"] > 0:
            freed_mb = result["freed_bytes"] / (1024 * 1024)
            log.info(f"Housekeeping: deleted {result['deleted_files']} log files, freed {freed_mb:.0f}MB")

        # Disk space check
        free_gb = check_disk_space()
        if free_gb < DISK_MIN_FREE_GB:
            log.warning(f"Low disk space: {free_gb:.1f}GB free")
            # Aggressive cleanup
            if not self.dry_run:
                clean_logs(self.state)
            send_alert(self.state, "low_disk", f"Only {free_gb:.1f}GB free after cleanup")

        # Reset daily Kimi token counter
        if self.state["kimi_usage"].get("date") != _today():
            self.state["kimi_usage"] = {"tokens_today": 0, "calls_today": 0, "date": _today()}

        save_state(self.state)

    async def _escalate_to_kimi(self, issue_type: str, context: dict):
        """Invoke Kimi reasoning for complex/unknown failures."""
        usage = self.state["kimi_usage"]
        if usage.get("tokens_today", 0) >= KIMI_DAILY_TOKEN_CAP:
            log.warning("Kimi daily token cap reached, skipping reasoning")
            send_alert(self.state, issue_type, f"Issue: {json.dumps(context)[:200]} (Kimi cap reached)")
            return

        if self.dry_run:
            log.info(f"[DRY RUN] Would invoke Kimi reasoning for {issue_type}")
            return

        diagnosis = self.reasoner.diagnose(issue_type, context)
        if diagnosis:
            usage["tokens_today"] = usage.get("tokens_today", 0) + diagnosis.get("tokens_used", 0)
            usage["calls_today"] = usage.get("calls_today", 0) + 1

            action = diagnosis.get("action")
            explanation = diagnosis.get("explanation", "")
            log.info(f"Kimi diagnosis: {explanation[:200]}")

            if action == "alert_only":
                send_alert(self.state, issue_type, f"Kimi: {explanation}")
            elif action == "restart" and diagnosis.get("target"):
                restart_service(self.state, diagnosis["target"])
            elif action == "refresh_token" and diagnosis.get("provider"):
                self.refresher.refresh_provider(diagnosis["provider"])

        save_state(self.state)

    def stop(self):
        self.running = False


def write_pid():
    PID_FILE.write_text(str(os.getpid()))


def cleanup_pid():
    if PID_FILE.exists():
        PID_FILE.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="nanoclaw self-healing daemon")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without executing")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--status", action="store_true", help="Print current state and exit")
    args = parser.parse_args()

    if args.status:
        state = load_state()
        print(json.dumps(state, indent=2, default=str))
        return

    daemon = Nanoclaw(dry_run=args.dry_run)

    def _signal_handler(sig, frame):
        log.info(f"Received signal {sig}")
        daemon.stop()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    try:
        if args.once:
            asyncio.run(_run_once(daemon))
        else:
            asyncio.run(daemon.run())
    finally:
        cleanup_pid()


async def _run_once(daemon: Nanoclaw):
    await daemon._health_cycle()
    await daemon._token_cycle()
    await daemon._provider_cycle()
    await daemon._housekeep_cycle()
    log.info("Single cycle complete")


if __name__ == "__main__":
    main()
