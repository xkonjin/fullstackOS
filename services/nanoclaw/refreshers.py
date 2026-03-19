"""Token refreshers for all 5 providers.

Each refresher reads the token file, checks expiry, and if needed
hits the OAuth endpoint to get a new access_token. The refreshed
token is written back to the file — CLIProxyAPI picks it up via fsnotify.

NO proxy restart needed.
"""

import json
import logging
import os
import time
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger("nanoclaw")

# ---------------------------------------------------------------------------
# Provider refresh configs
# ---------------------------------------------------------------------------

CLAUDE_REFRESH_URL = "https://console.anthropic.com/v1/oauth/token"
CLAUDE_CLIENT_ID = os.environ.get("NANOCLAW_CLAUDE_CLIENT_ID", "your-claude-client-id")

CODEX_REFRESH_URL = "https://auth.openai.com/oauth/token"
CODEX_CLIENT_ID = os.environ.get("NANOCLAW_CODEX_CLIENT_ID", "your-codex-client-id")

GOOGLE_REFRESH_URL = "https://oauth2.googleapis.com/token"
ANTIGRAVITY_CLIENT_ID = os.environ.get("NANOCLAW_ANTIGRAVITY_CLIENT_ID", "your-google-client-id.apps.googleusercontent.com")
ANTIGRAVITY_CLIENT_SECRET = os.environ.get("NANOCLAW_ANTIGRAVITY_SECRET", "your-google-client-secret")

GEMINI_CLIENT_IDS = {
    # Will read client_id and client_secret from the token file itself
}

KIMI_REFRESH_URL = "https://api.moonshot.cn/v1/token"
KIMI_REFRESH_TIMEOUT = 30  # Moonshot can be slow

# Refresh thresholds
REFRESH_LEAD_CLAUDE = timedelta(hours=1)
REFRESH_LEAD_CODEX = timedelta(hours=1)
REFRESH_LEAD_ANTIGRAVITY = timedelta(hours=1)
REFRESH_LEAD_GEMINI = timedelta(minutes=30)
REFRESH_LEAD_KIMI = timedelta(minutes=30)


def _now_utc():
    return datetime.now(timezone.utc)


def _parse_iso(s):
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        # Ensure timezone-aware to prevent TypeError when compared to _now_utc()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _now_iso() -> str:
    return _now_utc().isoformat()


def _http_post_form(url: str, params: dict, timeout: int = 15) -> tuple:
    import urllib.request
    import urllib.error
    data = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, {"error": body}
    except Exception as e:
        return 0, {"error": str(e)}


def _http_post_json(url: str, body: dict, timeout: int = 15) -> tuple:
    import urllib.request
    import urllib.error
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_str = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(body_str)
        except json.JSONDecodeError:
            return e.code, {"error": body_str}
    except Exception as e:
        return 0, {"error": str(e)}


def _http_get_json(url: str, headers: dict = None, timeout: int = 15) -> tuple:
    import urllib.request
    import urllib.error
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_str = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(body_str)
        except json.JSONDecodeError:
            return e.code, {"error": body_str}
    except Exception as e:
        return 0, {"error": str(e)}


# ---------------------------------------------------------------------------
# Individual refreshers
# ---------------------------------------------------------------------------

def _refresh_claude(token_file: Path, data: dict, dry_run: bool) -> tuple:
    """Refresh a Claude OAuth token.

    Uses CLIProxyAPI's built-in refresh via admin endpoint first,
    falls back to direct OAuth if available. Direct OAuth is blocked
    by Cloudflare (403 error 1010) from Python urllib, so we prefer
    delegating to the Go binary which has proper TLS fingerprints.
    """
    refresh_token = data.get("refresh_token")
    if not refresh_token:
        return False, "No refresh_token in file"

    expired = _parse_iso(data.get("expired"))
    if expired and expired - _now_utc() > REFRESH_LEAD_CLAUDE:
        return True, "Not yet due"

    if dry_run:
        return True, f"[DRY RUN] Would refresh (expires {expired})"

    # Strategy 1: Trigger CLIProxyAPI's built-in refresh for this token file
    # The Go binary handles Cloudflare properly
    try:
        import subprocess
        result = subprocess.run(
            ["cliproxyapi", "refresh", "--file", str(token_file)],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            # Re-read the file that CLIProxyAPI updated
            updated = _read_token(token_file)
            if updated and updated.get("access_token") != data.get("access_token"):
                return True, "Refreshed via CLIProxyAPI binary"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass  # CLIProxyAPI binary not available or timed out

    # Strategy 2: Direct OAuth (may fail with Cloudflare 403)
    code, resp = _http_post_json(CLAUDE_REFRESH_URL, {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLAUDE_CLIENT_ID,
    })

    if code == 200 and "access_token" in resp:
        data["access_token"] = resp["access_token"]
        if "refresh_token" in resp:
            data["refresh_token"] = resp["refresh_token"]
        expires_in = resp.get("expires_in", 3600)
        data["expired"] = (_now_utc() + timedelta(seconds=expires_in)).isoformat()
        data["last_refresh"] = _now_iso()
        _write_token(token_file, data)
        return True, f"Refreshed, new expiry in {expires_in}s"
    elif code == 403:
        # Cloudflare block — CLIProxyAPI's auto-refresh (config: check-interval 15m)
        # will handle this. Log but don't count as hard failure.
        return False, "Cloudflare 403 — delegating to CLIProxyAPI auto-refresh"
    else:
        return False, f"HTTP {code}: {json.dumps(resp)[:200]}"


def _refresh_codex(token_file: Path, data: dict, dry_run: bool) -> tuple:
    """Refresh a Codex (OpenAI) OAuth token."""
    refresh_token = data.get("refresh_token")
    if not refresh_token:
        return False, "No refresh_token in file"

    expired = _parse_iso(data.get("expired"))
    if expired and expired - _now_utc() > REFRESH_LEAD_CODEX:
        return True, "Not yet due"

    if dry_run:
        return True, f"[DRY RUN] Would refresh (expires {expired})"

    code, resp = _http_post_form(CODEX_REFRESH_URL, {
        "client_id": CODEX_CLIENT_ID,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "scope": "openid profile email",
    })

    if code == 200 and "access_token" in resp:
        data["access_token"] = resp["access_token"]
        if "refresh_token" in resp:
            data["refresh_token"] = resp["refresh_token"]
        if "id_token" in resp:
            data["id_token"] = resp["id_token"]
        expires_in = resp.get("expires_in", 3600)
        data["expired"] = (_now_utc() + timedelta(seconds=expires_in)).isoformat()
        data["last_refresh"] = _now_iso()
        # Decode account_id from id_token if present
        if resp.get("id_token"):
            try:
                import base64
                payload = resp["id_token"].split(".")[1]
                payload += "=" * (4 - len(payload) % 4)
                claims = json.loads(base64.urlsafe_b64decode(payload))
                if "sub" in claims:
                    data["account_id"] = claims["sub"]
            except Exception:
                pass
        _write_token(token_file, data)
        return True, f"Refreshed, new expiry in {expires_in}s"
    else:
        return False, f"HTTP {code}: {json.dumps(resp)[:200]}"


def _refresh_antigravity(token_file: Path, data: dict, dry_run: bool) -> tuple:
    """Refresh an Antigravity (Google OAuth) token and WRITE to file.

    This is the key fix — the Go code only refreshes in-memory.
    Nanoclaw writes it to disk so it persists across proxy restarts.
    """
    refresh_token = data.get("refresh_token")
    if not refresh_token:
        return False, "No refresh_token in file"

    expired = _parse_iso(data.get("expired"))
    if expired and expired - _now_utc() > REFRESH_LEAD_ANTIGRAVITY:
        return True, "Not yet due"

    if dry_run:
        return True, f"[DRY RUN] Would refresh (expires {expired})"

    code, resp = _http_post_form(GOOGLE_REFRESH_URL, {
        "client_id": ANTIGRAVITY_CLIENT_ID,
        "client_secret": ANTIGRAVITY_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    })

    if code == 200 and "access_token" in resp:
        data["access_token"] = resp["access_token"]
        if "refresh_token" in resp:
            data["refresh_token"] = resp["refresh_token"]
        expires_in = resp.get("expires_in", 3600)
        data["expired"] = (_now_utc() + timedelta(seconds=expires_in)).isoformat()
        data["expires_in"] = expires_in
        data["timestamp"] = int(time.time() * 1000)
        data["last_refresh"] = _now_iso()
        _write_token(token_file, data)
        return True, f"Refreshed, new expiry in {expires_in}s"
    else:
        return False, f"HTTP {code}: {json.dumps(resp)[:200]}"


def _refresh_gemini(token_file: Path, data: dict, dry_run: bool) -> tuple:
    """Refresh a Gemini (Google OAuth) token. Token fields are nested under 'token'."""
    token = data.get("token", {})
    refresh_token = token.get("refresh_token")
    if not refresh_token:
        return False, "No token.refresh_token in file"

    expiry = _parse_iso(token.get("expiry"))
    if expiry and expiry - _now_utc() > REFRESH_LEAD_GEMINI:
        return True, "Not yet due"

    client_id = token.get("client_id")
    client_secret = token.get("client_secret")
    if not client_id or not client_secret:
        return False, "No client_id/client_secret in token"

    if dry_run:
        return True, f"[DRY RUN] Would refresh (expires {expiry})"

    code, resp = _http_post_form(GOOGLE_REFRESH_URL, {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    })

    if code == 200 and "access_token" in resp:
        token["access_token"] = resp["access_token"]
        if "refresh_token" in resp:
            token["refresh_token"] = resp["refresh_token"]
        expires_in = resp.get("expires_in", 3600)
        token["expiry"] = (_now_utc() + timedelta(seconds=expires_in)).isoformat()
        if "scope" in resp:
            token["scopes"] = resp["scope"].split()
        data["token"] = token
        _write_token(token_file, data)
        return True, f"Refreshed, new expiry in {expires_in}s"
    else:
        return False, f"HTTP {code}: {json.dumps(resp)[:200]}"


def _refresh_kimi(token_file: Path, data: dict, dry_run: bool) -> tuple:
    """Refresh a Kimi token. Uses GET with Bearer refresh_token."""
    refresh_token = data.get("refresh_token")
    if not refresh_token:
        return False, "No refresh_token in file"

    # Decode JWT exp from access_token to check expiry
    expired = _parse_iso(data.get("expired"))
    if not expired:
        try:
            import base64
            payload = data["access_token"].split(".")[1]
            payload += "=" * (4 - len(payload) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
            expired = datetime.fromtimestamp(claims["exp"], tz=timezone.utc)
        except Exception:
            expired = _now_utc()  # Force refresh if can't parse

    if expired - _now_utc() > REFRESH_LEAD_KIMI:
        return True, "Not yet due"

    if dry_run:
        return True, f"[DRY RUN] Would refresh (expires {expired})"

    code, resp = _http_get_json(KIMI_REFRESH_URL, headers={
        "Authorization": f"Bearer {refresh_token}",
        "Content-Type": "application/json",
    }, timeout=KIMI_REFRESH_TIMEOUT)

    if code == 200 and "access_token" in resp:
        data["access_token"] = resp["access_token"]
        if "refresh_token" in resp:
            data["refresh_token"] = resp["refresh_token"]
        # Decode new expiry from JWT
        try:
            import base64
            payload = resp["access_token"].split(".")[1]
            payload += "=" * (4 - len(payload) % 4)
            claims = json.loads(base64.urlsafe_b64decode(payload))
            data["expired"] = datetime.fromtimestamp(claims["exp"], tz=timezone.utc).isoformat()
        except Exception:
            data["expired"] = (_now_utc() + timedelta(minutes=15)).isoformat()
        data["last_refresh"] = _now_iso()
        if "device_id" in resp:
            data["device_id"] = resp["device_id"]
        _write_token(token_file, data)
        return True, f"Refreshed, new expiry {data['expired']}"
    else:
        return False, f"HTTP {code}: {json.dumps(resp)[:200]}"


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def _write_token(path: Path, data: dict):
    """Atomically write token file. CLIProxyAPI fsnotify picks it up.
    Uses PID+timestamp suffix to avoid race with concurrent writers.
    """
    tmp = path.with_suffix(f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    try:
        tmp.write_text(json.dumps(data, indent=2))
        os.chmod(tmp, 0o600)
        tmp.rename(path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def _read_token(path: Path):
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


# ---------------------------------------------------------------------------
# Token Refresher (orchestrator)
# ---------------------------------------------------------------------------

PROVIDER_REFRESHERS = {
    "claude": _refresh_claude,
    "codex": _refresh_codex,
    "antigravity": _refresh_antigravity,
    "gemini": _refresh_gemini,
    "kimi": _refresh_kimi,
}

# Pattern: {type}-{rest}.json  e.g. claude-user@example.com.json
# Kimi OAuth refresh is disabled — api.moonshot.cn is geo-blocked from this network.
# Kimi models still work via the moonshot API key provider (kimi-api).
# The OAuth token file must exist for CLIProxyAPI routing but cannot be refreshed.
PROVIDER_PREFIXES = {
    "claude": "claude-",
    "codex": "codex-",
    "antigravity": "antigravity-",
    "gemini": "gemini-",
    # "kimi": "kimi-",  # Disabled: moonshot OAuth endpoint unreachable
}


class TokenRefresher:
    def __init__(self, auth_dir: Path, dry_run: bool = False):
        self.auth_dir = auth_dir
        self.dry_run = dry_run
        self._attempt_counts: dict[str, int] = {}  # file_path -> attempts

    def check_and_refresh_all(self) -> tuple:
        """Check all token files and refresh as needed.

        Returns: (successes, failures)
            successes: [(provider, email, message), ...]
            failures: [(provider, email, error, attempt_count), ...]
        """
        successes = []
        failures = []

        for provider, prefix in PROVIDER_PREFIXES.items():
            refresher = PROVIDER_REFRESHERS[provider]
            for token_file in self.auth_dir.glob(f"{prefix}*.json"):
                # Skip stale-trash, archive, backup subdirs
                # (but not the auth_dir itself which may start with .)
                rel = token_file.relative_to(self.auth_dir)
                if any(part.startswith(".") for part in rel.parts[:-1]):
                    continue

                data = _read_token(token_file)
                if not data:
                    continue

                # Skip disabled tokens
                if data.get("disabled"):
                    continue

                email = data.get("email", token_file.stem)
                file_key = str(token_file)

                try:
                    ok, msg = refresher(token_file, data, self.dry_run)
                    if ok:
                        if "Not yet due" not in msg:
                            successes.append((provider, email, msg))
                        # Reset attempt counter on success
                        self._attempt_counts.pop(file_key, None)
                    else:
                        attempts = self._attempt_counts.get(file_key, 0) + 1
                        self._attempt_counts[file_key] = attempts
                        failures.append((provider, email, msg, attempts))
                except Exception as e:
                    attempts = self._attempt_counts.get(file_key, 0) + 1
                    self._attempt_counts[file_key] = attempts
                    failures.append((provider, email, str(e), attempts))

        return successes, failures

    def refresh_provider(self, provider: str):
        """Force-refresh all tokens for a specific provider."""
        prefix = PROVIDER_PREFIXES.get(provider)
        if not prefix:
            log.warning(f"Unknown provider: {provider}")
            return

        refresher = PROVIDER_REFRESHERS[provider]
        for token_file in self.auth_dir.glob(f"{prefix}*.json"):
            rel = token_file.relative_to(self.auth_dir)
            if any(part.startswith(".") for part in rel.parts[:-1]):
                continue
            data = _read_token(token_file)
            if not data or data.get("disabled"):
                continue
            email = data.get("email", token_file.stem)
            try:
                # Override expiry check — force refresh
                data_copy = dict(data)
                data_copy["expired"] = _now_iso()  # Force it to look expired
                if "token" in data_copy and "expiry" in data_copy["token"]:
                    data_copy["token"]["expiry"] = _now_iso()
                ok, msg = refresher(token_file, data_copy, self.dry_run)
                if ok:
                    log.info(f"Force-refreshed {provider}/{email}: {msg}")
                else:
                    log.error(f"Force-refresh failed {provider}/{email}: {msg}")
            except Exception as e:
                log.error(f"Force-refresh exception {provider}/{email}: {e}")
