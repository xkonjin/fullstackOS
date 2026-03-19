"""Token health checks — scan ~/.cli-proxy-api/*.json files.

Token formats:
  claude-*.json:       {type, email, access_token, expired:"ISO", refresh_token, last_refresh}
  antigravity-*.json:  {type, email, access_token, expired:"ISO", refresh_token}
  codex-*.json:        {type, email, access_token, expired:"ISO", refresh_token, last_refresh}
  gemini-*.json:       {token:{access_token, expiry:"ISO", ...}, email}
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from ..config import TOKEN_DIR

log = logging.getLogger("sentinel.checks.token_health")


async def scan_tokens() -> list[dict]:
    """Scan token files and return health status for each."""
    tokens: list[dict] = []
    token_dir = TOKEN_DIR

    if not token_dir.is_dir():
        log.debug("token directory not found: %s", token_dir)
        return tokens

    for f in sorted(token_dir.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            tok = _parse_token(f, data)
            tokens.append(tok)
        except Exception:
            log.debug("failed to parse token file: %s", f.name)
            tokens.append({
                "file": f.name,
                "provider": _guess_provider(f.name),
                "email": "—",
                "status": "error",
                "expires_at": 0,
                "ttl_seconds": 0,
                "last_refreshed": 0,
            })

    return tokens


def _parse_token(path: Path, data: dict) -> dict:
    """Parse a token file into a normalized status dict."""
    provider = data.get("type", _guess_provider(path.name))
    email = data.get("email", "—")
    disabled = data.get("disabled", False)

    # Extract expiry — different formats per provider
    expires_dt = _extract_expiry(data, provider)
    now = datetime.now(timezone.utc)

    if expires_dt:
        ttl = max(0, (expires_dt - now).total_seconds())
        expires_at = expires_dt.timestamp()
    else:
        ttl = 0
        expires_at = 0

    # Determine status
    if disabled:
        status = "disabled"
    elif expires_at == 0:
        status = "unknown"
    elif ttl <= 0:
        status = "expired"
    elif ttl < 1800:  # 30 minutes
        status = "expiring"
    else:
        status = "healthy"

    # Last refresh time
    last_refresh = data.get("last_refresh", 0)
    if isinstance(last_refresh, str):
        try:
            last_refresh = datetime.fromisoformat(last_refresh).timestamp()
        except Exception:
            last_refresh = 0

    return {
        "file": path.name,
        "provider": provider,
        "email": email,
        "status": status,
        "expires_at": expires_at,
        "ttl_seconds": int(ttl),
        "last_refreshed": last_refresh,
    }


def _extract_expiry(data: dict, provider: str) -> datetime | None:
    """Extract expiry datetime from different token formats."""
    # Standard format: "expired" field with ISO date
    expired = data.get("expired")
    if expired and isinstance(expired, str):
        try:
            dt = datetime.fromisoformat(expired)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    # Gemini format: nested token.expiry
    token_obj = data.get("token", {})
    if isinstance(token_obj, dict):
        expiry = token_obj.get("expiry")
        if expiry and isinstance(expiry, str):
            try:
                dt = datetime.fromisoformat(expiry)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

    # Some providers persist expires_at as either epoch (int/float), numeric string,
    # or ISO datetime string.
    exp = data.get("expires_at")
    if isinstance(exp, (int, float)) and exp > 0:
        return datetime.fromtimestamp(exp, tz=timezone.utc)

    if isinstance(exp, str) and exp:
        # Numeric epoch as string
        if exp.replace(".", "", 1).isdigit():
            try:
                return datetime.fromtimestamp(float(exp), tz=timezone.utc)
            except Exception:
                pass

        # ISO datetime string
        try:
            dt = datetime.fromisoformat(exp)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass

    return None


def _guess_provider(filename: str) -> str:
    """Guess provider from filename pattern."""
    stem = Path(filename).stem.lower()
    for prefix in ("claude", "antigravity", "codex", "gemini"):
        if stem.startswith(prefix):
            return prefix
    # Fallback: first part before dash
    parts = stem.split("-", 1)
    return parts[0] if parts else "unknown"
