from __future__ import annotations

import json
import logging
import os
import random
import sqlite3
import time
from enum import Enum

import urllib3

log = logging.getLogger("aifleet.telegram")

# Default timeout (seconds) before a waiting_human gate auto-expires.
# 0 means wait forever (requires explicit approval).
DEFAULT_GATE_TIMEOUT_SECONDS = int(os.environ.get("GATE_TIMEOUT_SECONDS", str(48 * 3600)))

MAX_SEND_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# Rate-limiting: min seconds between messages to same chat
MIN_SEND_INTERVAL_SECONDS = 1

# Connection health thresholds
DEGRADED_THRESHOLD = 3
DISCONNECTED_THRESHOLD = 5
MAX_RECONNECT_RETRIES = 10
BASE_RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 300
JITTER_PERCENT = 0.25

# ---------------------------------------------------------------------------
# Connection-pooled HTTP client for Telegram API
# ---------------------------------------------------------------------------
# urllib.request.urlopen creates a new TCP+TLS connection per request.
# Using urllib3.PoolManager gives us keep-alive connection reuse, reducing
# latency and resource usage for notification bursts and polling loops.

_http_pool: urllib3.PoolManager | None = None


def _get_http_pool() -> urllib3.PoolManager:
    """Return a module-level connection pool (created on first use)."""
    global _http_pool
    if _http_pool is None:
        _http_pool = urllib3.PoolManager(
            num_pools=2,  # Telegram API + maybe Sentry
            maxsize=4,  # Concurrent connections per host
            retries=False,  # We handle retries ourselves
            timeout=urllib3.Timeout(connect=10, read=20),
        )
    return _http_pool


def _pool_request(
    method: str,
    url: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15,
) -> tuple[int, bytes]:
    """Make an HTTP request using the connection pool.

    Returns (status_code, response_body).
    Raises urllib3.exceptions.HTTPError on transport failure.
    """
    pool = _get_http_pool()
    resp = pool.request(
        method,
        url,
        body=body,
        headers=headers or {},
        timeout=urllib3.Timeout(connect=10, read=timeout),
    )
    return resp.status, resp.data


class ConnectionState(Enum):
    CONNECTED = "connected"
    DEGRADED = "degraded"
    DISCONNECTED = "disconnected"


class ConnectionHealth:
    """Track Telegram connection health and consecutive failures."""

    def __init__(self):
        self._consecutive_failures = 0
        self._state = ConnectionState.CONNECTED
        self._last_check_at = None

    def record_success(self):
        """Reset to healthy state on successful send."""
        self._consecutive_failures = 0
        self._state = ConnectionState.CONNECTED
        self._last_check_at = _now_ts()

    def record_failure(self):
        """Track failure and update state based on thresholds."""
        self._consecutive_failures += 1
        self._last_check_at = _now_ts()

        if self._consecutive_failures >= DISCONNECTED_THRESHOLD:
            self._state = ConnectionState.DISCONNECTED
        elif self._consecutive_failures >= DEGRADED_THRESHOLD:
            self._state = ConnectionState.DEGRADED

    def get_health(self) -> dict:
        """Return current health state with timestamp."""
        return {
            "state": self._state.value,
            "consecutive_failures": self._consecutive_failures,
            "last_check_at": self._last_check_at,
        }


# Global connection health tracker
_connection_health = ConnectionHealth()


def _now_ts() -> int:
    return int(time.time())


def send_telegram(
    bot_token: str,
    chat_id: str,
    message: str,
    parse_mode: str = "Markdown",
) -> tuple[bool, str, int | None]:
    """Send message via Telegram Bot API with retry and connection pooling.

    Returns (success, error_msg, message_id).
    message_id is the Telegram message ID on success, None on failure.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": parse_mode,
        }
    ).encode("utf-8")

    last_error = ""
    for attempt in range(1, MAX_SEND_RETRIES + 1):
        try:
            status, body = _pool_request(
                "POST",
                url,
                body=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            result = json.loads(body)
            if result.get("ok"):
                msg_id = result.get("result", {}).get("message_id")
                _connection_health.record_success()
                return (True, "", msg_id)
            last_error = f"telegram api error: {result.get('description', 'unknown')}"
            log.warning(
                "Telegram API error (attempt %d/%d): %s",
                attempt,
                MAX_SEND_RETRIES,
                last_error,
            )
        except Exception as e:
            last_error = f"send error: {str(e)}"
            log.warning(
                "Telegram send error (attempt %d/%d): %s",
                attempt,
                MAX_SEND_RETRIES,
                last_error,
            )

        if attempt < MAX_SEND_RETRIES:
            time.sleep(RETRY_DELAY_SECONDS * attempt)

    log.error(
        "Telegram send failed after %d attempts: %s", MAX_SEND_RETRIES, last_error
    )
    _connection_health.record_failure()
    return (False, last_error, None)


def is_configured() -> bool:
    """Check if Telegram HITL is configured."""
    return bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID"))


def check_connection() -> tuple[bool, str]:
    """Verify Telegram bot token is valid by calling getMe. Returns (ok, error)."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        return False, "TELEGRAM_BOT_TOKEN not set"

    url = f"https://api.telegram.org/bot{bot_token}/getMe"
    try:
        status, body = _pool_request("GET", url, timeout=10)
        result = json.loads(body)
        if result.get("ok"):
            bot_name = result.get("result", {}).get("username", "unknown")
            log.info("Telegram connection verified: @%s", bot_name)
            _connection_health.record_success()
            return True, ""
        _connection_health.record_failure()
        return False, f"getMe failed: {result.get('description', 'unknown')}"
    except Exception as e:
        _connection_health.record_failure()
        return False, f"connection check failed: {e}"


def reconnect_with_backoff() -> tuple[bool, str]:
    """Attempt to reconnect with exponential backoff. Returns (ok, error)."""
    delay = BASE_RECONNECT_DELAY

    for attempt in range(1, MAX_RECONNECT_RETRIES + 1):
        log.info("Reconnection attempt %d/%d", attempt, MAX_RECONNECT_RETRIES)
        ok, err = check_connection()

        if ok:
            log.info("Reconnection successful after %d attempts", attempt)
            return True, ""

        if attempt < MAX_RECONNECT_RETRIES:
            jitter = delay * JITTER_PERCENT * (random.random() * 2 - 1)
            sleep_time = delay + jitter
            log.info("Reconnection failed, waiting %.1fs before retry", sleep_time)
            time.sleep(sleep_time)
            delay = min(delay * 2, MAX_RECONNECT_DELAY)

    log.error("Reconnection failed after %d attempts", MAX_RECONNECT_RETRIES)
    return False, f"reconnection failed after {MAX_RECONNECT_RETRIES} attempts"


def send_gate_notification(
    pipeline_id: str,
    pipeline_title: str,
    stage_name: str,
    project_repo: str,
    conn: sqlite3.Connection | None = None,
) -> tuple[bool, str, int | None]:
    """Send a human gate notification via Telegram. Reads config from env vars.
    Returns (success, error_msg, telegram_msg_id)."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        log.warning(
            "Telegram not configured -- gate notification for pipeline %s stage %s will not be sent",
            pipeline_id,
            stage_name,
        )
        return (False, "telegram not configured", None)

    message = (
        f"*Pipeline Gate*\n\n"
        f"Pipeline: `{pipeline_id}`\n"
        f"Title: {pipeline_title}\n"
        f"Stage: *{stage_name}*\n"
        f"Project: `{project_repo}`\n\n"
        f"Approve: `ai-fleet pipeline approve {pipeline_id}`\n"
        f"Reject: `ai-fleet pipeline reject {pipeline_id}`"
    )

    ok, err, msg_id = send_telegram(bot_token, chat_id, message)
    if ok and msg_id:
        log.info(
            "Gate notification sent for pipeline %s stage %s (msg_id=%s)",
            pipeline_id,
            stage_name,
            msg_id,
        )
        if conn:
            record_delivery(conn, pipeline_id, msg_id, "gate_notification")
    else:
        log.error(
            "Failed to send gate notification for pipeline %s stage %s: %s",
            pipeline_id,
            stage_name,
            err,
        )
    return ok, err, msg_id


def send_pipeline_notification(
    pipeline_id: str,
    pipeline_title: str,
    status: str,
    details: str = "",
    conn: sqlite3.Connection | None = None,
) -> tuple[bool, str]:
    """Send pipeline status update via Telegram. Returns (success, error_msg)."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        return (False, "telegram not configured")

    if status == "completed":
        message = f"*Pipeline Complete*\n\n`{pipeline_id}`: {pipeline_title}\n{details}"
    elif status == "failed":
        message = f"*Pipeline Failed*\n\n`{pipeline_id}`: {pipeline_title}\n{details}"
    elif status == "gate_timeout":
        message = f"*Gate Timeout*\n\n`{pipeline_id}`: {pipeline_title}\nGate expired without approval.\n{details}"
    else:
        message = f"*Pipeline Update*\n\n`{pipeline_id}`: {pipeline_title}\nStatus: {status}\n{details}"

    ok, err, msg_id = send_telegram(bot_token, chat_id, message)
    if ok and msg_id and conn:
        record_delivery(conn, pipeline_id, msg_id, f"status_{status}")
    return ok, err


# ---------------------------------------------------------------------------
# Pending approval queue: persisted in DB for reliability across restarts
# ---------------------------------------------------------------------------


def init_approval_queue_table(conn: sqlite3.Connection) -> None:
    """Create the pending_approvals table for tracking outstanding HITL requests."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            telegram_msg_id INTEGER,
            requested_at INTEGER NOT NULL,
            timeout_at INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            response TEXT,
            responded_at INTEGER,
            reminder_count INTEGER DEFAULT 0,
            last_reminder_at INTEGER,
            UNIQUE(pipeline_id, stage_name)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pending_approvals_status ON pending_approvals(status)"
    )
    conn.commit()


def init_message_log_table(conn: sqlite3.Connection) -> None:
    """Create the telegram_message_log table for delivery tracking."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS telegram_message_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL,
            telegram_msg_id INTEGER NOT NULL,
            message_type TEXT NOT NULL,
            sent_at INTEGER NOT NULL,
            acknowledged_at INTEGER
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_message_log_pipeline ON telegram_message_log(pipeline_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_message_log_ack ON telegram_message_log(acknowledged_at)"
    )
    conn.commit()


def record_delivery(
    conn: sqlite3.Connection,
    pipeline_id: str,
    telegram_msg_id: int,
    message_type: str,
) -> None:
    """Record a sent message for delivery tracking."""
    conn.execute(
        """
        INSERT INTO telegram_message_log (pipeline_id, telegram_msg_id, message_type, sent_at)
        VALUES (?, ?, ?, ?)
    """,
        (pipeline_id, telegram_msg_id, message_type, _now_ts()),
    )
    conn.commit()


def get_unacknowledged_messages(
    conn: sqlite3.Connection,
    max_age_seconds: int = 3600,
) -> list[dict]:
    """Find messages sent but not acknowledged within max_age_seconds."""
    conn.row_factory = sqlite3.Row
    cutoff = _now_ts() - max_age_seconds
    rows = conn.execute(
        """
        SELECT * FROM telegram_message_log
        WHERE acknowledged_at IS NULL AND sent_at >= ?
        ORDER BY sent_at ASC
    """,
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def queue_approval(
    conn: sqlite3.Connection,
    pipeline_id: str,
    stage_name: str,
    telegram_msg_id: int | None = None,
    timeout_seconds: int = DEFAULT_GATE_TIMEOUT_SECONDS,
) -> int:
    """Add an approval request to the queue. Returns the queue entry id."""
    now = _now_ts()
    conn.execute(
        """
        INSERT OR REPLACE INTO pending_approvals
        (pipeline_id, stage_name, telegram_msg_id, requested_at, timeout_at, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
    """,
        (pipeline_id, stage_name, telegram_msg_id, now, now + timeout_seconds),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM pending_approvals WHERE pipeline_id = ? AND stage_name = ?",
        (pipeline_id, stage_name),
    ).fetchone()
    return row[0] if row else 0


def resolve_approval(
    conn: sqlite3.Connection,
    pipeline_id: str,
    response: str = "approved",
) -> bool:
    """Mark a pending approval as resolved. Returns True if found and updated."""
    result = conn.execute(
        """
        UPDATE pending_approvals
        SET status = 'resolved', response = ?, responded_at = ?
        WHERE pipeline_id = ? AND status = 'pending'
    """,
        (response, _now_ts(), pipeline_id),
    )
    conn.commit()
    return result.rowcount > 0


def get_pending_approvals(conn: sqlite3.Connection) -> list[dict]:
    """Get all pending approvals that haven't timed out."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM pending_approvals
        WHERE status = 'pending' AND timeout_at > ?
        ORDER BY requested_at ASC
    """,
        (_now_ts(),),
    ).fetchall()
    return [dict(r) for r in rows]


def get_timed_out_approvals(conn: sqlite3.Connection) -> list[dict]:
    """Get approvals that have exceeded their timeout."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM pending_approvals
        WHERE status = 'pending' AND timeout_at <= ?
        ORDER BY requested_at ASC
    """,
        (_now_ts(),),
    ).fetchall()
    return [dict(r) for r in rows]


def send_reminder(
    conn: sqlite3.Connection,
    pipeline_id: str,
    stage_name: str,
    pipeline_title: str,
    hours_waiting: float,
) -> tuple[bool, str]:
    """Send a reminder for a pending approval. Returns (success, error)."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        return False, "telegram not configured"

    message = (
        f"*Reminder: Approval Pending*\n\n"
        f"Pipeline `{pipeline_id}` ({pipeline_title}) "
        f"has been waiting {hours_waiting:.1f}h at stage *{stage_name}*.\n\n"
        f"Approve: `ai-fleet pipeline approve {pipeline_id}`\n"
        f"Reject: `ai-fleet pipeline reject {pipeline_id}`"
    )

    ok, err, _ = send_telegram(bot_token, chat_id, message)

    if ok:
        conn.execute(
            """
            UPDATE pending_approvals
            SET reminder_count = reminder_count + 1, last_reminder_at = ?
            WHERE pipeline_id = ? AND stage_name = ? AND status = 'pending'
        """,
            (_now_ts(), pipeline_id, stage_name),
        )
        conn.commit()

    return ok, err


def drain_pending_notifications(conn: sqlite3.Connection) -> tuple[int, int]:
    """Resend gate notifications for approvals that failed during disconnection.

    Returns (attempted, succeeded) counts.
    """
    # Find pending approvals without telegram_msg_id (failed to send)
    conn.row_factory = sqlite3.Row
    failed_notifications = conn.execute("""
        SELECT pipeline_id, stage_name FROM pending_approvals
        WHERE status = 'pending' AND telegram_msg_id IS NULL
        ORDER BY requested_at ASC
    """).fetchall()

    if not failed_notifications:
        log.info("No failed notifications to drain")
        return 0, 0

    log.info(
        "Draining %d failed notifications after reconnection", len(failed_notifications)
    )

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        log.warning("Cannot drain notifications: Telegram not configured")
        return 0, 0

    attempted = 0
    succeeded = 0

    for row in failed_notifications:
        pipeline_id = row["pipeline_id"]
        stage_name = row["stage_name"]
        attempted += 1

        # Fetch pipeline details for message
        pipeline_row = conn.execute(
            "SELECT title, project_repo FROM pipeline_runs WHERE pipeline_id = ?",
            (pipeline_id,),
        ).fetchone()

        if not pipeline_row:
            log.warning(
                "Pipeline %s not found, skipping notification drain", pipeline_id
            )
            continue

        pipeline_title = pipeline_row["title"] or "Unknown"
        project_repo = pipeline_row["project_repo"] or "Unknown"

        message = (
            f"*Pipeline Gate* (resent after reconnection)\n\n"
            f"Pipeline: `{pipeline_id}`\n"
            f"Title: {pipeline_title}\n"
            f"Stage: *{stage_name}*\n"
            f"Project: `{project_repo}`\n\n"
            f"Approve: `ai-fleet pipeline approve {pipeline_id}`\n"
            f"Reject: `ai-fleet pipeline reject {pipeline_id}`"
        )

        ok, err, msg_id = send_telegram(bot_token, chat_id, message)

        if ok and msg_id:
            conn.execute(
                """
                UPDATE pending_approvals
                SET telegram_msg_id = ?
                WHERE pipeline_id = ? AND stage_name = ?
            """,
                (msg_id, pipeline_id, stage_name),
            )
            conn.commit()
            succeeded += 1
            log.info(
                "Resent gate notification for %s stage %s (msg_id=%s)",
                pipeline_id,
                stage_name,
                msg_id,
            )
        else:
            log.error(
                "Failed to resend gate notification for %s stage %s: %s",
                pipeline_id,
                stage_name,
                err,
            )

    log.info(
        "Drain complete: %d/%d notifications resent successfully", succeeded, attempted
    )
    return attempted, succeeded


# ---------------------------------------------------------------------------
# Telegram command listener (polling-based)
# ---------------------------------------------------------------------------

_COMMAND_LISTENER_STOP = None
_COMMAND_LISTENER_THREAD = None
_LAST_UPDATE_ID = 0  # Only modified by single daemon thread — no lock needed


def _get_updates(bot_token: str, offset: int = 0, timeout: int = 30) -> list[dict]:
    """Long-poll for new Telegram updates (uses connection pool)."""
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    payload = json.dumps(
        {"offset": offset, "timeout": timeout, "allowed_updates": ["message"]}
    ).encode("utf-8")
    try:
        _status, body = _pool_request(
            "POST",
            url,
            body=payload,
            headers={"Content-Type": "application/json"},
            timeout=timeout + 10,
        )
        result = json.loads(body)
        if result.get("ok"):
            return result.get("result", [])
    except Exception as e:
        log.warning("Failed to get updates: %s", e)
    return []


def _handle_command(
    command: str,
    args_text: str,
    chat_id: str,
    bot_token: str,
    db_path: str = "",
) -> str:
    """Handle a Telegram command via unified command router."""
    from pipeline.command_router import dispatch

    return dispatch(command, args_text, db_path)


def _command_listener_loop(
    bot_token: str, chat_id: str, db_path: str, stop_event
) -> None:
    """Main command listener loop using long polling."""
    global _LAST_UPDATE_ID
    log.info("Telegram command listener started")

    while not stop_event.is_set():
        updates = _get_updates(bot_token, offset=_LAST_UPDATE_ID + 1, timeout=30)

        for update in updates:
            update_id = update.get("update_id", 0)
            if update_id > _LAST_UPDATE_ID:
                _LAST_UPDATE_ID = update_id

            message = update.get("message", {})
            msg_chat_id = str(message.get("chat", {}).get("id", ""))
            text = str(message.get("text", "")).strip()

            # Only respond to authorized chat
            if msg_chat_id != str(chat_id):
                continue

            if not text.startswith("/"):
                continue

            parts = text.split(None, 1)
            command = parts[0].lower().split("@")[0]  # Strip @botname suffix
            args_text = parts[1] if len(parts) > 1 else ""

            try:
                response = _handle_command(
                    command, args_text, chat_id, bot_token, db_path
                )
            except Exception as e:
                response = f"Error: {e}"

            send_telegram(bot_token, chat_id, response)

    log.info("Telegram command listener stopped")


def start_command_listener(db_path: str = "") -> bool:
    """Start the Telegram command listener in a background thread. Returns True if started."""
    import threading

    global _COMMAND_LISTENER_STOP, _COMMAND_LISTENER_THREAD

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        log.warning("Cannot start command listener: Telegram not configured")
        return False

    if _COMMAND_LISTENER_THREAD and _COMMAND_LISTENER_THREAD.is_alive():
        return False

    _COMMAND_LISTENER_STOP = threading.Event()
    _COMMAND_LISTENER_THREAD = threading.Thread(
        target=_command_listener_loop,
        args=(bot_token, chat_id, db_path, _COMMAND_LISTENER_STOP),
        daemon=True,
        name="telegram-command-listener",
    )
    _COMMAND_LISTENER_THREAD.start()
    return True


def stop_command_listener() -> bool:
    """Stop the Telegram command listener. Returns True if stopped."""
    global _COMMAND_LISTENER_STOP, _COMMAND_LISTENER_THREAD

    if not _COMMAND_LISTENER_THREAD or not _COMMAND_LISTENER_THREAD.is_alive():
        return False

    _COMMAND_LISTENER_STOP.set()
    _COMMAND_LISTENER_THREAD.join(timeout=35)
    _COMMAND_LISTENER_THREAD = None
    _COMMAND_LISTENER_STOP = None
    return True


def command_listener_running() -> bool:
    return _COMMAND_LISTENER_THREAD is not None and _COMMAND_LISTENER_THREAD.is_alive()
