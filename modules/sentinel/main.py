#!/usr/bin/env python3
"""Sentinel — Independent Infrastructure Watchdog.

Usage:
  python -m sentinel              # Full mode (menu bar + web UI + monitor)
  python -m sentinel --headless   # No menu bar (for LaunchAgent)
  python -m sentinel --web-only   # Just the web dashboard

Architecture:
  Main thread:       rumps menu bar app (macOS AppKit requirement)
  Background thread: asyncio event loop with monitor + FastAPI/uvicorn

  --headless: skip rumps, run asyncio on main thread
  --web-only: skip monitor, just serve dashboard
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .state import SharedState

try:
    from fastapi import Request as _FastAPIRequest  # noqa: F401 — resolved by route annotations
except ImportError:
    _FastAPIRequest = None  # type: ignore[assignment,misc]

log = logging.getLogger("sentinel")

_WEB_PORT = 8600
CHECK_INTERVAL = 10  # seconds


# ---------------------------------------------------------------------------
# Async monitor stub — imports the real checks when available
# ---------------------------------------------------------------------------

async def run_monitor(state: SharedState, stop_event: asyncio.Event) -> None:
    """Periodically run health checks and update shared state."""
    # Lazy import so the integration layer doesn't hard-depend on checks/
    try:
        from .checks import run_all_checks  # type: ignore[import-not-found]
    except ImportError:
        run_all_checks = None
        log.warning("sentinel.checks not found — monitor will report stubs only")

    while not stop_event.is_set():
        try:
            if run_all_checks is not None:
                results = await run_all_checks()
            else:
                results = _stub_check_results()

            for name, info in results.get("services", {}).items():
                state.update_service(name, info)
            if "tokens" in results:
                state.update_tokens(results["tokens"])

            snapshot = state.get_snapshot()
            state.push_sse("state_update", snapshot)
        except Exception:
            log.exception("monitor cycle failed")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=CHECK_INTERVAL)
        except asyncio.TimeoutError:
            pass  # normal — loop again


def _stub_check_results() -> dict:
    """Placeholder results when real checks aren't wired yet."""
    return {
        "services": {
            "CLIProxyAPI": {"status": "unknown", "port": 8317},
            "Orchestrator": {"status": "unknown", "port": 8318},
            "Fleet Gateway": {"status": "unknown", "port": 4105},
        },
    }


# ---------------------------------------------------------------------------
# Web server (FastAPI + uvicorn)
# ---------------------------------------------------------------------------

def _build_app(state: SharedState):
    """Build the FastAPI app with full template UI, static files, and SSE."""
    try:
        from fastapi import FastAPI, Request
        from fastapi.responses import HTMLResponse, JSONResponse
        from fastapi.staticfiles import StaticFiles
        from fastapi.templating import Jinja2Templates
        from starlette.responses import StreamingResponse
    except ImportError:
        log.error("fastapi not installed — web UI unavailable")
        return None

    import asyncio as _asyncio
    import json
    import time

    ROOT = Path(__file__).parent
    app = FastAPI(title="Sentinel", docs_url=None, redoc_url=None)
    app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")
    templates = Jinja2Templates(directory=ROOT / "templates")

    # SSE clients for HTML fragment streaming (app.py style)
    sse_clients: list[_asyncio.Queue] = []

    def _now() -> float:
        return time.time()

    def _relative_time(ts) -> str:
        if not ts or ts == "—":
            return "—"
        if isinstance(ts, str):
            try:
                from datetime import datetime, timezone
                dt = datetime.fromisoformat(ts)
                diff = (datetime.now(timezone.utc) - dt).total_seconds()
            except Exception:
                return "—"
        else:
            diff = _now() - float(ts)
        if diff < 0:
            diff = 0
        if diff < 5:
            return "just now"
        if diff < 60:
            return f"{int(diff)}s ago"
        if diff < 3600:
            return f"{int(diff // 60)}m ago"
        if diff < 86400:
            return f"{int(diff // 3600)}h ago"
        return f"{int(diff // 86400)}d ago"

    def _format_ttl(seconds: int) -> str:
        if seconds <= 0:
            return "expired"
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds // 60}m {seconds % 60}s"
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"

    def _overall_status() -> str:
        snap = state.get_snapshot()
        return snap.get("overall", "unknown")

    def _render_services_html() -> str:
        snap = state.get_snapshot()
        cards = []
        for key, svc in snap.get("services", {}).items():
            ago = _relative_time(svc.get("checked_at", 0))
            lat = svc.get("latency_ms", -1)
            latency = f'{lat}ms' if lat >= 0 else "—"
            status = svc.get("status", "unknown")
            name = svc.get("name", key)
            port = svc.get("port", "")
            cards.append(
                f'<div class="service-card glass-panel" data-status="{status}">'
                f'<div class="service-header">'
                f'<div class="service-name-row">'
                f'<span class="status-dot status-{status}"></span>'
                f'<h3 class="service-name">{name}</h3>'
                f'</div>'
                f'<span class="service-port font-mono">:{port}</span>'
                f'</div>'
                f'<div class="service-stats">'
                f'<div class="stat"><span class="stat-label">Latency</span><span class="stat-value font-mono">{latency}</span></div>'
                f'<div class="stat"><span class="stat-label">IPv4</span><span class="stat-value">{"✓" if svc.get("ipv4") else "✗"}</span></div>'
                f'<div class="stat"><span class="stat-label">IPv6</span><span class="stat-value">{"✓" if svc.get("ipv6") else "✗"}</span></div>'
                f'<div class="stat"><span class="stat-label">Checked</span><span class="stat-value">{ago}</span></div>'
                f'</div>'
                f'<button class="glass-btn glass-btn-danger" onclick="restartService(\'{key}\')">Restart</button>'
                f'</div>'
            )
        return "".join(cards).replace("\n", "")

    def _render_tokens_html() -> str:
        snap = state.get_snapshot()
        tokens = snap.get("tokens", [])
        if not tokens:
            return '<p class="text-secondary">No token files found.</p>'
        cards = []
        for tok in tokens:
            ttl = _format_ttl(tok.get("ttl_seconds", 0))
            refreshed = _relative_time(tok.get("last_refreshed", 0))
            cards.append(
                f'<div class="token-card glass-panel" data-status="{tok["status"]}">'
                f'<div class="token-header">'
                f'<span class="badge badge-{tok["provider"]}">{tok["provider"].title()}</span>'
                f'<span class="status-dot status-{tok["status"]}"></span>'
                f'</div>'
                f'<div class="token-email font-mono">{tok["email"]}</div>'
                f'<div class="token-stats">'
                f'<div class="stat"><span class="stat-label">Expires in</span><span class="stat-value font-mono">{ttl}</span></div>'
                f'<div class="stat"><span class="stat-label">Refreshed</span><span class="stat-value">{refreshed}</span></div>'
                f'</div>'
                f'<div class="token-file text-tertiary font-mono">{tok["file"]}</div>'
                f'</div>'
            )
        return "".join(cards).replace("\n", "")

    # -- SSE broadcast task --
    async def _sse_broadcast_loop():
        """Watch SharedState SSE queue and broadcast HTML fragments to SSE clients."""
        q: _asyncio.Queue[dict] = _asyncio.Queue(maxsize=64)
        state.register_sse_queue(q)
        try:
            while True:
                await q.get()  # triggered by monitor push
                html = _render_services_html()
                event_data = f"event: service_update\ndata: {html}\n\n"
                token_html = _render_tokens_html()
                event_data += f"event: token_update\ndata: {token_html}\n\n"
                for client_q in list(sse_clients):
                    try:
                        client_q.put_nowait(event_data)
                    except _asyncio.QueueFull:
                        pass
        except _asyncio.CancelledError:
            pass
        finally:
            state.unregister_sse_queue(q)

    @app.on_event("startup")
    async def startup():
        _asyncio.create_task(_sse_broadcast_loop())

    # -- Page routes --
    @app.get("/", response_class=HTMLResponse)
    async def page_dashboard(request: _FastAPIRequest):
        snap = state.get_snapshot()
        return templates.TemplateResponse(request, "dashboard.html", {
            "state": snap,
            "overall": snap.get("overall", "unknown"),
            "services": snap.get("services", {}),
            "incidents": snap.get("incidents", [])[:10],
            "page": "dashboard",
        })

    @app.get("/incidents", response_class=HTMLResponse)
    async def page_incidents(request: _FastAPIRequest):
        snap = state.get_snapshot()
        return templates.TemplateResponse(request, "incidents.html", {
            "incidents": snap.get("incidents", []),
            "page": "incidents",
        })

    @app.get("/tokens", response_class=HTMLResponse)
    async def page_tokens(request: _FastAPIRequest):
        snap = state.get_snapshot()
        return templates.TemplateResponse(request, "tokens.html", {
            "tokens": snap.get("tokens", []),
            "page": "tokens",
        })

    # -- API routes --
    @app.get("/api/state")
    async def api_state():
        return JSONResponse(state.get_snapshot())

    @app.get("/api/stream")
    async def api_stream():
        q: _asyncio.Queue = _asyncio.Queue(maxsize=32)
        sse_clients.append(q)

        async def event_generator():
            try:
                yield f"event: service_update\ndata: {_render_services_html()}\n\n"
                yield f"event: token_update\ndata: {_render_tokens_html()}\n\n"
                while True:
                    data = await q.get()
                    yield data
            except _asyncio.CancelledError:
                pass
            finally:
                if q in sse_clients:
                    sse_clients.remove(q)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/api/sse")
    async def api_sse():
        q: _asyncio.Queue[dict] = _asyncio.Queue(maxsize=64)
        state.register_sse_queue(q)

        async def event_stream():
            try:
                while True:
                    msg = await q.get()
                    yield f"event: {msg['event']}\ndata: {json.dumps(msg['data'])}\n\n"
            except _asyncio.CancelledError:
                pass
            finally:
                state.unregister_sse_queue(q)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.get("/api/status")
    async def api_status():
        snap = state.get_snapshot()
        return JSONResponse({
            "overall": snap.get("overall", "unknown"),
            "services": snap.get("services", {}),
            "last_check": snap.get("last_check"),
        })

    @app.get("/api/incidents")
    async def api_incidents():
        return JSONResponse({"incidents": state.get_snapshot().get("incidents", [])})

    @app.get("/api/tokens")
    async def api_tokens():
        return JSONResponse({"tokens": state.get_snapshot().get("tokens", [])})

    @app.post("/api/actions/{component}/restart")
    async def api_restart(component: str):
        result = {"component": component, "action": "restart", "status": "triggered", "time": _now()}
        state.add_incident({
            "severity": "info",
            "component": component,
            "signature": f"manual-restart:{component}",
            "summary": f"Manual restart triggered for {component}",
        })
        msg = f'event: action_result\ndata: {json.dumps(result)}\n\n'
        for q in list(sse_clients):
            try:
                q.put_nowait(msg)
            except _asyncio.QueueFull:
                pass
        return JSONResponse(result)

    @app.post("/api/actions/tokens/rescan")
    async def api_rescan_tokens():
        try:
            from .checks.token_health import scan_tokens
            tokens = await scan_tokens()
            state.update_tokens(tokens)
            return JSONResponse({"status": "rescanned", "count": len(tokens)})
        except Exception:
            return JSONResponse({"status": "error", "count": 0})

    @app.post("/api/action/{action}/{target}")
    async def api_action(action: str, target: str):
        _handle_action(state, action, target)
        return JSONResponse({"ok": True, "action": action, "target": target})

    @app.get("/health")
    async def health():
        snap = state.get_snapshot()
        return JSONResponse({
            "status": "ok",
            "service": "sentinel",
            "port": _WEB_PORT,
            "overall": snap.get("overall", "unknown"),
        })

    return app


async def _run_web(state: SharedState, stop_event: asyncio.Event) -> None:
    """Start uvicorn serving the FastAPI app."""
    try:
        import uvicorn
    except ImportError:
        log.error("uvicorn not installed — cannot serve web UI")
        return

    app = _build_app(state)
    if app is None:
        return

    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=_WEB_PORT,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)

    # Override shutdown to respect our stop_event
    original_shutdown = server.shutdown

    async def patched_shutdown(*a, **kw):
        await original_shutdown(*a, **kw)

    server.shutdown = patched_shutdown  # type: ignore[method-assign]

    async def watch_stop():
        await stop_event.wait()
        server.should_exit = True

    asyncio.create_task(watch_stop())
    await server.serve()


# ---------------------------------------------------------------------------
# Action handler
# ---------------------------------------------------------------------------

def _handle_action(state: SharedState, action: str, target: str) -> None:
    if action == "toggle_auto_heal":
        if state.is_auto_heal_active():
            state.set_auto_heal_paused(
                True,
                until=datetime.now(timezone.utc) + timedelta(minutes=15),
            )
            log.info("auto-heal paused for 15 minutes")
        else:
            state.set_auto_heal_paused(False)
            log.info("auto-heal resumed")
    elif action == "restart":
        log.info("restart requested for %s", target)
        state.add_incident({"type": "manual_restart", "target": target})
    else:
        log.warning("unknown action: %s %s", action, target)


# ---------------------------------------------------------------------------
# Background thread running asyncio event loop
# ---------------------------------------------------------------------------

def _run_async_loop(
    state: SharedState,
    stop_event_sync: threading.Event,
    run_monitor_flag: bool,
    run_web_flag: bool,
) -> None:
    """Entry point for the daemon thread running the asyncio event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop = asyncio.Event()

    # Bridge threading.Event → asyncio.Event
    def _watch_sync():
        stop_event_sync.wait()
        loop.call_soon_threadsafe(stop.set)

    watcher = threading.Thread(target=_watch_sync, daemon=True)
    watcher.start()

    tasks = []
    if run_monitor_flag:
        tasks.append(run_monitor(state, stop))
    if run_web_flag:
        tasks.append(_run_web(state, stop))

    if tasks:
        loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    global _WEB_PORT

    parser = argparse.ArgumentParser(description="Sentinel Infrastructure Watchdog")
    parser.add_argument("--headless", action="store_true", help="No menu bar (LaunchAgent mode)")
    parser.add_argument("--web-only", action="store_true", help="Only serve the web dashboard")
    parser.add_argument("--port", type=int, default=_WEB_PORT, help="Web UI port")
    args = parser.parse_args()

    _WEB_PORT = args.port

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    state = SharedState()
    stop_sync = threading.Event()

    def _signal_handler(sig, frame):
        log.info("shutdown signal received")
        stop_sync.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    run_monitor_flag = not args.web_only
    run_web_flag = True  # always serve web

    if args.headless or args.web_only:
        # No menu bar — run asyncio on main thread
        log.info(
            "sentinel starting (headless=%s, web_only=%s, port=%d)",
            args.headless, args.web_only, _WEB_PORT,
        )
        _run_async_loop(state, stop_sync, run_monitor_flag, run_web_flag)
    else:
        # Full mode: menu bar on main thread, asyncio in background
        log.info("sentinel starting (full mode, port=%d)", _WEB_PORT)

        bg = threading.Thread(
            target=_run_async_loop,
            args=(state, stop_sync, run_monitor_flag, run_web_flag),
            daemon=True,
        )
        bg.start()

        from .menubar import create_menubar

        def state_getter():
            return state.get_snapshot()

        def action_cb(action, target):
            _handle_action(state, action, target)

        app = create_menubar(state_getter, action_cb)
        if app is not None:
            app.run()  # blocks on main thread until quit
            stop_sync.set()
            bg.join(timeout=5)
        else:
            # rumps unavailable — fall back to headless
            log.info("falling back to headless mode (rumps unavailable)")
            stop_sync.clear()
            _run_async_loop(state, stop_sync, run_monitor_flag, run_web_flag)


if __name__ == "__main__":
    main()
