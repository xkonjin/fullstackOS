"""Dual-stack TCP + HTTP health probes for monitored services.

Probes both 127.0.0.1 (IPv4) and ::1 (IPv6) for each port.
Returns ServiceStatus dicts compatible with SharedState.update_service().
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time

from ..config import HTTP_PROBE_TIMEOUT, PROBE_SERVICE_TIMEOUT, SERVICES, TCP_CONNECT_TIMEOUT

log = logging.getLogger("sentinel.checks.port_health")


async def check_all_ports() -> dict[str, dict]:
    """Probe all configured services. Returns {name: status_dict}."""
    tasks = {name: probe_service(name, svc) for name, svc in SERVICES.items()}
    results = {}
    for name, coro in tasks.items():
        try:
            results[name] = await coro
        except Exception:
            log.exception("probe failed for %s", name)
            results[name] = _down_result(name, SERVICES[name].get("port", 0))
    return results


async def probe_service(name: str, svc: dict) -> dict:
    """Probe a single service on both IPv4 and IPv6.

    Wraps all probes in an overall timeout (``PROBE_SERVICE_TIMEOUT``,
    default 10 s) so the function cannot hang indefinitely even if
    individual ``asyncio.wait_for`` calls inside ``_http_probe`` fail
    to fire (e.g. DNS resolution stalls before the connection attempt).
    """
    try:
        return await asyncio.wait_for(
            _probe_service_inner(name, svc),
            timeout=PROBE_SERVICE_TIMEOUT,
        )
    except asyncio.TimeoutError:
        port = svc.get("port", 0)
        log.warning("probe_service overall timeout for %s (port %d)", name, port)
        return _down_result(name, port)


async def _probe_service_inner(name: str, svc: dict) -> dict:
    """Inner implementation of probe_service (no overall timeout guard)."""
    port = svc["port"]
    health_path = svc.get("health_path", "/health")

    ipv4_ok, ipv4_latency = await _http_probe("127.0.0.1", port, health_path)
    ipv6_ok, ipv6_latency = await _http_probe("::1", port, health_path)

    if ipv4_ok or ipv6_ok:
        latency = ipv4_latency if ipv4_ok else ipv6_latency
        if ipv4_ok and ipv6_ok:
            latency = min(ipv4_latency, ipv6_latency)

        # Mismatch = degraded
        if ipv4_ok != ipv6_ok:
            status = "degraded"
        else:
            status = "healthy"

        return {
            "status": status,
            "port": port,
            "latency_ms": latency,
            "ipv4": ipv4_ok,
            "ipv6": ipv6_ok,
        }

    return _down_result(name, port)


async def _http_probe(host: str, port: int, path: str) -> tuple[bool, int]:
    """HTTP GET probe. Returns (success, latency_ms)."""
    t0 = time.monotonic()
    try:
        # Determine socket family
        family = socket.AF_INET6 if ":" in host else socket.AF_INET

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, family=family),
            timeout=TCP_CONNECT_TIMEOUT,
        )

        # Build HTTP/1.0 request
        host_header = f"[{host}]" if ":" in host else host
        request = (
            f"GET {path} HTTP/1.0\r\n"
            f"Host: {host_header}:{port}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        writer.write(request.encode())
        await writer.drain()

        # Read response (just status line is enough)
        response = await asyncio.wait_for(
            reader.read(1024),
            timeout=HTTP_PROBE_TIMEOUT - TCP_CONNECT_TIMEOUT,
        )
        writer.close()

        latency = round((time.monotonic() - t0) * 1000)

        if response:
            status_line = response.split(b"\r\n", 1)[0].decode(errors="replace")
            # HTTP/1.x 2xx or 3xx = healthy
            parts = status_line.split(None, 2)
            if len(parts) >= 2:
                code = int(parts[1])
                if code < 400:
                    return True, latency

        return False, latency

    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        return False, -1
    except Exception:
        return False, -1


async def tcp_probe(host: str, port: int) -> tuple[bool, int]:
    """Raw TCP connect probe. Returns (reachable, latency_ms)."""
    t0 = time.monotonic()
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, family=family),
            timeout=TCP_CONNECT_TIMEOUT,
        )
        latency = round((time.monotonic() - t0) * 1000)
        writer.close()
        return True, latency
    except Exception:
        return False, -1


def _down_result(name: str, port: int) -> dict:
    return {
        "status": "down",
        "port": port,
        "latency_ms": -1,
        "ipv4": False,
        "ipv6": False,
    }
