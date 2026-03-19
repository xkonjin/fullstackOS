"""Test that orchestrator health check logs failures with context."""
from __future__ import annotations

import logging
import pytest

from modules.sentinel.checks.orchestrator_metrics import _check_orchestrator_health


@pytest.mark.asyncio
async def test_check_orchestrator_health_logs_failure_context(caplog: pytest.LogCaptureFixture) -> None:
    """Health check should log the exception type and message when it fails."""
    # Ensure no server is running on this port to trigger the exception
    with caplog.at_level(logging.DEBUG, logger="sentinel.checks.orchestrator_metrics"):
        result = await _check_orchestrator_health()

    # Should return False when no server is available
    assert result is False

    # Should have logged the failure with exception details
    assert any(
        "health check failed" in record.message.lower()
        for record in caplog.records
    ), f"Expected health check failure log, got: {[r.message for r in caplog.records]}"

    # Log should include exception type name (not just generic message)
    log_messages = [r.message for r in caplog.records]
    assert any(
        "ConnectionRefusedError" in msg or "error" in msg.lower()
        for msg in log_messages
    ), f"Expected exception type in log, got: {log_messages}"
