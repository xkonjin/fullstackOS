"""Pytest fixtures for Sentinel tests."""

from __future__ import annotations

import socket
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def token_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_port():
    """Find an available port for testing."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
def shared_state():
    from modules.sentinel.state import SharedState

    return SharedState()
