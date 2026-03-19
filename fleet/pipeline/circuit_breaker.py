"""Circuit breaker pattern — Loop 27 / Wave 30

Prevents cascade failures by stopping requests to failing services.
Includes state-transition metrics for observability.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Optional, Callable, Any, Dict, List, Tuple
from enum import Enum
from functools import wraps

log = logging.getLogger("aifleet.circuit")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if recovered


@dataclass
class CircuitConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    success_threshold: int = 2


@dataclass
class CircuitMetrics:
    """Tracks state-transition counts and rejected-call metrics.

    Provides operators with visibility into how often a circuit breaker
    trips, recovers, and how many calls it rejects while open.
    """

    transitions_to_open: int = 0
    transitions_to_half_open: int = 0
    transitions_to_closed: int = 0
    rejected_calls: int = 0
    total_successes: int = 0
    total_failures: int = 0
    transition_log: List[Tuple[float, str, str]] = field(default_factory=list)
    # Each entry: (timestamp, from_state, to_state)

    _MAX_LOG_ENTRIES: int = field(default=200, repr=False)

    def record_transition(
        self, from_state: CircuitState, to_state: CircuitState
    ) -> None:
        """Record a state transition with timestamp."""
        if to_state == CircuitState.OPEN:
            self.transitions_to_open += 1
        elif to_state == CircuitState.HALF_OPEN:
            self.transitions_to_half_open += 1
        elif to_state == CircuitState.CLOSED:
            self.transitions_to_closed += 1

        self.transition_log.append((time.time(), from_state.value, to_state.value))
        # Cap log size to avoid unbounded memory growth
        if len(self.transition_log) > self._MAX_LOG_ENTRIES:
            self.transition_log = self.transition_log[-self._MAX_LOG_ENTRIES :]

    def to_dict(self) -> Dict[str, Any]:
        """Export metrics as a plain dict for JSON serialisation."""
        return {
            "transitions_to_open": self.transitions_to_open,
            "transitions_to_half_open": self.transitions_to_half_open,
            "transitions_to_closed": self.transitions_to_closed,
            "rejected_calls": self.rejected_calls,
            "total_successes": self.total_successes,
            "total_failures": self.total_failures,
            "recent_transitions": self.transition_log[-10:],
        }


class CircuitBreaker:
    """Circuit breaker implementation with transition metrics."""

    def __init__(self, name: str, config: CircuitConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0
        self.metrics = CircuitMetrics()

    def _transition(self, new_state: CircuitState) -> None:
        """Transition to *new_state*, recording the change in metrics."""
        old = self.state
        self.state = new_state
        if old != new_state:
            self.metrics.record_transition(old, new_state)

    def can_execute(self) -> bool:
        """Check if operation can be executed."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if (
                self.last_failure_time
                and (time.time() - self.last_failure_time)
                > self.config.recovery_timeout
            ):
                log.info("Circuit %s: timeout expired, entering half-open", self.name)
                self._transition(CircuitState.HALF_OPEN)
                self.half_open_calls = 0
                self.success_count = 0
                return True
            self.metrics.rejected_calls += 1
            return False

        # Half-open
        if self.half_open_calls < self.config.half_open_max_calls:
            self.half_open_calls += 1
            return True
        return False

    def record_success(self) -> None:
        """Record successful execution."""
        self.metrics.total_successes += 1
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                log.info("Circuit %s: recovered, closing", self.name)
                self._transition(CircuitState.CLOSED)
                self.failure_count = 0
                self.success_count = 0
        else:
            self.failure_count = 0

    def record_failure(self) -> None:
        """Record failed execution."""
        self.failure_count += 1
        self.metrics.total_failures += 1
        self.last_failure_time = time.time()

        if self.state == CircuitState.HALF_OPEN:
            log.warning("Circuit %s: failed in half-open, opening", self.name)
            self._transition(CircuitState.OPEN)
        elif self.failure_count >= self.config.failure_threshold:
            log.warning("Circuit %s: failure threshold reached, opening", self.name)
            self._transition(CircuitState.OPEN)

    def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status including metrics."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure": self.last_failure_time,
            "metrics": self.metrics.to_dict(),
        }


class CircuitBreakerRegistry:
    """Registry of circuit breakers."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._breakers: Dict[str, CircuitBreaker] = {}

    def get(self, name: str, config: Optional[CircuitConfig] = None) -> CircuitBreaker:
        """Get or create circuit breaker."""
        if name not in self._breakers:
            cfg = config or CircuitConfig()
            self._breakers[name] = CircuitBreaker(name, cfg)
        return self._breakers[name]

    def execute(
        self,
        name: str,
        operation: Callable[[], Any],
        config: Optional[CircuitConfig] = None,
    ) -> Any:
        """Execute operation with circuit breaker."""
        breaker = self.get(name, config)

        if not breaker.can_execute():
            raise CircuitBreakerOpen(f"Circuit {name} is open")

        try:
            result = operation()
            breaker.record_success()
            return result
        except Exception:
            breaker.record_failure()
            raise

    def get_all_status(self) -> Dict[str, Dict]:
        """Get status of all circuit breakers."""
        return {name: cb.get_status() for name, cb in self._breakers.items()}


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open."""

    pass


# Global registry — thread-local for safety in threaded HTTP servers
import threading as _threading

_registry_local = _threading.local()


def get_registry(conn: sqlite3.Connection) -> CircuitBreakerRegistry:
    """Get circuit breaker registry (thread-local)."""
    existing = getattr(_registry_local, "instance", None)
    if existing is None or existing.conn is not conn:
        _registry_local.instance = CircuitBreakerRegistry(conn)
    return _registry_local.instance


def with_circuit_breaker(
    name: str, failure_threshold: int = 5, recovery_timeout: float = 60.0
):
    """Decorator for circuit breaker."""

    def decorator(func: Callable) -> Callable:
        """Wrap *func* with a circuit breaker looked up from a thread-local registry."""

        @wraps(func)
        def wrapper(*args, **kwargs):
            """Execute the wrapped function through its circuit breaker."""
            # Get connection from args
            conn = None
            for arg in args:
                if hasattr(arg, "execute"):
                    conn = arg
                    break

            if not conn:
                return func(*args, **kwargs)

            registry = get_registry(conn)
            config = CircuitConfig(
                failure_threshold=failure_threshold, recovery_timeout=recovery_timeout
            )
            return registry.execute(name, lambda: func(*args, **kwargs), config)

        return wrapper

    return decorator
