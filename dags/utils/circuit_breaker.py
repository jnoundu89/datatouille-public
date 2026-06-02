"""Circuit breaker pattern for source resilience.

Adapted from worldmonitor circuit-breaker.ts. Tracks failures per source
and trips open when max_failures is reached. Auto-resets after cooldown.

No persistence needed — Airflow workers are ephemeral, and daily runs
make circuit state transient by design.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a circuit breaker is open."""

    def __init__(self, source: str, remaining_seconds: float):
        self.source = source
        self.remaining_seconds = remaining_seconds
        super().__init__(f"Circuit open for '{source}', retry in {remaining_seconds:.0f}s")


@dataclass
class CircuitBreaker:
    """Per-source circuit breaker.

    Transitions:
        CLOSED -> OPEN: after max_failures consecutive failures
        OPEN -> HALF_OPEN: after cooldown_seconds elapsed
        HALF_OPEN -> CLOSED: on success
        HALF_OPEN -> OPEN: on failure
    """

    source: str
    max_failures: int = 2
    cooldown_seconds: float = 300.0
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.cooldown_seconds:
                self._state = CircuitState.HALF_OPEN
                logger.info("Circuit %s: OPEN -> HALF_OPEN (cooldown elapsed)", self.source)
        return self._state

    def _record_success(self) -> None:
        if self._state in (CircuitState.HALF_OPEN, CircuitState.CLOSED):
            self._failure_count = 0
            self._state = CircuitState.CLOSED

    def _record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._failure_count >= self.max_failures:
            self._state = CircuitState.OPEN
            logger.warning(
                "Circuit %s: TRIPPED OPEN after %d failures",
                self.source,
                self._failure_count,
            )

    def call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute function through circuit breaker (sync)."""
        current = self.state
        if current == CircuitState.OPEN:
            remaining = self.cooldown_seconds - (time.monotonic() - self._last_failure_time)
            raise CircuitOpenError(self.source, max(0, remaining))

        try:
            result = fn(*args, **kwargs)
            self._record_success()
            return result
        except Exception:
            self._record_failure()
            raise

    async def async_call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute async function through circuit breaker."""
        current = self.state
        if current == CircuitState.OPEN:
            remaining = self.cooldown_seconds - (time.monotonic() - self._last_failure_time)
            raise CircuitOpenError(self.source, max(0, remaining))

        try:
            result = await fn(*args, **kwargs)
            self._record_success()
            return result
        except Exception:
            self._record_failure()
            raise

    def get_status(self) -> dict[str, Any]:
        """Get current circuit breaker status for monitoring."""
        return {
            "source": self.source,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "max_failures": self.max_failures,
            "cooldown_seconds": self.cooldown_seconds,
        }


# Global registry of circuit breakers
_breakers: dict[str, CircuitBreaker] = {}


def get_breaker(source: str, max_failures: int = 2, cooldown_seconds: float = 300.0) -> CircuitBreaker:
    """Get or create a circuit breaker for a source."""
    if source not in _breakers:
        _breakers[source] = CircuitBreaker(
            source=source,
            max_failures=max_failures,
            cooldown_seconds=cooldown_seconds,
        )
    return _breakers[source]


def get_all_statuses() -> list[dict[str, Any]]:
    """Get status of all registered circuit breakers."""
    return [cb.get_status() for cb in _breakers.values()]


def reset_all() -> None:
    """Reset all circuit breakers. Useful for testing."""
    _breakers.clear()
