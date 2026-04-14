"""Circuit breaker for pipelines — trips after repeated failures and blocks runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


STATE_CLOSED = "closed"      # normal operation
STATE_OPEN = "open"          # tripped, blocking runs
STATE_HALF_OPEN = "half_open"  # testing recovery


@dataclass
class CircuitBreaker:
    pipeline: str
    failure_threshold: int = 3
    recovery_timeout: int = 60  # seconds before half-open probe
    state: str = STATE_CLOSED
    failure_count: int = 0
    tripped_at: Optional[datetime] = None
    last_probe_success: Optional[datetime] = None

    def is_open(self) -> bool:
        """Return True if the circuit is open (blocking runs)."""
        if self.state == STATE_OPEN:
            if self.tripped_at and datetime.utcnow() - self.tripped_at >= timedelta(seconds=self.recovery_timeout):
                self.state = STATE_HALF_OPEN
                return False
            return True
        return False

    def allow_run(self) -> bool:
        """Return True if a pipeline run should be permitted."""
        return not self.is_open()

    def record_success(self) -> None:
        """Record a successful run; resets the breaker if half-open."""
        if self.state == STATE_HALF_OPEN:
            self.state = STATE_CLOSED
            self.failure_count = 0
            self.tripped_at = None
            self.last_probe_success = datetime.utcnow()
        elif self.state == STATE_CLOSED:
            self.failure_count = 0

    def record_failure(self) -> None:
        """Record a failed run; trips the breaker when threshold is reached."""
        if self.state == STATE_HALF_OPEN:
            # probe failed — stay open
            self.state = STATE_OPEN
            self.tripped_at = datetime.utcnow()
            return
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.state = STATE_OPEN
            self.tripped_at = datetime.utcnow()

    def status_summary(self) -> str:
        parts = [f"pipeline={self.pipeline}", f"state={self.state}", f"failures={self.failure_count}"]
        if self.tripped_at:
            parts.append(f"tripped_at={self.tripped_at.isoformat()}")
        return " ".join(parts)


class CircuitBreakerRegistry:
    """Manages circuit breakers for multiple pipelines."""

    def __init__(self, failure_threshold: int = 3, recovery_timeout: int = 60) -> None:
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout

    def get(self, pipeline: str) -> CircuitBreaker:
        if pipeline not in self._breakers:
            self._breakers[pipeline] = CircuitBreaker(
                pipeline=pipeline,
                failure_threshold=self._failure_threshold,
                recovery_timeout=self._recovery_timeout,
            )
        return self._breakers[pipeline]

    def all(self) -> Dict[str, CircuitBreaker]:
        return dict(self._breakers)

    def reset(self, pipeline: str) -> None:
        """Manually reset a breaker to closed state."""
        cb = self.get(pipeline)
        cb.state = STATE_CLOSED
        cb.failure_count = 0
        cb.tripped_at = None
