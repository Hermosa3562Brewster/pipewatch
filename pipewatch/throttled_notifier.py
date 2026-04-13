"""A notifier wrapper that applies rate limiting before forwarding alert events."""

from typing import Callable, List, Optional

from pipewatch.notify import AlertEvent, InMemoryNotifier
from pipewatch.rate_limiter import RateLimiter


NotifierCallable = Callable[[AlertEvent], None]


class ThrottledNotifier:
    """Wraps another notifier and suppresses duplicate alerts within a cooldown window.

    The rate limit key is derived from the pipeline name and alert rule name,
    so each (pipeline, rule) pair is throttled independently.
    """

    def __init__(
        self,
        inner: NotifierCallable,
        default_interval_seconds: float = 300.0,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.inner = inner
        self.rate_limiter = rate_limiter or RateLimiter(default_interval_seconds)
        self._suppressed_count: int = 0

    def _key(self, event: AlertEvent) -> str:
        return f"{event.pipeline_name}::{event.rule_name}"

    def __call__(self, event: AlertEvent) -> None:
        key = self._key(event)
        if self.rate_limiter.try_acquire(key):
            self.inner(event)
        else:
            self._suppressed_count += 1

    def set_interval(self, pipeline_name: str, rule_name: str, seconds: float) -> None:
        """Set a custom throttle interval for a specific pipeline/rule pair."""
        key = f"{pipeline_name}::{rule_name}"
        self.rate_limiter.set_interval(key, seconds)

    @property
    def suppressed_count(self) -> int:
        """Total number of suppressed (throttled) alert events."""
        return self._suppressed_count

    def reset(self, pipeline_name: str, rule_name: str) -> None:
        """Manually reset throttle state for a specific pipeline/rule pair."""
        key = f"{pipeline_name}::{rule_name}"
        self.rate_limiter.reset(key)
