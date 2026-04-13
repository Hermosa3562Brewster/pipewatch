"""Rate limiter for controlling how frequently alerts and notifications are fired."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


@dataclass
class RateLimitEntry:
    last_fired: datetime
    fire_count: int = 0

    def seconds_since_last(self) -> float:
        return (datetime.utcnow() - self.last_fired).total_seconds()


class RateLimiter:
    """Tracks per-key firing times and enforces minimum intervals between events."""

    def __init__(self, default_interval_seconds: float = 60.0):
        self.default_interval = default_interval_seconds
        self._entries: Dict[str, RateLimitEntry] = {}
        self._custom_intervals: Dict[str, float] = {}

    def set_interval(self, key: str, interval_seconds: float) -> None:
        """Override the interval for a specific key."""
        self._custom_intervals[key] = interval_seconds

    def _interval_for(self, key: str) -> float:
        return self._custom_intervals.get(key, self.default_interval)

    def is_allowed(self, key: str) -> bool:
        """Return True if the key is allowed to fire now."""
        entry = self._entries.get(key)
        if entry is None:
            return True
        return entry.seconds_since_last() >= self._interval_for(key)

    def record(self, key: str) -> None:
        """Record a firing event for the given key."""
        entry = self._entries.get(key)
        if entry is None:
            self._entries[key] = RateLimitEntry(last_fired=datetime.utcnow(), fire_count=1)
        else:
            entry.last_fired = datetime.utcnow()
            entry.fire_count += 1

    def try_acquire(self, key: str) -> bool:
        """Atomically check and record if allowed. Returns True if acquired."""
        if self.is_allowed(key):
            self.record(key)
            return True
        return False

    def reset(self, key: str) -> None:
        """Clear the rate limit state for a key."""
        self._entries.pop(key, None)

    def fire_count(self, key: str) -> int:
        """Return how many times a key has fired."""
        entry = self._entries.get(key)
        return entry.fire_count if entry else 0

    def time_until_next(self, key: str) -> Optional[float]:
        """Return seconds until the key is allowed again, or None if already allowed."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        remaining = self._interval_for(key) - entry.seconds_since_last()
        return max(0.0, remaining) if remaining > 0 else None
