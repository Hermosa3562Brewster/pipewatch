"""Per-pipeline run throttling: enforce minimum intervals between runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


@dataclass
class ThrottleConfig:
    min_interval_seconds: float = 60.0
    enabled: bool = True

    @property
    def min_interval(self) -> timedelta:
        return timedelta(seconds=self.min_interval_seconds)


@dataclass
class ThrottleStatus:
    pipeline: str
    last_run_at: Optional[datetime]
    min_interval_seconds: float
    is_throttled: bool
    seconds_until_allowed: float

    def summary(self) -> str:
        if not self.is_throttled:
            return f"{self.pipeline}: allowed (next run ready)"
        return (
            f"{self.pipeline}: throttled "
            f"({self.seconds_until_allowed:.1f}s remaining)"
        )


class PipelineThrottleManager:
    def __init__(self) -> None:
        self._configs: Dict[str, ThrottleConfig] = {}
        self._last_run: Dict[str, datetime] = {}

    def configure(self, pipeline: str, min_interval_seconds: float = 60.0, enabled: bool = True) -> None:
        self._configs[pipeline] = ThrottleConfig(
            min_interval_seconds=min_interval_seconds,
            enabled=enabled,
        )

    def record_run(self, pipeline: str, at: Optional[datetime] = None) -> None:
        self._last_run[pipeline] = at or datetime.utcnow()

    def is_allowed(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        cfg = self._configs.get(pipeline)
        if cfg is None or not cfg.enabled:
            return True
        last = self._last_run.get(pipeline)
        if last is None:
            return True
        now = now or datetime.utcnow()
        return (now - last) >= cfg.min_interval

    def seconds_until_allowed(self, pipeline: str, now: Optional[datetime] = None) -> float:
        cfg = self._configs.get(pipeline)
        if cfg is None or not cfg.enabled:
            return 0.0
        last = self._last_run.get(pipeline)
        if last is None:
            return 0.0
        now = now or datetime.utcnow()
        elapsed = (now - last).total_seconds()
        remaining = cfg.min_interval_seconds - elapsed
        return max(0.0, remaining)

    def status(self, pipeline: str, now: Optional[datetime] = None) -> ThrottleStatus:
        now = now or datetime.utcnow()
        cfg = self._configs.get(pipeline)
        min_interval = cfg.min_interval_seconds if cfg else 0.0
        remaining = self.seconds_until_allowed(pipeline, now)
        return ThrottleStatus(
            pipeline=pipeline,
            last_run_at=self._last_run.get(pipeline),
            min_interval_seconds=min_interval,
            is_throttled=not self.is_allowed(pipeline, now),
            seconds_until_allowed=remaining,
        )

    def all_statuses(self, now: Optional[datetime] = None) -> Dict[str, ThrottleStatus]:
        pipelines = set(self._configs) | set(self._last_run)
        return {p: self.status(p, now) for p in sorted(pipelines)}
