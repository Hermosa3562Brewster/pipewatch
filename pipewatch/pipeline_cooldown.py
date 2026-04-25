"""Pipeline cooldown tracking — enforces a minimum wait period after a failure
before a pipeline is considered eligible to run again."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


@dataclass
class CooldownConfig:
    duration_seconds: float = 60.0

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.duration_seconds)


@dataclass
class CooldownStatus:
    pipeline: str
    in_cooldown: bool
    last_failure: Optional[datetime]
    eligible_at: Optional[datetime]
    seconds_remaining: float

    def summary(self) -> str:
        if not self.in_cooldown:
            return f"{self.pipeline}: ready"
        return (
            f"{self.pipeline}: cooling down — "
            f"{self.seconds_remaining:.0f}s remaining"
        )


class CooldownManager:
    """Tracks cooldown state for pipelines after failures."""

    def __init__(self) -> None:
        self._configs: Dict[str, CooldownConfig] = {}
        self._last_failure: Dict[str, datetime] = {}

    def configure(self, pipeline: str, duration_seconds: float = 60.0) -> None:
        self._configs[pipeline] = CooldownConfig(duration_seconds=duration_seconds)

    def record_failure(self, pipeline: str, at: Optional[datetime] = None) -> None:
        if pipeline not in self._configs:
            self._configs[pipeline] = CooldownConfig()
        self._last_failure[pipeline] = at or datetime.utcnow()

    def status(self, pipeline: str, now: Optional[datetime] = None) -> Optional[CooldownStatus]:
        if pipeline not in self._configs:
            return None
        cfg = self._configs[pipeline]
        last = self._last_failure.get(pipeline)
        now = now or datetime.utcnow()
        if last is None:
            return CooldownStatus(
                pipeline=pipeline,
                in_cooldown=False,
                last_failure=None,
                eligible_at=None,
                seconds_remaining=0.0,
            )
        eligible_at = last + cfg.window
        remaining = max(0.0, (eligible_at - now).total_seconds())
        return CooldownStatus(
            pipeline=pipeline,
            in_cooldown=remaining > 0,
            last_failure=last,
            eligible_at=eligible_at,
            seconds_remaining=remaining,
        )

    def is_in_cooldown(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        s = self.status(pipeline, now=now)
        return s.in_cooldown if s is not None else False

    def clear(self, pipeline: str) -> None:
        self._last_failure.pop(pipeline, None)

    def all_pipelines(self) -> list:
        return sorted(self._configs.keys())
