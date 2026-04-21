"""Track and report on pipelines that have not produced output recently."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class StalenessConfig:
    """Configuration for staleness detection."""
    pipeline: str
    max_age_seconds: float

    @property
    def max_age(self) -> timedelta:
        return timedelta(seconds=self.max_age_seconds)


@dataclass
class StalenessReport:
    """Result of a staleness check for a single pipeline."""
    pipeline: str
    last_seen: Optional[datetime]
    max_age_seconds: float
    is_stale: bool
    age_seconds: Optional[float]

    def summary(self) -> str:
        if self.last_seen is None:
            return f"{self.pipeline}: never seen (stale)"
        status = "STALE" if self.is_stale else "fresh"
        return (
            f"{self.pipeline}: last seen {self.age_seconds:.1f}s ago "
            f"(limit {self.max_age_seconds}s) [{status}]"
        )


class StalenessTracker:
    """Records heartbeats and checks pipelines for staleness."""

    def __init__(self) -> None:
        self._configs: Dict[str, StalenessConfig] = {}
        self._last_seen: Dict[str, datetime] = {}

    def configure(self, pipeline: str, max_age_seconds: float) -> None:
        """Register a pipeline with a maximum allowed age between heartbeats."""
        self._configs[pipeline] = StalenessConfig(
            pipeline=pipeline, max_age_seconds=max_age_seconds
        )

    def heartbeat(self, pipeline: str, at: Optional[datetime] = None) -> None:
        """Record that a pipeline produced output at the given time."""
        self._last_seen[pipeline] = at or datetime.utcnow()

    def check(self, pipeline: str, now: Optional[datetime] = None) -> Optional[StalenessReport]:
        """Return a StalenessReport, or None if the pipeline is not configured."""
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return None
        now = now or datetime.utcnow()
        last = self._last_seen.get(pipeline)
        if last is None:
            return StalenessReport(
                pipeline=pipeline,
                last_seen=None,
                max_age_seconds=cfg.max_age_seconds,
                is_stale=True,
                age_seconds=None,
            )
        age = (now - last).total_seconds()
        return StalenessReport(
            pipeline=pipeline,
            last_seen=last,
            max_age_seconds=cfg.max_age_seconds,
            is_stale=age > cfg.max_age_seconds,
            age_seconds=age,
        )

    def check_all(self, now: Optional[datetime] = None) -> List[StalenessReport]:
        """Return staleness reports for every configured pipeline."""
        return [self.check(p, now=now) for p in self._configs]

    def stale_pipelines(self, now: Optional[datetime] = None) -> List[StalenessReport]:
        """Return only the reports where is_stale is True."""
        return [r for r in self.check_all(now=now) if r.is_stale]
