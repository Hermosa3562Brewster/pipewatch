"""Pipeline quota tracking — enforce run limits over a rolling time window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class QuotaConfig:
    max_runs: int
    window_seconds: int = 3600  # default: 1 hour

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)


@dataclass
class QuotaStatus:
    pipeline: str
    max_runs: int
    window_seconds: int
    runs_in_window: int
    exceeded: bool
    remaining: int

    @property
    def utilisation_pct(self) -> float:
        if self.max_runs == 0:
            return 100.0
        return round(self.runs_in_window / self.max_runs * 100, 1)


class QuotaTracker:
    """Tracks run timestamps per pipeline and enforces configurable quotas."""

    def __init__(self) -> None:
        self._configs: Dict[str, QuotaConfig] = {}
        self._timestamps: Dict[str, List[datetime]] = {}

    def set_quota(self, pipeline: str, max_runs: int, window_seconds: int = 3600) -> None:
        self._configs[pipeline] = QuotaConfig(max_runs=max_runs, window_seconds=window_seconds)
        if pipeline not in self._timestamps:
            self._timestamps[pipeline] = []

    def record_run(self, pipeline: str, at: Optional[datetime] = None) -> None:
        ts = at or datetime.utcnow()
        self._timestamps.setdefault(pipeline, []).append(ts)

    def _prune(self, pipeline: str, now: datetime) -> List[datetime]:
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return self._timestamps.get(pipeline, [])
        cutoff = now - cfg.window
        pruned = [t for t in self._timestamps.get(pipeline, []) if t >= cutoff]
        self._timestamps[pipeline] = pruned
        return pruned

    def status(self, pipeline: str, now: Optional[datetime] = None) -> Optional[QuotaStatus]:
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return None
        now = now or datetime.utcnow()
        recent = self._prune(pipeline, now)
        runs = len(recent)
        return QuotaStatus(
            pipeline=pipeline,
            max_runs=cfg.max_runs,
            window_seconds=cfg.window_seconds,
            runs_in_window=runs,
            exceeded=runs > cfg.max_runs,
            remaining=max(0, cfg.max_runs - runs),
        )

    def all_statuses(self, now: Optional[datetime] = None) -> List[QuotaStatus]:
        now = now or datetime.utcnow()
        return [s for p in self._configs if (s := self.status(p, now)) is not None]

    def is_exceeded(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        s = self.status(pipeline, now)
        return s.exceeded if s else False
