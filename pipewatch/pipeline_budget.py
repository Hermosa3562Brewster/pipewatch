"""Pipeline run budget tracking — caps the number of runs allowed in a time window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class BudgetConfig:
    max_runs: int
    window_seconds: int

    def window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)


@dataclass
class BudgetStatus:
    pipeline: str
    max_runs: int
    window_seconds: int
    runs_in_window: int

    def remaining(self) -> int:
        return max(0, self.max_runs - self.runs_in_window)

    def is_exceeded(self) -> bool:
        return self.runs_in_window >= self.max_runs

    def utilisation_pct(self) -> float:
        if self.max_runs == 0:
            return 100.0
        return min(100.0, self.runs_in_window / self.max_runs * 100)

    def summary(self) -> str:
        state = "EXCEEDED" if self.is_exceeded() else "OK"
        return (
            f"{self.pipeline}: {self.runs_in_window}/{self.max_runs} runs "
            f"in {self.window_seconds}s window [{state}]"
        )


class BudgetTracker:
    def __init__(self) -> None:
        self._configs: Dict[str, BudgetConfig] = {}
        self._runs: Dict[str, List[datetime]] = {}

    def configure(self, pipeline: str, max_runs: int, window_seconds: int) -> None:
        self._configs[pipeline] = BudgetConfig(max_runs=max_runs, window_seconds=window_seconds)
        if pipeline not in self._runs:
            self._runs[pipeline] = []

    def record_run(self, pipeline: str, at: Optional[datetime] = None) -> None:
        ts = at or datetime.utcnow()
        self._runs.setdefault(pipeline, []).append(ts)

    def _active_runs(self, pipeline: str, now: datetime) -> List[datetime]:
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return []
        cutoff = now - cfg.window()
        return [t for t in self._runs.get(pipeline, []) if t >= cutoff]

    def status(self, pipeline: str, now: Optional[datetime] = None) -> Optional[BudgetStatus]:
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return None
        ts = now or datetime.utcnow()
        active = self._active_runs(pipeline, ts)
        return BudgetStatus(
            pipeline=pipeline,
            max_runs=cfg.max_runs,
            window_seconds=cfg.window_seconds,
            runs_in_window=len(active),
        )

    def is_exceeded(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        s = self.status(pipeline, now)
        return s.is_exceeded() if s is not None else False

    def all_statuses(self, now: Optional[datetime] = None) -> List[BudgetStatus]:
        ts = now or datetime.utcnow()
        return [self.status(p, ts) for p in self._configs]
