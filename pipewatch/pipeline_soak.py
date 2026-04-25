"""Soak testing tracker: monitors pipelines for sustained error-free operation over a time window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class SoakConfig:
    duration_seconds: float  # required soak window in seconds
    min_runs: int = 3  # minimum runs required within the window

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.duration_seconds)


@dataclass
class SoakReport:
    pipeline: str
    passed: bool
    runs_in_window: int
    failures_in_window: int
    window_start: Optional[datetime]
    window_end: Optional[datetime]
    required_seconds: float
    min_runs: int

    @property
    def error_rate(self) -> float:
        if self.runs_in_window == 0:
            return 0.0
        return self.failures_in_window / self.runs_in_window

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"{self.pipeline}: soak={status} "
            f"runs={self.runs_in_window} failures={self.failures_in_window} "
            f"window={self.required_seconds:.0f}s"
        )


class SoakTracker:
    def __init__(self) -> None:
        self._configs: Dict[str, SoakConfig] = {}
        # each entry: (timestamp, success)
        self._records: Dict[str, List[tuple]] = {}

    def configure(self, pipeline: str, duration_seconds: float, min_runs: int = 3) -> None:
        self._configs[pipeline] = SoakConfig(duration_seconds=duration_seconds, min_runs=min_runs)
        if pipeline not in self._records:
            self._records[pipeline] = []

    def record(self, pipeline: str, success: bool, ts: Optional[datetime] = None) -> None:
        ts = ts or datetime.utcnow()
        if pipeline not in self._records:
            self._records[pipeline] = []
        self._records[pipeline].append((ts, success))

    def check(self, pipeline: str, now: Optional[datetime] = None) -> Optional[SoakReport]:
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return None
        now = now or datetime.utcnow()
        cutoff = now - cfg.window
        all_records = self._records.get(pipeline, [])
        window_records = [(ts, ok) for ts, ok in all_records if ts >= cutoff]
        runs = len(window_records)
        failures = sum(1 for _, ok in window_records if not ok)
        window_start = window_records[0][0] if window_records else None
        window_end = window_records[-1][0] if window_records else None
        passed = runs >= cfg.min_runs and failures == 0
        return SoakReport(
            pipeline=pipeline,
            passed=passed,
            runs_in_window=runs,
            failures_in_window=failures,
            window_start=window_start,
            window_end=window_end,
            required_seconds=cfg.duration_seconds,
            min_runs=cfg.min_runs,
        )

    def check_all(self, now: Optional[datetime] = None) -> Dict[str, SoakReport]:
        return {p: self.check(p, now=now) for p in self._configs}

    def pipelines(self) -> List[str]:
        return list(self._configs.keys())
