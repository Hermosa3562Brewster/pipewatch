"""Sliding-window run statistics for pipelines."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional

from pipewatch.history import RunRecord


@dataclass
class WindowStats:
    pipeline: str
    window_seconds: int
    total_runs: int
    successes: int
    failures: int
    avg_duration: float  # seconds
    error_rate: float    # 0.0 – 1.0

    def summary(self) -> str:
        return (
            f"{self.pipeline}: {self.total_runs} runs in last "
            f"{self.window_seconds}s — error_rate={self.error_rate:.1%}"
        )


class WindowTracker:
    """Keeps a per-pipeline deque of RunRecords and computes window stats."""

    def __init__(self, window_seconds: int = 300) -> None:
        self.window_seconds = window_seconds
        self._records: Dict[str, Deque[RunRecord]] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def record(self, run: RunRecord) -> None:
        """Add a RunRecord and evict entries outside the window."""
        name = run.pipeline_name
        if name not in self._records:
            self._records[name] = deque()
        self._records[name].append(run)
        self._evict(name)

    def _evict(self, name: str) -> None:
        cutoff = datetime.utcnow() - timedelta(seconds=self.window_seconds)
        dq = self._records[name]
        while dq and dq[0].started_at < cutoff:
            dq.popleft()

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def stats(self, pipeline: str) -> Optional[WindowStats]:
        """Return WindowStats for *pipeline*, or None if no data."""
        self._evict(pipeline) if pipeline in self._records else None
        runs: List[RunRecord] = list(self._records.get(pipeline, []))
        if not runs:
            return None
        successes = sum(1 for r in runs if r.status == "success")
        failures = len(runs) - successes
        durations = [r.duration_seconds for r in runs if r.duration_seconds is not None]
        avg_dur = sum(durations) / len(durations) if durations else 0.0
        return WindowStats(
            pipeline=pipeline,
            window_seconds=self.window_seconds,
            total_runs=len(runs),
            successes=successes,
            failures=failures,
            avg_duration=avg_dur,
            error_rate=failures / len(runs),
        )

    def all_stats(self) -> List[WindowStats]:
        """Return WindowStats for every tracked pipeline."""
        return [s for name in list(self._records) if (s := self.stats(name)) is not None]

    def pipelines(self) -> List[str]:
        return list(self._records.keys())
