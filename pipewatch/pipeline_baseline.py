"""Baseline tracking for pipeline metrics — compare current performance against a stored baseline."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional


@dataclass
class BaselineSnapshot:
    pipeline: str
    avg_duration: float        # seconds
    error_rate: float          # 0.0 – 1.0
    avg_throughput: float      # records / run
    recorded_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "avg_duration": self.avg_duration,
            "error_rate": self.error_rate,
            "avg_throughput": self.avg_throughput,
            "recorded_at": self.recorded_at,
        }

    @staticmethod
    def from_dict(data: dict) -> "BaselineSnapshot":
        return BaselineSnapshot(
            pipeline=data["pipeline"],
            avg_duration=float(data["avg_duration"]),
            error_rate=float(data["error_rate"]),
            avg_throughput=float(data["avg_throughput"]),
            recorded_at=data.get("recorded_at", ""),
        )


@dataclass
class BaselineDelta:
    pipeline: str
    duration_delta_pct: float   # positive = slower
    error_rate_delta: float     # positive = more errors
    throughput_delta_pct: float # positive = more throughput

    @property
    def is_regression(self) -> bool:
        return self.duration_delta_pct > 10.0 or self.error_rate_delta > 0.05

    @property
    def is_improvement(self) -> bool:
        return self.duration_delta_pct < -10.0 and self.error_rate_delta <= 0.0


class BaselineTracker:
    """Stores one baseline snapshot per pipeline and computes deltas."""

    def __init__(self) -> None:
        self._baselines: Dict[str, BaselineSnapshot] = {}

    def set_baseline(self, snapshot: BaselineSnapshot) -> None:
        self._baselines[snapshot.pipeline] = snapshot

    def get_baseline(self, pipeline: str) -> Optional[BaselineSnapshot]:
        return self._baselines.get(pipeline)

    def all_baselines(self) -> Dict[str, BaselineSnapshot]:
        return dict(self._baselines)

    def compute_delta(self, pipeline: str, current: BaselineSnapshot) -> Optional[BaselineDelta]:
        base = self._baselines.get(pipeline)
        if base is None:
            return None

        def _pct(current_val: float, base_val: float) -> float:
            if base_val == 0.0:
                return 0.0
            return ((current_val - base_val) / base_val) * 100.0

        return BaselineDelta(
            pipeline=pipeline,
            duration_delta_pct=_pct(current.avg_duration, base.avg_duration),
            error_rate_delta=current.error_rate - base.error_rate,
            throughput_delta_pct=_pct(current.avg_throughput, base.avg_throughput),
        )

    def remove(self, pipeline: str) -> bool:
        if pipeline in self._baselines:
            del self._baselines[pipeline]
            return True
        return False
