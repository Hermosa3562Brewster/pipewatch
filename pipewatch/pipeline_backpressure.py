"""Back-pressure tracking for ETL pipelines.

Tracks queue depth and processing lag to detect when a pipeline
is accumulating work faster than it can consume it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class BackpressureReading:
    pipeline: str
    queue_depth: int
    lag_seconds: float
    recorded_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "queue_depth": self.queue_depth,
            "lag_seconds": self.lag_seconds,
            "recorded_at": self.recorded_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BackpressureReading":
        return cls(
            pipeline=data["pipeline"],
            queue_depth=data["queue_depth"],
            lag_seconds=data["lag_seconds"],
            recorded_at=datetime.fromisoformat(data["recorded_at"]),
        )


@dataclass
class BackpressureStats:
    pipeline: str
    readings: List[BackpressureReading]

    @property
    def latest(self) -> Optional[BackpressureReading]:
        return self.readings[-1] if self.readings else None

    @property
    def avg_queue_depth(self) -> float:
        if not self.readings:
            return 0.0
        return sum(r.queue_depth for r in self.readings) / len(self.readings)

    @property
    def max_lag_seconds(self) -> float:
        if not self.readings:
            return 0.0
        return max(r.lag_seconds for r in self.readings)

    @property
    def is_under_pressure(self) -> bool:
        """True when the latest queue depth is above the rolling average."""
        if not self.latest or len(self.readings) < 2:
            return False
        avg = sum(r.queue_depth for r in self.readings[:-1]) / len(self.readings[:-1])
        return self.latest.queue_depth > avg


class BackpressureTracker:
    def __init__(self, max_readings: int = 100) -> None:
        self._max = max_readings
        self._data: Dict[str, List[BackpressureReading]] = {}

    def record(self, pipeline: str, queue_depth: int, lag_seconds: float) -> BackpressureReading:
        reading = BackpressureReading(
            pipeline=pipeline,
            queue_depth=queue_depth,
            lag_seconds=lag_seconds,
        )
        bucket = self._data.setdefault(pipeline, [])
        bucket.append(reading)
        if len(bucket) > self._max:
            bucket.pop(0)
        return reading

    def stats(self, pipeline: str) -> BackpressureStats:
        return BackpressureStats(
            pipeline=pipeline,
            readings=list(self._data.get(pipeline, [])),
        )

    def all_pipelines(self) -> List[str]:
        return list(self._data.keys())

    def clear(self, pipeline: str) -> None:
        self._data.pop(pipeline, None)
