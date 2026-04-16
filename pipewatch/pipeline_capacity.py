"""Pipeline capacity tracking: record resource usage and warn when near limits."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class CapacityConfig:
    max_queue_depth: int = 1000
    max_memory_mb: float = 512.0
    max_cpu_pct: float = 90.0


@dataclass
class CapacityReading:
    pipeline: str
    queue_depth: int
    memory_mb: float
    cpu_pct: float
    recorded_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "queue_depth": self.queue_depth,
            "memory_mb": self.memory_mb,
            "cpu_pct": self.cpu_pct,
            "recorded_at": self.recorded_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CapacityReading":
        return cls(
            pipeline=d["pipeline"],
            queue_depth=d["queue_depth"],
            memory_mb=d["memory_mb"],
            cpu_pct=d["cpu_pct"],
            recorded_at=datetime.fromisoformat(d["recorded_at"]),
        )


@dataclass
class CapacityStatus:
    pipeline: str
    reading: CapacityReading
    config: CapacityConfig

    @property
    def queue_pct(self) -> float:
        if self.config.max_queue_depth == 0:
            return 0.0
        return self.reading.queue_depth / self.config.max_queue_depth * 100

    @property
    def memory_pct(self) -> float:
        if self.config.max_memory_mb == 0:
            return 0.0
        return self.reading.memory_mb / self.config.max_memory_mb * 100

    @property
    def is_overloaded(self) -> bool:
        return (
            self.queue_pct >= 100
            or self.memory_pct >= 100
            or self.reading.cpu_pct >= self.config.max_cpu_pct
        )

    def summary(self) -> str:
        return (
            f"{self.pipeline}: queue={self.queue_pct:.1f}% "
            f"mem={self.memory_pct:.1f}% cpu={self.reading.cpu_pct:.1f}%"
        )


class CapacityTracker:
    def __init__(self, max_history: int = 100) -> None:
        self._configs: Dict[str, CapacityConfig] = {}
        self._history: Dict[str, List[CapacityReading]] = {}
        self._max_history = max_history

    def configure(self, pipeline: str, config: CapacityConfig) -> None:
        self._configs[pipeline] = config

    def record(self, pipeline: str, queue_depth: int, memory_mb: float, cpu_pct: float) -> CapacityReading:
        reading = CapacityReading(pipeline=pipeline, queue_depth=queue_depth, memory_mb=memory_mb, cpu_pct=cpu_pct)
        self._history.setdefault(pipeline, []).append(reading)
        if len(self._history[pipeline]) > self._max_history:
            self._history[pipeline].pop(0)
        return reading

    def latest(self, pipeline: str) -> Optional[CapacityReading]:
        history = self._history.get(pipeline, [])
        return history[-1] if history else None

    def status(self, pipeline: str) -> Optional[CapacityStatus]:
        reading = self.latest(pipeline)
        if reading is None:
            return None
        config = self._configs.get(pipeline, CapacityConfig())
        return CapacityStatus(pipeline=pipeline, reading=reading, config=config)

    def all_pipelines(self) -> List[str]:
        return list(self._history.keys())
