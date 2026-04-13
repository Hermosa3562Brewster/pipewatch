"""Track named checkpoints within a pipeline run for granular progress monitoring."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Checkpoint:
    name: str
    reached_at: float = field(default_factory=time.time)
    duration_ms: Optional[float] = None
    metadata: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "reached_at": self.reached_at,
            "duration_ms": self.duration_ms,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        return cls(
            name=data["name"],
            reached_at=data["reached_at"],
            duration_ms=data.get("duration_ms"),
            metadata=data.get("metadata", {}),
        )


class CheckpointTracker:
    """Tracks checkpoints for a single pipeline."""

    def __init__(self, pipeline_name: str, max_checkpoints: int = 50) -> None:
        self.pipeline_name = pipeline_name
        self.max_checkpoints = max_checkpoints
        self._checkpoints: List[Checkpoint] = []
        self._pending: Dict[str, float] = {}

    def start_checkpoint(self, name: str) -> None:
        """Mark the start time of a checkpoint (for duration tracking)."""
        self._pending[name] = time.time()

    def reach(self, name: str, metadata: Optional[Dict[str, str]] = None) -> Checkpoint:
        """Record a checkpoint as reached, computing duration if started."""
        now = time.time()
        duration_ms: Optional[float] = None
        if name in self._pending:
            duration_ms = (now - self._pending.pop(name)) * 1000.0
        cp = Checkpoint(name=name, reached_at=now, duration_ms=duration_ms, metadata=metadata or {})
        self._checkpoints.append(cp)
        if len(self._checkpoints) > self.max_checkpoints:
            self._checkpoints = self._checkpoints[-self.max_checkpoints :]
        return cp

    def last_n(self, n: int = 10) -> List[Checkpoint]:
        return self._checkpoints[-n:]

    def latest(self) -> Optional[Checkpoint]:
        return self._checkpoints[-1] if self._checkpoints else None

    def clear(self) -> None:
        self._checkpoints.clear()
        self._pending.clear()

    def names(self) -> List[str]:
        return [cp.name for cp in self._checkpoints]


class CheckpointRegistry:
    """Global registry of CheckpointTrackers keyed by pipeline name."""

    def __init__(self) -> None:
        self._trackers: Dict[str, CheckpointTracker] = {}

    def tracker(self, pipeline_name: str) -> CheckpointTracker:
        if pipeline_name not in self._trackers:
            self._trackers[pipeline_name] = CheckpointTracker(pipeline_name)
        return self._trackers[pipeline_name]

    def all_pipeline_names(self) -> List[str]:
        return list(self._trackers.keys())

    def remove(self, pipeline_name: str) -> None:
        self._trackers.pop(pipeline_name, None)
