"""Pipeline profiler: tracks per-stage timing within a pipeline run."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class StageProfile:
    name: str
    duration_s: float
    started_at: float
    ended_at: float

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "duration_s": self.duration_s,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "StageProfile":
        return cls(
            name=d["name"],
            duration_s=d["duration_s"],
            started_at=d["started_at"],
            ended_at=d["ended_at"],
        )


@dataclass
class PipelineProfile:
    pipeline_name: str
    stages: List[StageProfile] = field(default_factory=list)

    def total_duration(self) -> float:
        return sum(s.duration_s for s in self.stages)

    def slowest_stage(self) -> Optional[StageProfile]:
        if not self.stages:
            return None
        return max(self.stages, key=lambda s: s.duration_s)

    def stage_share(self) -> Dict[str, float]:
        total = self.total_duration()
        if total == 0:
            return {s.name: 0.0 for s in self.stages}
        return {s.name: s.duration_s / total for s in self.stages}


class Profiler:
    """Context-manager-based stage profiler for a single pipeline run."""

    def __init__(self, pipeline_name: str) -> None:
        self.profile = PipelineProfile(pipeline_name=pipeline_name)
        self._stage_start: Optional[float] = None
        self._current_stage: Optional[str] = None

    def begin_stage(self, name: str) -> None:
        if self._current_stage is not None:
            self._finish_current()
        self._current_stage = name
        self._stage_start = time.monotonic()

    def end_stage(self) -> None:
        if self._current_stage is not None:
            self._finish_current()

    def _finish_current(self) -> None:
        ended = time.monotonic()
        assert self._stage_start is not None
        self.profile.stages.append(
            StageProfile(
                name=self._current_stage,  # type: ignore[arg-type]
                duration_s=ended - self._stage_start,
                started_at=self._stage_start,
                ended_at=ended,
            )
        )
        self._current_stage = None
        self._stage_start = None

    def finish(self) -> PipelineProfile:
        if self._current_stage is not None:
            self._finish_current()
        return self.profile
