"""Pipeline scorecard: composite scoring across multiple health dimensions."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ScorecardDimension:
    name: str
    score: float          # 0.0 – 100.0
    weight: float = 1.0
    note: str = ""


@dataclass
class ScorecardReport:
    pipeline: str
    dimensions: List[ScorecardDimension] = field(default_factory=list)

    @property
    def weighted_score(self) -> float:
        """Weighted average across all dimensions."""
        if not self.dimensions:
            return 0.0
        total_weight = sum(d.weight for d in self.dimensions)
        if total_weight == 0.0:
            return 0.0
        return sum(d.score * d.weight for d in self.dimensions) / total_weight

    @property
    def grade(self) -> str:
        s = self.weighted_score
        if s >= 90:
            return "A"
        if s >= 75:
            return "B"
        if s >= 60:
            return "C"
        if s >= 40:
            return "D"
        return "F"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "weighted_score": round(self.weighted_score, 2),
            "grade": self.grade,
            "dimensions": [
                {"name": d.name, "score": d.score, "weight": d.weight, "note": d.note}
                for d in self.dimensions
            ],
        }


class ScorecardBuilder:
    """Builds a ScorecardReport for a pipeline from individual dimension scores."""

    def __init__(self, pipeline: str) -> None:
        self._pipeline = pipeline
        self._dimensions: List[ScorecardDimension] = []

    def add(self, name: str, score: float, weight: float = 1.0, note: str = "") -> "ScorecardBuilder":
        score = max(0.0, min(100.0, score))
        self._dimensions.append(ScorecardDimension(name=name, score=score, weight=weight, note=note))
        return self

    def build(self) -> ScorecardReport:
        return ScorecardReport(pipeline=self._pipeline, dimensions=list(self._dimensions))


def compute_scorecard_map(
    pipelines: List[str],
    dimension_fn,
) -> Dict[str, ScorecardReport]:
    """Apply *dimension_fn(pipeline_name) -> List[ScorecardDimension]* for each pipeline."""
    result: Dict[str, ScorecardReport] = {}
    for name in pipelines:
        dims = dimension_fn(name)
        result[name] = ScorecardReport(pipeline=name, dimensions=dims)
    return result
