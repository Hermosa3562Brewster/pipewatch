"""Track and report estimated compute cost per pipeline run."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class CostRecord:
    pipeline_name: str
    run_id: str
    duration_seconds: float
    cost_per_second: float
    total_cost: float

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "duration_seconds": self.duration_seconds,
            "cost_per_second": self.cost_per_second,
            "total_cost": self.total_cost,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CostRecord":
        return cls(
            pipeline_name=data["pipeline_name"],
            run_id=data["run_id"],
            duration_seconds=data["duration_seconds"],
            cost_per_second=data["cost_per_second"],
            total_cost=data["total_cost"],
        )


@dataclass
class CostSummary:
    pipeline_name: str
    total_runs: int
    total_cost: float
    avg_cost_per_run: float
    max_cost_run: Optional[float]


class CostTracker:
    """Records per-run costs and provides summary statistics."""

    DEFAULT_COST_PER_SECOND = 0.0001

    def __init__(self, default_rate: float = DEFAULT_COST_PER_SECOND) -> None:
        self._default_rate = default_rate
        self._rates: Dict[str, float] = {}
        self._records: Dict[str, List[CostRecord]] = {}

    def set_rate(self, pipeline_name: str, cost_per_second: float) -> None:
        """Override the cost-per-second rate for a specific pipeline."""
        self._rates[pipeline_name] = cost_per_second

    def record(self, pipeline_name: str, run_id: str, duration_seconds: float) -> CostRecord:
        """Record a completed run and return the resulting CostRecord."""
        rate = self._rates.get(pipeline_name, self._default_rate)
        total = round(rate * duration_seconds, 6)
        rec = CostRecord(
            pipeline_name=pipeline_name,
            run_id=run_id,
            duration_seconds=duration_seconds,
            cost_per_second=rate,
            total_cost=total,
        )
        self._records.setdefault(pipeline_name, []).append(rec)
        return rec

    def get_records(self, pipeline_name: str) -> List[CostRecord]:
        return list(self._records.get(pipeline_name, []))

    def summary(self, pipeline_name: str) -> Optional[CostSummary]:
        records = self._records.get(pipeline_name)
        if not records:
            return None
        costs = [r.total_cost for r in records]
        return CostSummary(
            pipeline_name=pipeline_name,
            total_runs=len(records),
            total_cost=round(sum(costs), 6),
            avg_cost_per_run=round(sum(costs) / len(costs), 6),
            max_cost_run=max(costs),
        )

    def all_summaries(self) -> List[CostSummary]:
        return [s for name in self._records if (s := self.summary(name)) is not None]
