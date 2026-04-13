"""Track and report per-pipeline latency (run duration) statistics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.history import RunRecord


@dataclass
class LatencyStats:
    name: str
    samples: List[float] = field(default_factory=list)  # durations in seconds

    @property
    def count(self) -> int:
        return len(self.samples)

    @property
    def mean(self) -> Optional[float]:
        if not self.samples:
            return None
        return sum(self.samples) / len(self.samples)

    @property
    def minimum(self) -> Optional[float]:
        return min(self.samples) if self.samples else None

    @property
    def maximum(self) -> Optional[float]:
        return max(self.samples) if self.samples else None

    @property
    def p95(self) -> Optional[float]:
        """95th-percentile latency."""
        if not self.samples:
            return None
        sorted_s = sorted(self.samples)
        idx = max(0, int(len(sorted_s) * 0.95) - 1)
        return sorted_s[idx]


def compute_latency(records: List[RunRecord]) -> LatencyStats:
    """Compute latency stats from a list of RunRecords for one pipeline."""
    if not records:
        raise ValueError("records must not be empty")
    name = records[0].pipeline_name
    samples = [
        r.duration_seconds
        for r in records
        if r.duration_seconds is not None and r.duration_seconds >= 0
    ]
    return LatencyStats(name=name, samples=samples)


def compute_latency_map(
    history_map: Dict[str, List[RunRecord]]
) -> Dict[str, LatencyStats]:
    """Compute latency stats for every pipeline in a history map."""
    result: Dict[str, LatencyStats] = {}
    for name, records in history_map.items():
        if records:
            result[name] = compute_latency(records)
        else:
            result[name] = LatencyStats(name=name)
    return result
