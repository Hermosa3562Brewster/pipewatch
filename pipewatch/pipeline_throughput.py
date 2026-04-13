"""Throughput tracking for ETL pipelines.

Computes records-per-second and run-rate statistics from pipeline history.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import RunRecord


@dataclass
class ThroughputStats:
    pipeline_name: str
    total_records: int = 0
    total_duration_seconds: float = 0.0
    run_count: int = 0
    min_rps: Optional[float] = None
    max_rps: Optional[float] = None
    _rps_samples: List[float] = field(default_factory=list, repr=False)

    @property
    def avg_rps(self) -> float:
        """Average records per second across all runs."""
        if self.total_duration_seconds <= 0:
            return 0.0
        return self.total_records / self.total_duration_seconds

    @property
    def p95_rps(self) -> float:
        """95th-percentile records-per-second."""
        if not self._rps_samples:
            return 0.0
        sorted_samples = sorted(self._rps_samples)
        idx = max(0, int(len(sorted_samples) * 0.95) - 1)
        return sorted_samples[idx]

    @property
    def avg_records_per_run(self) -> float:
        if self.run_count == 0:
            return 0.0
        return self.total_records / self.run_count


def compute_throughput(name: str, records: List[RunRecord]) -> ThroughputStats:
    """Compute throughput statistics from a list of RunRecords.

    Only successful runs with a positive duration and record count contribute.
    """
    stats = ThroughputStats(pipeline_name=name)

    for record in records:
        if record.status != "success":
            continue
        duration = record.duration_seconds or 0.0
        rec_count = record.records_processed or 0
        if duration <= 0 or rec_count <= 0:
            continue

        rps = rec_count / duration
        stats.total_records += rec_count
        stats.total_duration_seconds += duration
        stats.run_count += 1
        stats._rps_samples.append(rps)

        if stats.min_rps is None or rps < stats.min_rps:
            stats.min_rps = rps
        if stats.max_rps is None or rps > stats.max_rps:
            stats.max_rps = rps

    return stats


def compute_throughput_map(
    records_map: dict[str, List[RunRecord]]
) -> dict[str, ThroughputStats]:
    """Compute throughput stats for multiple pipelines."""
    return {name: compute_throughput(name, recs) for name, recs in records_map.items()}
