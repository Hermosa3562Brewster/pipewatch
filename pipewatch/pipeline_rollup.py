"""Rollup: aggregate pipeline metrics over time windows (hourly, daily)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.history import RunRecord


@dataclass
class RollupBucket:
    label: str          # e.g. "2024-06-01" or "2024-06-01 14:00"
    window_start: datetime
    window_end: datetime
    total_runs: int = 0
    successes: int = 0
    failures: int = 0
    total_duration: float = 0.0

    @property
    def error_rate(self) -> float:
        if self.total_runs == 0:
            return 0.0
        return self.failures / self.total_runs

    @property
    def avg_duration(self) -> float:
        if self.total_runs == 0:
            return 0.0
        return self.total_duration / self.total_runs

    def to_dict(self) -> dict:
        return {
            "label": self.label,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "total_runs": self.total_runs,
            "successes": self.successes,
            "failures": self.failures,
            "total_duration": self.total_duration,
            "error_rate": round(self.error_rate, 4),
            "avg_duration": round(self.avg_duration, 4),
        }


@dataclass
class PipelineRollup:
    pipeline: str
    granularity: str  # "hourly" or "daily"
    buckets: List[RollupBucket] = field(default_factory=list)

    def total_runs(self) -> int:
        return sum(b.total_runs for b in self.buckets)

    def total_failures(self) -> int:
        return sum(b.failures for b in self.buckets)


def _bucket_key_and_window(ts: datetime, granularity: str):
    if granularity == "hourly":
        start = ts.replace(minute=0, second=0, microsecond=0)
        end = start + timedelta(hours=1)
        label = start.strftime("%Y-%m-%d %H:00")
    else:  # daily
        start = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        label = start.strftime("%Y-%m-%d")
    return label, start, end


def compute_rollup(
    pipeline: str,
    records: List[RunRecord],
    granularity: str = "daily",
) -> PipelineRollup:
    """Build rollup buckets for *pipeline* from a list of RunRecords."""
    if granularity not in ("hourly", "daily"):
        raise ValueError(f"granularity must be 'hourly' or 'daily', got {granularity!r}")

    bucket_map: Dict[str, RollupBucket] = {}
    for rec in records:
        ts = rec.started_at if rec.started_at else datetime.utcnow()
        label, start, end = _bucket_key_and_window(ts, granularity)
        if label not in bucket_map:
            bucket_map[label] = RollupBucket(label=label, window_start=start, window_end=end)
        b = bucket_map[label]
        b.total_runs += 1
        if rec.status == "success":
            b.successes += 1
        else:
            b.failures += 1
        b.total_duration += rec.duration_seconds or 0.0

    buckets = sorted(bucket_map.values(), key=lambda b: b.window_start)
    return PipelineRollup(pipeline=pipeline, granularity=granularity, buckets=buckets)


def compute_rollup_map(
    records_map: Dict[str, List[RunRecord]],
    granularity: str = "daily",
) -> Dict[str, PipelineRollup]:
    return {
        name: compute_rollup(name, recs, granularity)
        for name, recs in records_map.items()
    }
