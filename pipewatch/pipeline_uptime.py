"""Uptime tracking for pipelines based on run history."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.history import RunRecord


@dataclass
class UptimeReport:
    name: str
    total_runs: int
    successful_runs: int
    failed_runs: int
    uptime_pct: float
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    longest_gap_seconds: float

    def grade(self) -> str:
        if self.uptime_pct >= 99.0:
            return "A"
        if self.uptime_pct >= 95.0:
            return "B"
        if self.uptime_pct >= 85.0:
            return "C"
        if self.uptime_pct >= 70.0:
            return "D"
        return "F"


def _longest_gap(records: List[RunRecord]) -> float:
    """Return the longest gap in seconds between consecutive run start times."""
    if len(records) < 2:
        return 0.0
    times = sorted(r.started_at for r in records if r.started_at)
    gaps = [
        (times[i + 1] - times[i]).total_seconds()
        for i in range(len(times) - 1)
    ]
    return max(gaps) if gaps else 0.0


def compute_uptime(name: str, records: List[RunRecord]) -> UptimeReport:
    """Compute an uptime report for a single pipeline from its run records."""
    total = len(records)
    if total == 0:
        return UptimeReport(
            name=name,
            total_runs=0,
            successful_runs=0,
            failed_runs=0,
            uptime_pct=0.0,
            first_seen=None,
            last_seen=None,
            longest_gap_seconds=0.0,
        )

    successful = sum(1 for r in records if r.status == "success")
    failed = total - successful
    uptime_pct = (successful / total) * 100.0

    started_times = [r.started_at for r in records if r.started_at]
    first_seen = min(started_times) if started_times else None
    last_seen = max(started_times) if started_times else None

    return UptimeReport(
        name=name,
        total_runs=total,
        successful_runs=successful,
        failed_runs=failed,
        uptime_pct=round(uptime_pct, 2),
        first_seen=first_seen,
        last_seen=last_seen,
        longest_gap_seconds=_longest_gap(records),
    )


def compute_uptime_map(
    records_map: Dict[str, List[RunRecord]]
) -> Dict[str, UptimeReport]:
    """Compute uptime reports for multiple pipelines."""
    return {name: compute_uptime(name, recs) for name, recs in records_map.items()}
