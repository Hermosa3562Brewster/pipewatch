"""SLA (Service Level Agreement) tracking for pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.history import RunRecord


@dataclass
class SLAConfig:
    """Defines SLA thresholds for a pipeline."""
    max_error_rate: float = 0.05       # 5% default
    max_duration_seconds: float = 300.0  # 5 minutes default
    min_success_count: int = 0


@dataclass
class SLAReport:
    """Result of evaluating SLA compliance for a pipeline."""
    pipeline_name: str
    compliant: bool
    error_rate: float
    avg_duration: float
    total_runs: int
    violations: List[str] = field(default_factory=list)

    @property
    def grade(self) -> str:
        if self.compliant:
            return "OK"
        if len(self.violations) == 1:
            return "WARN"
        return "BREACH"


def _avg_duration(records: List[RunRecord]) -> float:
    durations = [r.duration_seconds for r in records if r.duration_seconds is not None]
    if not durations:
        return 0.0
    return sum(durations) / len(durations)


def _error_rate(records: List[RunRecord]) -> float:
    if not records:
        return 0.0
    failures = sum(1 for r in records if r.status == "failed")
    return failures / len(records)


def compute_sla(
    pipeline_name: str,
    records: List[RunRecord],
    config: Optional[SLAConfig] = None,
) -> SLAReport:
    """Evaluate SLA compliance for a pipeline given its run history."""
    cfg = config or SLAConfig()
    error_rate = _error_rate(records)
    avg_dur = _avg_duration(records)
    total = len(records)
    successes = sum(1 for r in records if r.status == "success")

    violations: List[str] = []
    if error_rate > cfg.max_error_rate:
        violations.append(
            f"error_rate {error_rate:.2%} exceeds limit {cfg.max_error_rate:.2%}"
        )
    if avg_dur > cfg.max_duration_seconds:
        violations.append(
            f"avg_duration {avg_dur:.1f}s exceeds limit {cfg.max_duration_seconds:.1f}s"
        )
    if successes < cfg.min_success_count:
        violations.append(
            f"success_count {successes} below minimum {cfg.min_success_count}"
        )

    return SLAReport(
        pipeline_name=pipeline_name,
        compliant=len(violations) == 0,
        error_rate=error_rate,
        avg_duration=avg_dur,
        total_runs=total,
        violations=violations,
    )


def compute_sla_map(
    records_map: Dict[str, List[RunRecord]],
    configs: Optional[Dict[str, SLAConfig]] = None,
) -> Dict[str, SLAReport]:
    """Compute SLA reports for multiple pipelines."""
    configs = configs or {}
    return {
        name: compute_sla(name, records, configs.get(name))
        for name, records in records_map.items()
    }
