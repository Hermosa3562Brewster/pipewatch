"""Pipeline health scoring and status classification."""

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class HealthReport:
    name: str
    score: float          # 0.0 – 1.0
    grade: str            # A / B / C / D / F
    status: str           # healthy / degraded / critical / unknown
    error_rate: float
    total_runs: int
    note: Optional[str] = None


def _grade(score: float) -> str:
    if score >= 0.90:
        return "A"
    if score >= 0.75:
        return "B"
    if score >= 0.55:
        return "C"
    if score >= 0.35:
        return "D"
    return "F"


def _status(score: float, total_runs: int) -> str:
    if total_runs == 0:
        return "unknown"
    if score >= 0.75:
        return "healthy"
    if score >= 0.40:
        return "degraded"
    return "critical"


def compute_health(metrics: PipelineMetrics) -> HealthReport:
    """Compute a HealthReport from a PipelineMetrics instance."""
    total = metrics.success_count + metrics.failure_count
    error_rate = metrics.error_rate()  # method already on PipelineMetrics

    if total == 0:
        score = 1.0
        note = "No runs recorded yet."
    else:
        score = 1.0 - error_rate
        note = None

    return HealthReport(
        name=metrics.name,
        score=round(score, 4),
        grade=_grade(score),
        status=_status(score, total),
        error_rate=round(error_rate, 4),
        total_runs=total,
        note=note,
    )


def compute_health_map(
    metrics_map: dict[str, PipelineMetrics]
) -> dict[str, HealthReport]:
    """Return a health report for every pipeline in *metrics_map*."""
    return {name: compute_health(m) for name, m in metrics_map.items()}
