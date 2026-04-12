"""Aggregates metrics across multiple pipelines for summary statistics."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.metrics import PipelineMetrics


@dataclass
class AggregatedStats:
    """Holds cross-pipeline aggregated statistics."""
    total_pipelines: int = 0
    active_pipelines: int = 0
    failed_pipelines: int = 0
    total_successes: int = 0
    total_failures: int = 0
    avg_error_rate: float = 0.0
    avg_throughput: float = 0.0
    pipeline_names: List[str] = field(default_factory=list)

    @property
    def overall_error_rate(self) -> float:
        total = self.total_successes + self.total_failures
        if total == 0:
            return 0.0
        return round(self.total_failures / total, 4)

    @property
    def health_score(self) -> float:
        """Returns a 0.0-1.0 health score across all pipelines."""
        if self.total_pipelines == 0:
            return 1.0
        return round(1.0 - self.overall_error_rate, 4)


def aggregate(metrics_map: Dict[str, PipelineMetrics]) -> AggregatedStats:
    """Compute aggregated stats from a dict of pipeline name -> PipelineMetrics."""
    stats = AggregatedStats()
    stats.total_pipelines = len(metrics_map)
    stats.pipeline_names = list(metrics_map.keys())

    throughputs: List[float] = []
    error_rates: List[float] = []

    for name, m in metrics_map.items():
        stats.total_successes += m.success_count
        stats.total_failures += m.failure_count

        if m.status == "running":
            stats.active_pipelines += 1
        elif m.status == "failed":
            stats.failed_pipelines += 1

        error_rates.append(m.error_rate)
        throughputs.append(m.throughput)

    if error_rates:
        stats.avg_error_rate = round(sum(error_rates) / len(error_rates), 4)
    if throughputs:
        stats.avg_throughput = round(sum(throughputs) / len(throughputs), 4)

    return stats


def top_failing(
    metrics_map: Dict[str, PipelineMetrics], n: int = 3
) -> List[str]:
    """Return names of the top-n pipelines by failure count."""
    sorted_pipelines = sorted(
        metrics_map.items(),
        key=lambda kv: kv[1].failure_count,
        reverse=True,
    )
    return [name for name, _ in sorted_pipelines[:n] if sorted_pipelines]


def healthy_pipelines(
    metrics_map: Dict[str, PipelineMetrics], threshold: float = 0.05
) -> List[str]:
    """Return names of pipelines with error_rate below threshold."""
    return [
        name for name, m in metrics_map.items()
        if m.error_rate < threshold
    ]
