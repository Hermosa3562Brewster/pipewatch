"""Anomaly detection for pipeline metrics using simple statistical methods."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import statistics

from pipewatch.history import RunRecord


@dataclass
class AnomalyResult:
    pipeline: str
    metric: str
    value: float
    mean: float
    std_dev: float
    z_score: float
    is_anomaly: bool
    severity: str  # "low", "medium", "high"

    def summary(self) -> str:
        direction = "above" if self.value > self.mean else "below"
        return (
            f"[{self.severity.upper()}] {self.pipeline}/{self.metric}: "
            f"{self.value:.3f} is {direction} mean "
            f"({self.mean:.3f} \u00b1 {self.std_dev:.3f}, z={self.z_score:.2f})"
        )


def _z_score(value: float, mean: float, std_dev: float) -> float:
    if std_dev == 0.0:
        return 0.0
    return (value - mean) / std_dev


def _severity(z: float) -> str:
    az = abs(z)
    if az >= 3.0:
        return "high"
    if az >= 2.0:
        return "medium"
    return "low"


def detect_anomaly(
    pipeline: str,
    metric: str,
    value: float,
    history: List[float],
    threshold: float = 2.0,
) -> Optional[AnomalyResult]:
    """Return an AnomalyResult if value is anomalous relative to history."""
    if len(history) < 3:
        return None
    mean = statistics.mean(history)
    std_dev = statistics.pstdev(history)
    z = _z_score(value, mean, std_dev)
    is_anomaly = abs(z) >= threshold
    return AnomalyResult(
        pipeline=pipeline,
        metric=metric,
        value=value,
        mean=mean,
        std_dev=std_dev,
        z_score=z,
        is_anomaly=is_anomaly,
        severity=_severity(z),
    )


def detect_error_rate_anomaly(
    pipeline: str, records: List[RunRecord], threshold: float = 2.0
) -> Optional[AnomalyResult]:
    """Detect anomalies in error rate derived from RunRecord history."""
    if len(records) < 4:
        return None
    window = records[:-1]
    current = records[-1]

    def _rate(r: RunRecord) -> float:
        total = r.successes + r.failures
        return r.failures / total if total > 0 else 0.0

    history_rates = [_rate(r) for r in window]
    current_rate = _rate(current)
    return detect_anomaly(pipeline, "error_rate", current_rate, history_rates, threshold)


@dataclass
class AnomalyDetector:
    threshold: float = 2.0
    _results: List[AnomalyResult] = field(default_factory=list)

    def analyse(self, pipeline: str, records: List[RunRecord]) -> Optional[AnomalyResult]:
        result = detect_error_rate_anomaly(pipeline, records, self.threshold)
        if result is not None:
            self._results.append(result)
        return result

    def anomalies(self) -> List[AnomalyResult]:
        return [r for r in self._results if r.is_anomaly]

    def clear(self) -> None:
        self._results.clear()
