"""Pipeline trend analysis — detects directional changes in error rate and throughput
over a sliding window of historical run records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.history import RunRecord


@dataclass
class TrendPoint:
    """A single data point used for trend computation."""
    error_rate: float
    duration: float  # seconds
    success_count: int
    failure_count: int


@dataclass
class TrendReport:
    """Trend analysis result for a single pipeline."""
    pipeline: str
    window: int                  # number of records analysed
    error_rate_slope: float      # positive = worsening, negative = improving
    duration_slope: float        # positive = getting slower
    direction: str               # "improving", "degrading", "stable"
    confidence: str              # "low", "medium", "high" based on sample size

    def summary(self) -> str:
        arrow = {"improving": "↓", "degrading": "↑", "stable": "→"}.get(self.direction, "?")
        return (
            f"{self.pipeline}: error-rate slope={self.error_rate_slope:+.4f} "
            f"dur slope={self.duration_slope:+.4f}s  {arrow} {self.direction} "
            f"[{self.confidence} confidence, n={self.window}]"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _linear_slope(values: List[float]) -> float:
    """Return the slope of the best-fit line through *values* (index as x-axis).

    Uses the closed-form OLS formula: slope = (n*Σxy − Σx*Σy) / (n*Σx² − (Σx)²).
    Returns 0.0 when the denominator is zero (constant or single-point series).
    """
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    sum_x = sum(xs)
    sum_y = sum(values)
    sum_xy = sum(x * y for x, y in zip(xs, values))
    sum_x2 = sum(x * x for x in xs)
    denom = n * sum_x2 - sum_x ** 2
    if denom == 0:
        return 0.0
    return (n * sum_xy - sum_x * sum_y) / denom


def _confidence(n: int) -> str:
    if n >= 20:
        return "high"
    if n >= 8:
        return "medium"
    return "low"


def _direction(error_slope: float, threshold: float = 0.005) -> str:
    """Classify overall trend direction from the error-rate slope."""
    if error_slope > threshold:
        return "degrading"
    if error_slope < -threshold:
        return "improving"
    return "stable"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_trend(
    pipeline: str,
    records: List[RunRecord],
    window: int = 30,
) -> Optional[TrendReport]:
    """Analyse the last *window* records for *pipeline* and return a TrendReport.

    Returns ``None`` when there are fewer than 2 records (slope is undefined).
    """
    recent = records[-window:] if len(records) > window else records
    if len(recent) < 2:
        return None

    error_rates: List[float] = []
    durations: List[float] = []

    for r in recent:
        total = r.success_count + r.failure_count
        er = r.failure_count / total if total > 0 else 0.0
        error_rates.append(er)
        durations.append(r.duration_seconds if r.duration_seconds is not None else 0.0)

    e_slope = _linear_slope(error_rates)
    d_slope = _linear_slope(durations)

    return TrendReport(
        pipeline=pipeline,
        window=len(recent),
        error_rate_slope=round(e_slope, 6),
        duration_slope=round(d_slope, 4),
        direction=_direction(e_slope),
        confidence=_confidence(len(recent)),
    )


def compute_trend_map(
    history_map: Dict[str, List[RunRecord]],
    window: int = 30,
) -> Dict[str, TrendReport]:
    """Compute trend reports for every pipeline in *history_map*.

    Pipelines with insufficient data are silently omitted.
    """
    result: Dict[str, TrendReport] = {}
    for name, records in history_map.items():
        report = compute_trend(name, records, window=window)
        if report is not None:
            result[name] = report
    return result
