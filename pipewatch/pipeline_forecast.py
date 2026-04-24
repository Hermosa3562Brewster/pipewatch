"""pipeline_forecast.py — Simple run-count and error-rate forecasting for pipelines.

Uses linear extrapolation over recent history to project future throughput
and failure rates over a configurable horizon.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict

from pipewatch.history import RunRecord


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ForecastPoint:
    """A single projected data point at a future timestamp."""
    at: datetime
    projected_success_rate: float   # 0.0 – 1.0
    projected_runs_per_hour: float  # extrapolated throughput

    def to_dict(self) -> dict:
        return {
            "at": self.at.isoformat(),
            "projected_success_rate": round(self.projected_success_rate, 4),
            "projected_runs_per_hour": round(self.projected_runs_per_hour, 4),
        }


@dataclass
class ForecastReport:
    """Forecast for a single pipeline over a given horizon."""
    pipeline: str
    generated_at: datetime
    horizon_hours: int
    points: List[ForecastPoint] = field(default_factory=list)
    warning: Optional[str] = None  # set when history is too sparse

    def summary(self) -> str:
        if self.warning:
            return f"{self.pipeline}: {self.warning}"
        if not self.points:
            return f"{self.pipeline}: no forecast available"
        last = self.points[-1]
        return (
            f"{self.pipeline}: in {self.horizon_hours}h — "
            f"~{last.projected_runs_per_hour:.1f} runs/h, "
            f"{last.projected_success_rate * 100:.1f}% success"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _linear_extrapolate(xs: List[float], ys: List[float], x_new: float) -> float:
    """Return y at *x_new* using ordinary least-squares slope/intercept."""
    n = len(xs)
    if n < 2:
        return ys[-1] if ys else 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(xs, ys))
    den = sum((xi - mean_x) ** 2 for xi in xs)
    if den == 0:
        return mean_y
    slope = num / den
    intercept = mean_y - slope * mean_x
    return slope * x_new + intercept


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# Core forecast logic
# ---------------------------------------------------------------------------

MIN_RECORDS = 3  # minimum history needed for a meaningful forecast


def compute_forecast(
    pipeline: str,
    records: List[RunRecord],
    horizon_hours: int = 24,
    step_hours: int = 6,
) -> ForecastReport:
    """Compute a linear forecast for *pipeline* from its run history.

    Args:
        pipeline:      Pipeline name (used for labelling only).
        records:       Historical run records, oldest-first.
        horizon_hours: How many hours ahead to project.
        step_hours:    Interval between forecast points.

    Returns:
        A :class:`ForecastReport` with evenly-spaced :class:`ForecastPoint`
        instances up to *horizon_hours* ahead of now.
    """
    now = datetime.utcnow()
    report = ForecastReport(
        pipeline=pipeline,
        generated_at=now,
        horizon_hours=horizon_hours,
    )

    if len(records) < MIN_RECORDS:
        report.warning = f"insufficient history ({len(records)} records, need {MIN_RECORDS})"
        return report

    # Build time-series anchored at the earliest record
    t0 = records[0].started_at
    xs = [(r.started_at - t0).total_seconds() / 3600.0 for r in records]
    success_ys = [1.0 if r.status == "success" else 0.0 for r in records]

    # Compute runs-per-hour in rolling 1-h buckets
    rph_ys: List[float] = []
    rph_xs: List[float] = []
    bucket_size = 1.0  # hours
    max_x = xs[-1] if xs else 0.0
    bucket = 0.0
    while bucket <= max_x:
        count = sum(1 for x in xs if bucket <= x < bucket + bucket_size)
        rph_ys.append(float(count))
        rph_xs.append(bucket + bucket_size / 2)
        bucket += bucket_size

    now_x = (now - t0).total_seconds() / 3600.0

    steps = max(1, horizon_hours // step_hours)
    for i in range(1, steps + 1):
        future_x = now_x + i * step_hours
        proj_sr = _clamp(_linear_extrapolate(xs, success_ys, future_x))
        proj_rph = max(0.0, _linear_extrapolate(rph_xs, rph_ys, future_x))
        report.points.append(ForecastPoint(
            at=now + timedelta(hours=i * step_hours),
            projected_success_rate=proj_sr,
            projected_runs_per_hour=proj_rph,
        ))

    return report


def compute_forecast_map(
    records_map: Dict[str, List[RunRecord]],
    horizon_hours: int = 24,
    step_hours: int = 6,
) -> Dict[str, ForecastReport]:
    """Compute forecasts for every pipeline in *records_map*."""
    return {
        name: compute_forecast(name, recs, horizon_hours, step_hours)
        for name, recs in records_map.items()
    }
