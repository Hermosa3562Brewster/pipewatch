"""Track and compute correlations between pipeline error rates over time."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import math


@dataclass
class CorrelationResult:
    pipeline_a: str
    pipeline_b: str
    coefficient: float  # Pearson r, range [-1, 1]
    sample_count: int

    def strength(self) -> str:
        """Return a human-readable strength label."""
        r = abs(self.coefficient)
        if r >= 0.8:
            return "strong"
        if r >= 0.5:
            return "moderate"
        if r >= 0.2:
            return "weak"
        return "negligible"

    def direction(self) -> str:
        return "positive" if self.coefficient >= 0 else "negative"

    def summary(self) -> str:
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b}: "
            f"r={self.coefficient:.3f} ({self.strength()} {self.direction()}, "
            f"n={self.sample_count})"
        )


def _pearson(xs: List[float], ys: List[float]) -> Optional[float]:
    """Compute Pearson correlation coefficient; return None if undefined."""
    n = len(xs)
    if n < 2:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    std_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    std_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if std_x == 0 or std_y == 0:
        return None
    return cov / (std_x * std_y)


class CorrelationTracker:
    """Accumulate error-rate samples per pipeline and compute pairwise correlations."""

    def __init__(self, max_samples: int = 100) -> None:
        self._max = max_samples
        self._series: Dict[str, List[float]] = {}

    def record(self, pipeline: str, error_rate: float) -> None:
        """Append an error-rate observation for *pipeline*."""
        buf = self._series.setdefault(pipeline, [])
        buf.append(error_rate)
        if len(buf) > self._max:
            buf.pop(0)

    def pipelines(self) -> List[str]:
        return list(self._series.keys())

    def series(self, pipeline: str) -> List[float]:
        return list(self._series.get(pipeline, []))

    def correlate(self, a: str, b: str) -> Optional[CorrelationResult]:
        """Return a CorrelationResult for pipelines *a* and *b*, or None."""
        xs = self._series.get(a, [])
        ys = self._series.get(b, [])
        n = min(len(xs), len(ys))
        if n < 2:
            return None
        r = _pearson(xs[-n:], ys[-n:])
        if r is None:
            return None
        return CorrelationResult(pipeline_a=a, pipeline_b=b, coefficient=round(r, 6), sample_count=n)

    def all_pairs(self) -> List[CorrelationResult]:
        """Return CorrelationResult for every unique pair of tracked pipelines."""
        names = self.pipelines()
        results: List[CorrelationResult] = []
        for i, a in enumerate(names):
            for b in names[i + 1 :]:
                result = self.correlate(a, b)
                if result is not None:
                    results.append(result)
        return results

    def top_correlated(self, n: int = 5) -> List[CorrelationResult]:
        """Return the *n* pairs with the highest absolute correlation."""
        pairs = self.all_pairs()
        pairs.sort(key=lambda r: abs(r.coefficient), reverse=True)
        return pairs[:n]
