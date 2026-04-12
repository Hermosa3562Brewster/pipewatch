"""Alert system for pipewatch — triggers notifications when pipeline metrics exceed thresholds."""

from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetrics


@dataclass
class AlertRule:
    """Defines a threshold-based alert rule for a pipeline metric."""

    name: str
    metric: str  # 'error_rate', 'throughput', 'duration'
    threshold: float
    operator: str  # 'gt', 'lt', 'gte', 'lte'
    message: Optional[str] = None

    def check(self, value: float) -> bool:
        """Return True if the rule is violated."""
        ops = {
            "gt": lambda v, t: v > t,
            "lt": lambda v, t: v < t,
            "gte": lambda v, t: v >= t,
            "lte": lambda v, t: v <= t,
        }
        op_fn = ops.get(self.operator)
        if op_fn is None:
            raise ValueError(f"Unknown operator: {self.operator!r}")
        return op_fn(value, self.threshold)

    def description(self, value: float) -> str:
        msg = self.message or f"{self.metric} {self.operator} {self.threshold}"
        return f"[ALERT] {self.name}: {msg} (current={value:.4f})"


@dataclass
class AlertManager:
    """Evaluates alert rules against pipeline metrics and collects triggered alerts."""

    rules: List[AlertRule] = field(default_factory=list)

    def add_rule(self, rule: AlertRule) -> None:
        self.rules.append(rule)

    def evaluate(self, metrics: PipelineMetrics) -> List[str]:
        """Evaluate all rules against the given metrics. Returns list of alert messages."""
        triggered: List[str] = []
        metric_values = {
            "error_rate": metrics.error_rate,
            "throughput": metrics.throughput,
            "duration": metrics.duration if metrics.duration is not None else 0.0,
        }
        for rule in self.rules:
            value = metric_values.get(rule.metric)
            if value is None:
                continue
            if rule.check(value):
                triggered.append(rule.description(value))
        return triggered
