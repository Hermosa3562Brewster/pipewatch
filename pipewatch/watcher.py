"""PipelineWatcher: ties together metrics, alerts, history, and the scheduler."""

from typing import Callable, Dict, List, Optional

from pipewatch.alerts import AlertManager
from pipewatch.history import PipelineHistory, RunRecord
from pipewatch.metrics import PipelineMetrics
from pipewatch.scheduler import Scheduler


class PipelineWatcher:
    """
    Orchestrates periodic evaluation of pipeline metrics and alert rules.
    Fires registered on_alert callbacks when a rule is triggered.
    """

    def __init__(
        self,
        metrics: Dict[str, PipelineMetrics],
        alert_manager: AlertManager,
        history: Optional[PipelineHistory] = None,
        check_interval: float = 5.0,
    ):
        self.metrics = metrics
        self.alert_manager = alert_manager
        self.history = history
        self._alert_callbacks: List[Callable[[str, str], None]] = []
        self._scheduler = Scheduler(tick_interval=1.0)
        self._scheduler.add_job(
            name="check_alerts",
            interval_seconds=check_interval,
            callback=self._evaluate_alerts,
        )

    def register_alert_callback(self, cb: Callable[[str, str], None]) -> None:
        """Register a function(pipeline_name, alert_description) called on alert."""
        self._alert_callbacks.append(cb)

    def _evaluate_alerts(self) -> None:
        for name, m in self.metrics.items():
            triggered = self.alert_manager.check_all(m)
            for description in triggered:
                for cb in self._alert_callbacks:
                    cb(name, description)

    def snapshot_to_history(self, pipeline_name: str) -> None:
        """Persist current metrics snapshot for a pipeline to history."""
        if self.history is None:
            return
        m = self.metrics.get(pipeline_name)
        if m is None:
            return
        record = RunRecord(
            pipeline_name=pipeline_name,
            status=m.status,
            success_count=m.success_count,
            failure_count=m.failure_count,
            duration_seconds=m.duration_seconds,
            error_rate=m.error_rate,
        )
        self.history.add_run(record)

    def start(self) -> None:
        self._scheduler.start()

    def stop(self) -> None:
        self._scheduler.stop()
