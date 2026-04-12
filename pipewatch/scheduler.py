"""Scheduler for periodic pipeline health checks and alert evaluation."""

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional


@dataclass
class ScheduledJob:
    name: str
    interval_seconds: float
    callback: Callable
    last_run: Optional[float] = None
    run_count: int = 0
    enabled: bool = True

    def is_due(self, now: float) -> bool:
        if not self.enabled:
            return False
        if self.last_run is None:
            return True
        return (now - self.last_run) >= self.interval_seconds

    def run(self) -> None:
        self.callback()
        self.last_run = time.monotonic()
        self.run_count += 1


class Scheduler:
    """Simple tick-based scheduler for running periodic jobs."""

    def __init__(self, tick_interval: float = 1.0):
        self.tick_interval = tick_interval
        self._jobs: Dict[str, ScheduledJob] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def add_job(self, name: str, interval_seconds: float, callback: Callable) -> None:
        """Register a named job to run at the given interval."""
        self._jobs[name] = ScheduledJob(
            name=name,
            interval_seconds=interval_seconds,
            callback=callback,
        )

    def remove_job(self, name: str) -> None:
        self._jobs.pop(name, None)

    def disable_job(self, name: str) -> None:
        if name in self._jobs:
            self._jobs[name].enabled = False

    def enable_job(self, name: str) -> None:
        if name in self._jobs:
            self._jobs[name].enabled = True

    def tick(self) -> List[str]:
        """Run all due jobs; return list of job names that ran."""
        now = time.monotonic()
        ran = []
        for job in self._jobs.values():
            if job.is_due(now):
                job.run()
                ran.append(job.name)
        return ran

    def start(self) -> None:
        """Start the scheduler in a background thread."""
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=self.tick_interval * 2)

    def _loop(self) -> None:
        while self._running:
            self.tick()
            time.sleep(self.tick_interval)

    @property
    def job_names(self) -> List[str]:
        return list(self._jobs.keys())
