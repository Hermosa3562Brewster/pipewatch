"""Notification helpers used by PipelineWatcher alert callbacks."""

import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import List


@dataclass
class AlertEvent:
    pipeline: str
    description: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def format(self) -> str:
        return f"[{self.timestamp}] ALERT [{self.pipeline}]: {self.description}"


class StdoutNotifier:
    """Writes alert events to stdout."""

    def __call__(self, pipeline: str, description: str) -> None:
        event = AlertEvent(pipeline=pipeline, description=description)
        print(event.format(), file=sys.stdout, flush=True)


class InMemoryNotifier:
    """Collects alert events in memory (useful for testing and dashboards)."""

    def __init__(self, max_events: int = 200):
        self.max_events = max_events
        self._events: List[AlertEvent] = []

    def __call__(self, pipeline: str, description: str) -> None:
        event = AlertEvent(pipeline=pipeline, description=description)
        self._events.append(event)
        if len(self._events) > self.max_events:
            self._events = self._events[-self.max_events :]

    @property
    def events(self) -> List[AlertEvent]:
        return list(self._events)

    def clear(self) -> None:
        self._events.clear()

    def latest(self, n: int = 10) -> List[AlertEvent]:
        return self._events[-n:]
