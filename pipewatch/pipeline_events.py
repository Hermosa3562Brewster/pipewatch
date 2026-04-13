"""Pipeline event log: record and query discrete pipeline lifecycle events."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict

EVENT_TYPES = {"started", "completed", "failed", "stalled", "recovered"}


@dataclass
class PipelineEvent:
    pipeline: str
    event_type: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "message": self.message,
        }

    @staticmethod
    def from_dict(data: dict) -> "PipelineEvent":
        return PipelineEvent(
            pipeline=data["pipeline"],
            event_type=data["event_type"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            message=data.get("message", ""),
        )


class EventLog:
    def __init__(self, max_events: int = 500):
        self._max = max_events
        self._events: List[PipelineEvent] = []

    def record(self, pipeline: str, event_type: str, message: str = "") -> PipelineEvent:
        if event_type not in EVENT_TYPES:
            raise ValueError(f"Unknown event type '{event_type}'. Valid: {EVENT_TYPES}")
        event = PipelineEvent(pipeline=pipeline, event_type=event_type, message=message)
        self._events.append(event)
        if len(self._events) > self._max:
            self._events = self._events[-self._max:]
        return event

    def events_for(self, pipeline: str) -> List[PipelineEvent]:
        return [e for e in self._events if e.pipeline == pipeline]

    def recent(self, n: int = 20) -> List[PipelineEvent]:
        return self._events[-n:]

    def by_type(self, event_type: str) -> List[PipelineEvent]:
        return [e for e in self._events if e.event_type == event_type]

    def summary(self) -> Dict[str, int]:
        counts: Dict[str, int] = {t: 0 for t in EVENT_TYPES}
        for e in self._events:
            counts[e.event_type] += 1
        return counts

    def clear(self) -> None:
        self._events.clear()

    def __len__(self) -> int:
        return len(self._events)
