"""Pipeline signal broadcasting — allows components to emit and subscribe to named signals."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional


@dataclass
class SignalEvent:
    pipeline: str
    signal: str
    payload: Optional[dict]
    emitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "signal": self.signal,
            "payload": self.payload,
            "emitted_at": self.emitted_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SignalEvent":
        return cls(
            pipeline=d["pipeline"],
            signal=d["signal"],
            payload=d.get("payload"),
            emitted_at=datetime.fromisoformat(d["emitted_at"]),
        )

    def summary(self) -> str:
        return f"[{self.pipeline}] signal={self.signal} at {self.emitted_at.strftime('%H:%M:%S')}"


Handler = Callable[[SignalEvent], None]


class SignalBus:
    """Lightweight pub/sub bus for pipeline signals."""

    def __init__(self, max_history: int = 200) -> None:
        self._handlers: Dict[str, List[Handler]] = {}
        self._wildcard: List[Handler] = []
        self._history: List[SignalEvent] = []
        self._max_history = max_history

    def subscribe(self, signal: str, handler: Handler) -> None:
        """Subscribe *handler* to a specific *signal* name."""
        self._handlers.setdefault(signal, []).append(handler)

    def subscribe_all(self, handler: Handler) -> None:
        """Subscribe *handler* to every signal regardless of name."""
        self._wildcard.append(handler)

    def unsubscribe(self, signal: str, handler: Handler) -> None:
        if signal in self._handlers:
            self._handlers[signal] = [
                h for h in self._handlers[signal] if h is not handler
            ]

    def emit(self, pipeline: str, signal: str, payload: Optional[dict] = None) -> SignalEvent:
        event = SignalEvent(pipeline=pipeline, signal=signal, payload=payload)
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history.pop(0)
        for handler in self._handlers.get(signal, []):
            handler(event)
        for handler in self._wildcard:
            handler(event)
        return event

    def history(self, signal: Optional[str] = None, pipeline: Optional[str] = None) -> List[SignalEvent]:
        result = self._history
        if signal:
            result = [e for e in result if e.signal == signal]
        if pipeline:
            result = [e for e in result if e.pipeline == pipeline]
        return list(result)

    def clear_history(self) -> None:
        self._history.clear()
