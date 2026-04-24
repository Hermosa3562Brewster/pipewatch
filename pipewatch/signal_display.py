"""Display helpers for pipeline signal events."""
from __future__ import annotations

from typing import List, Optional

from pipewatch.pipeline_signal import SignalBus, SignalEvent

_COLORS = {
    "started": "\033[32m",
    "completed": "\033[34m",
    "failed": "\033[31m",
    "retried": "\033[33m",
    "paused": "\033[35m",
}
_RESET = "\033[0m"
_BOLD = "\033[1m"


def _colored_signal(signal: str) -> str:
    color = _COLORS.get(signal, "\033[37m")
    return f"{color}{signal}{_RESET}"


def _format_event(event: SignalEvent) -> str:
    ts = event.emitted_at.strftime("%H:%M:%S")
    sig = _colored_signal(event.signal)
    payload_str = ""
    if event.payload:
        payload_str = " " + " ".join(f"{k}={v}" for k, v in event.payload.items())
    return f"  {ts}  {event.pipeline:<20}  {sig:<30}{payload_str}"


def render_signal_list(events: List[SignalEvent], title: str = "Signal Events") -> str:
    if not events:
        return "No signal events recorded."
    lines = [f"{_BOLD}{title}{_RESET}", "-" * 60]
    for event in events:
        lines.append(_format_event(event))
    return "\n".join(lines)


def render_signal_summary(bus: SignalBus) -> str:
    history = bus.history()
    if not history:
        return "Signal bus is empty."
    from collections import Counter
    counts: Counter = Counter(e.signal for e in history)
    lines = [f"{_BOLD}Signal Summary{_RESET}", "-" * 40]
    for signal, count in sorted(counts.items(), key=lambda x: -x[1]):
        lines.append(f"  {_colored_signal(signal):<30}  {count} event(s)")
    lines.append(f"  {'TOTAL':<30}  {len(history)}")
    return "\n".join(lines)


def print_signals(
    bus: SignalBus,
    signal: Optional[str] = None,
    pipeline: Optional[str] = None,
) -> None:
    events = bus.history(signal=signal, pipeline=pipeline)
    print(render_signal_list(events))
    print()
    print(render_signal_summary(bus))
