"""Render pipeline event logs to the terminal."""

from typing import List, Optional
from pipewatch.pipeline_events import PipelineEvent, EventLog

_TYPE_COLORS = {
    "started": "\033[94m",    # blue
    "completed": "\033[92m",  # green
    "failed": "\033[91m",     # red
    "stalled": "\033[93m",    # yellow
    "recovered": "\033[96m",  # cyan
}
_RESET = "\033[0m"


def _colored_type(event_type: str) -> str:
    color = _TYPE_COLORS.get(event_type, "")
    label = event_type.upper().ljust(9)
    return f"{color}{label}{_RESET}" if color else label


def _format_event(event: PipelineEvent) -> str:
    ts = event.timestamp.strftime("%H:%M:%S")
    ctype = _colored_type(event.event_type)
    msg = f"  {event.message}" if event.message else ""
    return f"  [{ts}] {ctype}  {event.pipeline}{msg}"


def render_event_list(events: List[PipelineEvent], title: str = "Pipeline Events") -> str:
    if not events:
        return f"  {title}\n  (no events recorded)\n"
    lines = [f"  {title}", "-" * 55]
    for e in events:
        lines.append(_format_event(e))
    return "\n".join(lines) + "\n"


def render_event_summary(log: EventLog) -> str:
    summary = log.summary()
    total = len(log)
    lines = ["  Event Summary", "-" * 40]
    lines.append(f"  Total events : {total}")
    for etype, count in summary.items():
        color = _TYPE_COLORS.get(etype, "")
        label = f"{color}{etype.ljust(10)}{_RESET}"
        lines.append(f"  {label}: {count}")
    return "\n".join(lines) + "\n"


def print_events(log: EventLog, pipeline: Optional[str] = None, n: int = 20) -> None:
    if pipeline:
        events = log.events_for(pipeline)[-n:]
        title = f"Events for '{pipeline}' (last {n})"
    else:
        events = log.recent(n)
        title = f"Recent Events (last {n})"
    print(render_event_list(events, title=title))
    print(render_event_summary(log))
