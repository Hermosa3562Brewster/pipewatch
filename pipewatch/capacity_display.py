"""Display helpers for pipeline capacity tracking."""
from __future__ import annotations
from typing import List
from pipewatch.pipeline_capacity import CapacityTracker, CapacityStatus


def _pct_bar(pct: float, width: int = 10) -> str:
    filled = min(int(pct / 100 * width), width)
    bar = "#" * filled + "-" * (width - filled)
    return f"[{bar}]"


def _overload_badge(status: CapacityStatus) -> str:
    return "[OVERLOADED]" if status.is_overloaded else "[OK]      "


def render_capacity_table(tracker: CapacityTracker) -> str:
    pipelines = tracker.all_pipelines()
    if not pipelines:
        return "No capacity data recorded."

    header = f"{'Pipeline':<20} {'Queue':>8} {'Memory':>8} {'CPU':>8}  {'Status'}"
    sep = "-" * len(header)
    rows = [header, sep]

    for name in sorted(pipelines):
        st = tracker.status(name)
        if st is None:
            continue
        q = f"{st.queue_pct:6.1f}%"
        m = f"{st.memory_pct:6.1f}%"
        c = f"{st.reading.cpu_pct:6.1f}%"
        badge = _overload_badge(st)
        rows.append(f"{name:<20} {q:>8} {m:>8} {c:>8}  {badge}")

    return "\n".join(rows)


def render_capacity_summary(tracker: CapacityTracker) -> str:
    pipelines = tracker.all_pipelines()
    total = len(pipelines)
    overloaded = sum(
        1 for p in pipelines if (s := tracker.status(p)) and s.is_overloaded
    )
    lines = [
        "=== Capacity Summary ===",
        f"Tracked pipelines : {total}",
        f"Overloaded        : {overloaded}",
    ]
    if overloaded:
        lines.append("Overloaded pipelines:")
        for p in sorted(pipelines):
            st = tracker.status(p)
            if st and st.is_overloaded:
                lines.append(f"  - {st.summary()}")
    return "\n".join(lines)


def print_capacity(tracker: CapacityTracker) -> None:
    print(render_capacity_table(tracker))
    print()
    print(render_capacity_summary(tracker))
