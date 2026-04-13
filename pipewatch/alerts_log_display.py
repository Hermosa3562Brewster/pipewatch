"""Rendering helpers for the AlertsLog."""

from __future__ import annotations

from typing import List

from pipewatch.pipeline_alerts_log import AlertsLog, FiredAlert


_SEV_COLOURS = {
    ">": "\033[91m",  # red for gt
    "<": "\033[93m",  # yellow for lt
}
_RESET = "\033[0m"


def _coloured_operator(op: str) -> str:
    colour = _SEV_COLOURS.get(op, "\033[0m")
    return f"{colour}{op}{_RESET}"


def render_alerts_log_table(log: AlertsLog, last_n: int = 20) -> str:
    entries = log.get_last_n(last_n)
    if not entries:
        return "No alerts fired yet."

    header = f"{'Fired At':<22} {'Pipeline':<20} {'Rule':<20} {'Metric':<18} {'Op':>3} {'Threshold':>10} {'Actual':>10}"
    sep = "-" * len(header)
    lines = [header, sep]
    for e in entries:
        ts = e.fired_at.strftime("%Y-%m-%d %H:%M:%S")
        lines.append(
            f"{ts:<22} {e.pipeline:<20} {e.rule_name:<20} {e.metric:<18} "
            f"{_coloured_operator(e.operator):>3} {e.threshold:>10.4f} {e.actual_value:>10.4f}"
        )
    return "\n".join(lines)


def render_alerts_log_summary(log: AlertsLog) -> str:
    total = log.count()
    pipelines = log.pipelines_with_alerts()
    lines = [
        "=== Alerts Log Summary ===",
        f"Total fired : {total}",
        f"Pipelines   : {len(pipelines)}",
    ]
    for p in pipelines:
        count = len(log.get_for_pipeline(p))
        lines.append(f"  {p}: {count} alert(s)")
    return "\n".join(lines)


def print_alerts_log(log: AlertsLog, last_n: int = 20) -> None:
    print(render_alerts_log_summary(log))
    print()
    print(render_alerts_log_table(log, last_n=last_n))
