"""Display helpers for pipeline rollup data."""
from __future__ import annotations

from typing import Dict, List

from pipewatch.pipeline_rollup import PipelineRollup, RollupBucket


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _fmt_dur(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds / 60:.1f}m"


def render_rollup_table(rollup: PipelineRollup) -> str:
    if not rollup.buckets:
        return f"[{rollup.pipeline}] No rollup data available."

    header = (
        f"Pipeline: {rollup.pipeline}  "
        f"Granularity: {rollup.granularity}\n"
    )
    col_w = [18, 8, 9, 9, 10, 12]
    titles = ["Window", "Runs", "Success", "Failure", "Err Rate", "Avg Dur"]
    sep = "+" + "+".join("-" * (w + 2) for w in col_w) + "+"
    row_fmt = "|" + "|".join(f" {{:<{w}}} " for w in col_w) + "|"

    lines = [header, sep, row_fmt.format(*titles), sep]
    for b in rollup.buckets:
        lines.append(
            row_fmt.format(
                b.label,
                str(b.total_runs),
                str(b.successes),
                str(b.failures),
                _fmt_pct(b.error_rate),
                _fmt_dur(b.avg_duration),
            )
        )
    lines.append(sep)
    return "\n".join(lines)


def render_rollup_summary(rollups: Dict[str, PipelineRollup]) -> str:
    if not rollups:
        return "No rollup data."

    lines = [f"Rollup Summary ({len(rollups)} pipeline(s)):", ""]
    for name, rollup in sorted(rollups.items()):
        total = rollup.total_runs()
        fails = rollup.total_failures()
        err = f"{(fails / total * 100):.1f}%" if total else "n/a"
        lines.append(
            f"  {name:<24}  runs={total:<6}  failures={fails:<6}  err_rate={err}"
        )
    return "\n".join(lines)


def print_rollup(rollup: PipelineRollup) -> None:
    print(render_rollup_table(rollup))


def print_rollup_summary(rollups: Dict[str, PipelineRollup]) -> None:
    print(render_rollup_summary(rollups))
