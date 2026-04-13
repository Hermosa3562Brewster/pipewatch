"""Display helpers for pipeline profiler data."""
from __future__ import annotations

from typing import Dict

from pipewatch.pipeline_profiler import PipelineProfile

_BAR_WIDTH = 20


def _bar(share: float, width: int = _BAR_WIDTH) -> str:
    filled = round(share * width)
    return "█" * filled + "░" * (width - filled)


def render_profile_table(profile: PipelineProfile) -> str:
    if not profile.stages:
        return f"No stages recorded for pipeline '{profile.pipeline_name}'."

    shares = profile.stage_share()
    slowest = profile.slowest_stage()
    lines = [
        f"Pipeline: {profile.pipeline_name}",
        f"Total duration : {profile.total_duration():.3f}s",
        "",
        f"{'Stage':<20} {'Duration':>9}  {'Share':>6}  Bar",
        "-" * 60,
    ]
    for stage in profile.stages:
        share = shares[stage.name]
        marker = " ◀ slowest" if slowest and stage.name == slowest.name else ""
        lines.append(
            f"{stage.name:<20} {stage.duration_s:>8.3f}s  {share:>5.1%}  {_bar(share)}{marker}"
        )
    return "\n".join(lines)


def render_profile_summary(profiles: Dict[str, PipelineProfile]) -> str:
    if not profiles:
        return "No profile data available."

    lines = ["Pipeline Profiler Summary", "=" * 40]
    for name, profile in profiles.items():
        slowest = profile.slowest_stage()
        slowest_info = f"{slowest.name} ({slowest.duration_s:.3f}s)" if slowest else "n/a"
        lines.append(
            f"  {name:<25} total={profile.total_duration():.3f}s  slowest={slowest_info}"
        )
    return "\n".join(lines)


def print_profile(profile: PipelineProfile) -> None:
    print(render_profile_table(profile))
