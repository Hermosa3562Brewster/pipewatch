"""Render a Rich table summarising all pipelines in a PipelineRegistry."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table
from rich import box

from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.display import _status_badge

_console = Console()


def render_registry_table(registry: PipelineRegistry) -> Table:
    """Build and return a Rich Table for all pipelines in *registry*."""
    table = Table(
        title="Pipeline Registry",
        box=box.SIMPLE_HEAVY,
        show_lines=False,
        expand=False,
    )

    table.add_column("Pipeline", style="bold cyan", no_wrap=True)
    table.add_column("Status", justify="center")
    table.add_column("Successes", justify="right")
    table.add_column("Failures", justify="right")
    table.add_column("Error Rate", justify="right")
    table.add_column("Interval (s)", justify="right")

    for name in registry.names():
        pm = registry.get(name)  # type: ignore[assignment]
        error_rate = pm.error_rate()
        rate_str = f"{error_rate:.1%}"
        rate_style = "red" if error_rate >= 0.5 else ("yellow" if error_rate > 0 else "green")

        table.add_row(
            pm.name,
            _status_badge(pm.status),
            str(pm.success_count),
            str(pm.failure_count),
            f"[{rate_style}]{rate_str}[/{rate_style}]",
            str(pm.interval),
        )

    return table


def render_registry_summary(registry: PipelineRegistry) -> str:
    """Return a one-line plaintext summary of the registry."""
    total = len(registry)
    active = len(registry.active())
    failed = len(registry.failed())
    idle = total - active - failed
    return (
        f"Registry: {total} pipeline(s) — "
        f"{active} running, {failed} failed, {idle} idle"
    )


def print_registry(registry: PipelineRegistry) -> None:
    """Print the registry summary and table to stdout."""
    _console.print(render_registry_summary(registry))
    _console.print(render_registry_table(registry))
