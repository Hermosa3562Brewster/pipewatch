"""Display helpers for circuit breaker state."""

from __future__ import annotations

from typing import Dict

from pipewatch.pipeline_circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerRegistry,
    STATE_CLOSED,
    STATE_OPEN,
    STATE_HALF_OPEN,
)


def _colored_state(state: str) -> str:
    if state == STATE_CLOSED:
        return f"\033[32m{state}\033[0m"      # green
    if state == STATE_OPEN:
        return f"\033[31m{state}\033[0m"       # red
    if state == STATE_HALF_OPEN:
        return f"\033[33m{state}\033[0m"       # yellow
    return state


def render_circuit_breaker_table(registry: CircuitBreakerRegistry) -> str:
    breakers = registry.all()
    if not breakers:
        return "No circuit breakers registered."

    header = f"{'Pipeline':<24} {'State':<12} {'Failures':>8} {'Threshold':>10} {'Tripped At'}"
    sep = "-" * 72
    rows = [header, sep]
    for name, cb in sorted(breakers.items()):
        tripped = cb.tripped_at.isoformat() if cb.tripped_at else "-"
        state_str = _colored_state(cb.state)
        rows.append(
            f"{name:<24} {state_str:<20} {cb.failure_count:>8} {cb.failure_threshold:>10} {tripped}"
        )
    return "\n".join(rows)


def render_circuit_breaker_summary(registry: CircuitBreakerRegistry) -> str:
    breakers = registry.all()
    total = len(breakers)
    open_count = sum(1 for cb in breakers.values() if cb.state == STATE_OPEN)
    half_open = sum(1 for cb in breakers.values() if cb.state == STATE_HALF_OPEN)
    closed = total - open_count - half_open

    lines = [
        "=== Circuit Breaker Summary ===",
        f"Total  : {total}",
        f"Closed : {closed}",
        f"Open   : {open_count}",
        f"Half   : {half_open}",
    ]
    return "\n".join(lines)


def print_circuit_breakers(registry: CircuitBreakerRegistry) -> None:
    print(render_circuit_breaker_summary(registry))
    print()
    print(render_circuit_breaker_table(registry))
