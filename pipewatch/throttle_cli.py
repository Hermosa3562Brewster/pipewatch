"""CLI commands for managing pipeline throttle settings."""
from __future__ import annotations

import argparse

from pipewatch.pipeline_throttle import PipelineThrottleManager
from pipewatch.throttle_display import print_throttle

_manager = PipelineThrottleManager()


def get_manager() -> PipelineThrottleManager:
    return _manager


def cmd_throttle_set(args: argparse.Namespace) -> None:
    mgr = get_manager()
    mgr.configure(
        pipeline=args.pipeline,
        min_interval_seconds=args.interval,
        enabled=not args.disabled,
    )
    state = "disabled" if args.disabled else f"interval={args.interval}s"
    print(f"Throttle configured for '{args.pipeline}': {state}")


def cmd_throttle_check(args: argparse.Namespace) -> None:
    mgr = get_manager()
    st = mgr.status(args.pipeline)
    print(st.summary())


def cmd_throttle_list(args: argparse.Namespace) -> None:
    print_throttle(get_manager())


def build_throttle_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("throttle", help="Manage pipeline run throttling")
    sub = p.add_subparsers(dest="throttle_cmd")

    p_set = sub.add_parser("set", help="Configure throttle for a pipeline")
    p_set.add_argument("pipeline", help="Pipeline name")
    p_set.add_argument("--interval", type=float, default=60.0, help="Min seconds between runs")
    p_set.add_argument("--disabled", action="store_true", help="Disable throttling for this pipeline")
    p_set.set_defaults(func=cmd_throttle_set)

    p_check = sub.add_parser("check", help="Check if a pipeline is currently throttled")
    p_check.add_argument("pipeline", help="Pipeline name")
    p_check.set_defaults(func=cmd_throttle_check)

    p_list = sub.add_parser("list", help="List all throttle statuses")
    p_list.set_defaults(func=cmd_throttle_list)
