"""CLI entry point for pipewatch — export subcommand."""

import argparse
import sys

from pipewatch.metrics import PipelineMetrics
from pipewatch.exporter import export_to_file, export_json


# ---------------------------------------------------------------------------
# Demo helper — builds a couple of fake pipelines for showcase / testing
# ---------------------------------------------------------------------------

def _build_demo_metrics() -> list:
    """Return a list of demo PipelineMetrics objects."""
    pipelines = [
        ("orders_etl", 950, 50),
        ("users_sync", 200, 0),
        ("inventory_load", 780, 20),
    ]
    result = []
    for name, successes, failures in pipelines:
        m = PipelineMetrics(name=name)
        m.start()
        for i in range(successes):
            m.record_success(latency_ms=float(50 + (i % 30)))
        for _ in range(failures):
            m.record_failure()
        m.complete()
        result.append(m)
    return result


# ---------------------------------------------------------------------------
# Sub-command handlers
# ---------------------------------------------------------------------------

def cmd_export(args: argparse.Namespace) -> int:
    """Handle the `pipewatch export` sub-command."""
    metrics_list = _build_demo_metrics()

    if args.output:
        export_to_file(metrics_list, args.output, fmt=args.format)
        print(f"Exported {len(metrics_list)} pipeline(s) to '{args.output}' [{args.format}].")
    else:
        if args.format == "json":
            print(export_json(metrics_list))
        else:
            from pipewatch.exporter import export_csv
            print(export_csv(metrics_list), end="")
    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Lightweight CLI monitor for ETL pipeline health.",
    )
    subparsers = parser.add_subparsers(dest="command")

    export_parser = subparsers.add_parser("export", help="Export pipeline metrics to a file or stdout.")
    export_parser.add_argument(
        "--format", "-f",
        choices=["json", "csv"],
        default="json",
        help="Output format (default: json).",
    )
    export_parser.add_argument(
        "--output", "-o",
        metavar="FILE",
        default=None,
        help="Write output to FILE instead of stdout.",
    )

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "export":
        return cmd_export(args)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
