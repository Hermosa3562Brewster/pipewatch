"""CLI commands for pipeline lineage management."""
from __future__ import annotations
import argparse
from pipewatch.pipeline_lineage import LineageRegistry
from pipewatch.lineage_display import print_lineage, render_dataset_map

_registry = LineageRegistry()


def get_registry() -> LineageRegistry:
    return _registry


def cmd_lineage_add(args: argparse.Namespace) -> None:
    reg = get_registry()
    inputs = args.inputs.split(",") if args.inputs else []
    outputs = args.outputs.split(",") if args.outputs else []
    reg.register(args.pipeline, inputs=[i.strip() for i in inputs if i.strip()],
                 outputs=[o.strip() for o in outputs if o.strip()])
    print(f"Lineage updated for '{args.pipeline}'.")


def cmd_lineage_show(args: argparse.Namespace) -> None:
    reg = get_registry()
    print_lineage(reg)


def cmd_lineage_datasets(args: argparse.Namespace) -> None:
    reg = get_registry()
    print(render_dataset_map(reg))


def cmd_lineage_remove(args: argparse.Namespace) -> None:
    reg = get_registry()
    reg.remove(args.pipeline)
    print(f"Lineage removed for '{args.pipeline}'.")


def build_lineage_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("lineage", help="Pipeline lineage commands")
    sub = p.add_subparsers(dest="lineage_cmd")

    add = sub.add_parser("add", help="Register lineage for a pipeline")
    add.add_argument("pipeline")
    add.add_argument("--inputs", default="", help="Comma-separated input datasets")
    add.add_argument("--outputs", default="", help="Comma-separated output datasets")
    add.set_defaults(func=cmd_lineage_add)

    show = sub.add_parser("show", help="Show lineage table")
    show.set_defaults(func=cmd_lineage_show)

    ds = sub.add_parser("datasets", help="Show dataset map")
    ds.set_defaults(func=cmd_lineage_datasets)

    rm = sub.add_parser("remove", help="Remove lineage for a pipeline")
    rm.add_argument("pipeline")
    rm.set_defaults(func=cmd_lineage_remove)
