"""CLI helpers for pipeline dependency commands."""

from __future__ import annotations
import argparse
from pipewatch.pipeline_dependencies import DependencyGraph
from pipewatch.dependency_display import (
    print_dependency_table,
    print_impact_summary,
    render_dependency_tree,
)


# Module-level shared graph (can be replaced in tests or wired via DI)
_default_graph: DependencyGraph = DependencyGraph()


def get_graph() -> DependencyGraph:
    return _default_graph


def cmd_dep_add(args: argparse.Namespace) -> None:
    """Add a dependency edge: *pipeline* depends on *upstream*."""
    graph = get_graph()
    graph.add_dependency(args.pipeline, args.upstream)
    print(f"Registered: {args.pipeline} depends on {args.upstream}")


def cmd_dep_remove(args: argparse.Namespace) -> None:
    """Remove a dependency edge."""
    graph = get_graph()
    graph.remove_dependency(args.pipeline, args.upstream)
    print(f"Removed dependency: {args.pipeline} -> {args.upstream}")


def cmd_dep_list(args: argparse.Namespace) -> None:
    """List all registered dependencies."""
    print_dependency_table(get_graph())


def cmd_dep_tree(args: argparse.Namespace) -> None:
    """Print a dependency tree rooted at a given pipeline."""
    print(render_dependency_tree(get_graph(), args.pipeline))


def cmd_dep_impact(args: argparse.Namespace) -> None:
    """Show the blast radius for a given pipeline."""
    print_impact_summary(get_graph(), args.pipeline)


def build_dep_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    dep = subparsers.add_parser("dep", help="Manage pipeline dependencies")
    dep_sub = dep.add_subparsers(dest="dep_cmd")

    add_p = dep_sub.add_parser("add", help="Add dependency")
    add_p.add_argument("pipeline")
    add_p.add_argument("upstream")
    add_p.set_defaults(func=cmd_dep_add)

    rm_p = dep_sub.add_parser("remove", help="Remove dependency")
    rm_p.add_argument("pipeline")
    rm_p.add_argument("upstream")
    rm_p.set_defaults(func=cmd_dep_remove)

    list_p = dep_sub.add_parser("list", help="List all dependencies")
    list_p.set_defaults(func=cmd_dep_list)

    tree_p = dep_sub.add_parser("tree", help="Show dependency tree")
    tree_p.add_argument("pipeline")
    tree_p.set_defaults(func=cmd_dep_tree)

    impact_p = dep_sub.add_parser("impact", help="Show impact of a failure")
    impact_p.add_argument("pipeline")
    impact_p.set_defaults(func=cmd_dep_impact)
