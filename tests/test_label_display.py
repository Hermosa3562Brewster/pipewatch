"""Tests for pipewatch.label_display."""
import pytest
from pipewatch.pipeline_labels import LabelRegistry
from pipewatch.label_display import (
    render_labels_for_pipeline,
    render_labels_table,
    render_label_summary,
)


@pytest.fixture()
def registry() -> LabelRegistry:
    reg = LabelRegistry()
    reg.set("pipe_a", "env", "prod")
    reg.set("pipe_a", "team", "data")
    reg.set("pipe_b", "env", "staging")
    return reg


def test_render_labels_for_pipeline_contains_name(registry):
    out = render_labels_for_pipeline(registry, "pipe_a")
    assert "pipe_a" in out


def test_render_labels_for_pipeline_contains_keys_and_values(registry):
    out = render_labels_for_pipeline(registry, "pipe_a")
    assert "env" in out
    assert "prod" in out
    assert "team" in out
    assert "data" in out


def test_render_labels_for_pipeline_no_labels(registry):
    out = render_labels_for_pipeline(registry, "pipe_c")
    assert "No labels set" in out


def test_render_labels_table_contains_pipeline_names(registry):
    out = render_labels_table(registry, ["pipe_a", "pipe_b"])
    assert "pipe_a" in out
    assert "pipe_b" in out


def test_render_labels_table_contains_header_keys(registry):
    out = render_labels_table(registry, ["pipe_a", "pipe_b"])
    assert "env" in out
    assert "team" in out


def test_render_labels_table_empty_pipelines(registry):
    out = render_labels_table(registry, [])
    assert "No pipelines" in out


def test_render_labels_table_pipeline_with_no_labels(registry):
    out = render_labels_table(registry, ["pipe_a", "pipe_c"])
    # pipe_c has no labels; dash placeholder should appear
    assert "-" in out


def test_render_label_summary_counts(registry):
    out = render_label_summary(registry, ["pipe_a", "pipe_b", "pipe_c"])
    # 2 out of 3 have labels
    assert "2/3" in out


def test_render_label_summary_distinct_keys(registry):
    out = render_label_summary(registry, ["pipe_a", "pipe_b"])
    assert "env" in out
    assert "team" in out


def test_render_label_summary_no_pipelines():
    reg = LabelRegistry()
    out = render_label_summary(reg, [])
    assert "0/0" in out
