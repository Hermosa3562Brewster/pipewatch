"""Tests for metadata_display module."""
from pipewatch.pipeline_metadata import MetadataStore
from pipewatch.metadata_display import (
    render_metadata_for_pipeline,
    render_metadata_table,
    render_metadata_summary,
)


def _store_with_data() -> MetadataStore:
    s = MetadataStore()
    s.set("pipe_a", "owner", "alice")
    s.set("pipe_a", "env", "prod")
    s.set("pipe_b", "team", "data-eng")
    return s


def test_render_for_pipeline_contains_name():
    s = _store_with_data()
    out = render_metadata_for_pipeline(s, "pipe_a")
    assert "pipe_a" in out


def test_render_for_pipeline_contains_keys_and_values():
    s = _store_with_data()
    out = render_metadata_for_pipeline(s, "pipe_a")
    assert "owner" in out
    assert "alice" in out


def test_render_for_pipeline_no_data():
    s = MetadataStore()
    out = render_metadata_for_pipeline(s, "ghost")
    assert "No metadata" in out


def test_render_table_contains_all_pipelines():
    s = _store_with_data()
    out = render_metadata_table(s)
    assert "pipe_a" in out
    assert "pipe_b" in out


def test_render_table_contains_header():
    s = _store_with_data()
    out = render_metadata_table(s)
    assert "Pipeline" in out
    assert "Key" in out
    assert "Value" in out


def test_render_table_empty_store():
    s = MetadataStore()
    out = render_metadata_table(s)
    assert "No metadata" in out


def test_render_summary_counts():
    s = _store_with_data()
    out = render_metadata_summary(s)
    assert "2 pipeline" in out
    assert "3 total key" in out


def test_render_summary_empty():
    s = MetadataStore()
    out = render_metadata_summary(s)
    assert "0 pipeline" in out
