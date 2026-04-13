"""Tests for pipewatch.pipeline_labels."""
import pytest
from pipewatch.pipeline_labels import LabelRegistry


@pytest.fixture()
def registry() -> LabelRegistry:
    return LabelRegistry()


def test_set_and_get(registry):
    registry.set("pipe_a", "env", "prod")
    assert registry.get("pipe_a", "env") == "prod"


def test_get_missing_key_returns_none(registry):
    assert registry.get("pipe_a", "env") is None


def test_get_missing_pipeline_returns_none(registry):
    assert registry.get("ghost", "team") is None


def test_labels_for_returns_dict(registry):
    registry.set("pipe_a", "env", "prod")
    registry.set("pipe_a", "team", "data")
    labels = registry.labels_for("pipe_a")
    assert labels == {"env": "prod", "team": "data"}


def test_labels_for_unknown_pipeline_is_empty(registry):
    assert registry.labels_for("ghost") == {}


def test_remove_existing_key(registry):
    registry.set("pipe_a", "env", "prod")
    removed = registry.remove("pipe_a", "env")
    assert removed is True
    assert registry.get("pipe_a", "env") is None


def test_remove_missing_key_returns_false(registry):
    assert registry.remove("pipe_a", "env") is False


def test_clear_removes_all_labels(registry):
    registry.set("pipe_a", "env", "prod")
    registry.set("pipe_a", "team", "data")
    registry.clear("pipe_a")
    assert registry.labels_for("pipe_a") == {}


def test_pipelines_with_label_key_only(registry):
    registry.set("pipe_a", "env", "prod")
    registry.set("pipe_b", "env", "staging")
    registry.set("pipe_c", "team", "data")
    result = registry.pipelines_with_label("env")
    assert set(result) == {"pipe_a", "pipe_b"}


def test_pipelines_with_label_key_and_value(registry):
    registry.set("pipe_a", "env", "prod")
    registry.set("pipe_b", "env", "staging")
    result = registry.pipelines_with_label("env", "prod")
    assert result == ["pipe_a"]


def test_all_keys_returns_union(registry):
    registry.set("pipe_a", "env", "prod")
    registry.set("pipe_b", "team", "data")
    assert registry.all_keys() == ["env", "team"]


def test_all_keys_empty_registry(registry):
    assert registry.all_keys() == []


def test_filter_by_labels_all_match(registry):
    registry.set("pipe_a", "env", "prod")
    registry.set("pipe_a", "team", "data")
    registry.set("pipe_b", "env", "prod")
    result = registry.filter_by_labels(
        {"env": "prod", "team": "data"}, ["pipe_a", "pipe_b"]
    )
    assert result == ["pipe_a"]


def test_filter_by_labels_empty_requirements(registry):
    registry.set("pipe_a", "env", "prod")
    result = registry.filter_by_labels({}, ["pipe_a", "pipe_b"])
    assert set(result) == {"pipe_a", "pipe_b"}
