"""Tests for pipewatch.pipeline_sorter."""

import pytest
from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_sorter import (
    sort_pipelines,
    top_n,
    available_sort_keys,
)


def _make_metrics(name: str, successes: int = 0, failures: int = 0) -> PipelineMetrics:
    m = PipelineMetrics(name=name)
    m.success_count = successes
    m.failure_count = failures
    return m


@pytest.fixture()
def sample_map():
    return {
        "gamma": _make_metrics("gamma", successes=10, failures=5),
        "alpha": _make_metrics("alpha", successes=20, failures=0),
        "beta": _make_metrics("beta", successes=5, failures=15),
    }


def test_available_sort_keys_returns_list():
    keys = available_sort_keys()
    assert isinstance(keys, list)
    assert "name" in keys
    assert "error_rate" in keys


def test_sort_by_name_ascending(sample_map):
    result = sort_pipelines(sample_map, key="name", reverse=False)
    names = [name for name, _ in result]
    assert names == ["alpha", "beta", "gamma"]


def test_sort_by_name_descending(sample_map):
    result = sort_pipelines(sample_map, key="name", reverse=True)
    names = [name for name, _ in result]
    assert names == ["gamma", "beta", "alpha"]


def test_sort_by_error_rate_descending(sample_map):
    # beta: 15/20 = 0.75, gamma: 5/15 ≈ 0.33, alpha: 0/20 = 0.0
    result = sort_pipelines(sample_map, key="error_rate", reverse=True)
    names = [name for name, _ in result]
    assert names[0] == "beta"
    assert names[-1] == "alpha"


def test_sort_by_successes_ascending(sample_map):
    result = sort_pipelines(sample_map, key="successes", reverse=False)
    names = [name for name, _ in result]
    assert names[0] == "beta"   # 5 successes
    assert names[-1] == "alpha"  # 20 successes


def test_sort_by_failures_descending(sample_map):
    result = sort_pipelines(sample_map, key="failures", reverse=True)
    names = [name for name, _ in result]
    assert names[0] == "beta"   # 15 failures
    assert names[-1] == "alpha"  # 0 failures


def test_sort_by_throughput(sample_map):
    # alpha: 20, gamma: 15, beta: 20 — alpha and beta tied at 20
    result = sort_pipelines(sample_map, key="throughput", reverse=True)
    names = [name for name, _ in result]
    assert "gamma" == names[-1]  # gamma has lowest throughput (15)


def test_sort_invalid_key_raises(sample_map):
    with pytest.raises(ValueError, match="Unknown sort key"):
        sort_pipelines(sample_map, key="nonexistent")


def test_sort_returns_all_items(sample_map):
    result = sort_pipelines(sample_map, key="name")
    assert len(result) == len(sample_map)


def test_top_n_returns_correct_count(sample_map):
    result = top_n(sample_map, n=2, key="error_rate")
    assert len(result) == 2


def test_top_n_returns_highest_error_rate_first(sample_map):
    result = top_n(sample_map, n=1, key="error_rate")
    assert result[0][0] == "beta"


def test_top_n_larger_than_map(sample_map):
    result = top_n(sample_map, n=100, key="name")
    assert len(result) == len(sample_map)
