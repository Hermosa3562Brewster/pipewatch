"""Tests for pipewatch/exporter.py"""

import csv
import io
import json
import os
import tempfile

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.exporter import (
    export_csv,
    export_json,
    export_to_file,
    metrics_to_dict,
)


@pytest.fixture()
def sample_metrics():
    m = PipelineMetrics(name="orders_etl")
    m.start()
    m.record_success(latency_ms=120.0)
    m.record_success(latency_ms=80.0)
    m.record_failure()
    m.complete()
    return m


def test_metrics_to_dict_keys(sample_metrics):
    d = metrics_to_dict(sample_metrics)
    expected_keys = {
        "name", "status", "total_records", "success_count",
        "failure_count", "error_rate", "avg_latency_ms",
        "started_at", "completed_at", "exported_at",
    }
    assert expected_keys == set(d.keys())


def test_metrics_to_dict_values(sample_metrics):
    d = metrics_to_dict(sample_metrics)
    assert d["name"] == "orders_etl"
    assert d["total_records"] == 3
    assert d["success_count"] == 2
    assert d["failure_count"] == 1
    assert d["error_rate"] == round(1 / 3, 4)
    assert d["avg_latency_ms"] == 100.0


def test_export_json_returns_valid_json(sample_metrics):
    result = export_json([sample_metrics])
    parsed = json.loads(result)
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert parsed[0]["name"] == "orders_etl"


def test_export_json_multiple_pipelines(sample_metrics):
    m2 = PipelineMetrics(name="users_etl")
    result = export_json([sample_metrics, m2])
    parsed = json.loads(result)
    assert len(parsed) == 2
    names = {p["name"] for p in parsed}
    assert names == {"orders_etl", "users_etl"}


def test_export_csv_returns_header_and_row(sample_metrics):
    result = export_csv([sample_metrics])
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["name"] == "orders_etl"
    assert rows[0]["success_count"] == "2"


def test_export_csv_empty_list():
    assert export_csv([]) == ""


def test_export_to_file_json(sample_metrics):
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        path = tmp.name
    try:
        export_to_file([sample_metrics], path, fmt="json")
        with open(path, "r") as fh:
            parsed = json.load(fh)
        assert parsed[0]["name"] == "orders_etl"
    finally:
        os.unlink(path)


def test_export_to_file_csv(sample_metrics):
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        path = tmp.name
    try:
        export_to_file([sample_metrics], path, fmt="csv")
        with open(path, "r") as fh:
            content = fh.read()
        assert "orders_etl" in content
    finally:
        os.unlink(path)


def test_export_to_file_invalid_format(sample_metrics):
    with pytest.raises(ValueError, match="Unsupported export format"):
        export_to_file([sample_metrics], "/tmp/out.xml", fmt="xml")
