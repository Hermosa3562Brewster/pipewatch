"""Tests for pipeline_signal and signal_display."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_signal import SignalBus, SignalEvent
from pipewatch.signal_display import (
    render_signal_list,
    render_signal_summary,
)


@pytest.fixture()
def bus() -> SignalBus:
    return SignalBus(max_history=50)


# --- SignalEvent ---

def test_signal_event_to_dict_keys():
    bus = SignalBus()
    event = bus.emit("etl", "started")
    d = event.to_dict()
    assert set(d.keys()) == {"pipeline", "signal", "payload", "emitted_at"}


def test_signal_event_from_dict_roundtrip():
    bus = SignalBus()
    event = bus.emit("etl", "completed", payload={"rows": 42})
    d = event.to_dict()
    restored = SignalEvent.from_dict(d)
    assert restored.pipeline == event.pipeline
    assert restored.signal == event.signal
    assert restored.payload == event.payload


def test_signal_event_summary_contains_pipeline_and_signal():
    bus = SignalBus()
    event = bus.emit("my_pipe", "failed")
    s = event.summary()
    assert "my_pipe" in s
    assert "failed" in s


# --- SignalBus emit & history ---

def test_emit_returns_signal_event(bus):
    event = bus.emit("pipe_a", "started")
    assert isinstance(event, SignalEvent)
    assert event.pipeline == "pipe_a"
    assert event.signal == "started"


def test_history_grows_with_emits(bus):
    bus.emit("p", "started")
    bus.emit("p", "completed")
    assert len(bus.history()) == 2


def test_history_filtered_by_signal(bus):
    bus.emit("p", "started")
    bus.emit("p", "failed")
    bus.emit("p", "started")
    result = bus.history(signal="started")
    assert len(result) == 2
    assert all(e.signal == "started" for e in result)


def test_history_filtered_by_pipeline(bus):
    bus.emit("alpha", "started")
    bus.emit("beta", "started")
    result = bus.history(pipeline="alpha")
    assert len(result) == 1
    assert result[0].pipeline == "alpha"


def test_max_history_is_respected():
    small_bus = SignalBus(max_history=3)
    for _ in range(5):
        small_bus.emit("p", "started")
    assert len(small_bus.history()) == 3


def test_clear_history(bus):
    bus.emit("p", "started")
    bus.clear_history()
    assert bus.history() == []


# --- subscribe / unsubscribe ---

def test_subscribe_handler_called_on_emit(bus):
    received = []
    bus.subscribe("started", received.append)
    bus.emit("p", "started")
    assert len(received) == 1
    assert received[0].signal == "started"


def test_subscribe_not_called_for_different_signal(bus):
    received = []
    bus.subscribe("started", received.append)
    bus.emit("p", "failed")
    assert received == []


def test_subscribe_all_receives_every_signal(bus):
    received = []
    bus.subscribe_all(received.append)
    bus.emit("p", "started")
    bus.emit("p", "failed")
    assert len(received) == 2


def test_unsubscribe_stops_delivery(bus):
    received = []
    handler = received.append
    bus.subscribe("started", handler)
    bus.unsubscribe("started", handler)
    bus.emit("p", "started")
    assert received == []


# --- display ---

def test_render_signal_list_empty():
    result = render_signal_list([])
    assert "No signal events" in result


def test_render_signal_list_contains_pipeline_name(bus):
    bus.emit("my_pipeline", "completed")
    result = render_signal_list(bus.history())
    assert "my_pipeline" in result


def test_render_signal_summary_shows_counts(bus):
    bus.emit("p", "started")
    bus.emit("p", "started")
    bus.emit("p", "failed")
    result = render_signal_summary(bus)
    assert "started" in result
    assert "failed" in result
    assert "TOTAL" in result


def test_render_signal_summary_empty_bus():
    empty_bus = SignalBus()
    result = render_signal_summary(empty_bus)
    assert "empty" in result.lower()
