"""Tests for pipewatch.scheduler."""

import time

import pytest

from pipewatch.scheduler import Scheduler, ScheduledJob


def test_scheduled_job_is_due_on_first_call():
    job = ScheduledJob(name="j", interval_seconds=10, callback=lambda: None)
    assert job.is_due(time.monotonic()) is True


def test_scheduled_job_not_due_before_interval():
    counter = []
    job = ScheduledJob(name="j", interval_seconds=60, callback=lambda: counter.append(1))
    job.run()
    assert not job.is_due(time.monotonic())


def test_scheduled_job_due_after_interval():
    job = ScheduledJob(name="j", interval_seconds=0.01, callback=lambda: None)
    job.run()
    time.sleep(0.05)
    assert job.is_due(time.monotonic()) is True


def test_scheduled_job_disabled_never_due():
    job = ScheduledJob(name="j", interval_seconds=0, callback=lambda: None, enabled=False)
    assert job.is_due(time.monotonic()) is False


def test_scheduler_tick_runs_due_jobs():
    results = []
    s = Scheduler()
    s.add_job("a", 0, lambda: results.append("a"))
    ran = s.tick()
    assert "a" in ran
    assert "a" in results


def test_scheduler_tick_skips_not_due():
    results = []
    s = Scheduler()
    s.add_job("b", 9999, lambda: results.append("b"))
    s.tick()  # first tick runs it
    results.clear()
    ran = s.tick()  # second tick should skip
    assert "b" not in ran
    assert results == []


def test_scheduler_remove_job():
    s = Scheduler()
    s.add_job("x", 0, lambda: None)
    s.remove_job("x")
    assert "x" not in s.job_names


def test_scheduler_enable_disable():
    results = []
    s = Scheduler()
    s.add_job("c", 0, lambda: results.append(1))
    s.disable_job("c")
    s.tick()
    assert results == []
    s.enable_job("c")
    s.tick()
    assert results == [1]


def test_scheduler_background_thread():
    results = []
    s = Scheduler(tick_interval=0.05)
    s.add_job("bg", 0.05, lambda: results.append(1))
    s.start()
    time.sleep(0.3)
    s.stop()
    assert len(results) >= 2
