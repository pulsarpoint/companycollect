"""The spawned-child timeout wrapper shared by the per-document extractors.

The helper functions below are module-level on purpose: the child is a
``spawn`` process and unpickles the target by import path.
"""

import os
import time

import pytest

from dagster_v3.defs.common.child_timeout import (
    ChildFailedError,
    ChildTimeoutError,
    run_in_child_with_timeout,
)


def _double(value: int) -> int:
    return value * 2


def _sleep_forever() -> None:
    while True:
        time.sleep(1)


def _raise_value_error() -> None:
    raise ValueError("boom")


def _exit_hard() -> None:
    os._exit(3)


def _large(n: int) -> bytes:
    return b"x" * n


def test_returns_the_child_result() -> None:
    assert run_in_child_with_timeout(_double, (21,), timeout_seconds=60) == 42


def test_kills_a_child_that_outlives_its_budget() -> None:
    started = time.monotonic()
    with pytest.raises(ChildTimeoutError) as excinfo:
        run_in_child_with_timeout(_sleep_forever, (), timeout_seconds=1)
    assert excinfo.value.timeout_seconds == 1
    assert time.monotonic() - started < 30


def test_reports_an_exception_raised_in_the_child() -> None:
    with pytest.raises(ChildFailedError) as excinfo:
        run_in_child_with_timeout(_raise_value_error, (), timeout_seconds=60)
    assert "ValueError: boom" in excinfo.value.detail


def test_reports_a_child_that_died_without_a_result() -> None:
    with pytest.raises(ChildFailedError) as excinfo:
        run_in_child_with_timeout(_exit_hard, (), timeout_seconds=60)
    assert "code 3" in excinfo.value.detail


def test_returns_a_result_larger_than_the_pipe_buffer() -> None:
    # A pickled result bigger than the OS pipe buffer (~64 KiB) blocks the child in
    # send() until the parent reads; joining before reading would deadlock until the
    # budget kills the child (final review 2026-09-13).
    started = time.monotonic()
    result = run_in_child_with_timeout(_large, (500_000,), timeout_seconds=30)
    assert result == b"x" * 500_000
    assert time.monotonic() - started < 20


def test_errors_survive_pickling() -> None:
    import pickle

    timeout = pickle.loads(pickle.dumps(ChildTimeoutError(7)))
    failed = pickle.loads(pickle.dumps(ChildFailedError("x")))
    assert timeout.timeout_seconds == 7
    assert failed.detail == "x"
