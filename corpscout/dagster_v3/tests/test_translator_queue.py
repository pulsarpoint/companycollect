"""Integration tests for TranslationQueue retry / terminal logic.

All tests run against a REAL temp DuckDB (no SQL-string mocking).
"""

import pytest

from translator.queue import (
    QUEUE_STATUS_FAILED,
    QUEUE_STATUS_FAILED_RETRYABLE,
    TranslationQueue,
    TranslationQueueItem,
)


def _make_item(text: str, index: int = 0) -> TranslationQueueItem:
    return TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.no_companies",
        source_pk=f"pk-{index}-{hash(text)}",
        source_field="activity_text_original",
        source_text=text,
        target_language="en",
    )


def _seed_queue(tmp_path, texts: list[str]) -> TranslationQueue:
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    q.enqueue_items([_make_item(t, i) for i, t in enumerate(texts)])
    return q


# ---------------------------------------------------------------------------
# fail_batch with non-retryable category → terminal on first failure
# ---------------------------------------------------------------------------


def test_fail_batch_invalid_json_becomes_terminal_immediately(tmp_path):
    """fail_batch with error_category='invalid_json' sets items to terminal QUEUE_STATUS_FAILED
    on the very first failure (attempt_count=1, non-retryable)."""
    q = _seed_queue(tmp_path, ["Holdingselskap"])

    claimed = q.claim_batch(limit=10, worker_id="w1")
    assert len(claimed) == 1
    assert claimed[0].attempt_count == 0

    q.fail_batch(
        claimed,
        error_category="invalid_json",
        error_message="bad json",
        duration_seconds=0.01,
    )

    # Item must be terminal — not re-claimable.
    reclaimed = q.claim_batch(limit=10, worker_id="w2")
    assert reclaimed == [], (
        "terminal failed items must never be re-claimed by claim_batch"
    )

    # Confirm the status is the terminal constant.
    with q._connect() as conn:
        row = conn.execute(
            "select status, attempt_count from translation_items where 1=1"
        ).fetchone()
    assert row[0] == QUEUE_STATUS_FAILED, f"expected '{QUEUE_STATUS_FAILED}', got '{row[0]}'"
    assert row[1] == 1, f"expected attempt_count=1, got {row[1]}"


# ---------------------------------------------------------------------------
# fail_batch with retryable category → stays failed_retryable INDEFINITELY
# ---------------------------------------------------------------------------


def test_fail_batch_timeout_retryable_never_becomes_terminal(tmp_path):
    """fail_batch with a retryable category (timeout) NEVER terminates items —
    they stay failed_retryable and remain claimable regardless of how many times
    they fail.  A transient LLM outage must not permanently lose work."""
    q = _seed_queue(tmp_path, ["Bygg og anlegg"])

    for attempt in range(1, 7):  # 6 attempts — well beyond any former cap
        claimed = q.claim_batch(limit=10, worker_id=f"w{attempt}")
        assert len(claimed) == 1, (
            f"expected to claim 1 item on attempt {attempt}, got {len(claimed)}"
        )
        assert claimed[0].attempt_count == attempt - 1

        q.fail_batch(
            claimed,
            error_category="timeout",
            error_message="timed out",
            duration_seconds=0.01,
        )

        with q._connect() as conn:
            row = conn.execute(
                "select status, attempt_count from translation_items where 1=1"
            ).fetchone()

        # Must ALWAYS remain retryable — no cap.
        assert row[0] == QUEUE_STATUS_FAILED_RETRYABLE, (
            f"expected failed_retryable after attempt {attempt}, got '{row[0]}' "
            "(retryable items must never become terminal)"
        )
        assert row[1] == attempt

    # Even after 6 failures the item is still claimable.
    reclaimed = q.claim_batch(limit=10, worker_id="w7")
    assert len(reclaimed) == 1, (
        "retryable items must remain claimable indefinitely; got empty claim after 6 failures"
    )


# ---------------------------------------------------------------------------
# Drain simulation: claim → fail(invalid_json) must terminate (no infinite loop)
# ---------------------------------------------------------------------------


def test_drain_with_nonretryable_failures_terminates(tmp_path):
    """With N items all failing with 'invalid_json' (terminal on first failure),
    the claim→fail loop must drain completely and claim eventually returns [].

    This proves there is no infinite loop when all failures are non-retryable.
    """
    texts = [f"item-{i}" for i in range(5)]
    q = _seed_queue(tmp_path, texts)

    iterations = 0
    max_iterations = 100  # guard against actual infinite loops in tests

    while True:
        claimed = q.claim_batch(limit=10, worker_id="drain-worker")
        if not claimed:
            break
        q.fail_batch(
            claimed,
            error_category="invalid_json",
            error_message="bad json",
            duration_seconds=0.01,
        )
        iterations += 1
        assert iterations < max_iterations, (
            f"drain loop did not terminate after {max_iterations} iterations — infinite loop detected"
        )

    # All 5 items must now be terminal failed.
    summary = q.summary()
    assert summary.failed_items == 5, (
        f"expected all 5 items to be terminal failed, got {summary.failed_items}"
    )
    assert summary.pending_items == 0
    assert summary.failed_retryable_items == 0
    assert summary.total_items == 5
    assert iterations == 1, (
        f"non-retryable items should have all been claimed in 1 batch (batch_size=10 > 5 items), "
        f"but loop ran {iterations} iterations"
    )


# ---------------------------------------------------------------------------
# summary() reports terminal failed vs failed_retryable correctly
# ---------------------------------------------------------------------------


def test_summary_counts_failed_and_failed_retryable_separately(tmp_path):
    """summary() must distinguish terminal failed_items from failed_retryable_items."""
    q = _seed_queue(tmp_path, ["A", "B", "C"])

    # Claim all 3, fail A with invalid_json (terminal), B with timeout (retryable),
    # leave C leased (won't complete in this test).
    all_claimed = q.claim_batch(limit=10, worker_id="w1")
    assert len(all_claimed) == 3

    # We must fail them individually (fail_batch needs a single batch_id).
    # Re-seed individually instead.
    q2 = TranslationQueue(tmp_path / "q2.duckdb")
    q2.initialize()
    q2.enqueue_items([_make_item("terminal-item", 0)])
    q2.enqueue_items([_make_item("retryable-item", 1)])

    claimed_t = q2.claim_batch(limit=1, worker_id="w-terminal")
    q2.fail_batch(
        claimed_t,
        error_category="invalid_json",
        error_message="bad",
        duration_seconds=0.0,
    )

    claimed_r = q2.claim_batch(limit=1, worker_id="w-retryable")
    q2.fail_batch(
        claimed_r,
        error_category="timeout",
        error_message="timed out",
        duration_seconds=0.0,
    )

    summary = q2.summary()
    assert summary.failed_items == 1, (
        f"expected 1 terminal failed item, got {summary.failed_items}"
    )
    assert summary.failed_retryable_items == 1, (
        f"expected 1 failed_retryable item, got {summary.failed_retryable_items}"
    )
    assert summary.total_items == 2
