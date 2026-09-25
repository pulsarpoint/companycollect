"""Shared queue lifecycle against a disposable PostgreSQL; ClickHouse is a stub snapshot."""

from uuid import uuid4

import pytest

from dagster_v3.defs.common import draft_queue, queue_execution
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,  # noqa: F401
    store as store,  # noqa: F401
)

PROCESSOR = "test-queue-v1"


def draft(processing, *, scope="workspace"):
    task_id = draft_queue.find_draft(
        processing, scope=scope, processor=PROCESSOR, task_id=None
    )
    with processing.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET total=2 WHERE task_id=%s", (task_id,)
        )
    return task_id


def start(processing, task_id, **changes):
    settings = dict(
        task_id=task_id,
        processor=PROCESSOR,
        profile={"mode": "a", "batch_size": 10},
        execution_id=None,
        freshness_days=30,
        run_id=str(uuid4()),
        snapshot=lambda: ({"relation": "corpscout.test_queue", "total": 2}, 2),
        transport_keys=("batch_size",),
        label="Test",
    )
    settings.update(changes)
    with processing.selection_lock(task_id):
        return queue_execution.start_execution(processing, **settings)


def test_new_execution_uses_the_default_identity_and_freezes(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    run_id = str(uuid4())
    task = start(processing, task_id, default_execution_id=run_id)
    execution = task["config"]["execution"]
    assert task["status"] == "selected" and task["frozen_at"] is not None
    assert execution["execution_id"] == run_id
    assert execution["profile"] == {"mode": "a", "batch_size": 10}
    assert (
        task["total"] == 2 and task["source_info"]["relation"] == "corpscout.test_queue"
    )
    # Without a default, a fresh UUID identifies the execution.
    other = draft(processing, scope="other")
    assert start(processing, other)["config"]["execution"]["execution_id"] != run_id


def test_resume_ignores_transport_keys_but_not_the_profile(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    first = start(processing, task_id)
    resumed = start(processing, task_id, profile={"mode": "a", "batch_size": 99})
    assert resumed["config"] == first["config"]
    with pytest.raises(ValueError, match="frozen"):
        start(processing, task_id, profile={"mode": "b", "batch_size": 10})
    with pytest.raises(ValueError, match="existing execution"):
        start(processing, task_id, execution_id=str(uuid4()))


def test_outstanding_imports_and_empty_queues_block_start(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    receipt = str(uuid4())
    with processing.selection_lock(task_id):
        draft_queue.prepare_submission(
            processing,
            task_id=task_id,
            submission_id=receipt,
            source="manual",
            selection={},
            fingerprint="0" * 64,
        )
    with pytest.raises(ValueError, match="outstanding imports"):
        start(processing, task_id)
    draft_queue.finish_submission(
        processing, submission_id=receipt, task_id=task_id, count=0, total=0
    )
    with pytest.raises(ValueError, match="empty queue"):
        start(processing, task_id, snapshot=lambda: ({"relation": "x", "total": 0}, 0))


def test_record_completion_refuses_remaining_and_derives_skips(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    start(processing, task_id)
    with pytest.raises(ValueError, match="published outcome"):
        queue_execution.record_completion(
            processing, task_id=task_id, remaining=1, succeeded=1, failed=0
        )
    task = queue_execution.record_completion(
        processing, task_id=task_id, remaining=0, succeeded=1, failed=0
    )
    assert task["status"] == "completed" and task["completed_at"] is not None
    assert (
        task["succeeded_count"],
        task["terminal_failed_count"],
        task["skipped_count"],
    ) == (1, 0, 1)
    assert task["admitted_count"] == 2 and task["work_config"] == {"finished": True}


def test_completion_never_precedes_a_freeze_from_a_faster_clock(store):  # noqa: F811
    # frozen_at comes from the Dagster host clock, completed_at from PostgreSQL's.
    processing, _ = store
    task_id = draft(processing)
    start(processing, task_id)
    with processing.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET frozen_at=now() + interval '1 minute' WHERE task_id=%s",
            (task_id,),
        )
    task = queue_execution.record_completion(
        processing, task_id=task_id, remaining=0, succeeded=2, failed=0
    )
    assert task["status"] == "completed"
    assert task["completed_at"] >= task["frozen_at"]


def test_explicit_execution_id_only_resumes(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    with pytest.raises(ValueError, match="No saved execution to resume"):
        start(processing, task_id, execution_id=str(uuid4()))
    assert processing.task(task_id)["status"] == "draft"
