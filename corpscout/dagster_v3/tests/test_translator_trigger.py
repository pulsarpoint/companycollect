from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
    build_norway_brreg_build_queue_input,
)
from translator.task_queues import BUILD_TASK_QUEUE


def test_build_queue_workflow_id_is_stable():
    assert NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID == "build-queue-norway_brreg"


def test_build_queue_input_targets_norway_brreg():
    params = build_norway_brreg_build_queue_input()
    assert params.source_slug == "norway_brreg"
    assert params.queue_duckdb_path == "data/translator/norway_brreg.duckdb"
    assert params.translate_workflow_id == "translate-norway_brreg"
    # The build-queue trigger and the Translate handoff both run on the build queue.
    assert params.translate_task_queue == BUILD_TASK_QUEUE
    assert BUILD_TASK_QUEUE == "translation-build"
    assert params.batch_size == 50
    assert params.max_tokens == 8192
    assert params.max_batch_failures == 20


def test_build_queue_input_has_no_scan_or_flush_timeout():
    """Confirm the removed fields are absent from the new input dataclass."""
    params = build_norway_brreg_build_queue_input()
    assert not hasattr(params, "scan_timeout_seconds")
    assert not hasattr(params, "flush_timeout_seconds")
    assert not hasattr(params, "source_slug") or params.source_slug == "norway_brreg"
