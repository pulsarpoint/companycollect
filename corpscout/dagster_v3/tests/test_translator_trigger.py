from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
    NorwayBrregTranslationConfig,
    build_norway_brreg_build_queue_input,
)
from translator.task_queues import BUILD_TASK_QUEUE


def test_build_queue_workflow_id_is_stable():
    assert NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID == "build-queue-norway_brreg"


def test_build_queue_input_from_default_config():
    """build_norway_brreg_build_queue_input uses config values, not hardcoded literals."""
    config = NorwayBrregTranslationConfig()
    params = build_norway_brreg_build_queue_input(config)
    assert params.source_slug == "norway_brreg"
    assert params.queue_duckdb_path == "data/translator/norway_brreg.duckdb"
    assert params.translate_workflow_id == "translate-norway_brreg"
    assert params.translate_task_queue == BUILD_TASK_QUEUE
    assert BUILD_TASK_QUEUE == "translation-build"
    # Default config values
    assert params.batch_size == 50
    assert params.max_tokens == 32768
    assert params.extra_body_json == '{"chat_template_kwargs": {"enable_thinking": false}}'


def test_build_queue_input_respects_custom_config():
    """build_norway_brreg_build_queue_input reads knobs from the config object."""
    config = NorwayBrregTranslationConfig(
        batch_size=10,
        max_tokens=1024,
        extra_body_json='{"foo": "bar"}',
    )
    params = build_norway_brreg_build_queue_input(config)
    assert params.batch_size == 10
    assert params.max_tokens == 1024
    assert params.extra_body_json == '{"foo": "bar"}'


def test_build_queue_input_has_no_max_batch_failures():
    """max_batch_failures must not exist on the returned input (removed in Change 2)."""
    config = NorwayBrregTranslationConfig()
    params = build_norway_brreg_build_queue_input(config)
    assert not hasattr(params, "max_batch_failures"), (
        "max_batch_failures must have been removed from BuildQueueWorkflowInput"
    )


def test_build_queue_input_has_no_scan_or_flush_timeout():
    """Confirm the removed fields are absent from the new input dataclass."""
    config = NorwayBrregTranslationConfig()
    params = build_norway_brreg_build_queue_input(config)
    assert not hasattr(params, "scan_timeout_seconds")
    assert not hasattr(params, "flush_timeout_seconds")
    assert not hasattr(params, "source_slug") or params.source_slug == "norway_brreg"
