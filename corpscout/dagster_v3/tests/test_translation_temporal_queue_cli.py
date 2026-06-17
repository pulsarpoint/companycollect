from __future__ import annotations

from temporal.translations.queue import (
    LOCAL_LLM_TRANSLATION_TASK_QUEUE,
    TranslationQueueWorkflowInput,
)
from temporal.translations.queue_cli import (
    build_worker_config,
    parse_start_args,
)


def test_build_worker_config_serializes_local_llm_activities() -> None:
    config = build_worker_config()

    assert config["task_queue"] == LOCAL_LLM_TRANSLATION_TASK_QUEUE
    assert config["max_concurrent_activities"] == 1


def test_parse_start_args_builds_workflow_input() -> None:
    workflow_id, params = parse_start_args(
        [
            "--duckdb-path",
            "/tmp/translations.duckdb",
            "--source",
            "norway-brreg",
            "--batch-size",
            "50",
            "--timeout-seconds",
            "120",
            "--max-tokens",
            "4096",
        ]
    )

    assert workflow_id.startswith("translation-norway-brreg-")
    assert params == TranslationQueueWorkflowInput(
        duckdb_path="/tmp/translations.duckdb",
        batch_size=50,
        timeout_seconds=120,
        max_batch_failures=0,
        worker_id="translation-temporal-worker",
        max_tokens=4096,
        extra_body_json='{"chat_template_kwargs":{"enable_thinking":false}}',
        initialize_timeout_seconds=300,
        batch_timeout_buffer_seconds=30,
        summarize_timeout_seconds=30,
        activity_maximum_attempts=1,
    )
