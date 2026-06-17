from __future__ import annotations

from typing import Any

from temporalio.common import WorkflowIDConflictPolicy

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.translations.assets import (
    start_translation_workflow,
)
from temporal.translations.queue import TranslationQueueWorkflowInput


class _FakeWorkflowHandle:
    id = "translation-smoke-source-fixed"
    result_run_id = "run-123"

    async def describe(self) -> Any:
        return _FakeWorkflowDescription()


class _FakeWorkflowDescription:
    run_id = "run-123"
    status = type("Status", (), {"name": "RUNNING"})()


class _FakeTemporalClient:
    def __init__(self) -> None:
        self.started: list[dict[str, Any]] = []

    async def start_workflow(
        self,
        workflow: Any,
        params: Any,
        *,
        id: str,
        task_queue: str,
        id_conflict_policy: WorkflowIDConflictPolicy = WorkflowIDConflictPolicy.UNSPECIFIED,
    ) -> Any:
        self.started.append(
            {
                "workflow": workflow,
                "params": params,
                "id": id,
                "task_queue": task_queue,
                "id_conflict_policy": id_conflict_policy,
            }
        )
        return _FakeWorkflowHandle()

    def get_workflow_handle(self, workflow_id: str) -> _FakeWorkflowHandle:
        assert workflow_id == "translation-smoke-source-fixed"
        return _FakeWorkflowHandle()


def test_generic_translation_assets_are_not_loaded_in_project_defs() -> None:
    definitions = load_project_defs()
    asset_keys = {key.to_user_string() for key in definitions.resolve_asset_graph().get_all_asset_keys()}

    assert "translation_temporal_workflow_started" not in asset_keys
    assert "translation_temporal_workflow_status" not in asset_keys


def test_start_translation_workflow_uses_generic_temporal_queue() -> None:
    client = _FakeTemporalClient()
    params = TranslationQueueWorkflowInput(
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

    result = start_translation_workflow(
        workflow_id="translation-smoke-source-fixed",
        params=params,
        client=client,
    )

    assert result["workflow_id"] == "translation-smoke-source-fixed"
    assert result["run_id"] == "run-123"
    assert client.started[0]["task_queue"] == "translation-local-llm"
    assert client.started[0]["id"] == "translation-smoke-source-fixed"
    assert client.started[0]["id_conflict_policy"] == WorkflowIDConflictPolicy.USE_EXISTING
    assert client.started[0]["params"] == params
