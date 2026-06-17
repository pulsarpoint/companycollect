import asyncio
from dataclasses import asdict
import os
from typing import Any, Protocol

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from temporal.translations.queue import (
    LOCAL_LLM_TRANSLATION_TASK_QUEUE,
    TranslationQueueWorkflow,
    TranslationQueueWorkflowInput,
)


class TemporalWorkflowHandle(Protocol):
    id: str
    result_run_id: str

    async def describe(self) -> Any: ...


class TemporalClient(Protocol):
    async def start_workflow(
        self,
        workflow: Any,
        params: TranslationQueueWorkflowInput,
        *,
        id: str,
        task_queue: str,
        id_conflict_policy: WorkflowIDConflictPolicy = WorkflowIDConflictPolicy.UNSPECIFIED,
    ) -> TemporalWorkflowHandle: ...

    def get_workflow_handle(self, workflow_id: str) -> TemporalWorkflowHandle: ...


def start_translation_workflow(
    *,
    workflow_id: str,
    params: TranslationQueueWorkflowInput,
    temporal_address: str = "",
    client: TemporalClient | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        _start_translation_workflow(
            workflow_id=workflow_id,
            params=params,
            temporal_address=temporal_address,
            client=client,
        )
    )


async def _start_translation_workflow(
    *,
    workflow_id: str,
    params: TranslationQueueWorkflowInput,
    temporal_address: str,
    client: TemporalClient | None = None,
) -> dict[str, Any]:
    active_client = client or await Client.connect(_temporal_address(temporal_address))
    handle = await active_client.start_workflow(
        TranslationQueueWorkflow.run,
        params,
        id=workflow_id,
        task_queue=LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return {
        "workflow_id": handle.id,
        "run_id": handle.result_run_id,
        "task_queue": LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        "input": asdict(params),
    }


def _temporal_address(config_value: str) -> str:
    if config_value:
        return config_value
    return os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
