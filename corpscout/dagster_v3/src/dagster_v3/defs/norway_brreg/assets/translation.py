from dataclasses import dataclass

import dagster as dg
from dagster import AssetExecutionContext
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from translator.task_queues import BUILD_TASK_QUEUE

GROUP_NAME = "norway_brreg"
NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID = "build-queue-norway_brreg"


class NorwayBrregTranslationConfig(dg.Config):
    batch_size: int = 50
    max_tokens: int = 32768
    extra_body_json: str = '{"chat_template_kwargs": {"enable_thinking": false}}'


@dataclass(frozen=True)
class BuildQueueWorkflowInput:
    source_slug: str
    queue_duckdb_path: str
    translate_workflow_id: str
    translate_task_queue: str
    batch_size: int
    max_tokens: int
    extra_body_json: str


def build_norway_brreg_build_queue_input(
    config: NorwayBrregTranslationConfig,
) -> BuildQueueWorkflowInput:
    return BuildQueueWorkflowInput(
        source_slug="norway_brreg",
        queue_duckdb_path="data/translator/norway_brreg.duckdb",
        translate_workflow_id="translate-norway_brreg",
        translate_task_queue=BUILD_TASK_QUEUE,
        batch_size=config.batch_size,
        max_tokens=config.max_tokens,
        extra_body_json=config.extra_body_json,
    )


async def _start_norway_brreg_build_queue(
    temporal_address: str,
    config: NorwayBrregTranslationConfig,
) -> str:
    from translator.norway_brreg.workflows import BuildQueueWorkflow

    client = await Client.connect(temporal_address)
    handle = await client.start_workflow(
        BuildQueueWorkflow.run,
        build_norway_brreg_build_queue_input(config),
        id=NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
        task_queue=BUILD_TASK_QUEUE,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return handle.result_run_id


@dg.asset(
    deps=[dg.AssetKey("norway_brreg_entities_snapshot_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "temporal"},
    description=(
        "Fire-and-forget: start (or reuse) the BuildQueueWorkflow Temporal workflow "
        "after no_companies lands in ClickHouse. BuildQueueWorkflow seeds the DuckDB "
        "queue then starts TranslateWorkflow autonomously. Does not wait for completion."
    ),
)
def norway_brreg_translation_trigger(
    context: AssetExecutionContext,
    config: NorwayBrregTranslationConfig,
) -> dg.MaterializeResult:
    import asyncio as _asyncio
    import os

    address = os.environ.get("TEMPORAL_ADDRESS", "companycollect:7233")
    run_id = _asyncio.run(_start_norway_brreg_build_queue(address, config))
    context.log.info(
        "Started Norway Brreg BuildQueueWorkflow: workflow_id=%s run_id=%s",
        NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
        run_id,
    )
    return dg.MaterializeResult(
        metadata={
            "workflow_id": NORWAY_BRREG_BUILD_QUEUE_WORKFLOW_ID,
            "workflow_run_id": run_id,
            "task_queue": BUILD_TASK_QUEUE,
        }
    )


# Monthly full entity refresh. The translation trigger runs after the parquet-backed
# snapshot publish lands corpscout.no_companies in ClickHouse.
norway_brreg_refresh_job = dg.define_asset_job(
    "norway_brreg_refresh_job",
    selection=dg.AssetSelection.assets(
        "norway_brreg_translation_trigger"
    ).upstream().required_multi_asset_neighbors(),
)
norway_brreg_refresh_schedule = dg.ScheduleDefinition(
    name="norway_brreg_refresh_schedule",
    job=norway_brreg_refresh_job,
    cron_schedule="0 6 7 * *",  # monthly, 7th 06:00 (staggered vs estonia/latvia)
    execution_timezone="Europe/Belgrade",
)

norway_brreg_entity_updates_job = dg.define_asset_job(
    "norway_brreg_entity_updates_job",
    selection=dg.AssetSelection.assets(
        "norway_brreg_entity_updates_clickhouse"
    ).upstream().required_multi_asset_neighbors(),
)
norway_brreg_entity_updates_schedule = dg.build_schedule_from_partitioned_job(
    norway_brreg_entity_updates_job,
    name="norway_brreg_entity_updates_schedule",
)
