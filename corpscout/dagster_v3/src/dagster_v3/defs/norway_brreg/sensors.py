import dagster as dg

from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_TRANSLATION_SOURCE_SLUG,
    NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
    NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
    describe_norway_brreg_translation_workflow,
    log_norway_brreg_translation_workflow_status,
    norway_brreg_translation_completion_job,
    norway_brreg_translation_queue_status_metadata,
    norway_brreg_translation_workflow_status_metadata,
)
from dagster_v3.defs.translations.assets import TemporalClient


@dg.sensor(
    job=norway_brreg_translation_completion_job,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def norway_brreg_translation_completion_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult:
    return build_norway_brreg_translation_completion_sensor_result(context)


def build_norway_brreg_translation_completion_sensor_result(
    context: dg.SensorEvaluationContext,
    *,
    temporal_client: TemporalClient | None = None,
) -> dg.SensorResult:
    queue_metadata = norway_brreg_translation_queue_status_metadata()
    try:
        workflow = describe_norway_brreg_translation_workflow(temporal_client=temporal_client)
    except Exception as exc:
        metadata = norway_brreg_translation_workflow_status_metadata(
            error=str(exc),
            queue_metadata=queue_metadata,
        )
        log_norway_brreg_translation_workflow_status(context.log.info, metadata)
        observation = dg.AssetObservation(
            asset_key=NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
            metadata=metadata,
        )
        return dg.SensorResult(
            skip_reason=f"Norway Brreg translation workflow status not available: {exc}",
            cursor=context.cursor,
            asset_events=[observation],
        )
    status = workflow["workflow_status"]
    run_id = workflow["workflow_run_id"]
    cursor = f"{NORWAY_BRREG_TRANSLATION_WORKFLOW_ID}:{run_id}"
    metadata = norway_brreg_translation_workflow_status_metadata(
        workflow,
        queue_metadata=queue_metadata,
    )
    log_norway_brreg_translation_workflow_status(context.log.info, metadata)
    observation = dg.AssetObservation(
        asset_key=NORWAY_BRREG_TRANSLATION_WORKFLOW_STATUS_ASSET_KEY,
        metadata=metadata,
    )
    if status != "COMPLETED":
        return dg.SensorResult(
            skip_reason=f"Norway Brreg translation workflow is not complete: status={status}",
            cursor=context.cursor,
            asset_events=[observation],
        )
    if context.cursor == cursor:
        return dg.SensorResult(
            skip_reason=f"Norway Brreg translations already triggered for {cursor}",
            cursor=context.cursor,
            asset_events=[observation],
        )
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                run_key=cursor,
                tags={
                    "source_slug": NORWAY_BRREG_TRANSLATION_SOURCE_SLUG,
                    "workflow_id": NORWAY_BRREG_TRANSLATION_WORKFLOW_ID,
                    "workflow_run_id": run_id,
                },
            )
        ],
        cursor=cursor,
        asset_events=[observation],
    )
