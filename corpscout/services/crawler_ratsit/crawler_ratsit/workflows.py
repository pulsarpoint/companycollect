from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

from crawler_ratsit.constants import (
    CRAWL_AND_UPLOAD_ACTIVITY,
    CRAWL_COMPANY_WORKFLOW,
    CRAWL_MAX_ATTEMPTS,
    RECORD_RESULT_ACTIVITY,
)
from crawler_ratsit.models import (
    CrawlActivityInput,
    CrawlCompanyInput,
    CrawlResult,
    ratsit_url,
)


@workflow.defn(name=CRAWL_COMPANY_WORKFLOW)
class RatsitCompanyWorkflow:
    @workflow.run
    async def run(self, workflow_input: CrawlCompanyInput) -> CrawlResult:
        selected_at = workflow.now()
        workflow_info = workflow.info()
        activity_input = CrawlActivityInput(
            company_id=workflow_input.company_id,
            batch_id=workflow_input.batch_id,
            selected_at=selected_at.isoformat(),
            temporal_workflow_id=workflow_info.workflow_id,
            temporal_run_id=workflow_info.run_id,
        )

        try:
            result = await workflow.execute_activity(
                CRAWL_AND_UPLOAD_ACTIVITY,
                activity_input,
                result_type=CrawlResult,
                start_to_close_timeout=timedelta(seconds=90),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    backoff_coefficient=2,
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=CRAWL_MAX_ATTEMPTS,
                ),
            )
        except ActivityError as error:
            if isinstance(error.cause, CancelledError):
                raise
            completed_at = workflow.now()
            error_type, error_message = _activity_failure(error)
            result = CrawlResult(
                company_id=workflow_input.company_id,
                batch_id=workflow_input.batch_id,
                outcome="retry_exhausted",
                selected_at=selected_at.isoformat(),
                attempted_at=selected_at.isoformat(),
                completed_at=completed_at.isoformat(),
                http_status=None,
                source_url=ratsit_url(workflow_input.company_id),
                source_bucket="",
                source_object_key="",
                content_size_bytes=0,
                duration_ms=max(
                    0,
                    int((completed_at - selected_at).total_seconds() * 1000),
                ),
                attempt_count=CRAWL_MAX_ATTEMPTS,
                error_type=error_type,
                error_message=error_message,
                temporal_workflow_id=workflow_info.workflow_id,
                temporal_run_id=workflow_info.run_id,
            )

        await workflow.execute_activity(
            RECORD_RESULT_ACTIVITY,
            result,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2,
                maximum_interval=timedelta(minutes=5),
            ),
        )
        return result


def _activity_failure(error: ActivityError) -> tuple[str, str]:
    cause = error.cause
    if isinstance(cause, ApplicationError) and cause.type:
        return cause.type, str(cause)[:4000]
    if cause is not None:
        return type(cause).__name__, str(cause)[:4000]
    return type(error).__name__, str(error)[:4000]
