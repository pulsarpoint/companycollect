import asyncio
from datetime import timedelta
from uuid import uuid4

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from crawler_ratsit.constants import (
    CRAWL_AND_UPLOAD_ACTIVITY,
    CRAWL_MAX_ATTEMPTS,
    RECORD_RESULT_ACTIVITY,
)
from crawler_ratsit.models import (
    CrawlActivityInput,
    CrawlCompanyInput,
    CrawlResult,
    ratsit_url,
)
from crawler_ratsit.workflows import RatsitCompanyWorkflow


def test_workflow_captures_then_records_success() -> None:
    async def run_test() -> None:
        recorded: list[CrawlResult] = []

        @activity.defn(name=CRAWL_AND_UPLOAD_ACTIVITY)
        async def crawl(activity_input: CrawlActivityInput) -> CrawlResult:
            return _success_result(activity_input)

        @activity.defn(name=RECORD_RESULT_ACTIVITY)
        async def record(result: CrawlResult) -> None:
            recorded.append(result)

        environment = await WorkflowEnvironment.start_time_skipping()
        try:
            async with Worker(
                environment.client,
                task_queue="ratsit-test",
                workflows=[RatsitCompanyWorkflow],
                activities=[crawl, record],
            ):
                result = await environment.client.execute_workflow(
                    RatsitCompanyWorkflow.run,
                    CrawlCompanyInput(
                        company_id="195562434182",
                        batch_id=str(uuid4()),
                    ),
                    id=f"ratsit-test-{uuid4()}",
                    task_queue="ratsit-test",
                )
        finally:
            await environment.shutdown()

        assert result.outcome == "success"
        assert result.company_id == "195562434182"
        assert result.source_url == "https://www.ratsit.se/5562434182"
        assert recorded == [result]

    asyncio.run(run_test())


def test_workflow_records_retry_exhaustion_without_an_s3_object() -> None:
    async def run_test() -> None:
        attempts = 0
        recorded: list[CrawlResult] = []

        @activity.defn(name=CRAWL_AND_UPLOAD_ACTIVITY)
        async def crawl(_activity_input: CrawlActivityInput) -> CrawlResult:
            nonlocal attempts
            attempts += 1
            raise ApplicationError("browser timed out", type="browser_timeout")

        @activity.defn(name=RECORD_RESULT_ACTIVITY)
        async def record(result: CrawlResult) -> None:
            recorded.append(result)

        environment = await WorkflowEnvironment.start_time_skipping()
        try:
            async with Worker(
                environment.client,
                task_queue="ratsit-retry-test",
                workflows=[RatsitCompanyWorkflow],
                activities=[crawl, record],
            ):
                result = await environment.client.execute_workflow(
                    RatsitCompanyWorkflow.run,
                    CrawlCompanyInput(
                        company_id="195562434182",
                        batch_id=str(uuid4()),
                    ),
                    id=f"ratsit-retry-test-{uuid4()}",
                    task_queue="ratsit-retry-test",
                    execution_timeout=timedelta(minutes=1),
                )
        finally:
            await environment.shutdown()

        assert attempts == CRAWL_MAX_ATTEMPTS
        assert result.outcome == "retry_exhausted"
        assert result.company_id == "195562434182"
        assert result.source_url == "https://www.ratsit.se/5562434182"
        assert result.source_object_key == ""
        assert result.attempt_count == CRAWL_MAX_ATTEMPTS
        assert result.error_type == "browser_timeout"
        assert recorded == [result]

    asyncio.run(run_test())


def _success_result(activity_input: CrawlActivityInput) -> CrawlResult:
    return CrawlResult(
        company_id=activity_input.company_id,
        batch_id=activity_input.batch_id,
        outcome="success",
        selected_at=activity_input.selected_at,
        attempted_at=activity_input.selected_at,
        completed_at=activity_input.selected_at,
        http_status=200,
        source_url=ratsit_url(activity_input.company_id),
        source_bucket="source-sweden-ratsit",
        source_object_key="raw/response.json",
        content_size_bytes=100,
        duration_ms=10,
        attempt_count=1,
        error_type="",
        error_message="",
        temporal_workflow_id=activity_input.temporal_workflow_id,
        temporal_run_id=activity_input.temporal_run_id,
    )
