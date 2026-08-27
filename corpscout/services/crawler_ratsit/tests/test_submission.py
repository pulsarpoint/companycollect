import asyncio
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

from temporalio import activity
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from crawler_ratsit.constants import (
    CRAWL_AND_UPLOAD_ACTIVITY,
    CRAWL_COMPANY_WORKFLOW,
    HTTP_TASK_QUEUE,
    RECORD_RESULT_ACTIVITY,
)
from crawler_ratsit.models import (
    CrawlActivityInput,
    CrawlCompanyInput,
    CrawlResult,
    ratsit_url,
)
from crawler_ratsit.submission import company_workflow_id, submit_company_workflow
from crawler_ratsit.workflows import RatsitCompanyWorkflow


def test_company_workflow_id_is_stable() -> None:
    assert company_workflow_id("5562434182") == "ratsit/company/5562434182"
    assert company_workflow_id("195562434182") == "ratsit/company/195562434182"


def test_submit_company_workflow_starts_without_waiting_for_a_result() -> None:
    async def run_test() -> None:
        client = AsyncMock(spec=Client)
        client.start_workflow.return_value = Mock(first_execution_run_id="run-1")
        workflow_input = CrawlCompanyInput(
            company_id="5562434182",
            batch_id=str(uuid4()),
        )

        submission = await submit_company_workflow(
            client,
            workflow_input,
            task_queue="ratsit-test",
        )

        assert submission.status == "submitted"
        assert submission.workflow_id == "ratsit/company/5562434182"
        assert submission.temporal_run_id == "run-1"
        client.start_workflow.assert_awaited_once_with(
            RatsitCompanyWorkflow.run,
            workflow_input,
            id="ratsit/company/5562434182",
            task_queue="ratsit-test",
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            memo={
                "batch_id": workflow_input.batch_id,
                "company_id": workflow_input.company_id,
            },
        )

    asyncio.run(run_test())


def test_submit_company_workflow_reports_an_open_duplicate() -> None:
    async def run_test() -> None:
        client = AsyncMock(spec=Client)
        client.start_workflow.side_effect = WorkflowAlreadyStartedError(
            "ratsit/company/5562434182",
            CRAWL_COMPANY_WORKFLOW,
            run_id="existing-run",
        )
        workflow_input = CrawlCompanyInput(
            company_id="5562434182",
            batch_id=str(uuid4()),
        )

        submission = await submit_company_workflow(
            client,
            workflow_input,
            task_queue="ratsit-test",
        )

        assert submission.status == "already_running"
        assert submission.workflow_id == "ratsit/company/5562434182"
        assert submission.temporal_run_id == "existing-run"

    asyncio.run(run_test())


def test_temporal_rejects_an_open_company_and_reuses_its_id_after_completion() -> None:
    async def run_test() -> None:
        crawl_started = asyncio.Event()
        release_crawl = asyncio.Event()
        recorded: list[CrawlResult] = []

        @activity.defn(name=CRAWL_AND_UPLOAD_ACTIVITY)
        async def crawl(activity_input: CrawlActivityInput) -> CrawlResult:
            crawl_started.set()
            await release_crawl.wait()
            return _success_result(activity_input)

        @activity.defn(name=RECORD_RESULT_ACTIVITY)
        async def record(result: CrawlResult) -> None:
            recorded.append(result)

        environment = await WorkflowEnvironment.start_time_skipping()
        task_queue = f"ratsit-submission-{uuid4()}"
        first_input = CrawlCompanyInput(
            company_id="5562434182",
            batch_id=str(uuid4()),
        )
        second_input = CrawlCompanyInput(
            company_id=first_input.company_id,
            batch_id=str(uuid4()),
        )
        try:
            workflow_worker = Worker(
                environment.client,
                task_queue=task_queue,
                workflows=[RatsitCompanyWorkflow],
                activities=[record],
            )
            http_worker = Worker(
                environment.client,
                task_queue=HTTP_TASK_QUEUE,
                activities=[crawl],
            )
            async with workflow_worker, http_worker:
                first = await submit_company_workflow(
                    environment.client,
                    first_input,
                    task_queue=task_queue,
                )
                await asyncio.wait_for(crawl_started.wait(), timeout=5)

                duplicate = await submit_company_workflow(
                    environment.client,
                    second_input,
                    task_queue=task_queue,
                )

                assert first.status == "submitted"
                assert duplicate.status == "already_running"
                assert duplicate.temporal_run_id == first.temporal_run_id

                release_crawl.set()
                assert first.temporal_run_id is not None
                first_result = await environment.client.get_workflow_handle_for(
                    RatsitCompanyWorkflow.run,
                    first.workflow_id,
                    run_id=first.temporal_run_id,
                ).result()

                second = await submit_company_workflow(
                    environment.client,
                    second_input,
                    task_queue=task_queue,
                )
                assert second.status == "submitted"
                assert second.temporal_run_id != first.temporal_run_id
                assert second.temporal_run_id is not None
                second_result = await environment.client.get_workflow_handle_for(
                    RatsitCompanyWorkflow.run,
                    second.workflow_id,
                    run_id=second.temporal_run_id,
                ).result()
        finally:
            await environment.shutdown()

        assert first_result.batch_id == first_input.batch_id
        assert second_result.batch_id == second_input.batch_id
        assert recorded == [first_result, second_result]

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
