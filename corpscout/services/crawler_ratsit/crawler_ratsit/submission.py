from dataclasses import dataclass
from typing import Literal

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from crawler_ratsit.models import CrawlCompanyInput, validate_company_id
from crawler_ratsit.workflows import RatsitCompanyWorkflow

type SubmissionStatus = Literal["submitted", "already_running"]


@dataclass(frozen=True, slots=True)
class CompanyWorkflowSubmission:
    company_id: str
    batch_id: str
    workflow_id: str
    temporal_run_id: str | None
    status: SubmissionStatus


def company_workflow_id(company_id: str) -> str:
    validate_company_id(company_id)
    return f"ratsit/company/{company_id}"


async def submit_company_workflow(
    client: Client,
    workflow_input: CrawlCompanyInput,
    *,
    task_queue: str,
) -> CompanyWorkflowSubmission:
    """Submit one crawl without waiting for its workflow to finish."""
    if task_queue.strip() == "":
        raise ValueError("task_queue must not be blank")

    workflow_id = company_workflow_id(workflow_input.company_id)
    try:
        handle = await client.start_workflow(
            RatsitCompanyWorkflow.run,
            workflow_input,
            id=workflow_id,
            task_queue=task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            memo={
                "batch_id": workflow_input.batch_id,
                "company_id": workflow_input.company_id,
            },
        )
    except WorkflowAlreadyStartedError as error:
        return CompanyWorkflowSubmission(
            company_id=workflow_input.company_id,
            batch_id=workflow_input.batch_id,
            workflow_id=workflow_id,
            temporal_run_id=error.run_id,
            status="already_running",
        )

    if handle.first_execution_run_id is None:
        raise RuntimeError("Temporal did not return the started workflow run ID")
    return CompanyWorkflowSubmission(
        company_id=workflow_input.company_id,
        batch_id=workflow_input.batch_id,
        workflow_id=workflow_id,
        temporal_run_id=handle.first_execution_run_id,
        status="submitted",
    )
