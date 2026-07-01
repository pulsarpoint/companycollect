from dataclasses import dataclass
from datetime import UTC
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

DEFAULT_TEMPORAL_ADDRESS = "companycollect:7233"
DEFAULT_TASK_QUEUE = "norway-financial-bootstrap"
DEFAULT_SLOT_COUNT = 4

ACTIVITY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=10)
FETCH_START_TO_CLOSE_TIMEOUT = timedelta(hours=12)
FETCH_HEARTBEAT_TIMEOUT = timedelta(minutes=5)
FETCH_RETRY_POLICY = RetryPolicy(maximum_attempts=3)

with workflow.unsafe.imports_passed_through():
    from norway_financial_bootstrap.activities import (
        CandidateMarkerStatusInput,
        FetchAndStoreCandidateInput,
        GetCandidateInput,
        MarkerWriteInput,
        candidate_marker_status,
        fetch_and_store_candidate,
        get_candidate,
        write_done_marker,
        write_failed_marker,
    )
    from norway_financial_bootstrap.types import CandidateMarkerStatus


@dataclass(frozen=True)
class BootstrapSlotInput:
    run_id: str
    slot_id: int
    slot_index: int
    slot_count: int
    fetched_at: str


@dataclass(frozen=True)
class BootstrapSlotResult:
    run_id: str
    slot_id: int
    slot_index: int
    completed: bool
    org_number: str | None


@dataclass(frozen=True)
class ActivityFailureDetails:
    fetch_status: str
    error_type: str
    error_message: str


def slot_workflow_id(run_id: str, slot_id: int) -> str:
    return f"{run_id}-slot-{slot_id}"


@workflow.defn
class NorwayBrregFinancialBootstrapSlotWorkflow:
    @workflow.run
    async def run(self, input: BootstrapSlotInput) -> BootstrapSlotResult:
        candidate = await workflow.execute_activity(
            get_candidate,
            GetCandidateInput(
                run_id=input.run_id,
                slot_id=input.slot_id,
                slot_index=input.slot_index,
            ),
            start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
        )
        if candidate is None:
            return BootstrapSlotResult(
                run_id=input.run_id,
                slot_id=input.slot_id,
                slot_index=input.slot_index,
                completed=True,
                org_number=None,
            )

        marker_status = await workflow.execute_activity(
            candidate_marker_status,
            CandidateMarkerStatusInput(org_number=candidate.org_number),
            start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
        )
        if marker_status in {CandidateMarkerStatus.DONE, CandidateMarkerStatus.FAILED}:
            workflow.continue_as_new(_next_input(input))

        fetched_at = _workflow_utc_now_iso()
        try:
            result = await workflow.execute_activity(
                fetch_and_store_candidate,
                FetchAndStoreCandidateInput(
                    run_id=input.run_id,
                    org_number=candidate.org_number,
                    fetched_at=fetched_at,
                ),
                heartbeat_timeout=FETCH_HEARTBEAT_TIMEOUT,
                start_to_close_timeout=FETCH_START_TO_CLOSE_TIMEOUT,
                retry_policy=FETCH_RETRY_POLICY,
            )
        except ActivityError as exc:
            if _activity_failure_was_cancelled(exc):
                raise
            failure = _activity_failure_details(exc)
            await workflow.execute_activity(
                write_failed_marker,
                MarkerWriteInput(
                    org_number=candidate.org_number,
                    fetch_status=failure.fetch_status,
                    report_count=0,
                    raw_report_keys=[],
                    timestamp=_workflow_utc_now_iso(),
                    error_type=failure.error_type,
                    error_message=failure.error_message,
                ),
                start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
            )
            workflow.continue_as_new(_next_input(input))

        await workflow.execute_activity(
            write_done_marker,
            MarkerWriteInput(
                org_number=result.org_number,
                fetch_status=result.fetch_status,
                report_count=result.report_count,
                raw_report_keys=result.raw_report_keys,
                timestamp=result.fetched_at,
                error_type="",
                error_message="",
            ),
            start_to_close_timeout=ACTIVITY_START_TO_CLOSE_TIMEOUT,
        )
        workflow.continue_as_new(_next_input(input))


def _next_input(input: BootstrapSlotInput) -> BootstrapSlotInput:
    return BootstrapSlotInput(
        run_id=input.run_id,
        slot_id=input.slot_id,
        slot_index=input.slot_index + 1,
        slot_count=input.slot_count,
        fetched_at=input.fetched_at,
    )


def _activity_failure_details(exc: ActivityError) -> ActivityFailureDetails:
    cause = _root_cause(exc)
    if isinstance(cause, ApplicationError):
        return ActivityFailureDetails(
            fetch_status=_application_error_fetch_status(cause),
            error_type=cause.type or type(cause).__name__,
            error_message=cause.message or str(cause),
        )
    return ActivityFailureDetails(
        fetch_status="",
        error_type=type(cause).__name__,
        error_message=str(cause),
    )


def _activity_failure_was_cancelled(exc: ActivityError) -> bool:
    cause: BaseException | None = exc
    while cause is not None:
        if isinstance(cause, CancelledError):
            return True
        cause = cause.__cause__
    return False


def _root_cause(exc: BaseException) -> BaseException:
    cause = exc
    while cause.__cause__ is not None:
        cause = cause.__cause__
    return cause


def _application_error_fetch_status(error: ApplicationError) -> str:
    if not error.details:
        return ""
    return str(error.details[0])


def _workflow_utc_now_iso() -> str:
    return (
        workflow.now()
        .astimezone(UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
