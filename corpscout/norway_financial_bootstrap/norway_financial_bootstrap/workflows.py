import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

DEFAULT_TEMPORAL_ADDRESS = "companycollect:7233"
DEFAULT_TASK_QUEUE = "norway-financial-bootstrap"
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_CONCURRENT_BATCHES = 4

FETCH_BATCH_HEARTBEAT_TIMEOUT = timedelta(minutes=5)
FETCH_BATCH_START_TO_CLOSE_TIMEOUT = timedelta(hours=12)
FETCH_BATCH_RETRY_POLICY = RetryPolicy(maximum_attempts=3)

with workflow.unsafe.imports_passed_through():
    from norway_financial_bootstrap.activities import (
        FetchBatchInput,
        FetchBatchResult,
        fetch_batch,
    )
    from norway_financial_bootstrap.candidates import FinancialCandidate


@dataclass(frozen=True)
class BootstrapInput:
    source_run_id: str
    fetched_at: str
    candidate_count: int
    batch_keys: list[str]
    max_concurrent_batches: int = DEFAULT_MAX_CONCURRENT_BATCHES


@dataclass(frozen=True)
class BootstrapResult:
    candidate_count: int
    batch_count: int
    fetched_count: int
    skipped_count: int
    status_counts: dict[str, int]


def partition_batches(
    candidates: list[FinancialCandidate], *, batch_size: int
) -> list[list[FinancialCandidate]]:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    return [
        candidates[index : index + batch_size]
        for index in range(0, len(candidates), batch_size)
    ]


def aggregate_batch_results(
    *, candidate_count: int, batch_results: list[FetchBatchResult]
) -> BootstrapResult:
    status_counts: dict[str, int] = {}
    fetched_count = 0
    skipped_count = 0
    for result in batch_results:
        fetched_count += result.fetched_count
        skipped_count += result.skipped_count
        for status, count in result.status_counts.items():
            status_counts[status] = status_counts.get(status, 0) + count

    return BootstrapResult(
        candidate_count=candidate_count,
        batch_count=len(batch_results),
        fetched_count=fetched_count,
        skipped_count=skipped_count,
        status_counts=status_counts,
    )


@workflow.defn
class NorwayBrregInitialFinancialRawFetchWorkflow:
    @workflow.run
    async def run(self, input: BootstrapInput) -> BootstrapResult:
        if input.max_concurrent_batches < 1:
            raise ValueError("max_concurrent_batches must be at least 1")

        results: list[FetchBatchResult] = []

        for index in range(0, len(input.batch_keys), input.max_concurrent_batches):
            window = input.batch_keys[index : index + input.max_concurrent_batches]
            window_results = await asyncio.gather(
                *(
                    workflow.execute_activity(
                        fetch_batch,
                        FetchBatchInput(
                            source_run_id=input.source_run_id,
                            fetched_at=input.fetched_at,
                            candidate_batch_key=batch_key,
                        ),
                        heartbeat_timeout=FETCH_BATCH_HEARTBEAT_TIMEOUT,
                        start_to_close_timeout=FETCH_BATCH_START_TO_CLOSE_TIMEOUT,
                        retry_policy=FETCH_BATCH_RETRY_POLICY,
                    )
                    for batch_key in window
                )
            )
            results.extend(window_results)

        return aggregate_batch_results(
            candidate_count=input.candidate_count,
            batch_results=results,
        )
