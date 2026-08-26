import asyncio
import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

DATABASE = "corpscout"
ACTIVE_COMPANIES_TABLE = f"{DATABASE}.se_company_info"
RATSIT_CURRENT_VIEW = f"{DATABASE}.se_company_ratsit_current"
RATSIT_RESULTS_TABLE = f"{DATABASE}.se_company_ratsit_crawl_results"

CRAWL_COMPANY_WORKFLOW = "ratsit.crawl-company"
DEFAULT_BATCH_SIZE = 10
ABSOLUTE_MAX_BATCH_SIZE = 5_000
BATCH_ID_NAMESPACE = uuid.UUID("aee846ab-08a1-57cb-b475-a07d1a52449d")
EPOCH_SQL = "toDateTime64('1970-01-01 00:00:00', 3, 'UTC')"

type SubmissionStatus = Literal["submitted", "already_running"]


class RatsitDispatchConfig(dg.Config):
    """Runtime size of one manually dispatched Ratsit batch."""

    batch_size: int = Field(default=DEFAULT_BATCH_SIZE, ge=1)


@dataclass(frozen=True)
class CrawlCompanyInput:
    """Temporal wire input shared with the Ratsit crawler worker."""

    company_id: str
    batch_id: str

    def __post_init__(self) -> None:
        validate_company_id(self.company_id)
        if not isinstance(self.batch_id, str):
            raise ValueError("batch_id must be a UUID")
        try:
            uuid.UUID(self.batch_id)
        except ValueError as error:
            raise ValueError("batch_id must be a UUID") from error


@dataclass(frozen=True)
class CompanyWorkflowSubmission:
    company_id: str
    batch_id: str
    workflow_id: str
    temporal_run_id: str | None
    status: SubmissionStatus


@dataclass(frozen=True)
class RatsitCoverage:
    active_companies: int
    never_scanned: int
    fresh_successes: int
    stale_successes: int
    cooling_down_failures: int
    retry_eligible_failures: int
    due_companies: int
    latest_crawl_completed_at: datetime | None
    latest_batch_id: str | None
    latest_batch_companies: int
    latest_batch_successes: int
    latest_batch_failures: int
    latest_batch_completed_at: datetime | None

    def metadata(
        self,
    ) -> dict[str, int | str | dg.TimestampMetadataValue | None]:
        return {
            "active_companies": self.active_companies,
            "never_scanned": self.never_scanned,
            "fresh_successes": self.fresh_successes,
            "stale_successes": self.stale_successes,
            "cooling_down_failures": self.cooling_down_failures,
            "retry_eligible_failures": self.retry_eligible_failures,
            "due_companies": self.due_companies,
            "latest_crawl_completed_at": _timestamp_metadata(
                self.latest_crawl_completed_at
            ),
            "latest_batch_id": self.latest_batch_id,
            "latest_batch_companies": self.latest_batch_companies,
            "latest_batch_successes": self.latest_batch_successes,
            "latest_batch_failures": self.latest_batch_failures,
            "latest_batch_completed_at": _timestamp_metadata(
                self.latest_batch_completed_at
            ),
        }


def _timestamp_metadata(
    value: datetime | None,
) -> dg.TimestampMetadataValue | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return dg.MetadataValue.timestamp(value)


def validate_company_id(company_id: str) -> None:
    if (
        not isinstance(company_id, str)
        or len(company_id) != 10
        or not company_id.isascii()
        or not company_id.isdigit()
    ):
        raise ValueError("company_id must contain exactly ten ASCII digits")


def company_workflow_id(company_id: str) -> str:
    validate_company_id(company_id)
    return f"ratsit/company/{company_id}"


def batch_id_for(run_id: str, company_ids: list[str]) -> str:
    """Return a stable UUID for this Dagster run and exact company set."""
    if run_id.strip() == "":
        raise ValueError("run_id must not be blank")
    if len(company_ids) != len(set(company_ids)):
        raise ValueError("company_ids must not contain duplicates")
    for company_id in company_ids:
        validate_company_id(company_id)

    company_digest = hashlib.sha256("\n".join(sorted(company_ids)).encode()).hexdigest()
    return str(uuid.uuid5(BATCH_ID_NAMESPACE, f"{run_id}:{company_digest}"))


def build_candidate_sql(
    *,
    batch_size: int,
    freshness_days: int,
    failure_cooldown_hours: int,
) -> str:
    _validate_policy_values(
        batch_size=batch_size,
        freshness_days=freshness_days,
        failure_cooldown_hours=failure_cooldown_hours,
    )
    return f"""
        SELECT active.company_id
        FROM
        (
            SELECT company_id
            FROM {ACTIVE_COMPANIES_TABLE} FINAL
            WHERE status = 'active'
        ) AS active
        LEFT JOIN {RATSIT_CURRENT_VIEW} AS ratsit
            ON ratsit.company_id = active.company_id
        WHERE
            isNull(ratsit.company_id)
            OR (
                ratsit.latest_outcome = 'success'
                AND ifNull(ratsit.latest_success_at, {EPOCH_SQL})
                    < now64(3, 'UTC') - INTERVAL {freshness_days} DAY
            )
            OR (
                ratsit.latest_outcome != 'success'
                AND ratsit.latest_completed_at
                    < now64(3, 'UTC') - INTERVAL {failure_cooldown_hours} HOUR
            )
        ORDER BY
            isNotNull(ratsit.company_id),
            ifNull(ratsit.latest_completed_at, {EPOCH_SQL}),
            active.company_id
        LIMIT {batch_size}
        SETTINGS join_use_nulls = 1
    """


def build_coverage_sql(*, freshness_days: int, failure_cooldown_hours: int) -> str:
    _validate_policy_values(
        batch_size=1,
        freshness_days=freshness_days,
        failure_cooldown_hours=failure_cooldown_hours,
    )
    return f"""
        SELECT
            count() AS active_companies,
            countIf(isNull(ratsit.company_id)) AS never_scanned,
            countIf(
                ratsit.latest_outcome = 'success'
                AND ifNull(ratsit.latest_success_at, {EPOCH_SQL})
                    >= now64(3, 'UTC') - INTERVAL {freshness_days} DAY
            ) AS fresh_successes,
            countIf(
                ratsit.latest_outcome = 'success'
                AND ifNull(ratsit.latest_success_at, {EPOCH_SQL})
                    < now64(3, 'UTC') - INTERVAL {freshness_days} DAY
            ) AS stale_successes,
            countIf(
                isNotNull(ratsit.company_id)
                AND ratsit.latest_outcome != 'success'
                AND ratsit.latest_completed_at
                    >= now64(3, 'UTC') - INTERVAL {failure_cooldown_hours} HOUR
            ) AS cooling_down_failures,
            countIf(
                isNotNull(ratsit.company_id)
                AND ratsit.latest_outcome != 'success'
                AND ratsit.latest_completed_at
                    < now64(3, 'UTC') - INTERVAL {failure_cooldown_hours} HOUR
            ) AS retry_eligible_failures,
            countIf(
                isNull(ratsit.company_id)
                OR (
                    ratsit.latest_outcome = 'success'
                    AND ifNull(ratsit.latest_success_at, {EPOCH_SQL})
                        < now64(3, 'UTC') - INTERVAL {freshness_days} DAY
                )
                OR (
                    ratsit.latest_outcome != 'success'
                    AND ratsit.latest_completed_at
                        < now64(3, 'UTC') - INTERVAL {failure_cooldown_hours} HOUR
                )
            ) AS due_companies,
            max(ratsit.latest_completed_at) AS latest_crawl_completed_at
        FROM
        (
            SELECT company_id
            FROM {ACTIVE_COMPANIES_TABLE} FINAL
            WHERE status = 'active'
        ) AS active
        LEFT JOIN {RATSIT_CURRENT_VIEW} AS ratsit
            ON ratsit.company_id = active.company_id
        SETTINGS join_use_nulls = 1
    """


LATEST_BATCH_SQL = f"""
    SELECT
        toString(batch_id) AS batch_id,
        uniqExact(company_id) AS companies,
        countIf(outcome = 'success') AS successes,
        countIf(outcome != 'success') AS failures,
        max(completed_at) AS completed_at
    FROM {RATSIT_RESULTS_TABLE} FINAL
    GROUP BY batch_id
    ORDER BY max(recorded_at) DESC, batch_id DESC
    LIMIT 1
"""


def select_company_ids(
    clickhouse: ClickhouseResource,
    *,
    batch_size: int,
    freshness_days: int,
    failure_cooldown_hours: int,
) -> list[str]:
    sql = build_candidate_sql(
        batch_size=batch_size,
        freshness_days=freshness_days,
        failure_cooldown_hours=failure_cooldown_hours,
    )
    with clickhouse.get_connection() as client:
        company_ids = [str(row[0]) for row in client.execute(sql)]

    for company_id in company_ids:
        validate_company_id(company_id)
    if len(company_ids) != len(set(company_ids)):
        raise RuntimeError("Ratsit candidate query returned duplicate company IDs")
    return company_ids


def read_coverage(
    clickhouse: ClickhouseResource,
    *,
    freshness_days: int,
    failure_cooldown_hours: int,
) -> RatsitCoverage:
    with clickhouse.get_connection() as client:
        coverage_rows = client.execute(
            build_coverage_sql(
                freshness_days=freshness_days,
                failure_cooldown_hours=failure_cooldown_hours,
            )
        )
        latest_batch_rows = client.execute(LATEST_BATCH_SQL)

    if len(coverage_rows) != 1:
        raise RuntimeError("Ratsit coverage query must return exactly one row")
    (
        active_companies,
        never_scanned,
        fresh_successes,
        stale_successes,
        cooling_down_failures,
        retry_eligible_failures,
        due_companies,
        latest_crawl_completed_at,
    ) = coverage_rows[0]

    if latest_batch_rows:
        (
            latest_batch_id,
            latest_batch_companies,
            latest_batch_successes,
            latest_batch_failures,
            latest_batch_completed_at,
        ) = latest_batch_rows[0]
    else:
        latest_batch_id = None
        latest_batch_companies = 0
        latest_batch_successes = 0
        latest_batch_failures = 0
        latest_batch_completed_at = None

    return RatsitCoverage(
        active_companies=int(active_companies),
        never_scanned=int(never_scanned),
        fresh_successes=int(fresh_successes),
        stale_successes=int(stale_successes),
        cooling_down_failures=int(cooling_down_failures),
        retry_eligible_failures=int(retry_eligible_failures),
        due_companies=int(due_companies),
        latest_crawl_completed_at=latest_crawl_completed_at,
        latest_batch_id=str(latest_batch_id) if latest_batch_id is not None else None,
        latest_batch_companies=int(latest_batch_companies),
        latest_batch_successes=int(latest_batch_successes),
        latest_batch_failures=int(latest_batch_failures),
        latest_batch_completed_at=latest_batch_completed_at,
    )


async def submit_company_workflow(
    client: Client,
    workflow_input: CrawlCompanyInput,
    *,
    task_queue: str,
) -> CompanyWorkflowSubmission:
    if task_queue.strip() == "":
        raise ValueError("task_queue must not be blank")

    workflow_id = company_workflow_id(workflow_input.company_id)
    try:
        handle = await client.start_workflow(
            CRAWL_COMPANY_WORKFLOW,
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


async def submit_company_batch(
    *,
    temporal_address: str,
    temporal_namespace: str,
    temporal_task_queue: str,
    company_ids: list[str],
    batch_id: str,
) -> list[CompanyWorkflowSubmission]:
    if temporal_address.strip() == "":
        raise ValueError("temporal_address must not be blank")
    if temporal_namespace.strip() == "":
        raise ValueError("temporal_namespace must not be blank")

    client = await Client.connect(
        temporal_address,
        namespace=temporal_namespace,
    )
    submissions: list[CompanyWorkflowSubmission] = []
    for company_id in company_ids:
        submissions.append(
            await submit_company_workflow(
                client,
                CrawlCompanyInput(company_id=company_id, batch_id=batch_id),
                task_queue=temporal_task_queue,
            )
        )
    return submissions


class RatsitTemporalComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dispatch active Swedish companies to the Ratsit Temporal worker.

    The component selects due companies from ClickHouse, but Temporal remains
    authoritative for pending, running, retrying, and completed crawl work.
    """

    dispatch_asset: dg.ResolvedAssetSpec
    coverage_asset: dg.ResolvedAssetSpec
    temporal_address: str
    temporal_namespace: str = "corpscout"
    temporal_task_queue: str = "ratsit-crawler"
    freshness_days: int = Field(default=20, ge=1)
    failure_cooldown_hours: int = Field(default=24, ge=1)
    max_batch_size: int = Field(
        default=ABSOLUTE_MAX_BATCH_SIZE,
        ge=1,
        le=ABSOLUTE_MAX_BATCH_SIZE,
    )

    def build_defs(self, _context: dg.ComponentLoadContext) -> dg.Definitions:
        @dg.multi_asset(specs=[self.dispatch_asset])
        def se_ratsit_scan_dispatch(
            context: dg.AssetExecutionContext,
            config: RatsitDispatchConfig,
            clickhouse: ClickhouseResource,
        ) -> dg.MaterializeResult:
            if config.batch_size > self.max_batch_size:
                raise dg.Failure(
                    description=(
                        f"batch_size {config.batch_size:,} exceeds the deployed "
                        f"Ratsit safety ceiling of {self.max_batch_size:,}"
                    )
                )

            company_ids = select_company_ids(
                clickhouse,
                batch_size=config.batch_size,
                freshness_days=self.freshness_days,
                failure_cooldown_hours=self.failure_cooldown_hours,
            )
            if not company_ids:
                return dg.MaterializeResult(
                    asset_key=self.dispatch_asset.key,
                    metadata={
                        "selected_companies": 0,
                        "submitted_workflows": 0,
                        "already_running_workflows": 0,
                        "requested_batch_size": config.batch_size,
                        "message": "No active companies are currently due for a Ratsit scan",
                    },
                )

            batch_id = batch_id_for(context.run.run_id, company_ids)
            submissions = asyncio.run(
                submit_company_batch(
                    temporal_address=self.temporal_address,
                    temporal_namespace=self.temporal_namespace,
                    temporal_task_queue=self.temporal_task_queue,
                    company_ids=company_ids,
                    batch_id=batch_id,
                )
            )
            submitted = sum(item.status == "submitted" for item in submissions)
            already_running = sum(
                item.status == "already_running" for item in submissions
            )
            context.log.info(
                "Dispatched Ratsit batch %s: selected=%d submitted=%d already_running=%d",
                batch_id,
                len(company_ids),
                submitted,
                already_running,
            )
            return dg.MaterializeResult(
                asset_key=self.dispatch_asset.key,
                metadata={
                    "batch_id": batch_id,
                    "selected_companies": len(company_ids),
                    "submitted_workflows": submitted,
                    "already_running_workflows": already_running,
                    "requested_batch_size": config.batch_size,
                    "temporal_namespace": self.temporal_namespace,
                    "temporal_task_queue": self.temporal_task_queue,
                    "first_company_id": company_ids[0],
                    "last_company_id": company_ids[-1],
                    "company_id_sample": dg.MetadataValue.json(company_ids[:20]),
                    "freshness_days": self.freshness_days,
                    "failure_cooldown_hours": self.failure_cooldown_hours,
                },
            )

        @dg.multi_asset(specs=[self.coverage_asset])
        def se_ratsit_scan_coverage(
            clickhouse: ClickhouseResource,
        ) -> dg.MaterializeResult:
            coverage = read_coverage(
                clickhouse,
                freshness_days=self.freshness_days,
                failure_cooldown_hours=self.failure_cooldown_hours,
            )
            return dg.MaterializeResult(
                asset_key=self.coverage_asset.key,
                metadata=coverage.metadata(),
            )

        dispatch_job = dg.define_asset_job(
            "se_ratsit_scan_dispatch_job",
            selection=dg.AssetSelection.assets(self.dispatch_asset.key),
            description=(
                "Manually select active Swedish companies due for Ratsit and submit "
                "their durable Temporal workflows without waiting for completion."
            ),
        )
        coverage_job = dg.define_asset_job(
            "se_ratsit_scan_coverage_job",
            selection=dg.AssetSelection.assets(self.coverage_asset.key),
            description="Refresh read-only Ratsit scan coverage metrics from ClickHouse.",
        )
        return dg.Definitions(
            assets=[se_ratsit_scan_dispatch, se_ratsit_scan_coverage],
            jobs=[dispatch_job, coverage_job],
        )


def _validate_policy_values(
    *,
    batch_size: int,
    freshness_days: int,
    failure_cooldown_hours: int,
) -> None:
    if not 1 <= batch_size <= ABSOLUTE_MAX_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {ABSOLUTE_MAX_BATCH_SIZE}")
    if freshness_days < 1:
        raise ValueError("freshness_days must be positive")
    if failure_cooldown_hours < 1:
        raise ValueError("failure_cooldown_hours must be positive")
