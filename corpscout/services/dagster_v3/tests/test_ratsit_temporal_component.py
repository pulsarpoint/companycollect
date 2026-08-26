import asyncio
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import dagster as dg
import pytest
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from dagster_v3.components.ratsit_temporal_component import (
    ABSOLUTE_MAX_BATCH_SIZE,
    CRAWL_COMPANY_WORKFLOW,
    CrawlCompanyInput,
    batch_id_for,
    build_candidate_sql,
    build_coverage_sql,
    company_workflow_id,
    read_coverage,
    select_company_ids,
    submit_company_workflow,
)


class FakeClickHouseClient:
    def __init__(self, responses: list[list[tuple[Any, ...]]]) -> None:
        self.responses = responses
        self.statements: list[str] = []

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        self.statements.append(sql)
        return self.responses.pop(0)


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


class FakeWorkflowHandle:
    first_execution_run_id = "temporal-run-1"


class FakeTemporalClient:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def start_workflow(self, *args: Any, **kwargs: Any) -> FakeWorkflowHandle:
        self.calls.append((args, kwargs))
        if self.error is not None:
            raise self.error
        return FakeWorkflowHandle()


def test_candidate_query_selects_only_active_due_companies_in_stable_order() -> None:
    sql = build_candidate_sql(
        batch_size=10,
        freshness_days=20,
        failure_cooldown_hours=24,
    )

    assert "FROM corpscout.se_company_info FINAL" in sql
    assert "WHERE status = 'active'" in sql
    assert "LEFT JOIN corpscout.se_company_ratsit_current" in sql
    assert "INTERVAL 20 DAY" in sql
    assert "INTERVAL 24 HOUR" in sql
    assert "active.company_id" in sql
    assert "LIMIT 10" in sql
    assert "SETTINGS join_use_nulls = 1" in sql


def test_candidate_query_enforces_absolute_safety_ceiling() -> None:
    with pytest.raises(ValueError, match="batch_size must be between"):
        build_candidate_sql(
            batch_size=ABSOLUTE_MAX_BATCH_SIZE + 1,
            freshness_days=20,
            failure_cooldown_hours=24,
        )


def test_batch_id_is_stable_for_the_same_run_and_company_set() -> None:
    first = batch_id_for("dagster-run-1", ["5562434182", "5566778899"])
    reordered = batch_id_for("dagster-run-1", ["5566778899", "5562434182"])
    another_run = batch_id_for("dagster-run-2", ["5562434182", "5566778899"])

    assert first == reordered
    assert first != another_run
    assert str(uuid.UUID(first)) == first


def test_select_company_ids_uses_the_requested_batch_policy() -> None:
    client = FakeClickHouseClient([[("5562434182",), ("5566778899",)]])

    selected = select_company_ids(
        FakeClickHouseResource(client),  # type: ignore[arg-type]
        batch_size=2,
        freshness_days=30,
        failure_cooldown_hours=48,
    )

    assert selected == ["5562434182", "5566778899"]
    assert "INTERVAL 30 DAY" in client.statements[0]
    assert "INTERVAL 48 HOUR" in client.statements[0]
    assert "LIMIT 2" in client.statements[0]


def test_select_company_ids_rejects_duplicates_from_clickhouse() -> None:
    client = FakeClickHouseClient([[("5562434182",), ("5562434182",)]])

    with pytest.raises(RuntimeError, match="duplicate company IDs"):
        select_company_ids(
            FakeClickHouseResource(client),  # type: ignore[arg-type]
            batch_size=2,
            freshness_days=20,
            failure_cooldown_hours=24,
        )


def test_coverage_reads_policy_counts_and_latest_batch() -> None:
    crawl_completed_at = datetime(2026, 8, 26, 20, 0)
    batch_completed_at = datetime(2026, 8, 26, 19, 59, tzinfo=UTC)
    client = FakeClickHouseClient(
        [
            [
                (
                    1_000,
                    700,
                    200,
                    25,
                    10,
                    65,
                    790,
                    crawl_completed_at,
                )
            ],
            [("82ba43c8-abf9-4aad-9374-92bc5e19fc34", 10, 8, 2, batch_completed_at)],
        ]
    )

    coverage = read_coverage(
        FakeClickHouseResource(client),  # type: ignore[arg-type]
        freshness_days=20,
        failure_cooldown_hours=24,
    )

    assert coverage.active_companies == 1_000
    assert coverage.due_companies == 790
    assert coverage.latest_batch_companies == 10
    assert coverage.latest_batch_successes == 8
    assert coverage.latest_batch_failures == 2
    assert coverage.latest_batch_completed_at == batch_completed_at
    metadata = coverage.metadata()
    assert metadata["latest_crawl_completed_at"] == dg.MetadataValue.timestamp(
        crawl_completed_at.replace(tzinfo=UTC)
    )
    assert metadata["latest_batch_completed_at"] == dg.MetadataValue.timestamp(
        batch_completed_at
    )
    dg.AssetMaterialization(
        asset_key="se_ratsit_scan_coverage",
        metadata=metadata,
    )
    assert "INTERVAL 20 DAY" in client.statements[0]
    assert "GROUP BY batch_id" in client.statements[1]


def test_coverage_query_uses_the_same_eligibility_policy_as_dispatch() -> None:
    sql = build_coverage_sql(freshness_days=20, failure_cooldown_hours=24)

    assert "WHERE status = 'active'" in sql
    assert "AS never_scanned" in sql
    assert "AS fresh_successes" in sql
    assert "AS stale_successes" in sql
    assert "AS retry_eligible_failures" in sql
    assert "AS due_companies" in sql


def test_temporal_submission_uses_stable_company_id_and_nonblocking_start() -> None:
    client = FakeTemporalClient()
    workflow_input = CrawlCompanyInput(
        company_id="5562434182",
        batch_id="82ba43c8-abf9-4aad-9374-92bc5e19fc34",
    )

    result = asyncio.run(
        submit_company_workflow(  # type: ignore[arg-type]
            client,
            workflow_input,
            task_queue="ratsit-crawler",
        )
    )

    assert result.status == "submitted"
    assert result.temporal_run_id == "temporal-run-1"
    [(args, kwargs)] = client.calls
    assert args == (CRAWL_COMPANY_WORKFLOW, workflow_input)
    assert kwargs["id"] == "ratsit/company/5562434182"
    assert kwargs["task_queue"] == "ratsit-crawler"
    assert kwargs["id_reuse_policy"] == WorkflowIDReusePolicy.ALLOW_DUPLICATE
    assert kwargs["id_conflict_policy"] == WorkflowIDConflictPolicy.FAIL
    assert kwargs["memo"] == {
        "batch_id": workflow_input.batch_id,
        "company_id": workflow_input.company_id,
    }


def test_temporal_submission_reports_an_already_running_company() -> None:
    client = FakeTemporalClient(
        WorkflowAlreadyStartedError(
            "ratsit/company/5562434182",
            CRAWL_COMPANY_WORKFLOW,
            run_id="existing-run",
        )
    )
    workflow_input = CrawlCompanyInput(
        company_id="5562434182",
        batch_id="82ba43c8-abf9-4aad-9374-92bc5e19fc34",
    )

    result = asyncio.run(
        submit_company_workflow(  # type: ignore[arg-type]
            client,
            workflow_input,
            task_queue="ratsit-crawler",
        )
    )

    assert result.status == "already_running"
    assert result.temporal_run_id == "existing-run"


def test_ratsit_assets_and_manual_jobs_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    graph = repository.asset_graph
    dispatch = graph.get(dg.AssetKey("se_ratsit_scan_dispatch"))
    coverage = graph.get(dg.AssetKey("se_ratsit_scan_coverage"))

    assert dispatch.group_name == "sweden_ratsit"
    assert coverage.group_name == "sweden_ratsit"
    assert dispatch.parent_keys == {dg.AssetKey("se_company_info_clickhouse")}
    assert coverage.parent_keys == {dg.AssetKey("se_company_info_clickhouse")}
    assert {
        key.path[-1]
        for key in repository.get_job(
            "se_ratsit_scan_dispatch_job"
        ).asset_layer.executable_asset_keys
    } == {"se_ratsit_scan_dispatch"}
    assert {
        key.path[-1]
        for key in repository.get_job(
            "se_ratsit_scan_coverage_job"
        ).asset_layer.executable_asset_keys
    } == {"se_ratsit_scan_coverage"}


def test_company_workflow_id_rejects_non_swedish_company_ids() -> None:
    with pytest.raises(ValueError, match="ten ASCII digits"):
        company_workflow_id("ABC")
