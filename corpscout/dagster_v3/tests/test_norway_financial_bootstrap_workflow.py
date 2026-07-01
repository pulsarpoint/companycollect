import concurrent.futures

import polars as pl

from norway_financial_bootstrap.activities import FetchBatchInput, FetchBatchResult, fetch_batch
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.cli import build_parser as build_start_parser
from norway_financial_bootstrap.worker import build_parser as build_worker_parser
from norway_financial_bootstrap.worker import build_worker
from norway_financial_bootstrap.workflows import (
    BootstrapResult,
    NorwayBrregInitialFinancialRawFetchWorkflow,
    aggregate_batch_results,
    partition_batches,
)


class FakeClient:
    def __init__(self) -> None:
        self.fetched: list[str] = []

    def fetch_candidate(self, candidate, *, source_run_id, source_line_number, fetched_at):
        self.fetched.append(candidate.org_number)
        return {
            "country_iso2": "NO",
            "source_slug": "norway_brregregnskap_fetch",
            "source_run_id": source_run_id,
            "source_line_number": source_line_number,
            "source_record_id": candidate.org_number,
            "source_payload_hash": "0" * 64,
            "org_number": candidate.org_number,
            "legal_name": candidate.legal_name,
            "website": candidate.website,
            "last_submitted_accounts_year": candidate.last_submitted_accounts_year,
            "source_url": f"https://example.test/{candidate.org_number}",
            "fetch_status": "success",
            "http_status": 200,
            "error_type": "",
            "error_message": "",
            "attempt_count": 1,
            "fetched_at": fetched_at,
            "raw_response": "[]",
        }


class StatusClient:
    def __init__(self, statuses: list[str]) -> None:
        self.statuses = statuses

    def fetch_candidate(self, candidate, *, source_run_id, source_line_number, fetched_at):
        status = self.statuses.pop(0)
        return {
            "country_iso2": "NO",
            "source_slug": "norway_brregregnskap_fetch",
            "source_run_id": source_run_id,
            "source_line_number": source_line_number,
            "source_record_id": candidate.org_number,
            "source_payload_hash": "0" * 64,
            "org_number": candidate.org_number,
            "legal_name": candidate.legal_name,
            "website": candidate.website,
            "last_submitted_accounts_year": candidate.last_submitted_accounts_year,
            "source_url": f"https://example.test/{candidate.org_number}",
            "fetch_status": status,
            "http_status": 200 if status == "success" else 404,
            "error_type": "",
            "error_message": "",
            "attempt_count": 1,
            "fetched_at": fetched_at,
            "raw_response": "[]",
        }


class FakeStorage:
    def __init__(self, completed: set[tuple[str, str]]) -> None:
        self.completed = completed
        self.writes: dict[tuple[str, str], pl.DataFrame] = {}

    def existing_raw_fetch_org_years(self) -> set[tuple[str, str]]:
        return set(self.completed)

    def write_raw_fetch(self, org_number: str, accounts_year: str, frame: pl.DataFrame) -> str:
        self.writes[(org_number, accounts_year)] = frame
        return f"raw/{org_number}/{accounts_year}.parquet"


class FakeWorker:
    instances: list[dict] = []

    def __init__(
        self,
        client,
        *,
        task_queue,
        workflows,
        activities,
        max_concurrent_activities,
        activity_executor,
    ) -> None:
        FakeWorker.instances.append(
            {
                "task_queue": task_queue,
                "workflows": workflows,
                "activities": activities,
                "max_concurrent_activities": max_concurrent_activities,
                "activity_executor": activity_executor,
            }
        )


def test_fetch_batch_skips_existing_raw_fetches_and_writes_one_parquet_per_missing_candidate() -> None:
    client = FakeClient()
    storage = FakeStorage(completed={("100", "2024")})

    result = fetch_batch(
        FetchBatchInput(
            source_run_id="run-1",
            fetched_at="2026-07-01T00:00:00.000Z",
            candidates=[
                FinancialCandidate("100", "EXISTING AS", "", "2024"),
                FinancialCandidate("200", "MISSING AS", "", "2024"),
            ],
        ),
        storage=storage,
        client=client,
    )

    assert client.fetched == ["200"]
    assert result.fetched_count == 1
    assert result.skipped_count == 1
    assert ("200", "2024") in storage.writes


def test_fetch_batch_tracks_status_counts_and_writes_one_row_frame_per_missing_candidate() -> None:
    storage = FakeStorage(completed=set())

    result = fetch_batch(
        FetchBatchInput(
            source_run_id="run-1",
            fetched_at="2026-07-01T00:00:00.000Z",
            candidates=[
                FinancialCandidate("100", "SUCCESS AS", "", "2024"),
                FinancialCandidate("200", "MISSING AS", "", "2024"),
            ],
        ),
        storage=storage,
        client=StatusClient(["success", "not_found"]),
    )

    assert result.status_counts == {"success": 1, "not_found": 1}
    assert result.fetched_count == 2
    assert storage.writes[("100", "2024")].height == 1
    assert storage.writes[("200", "2024")].height == 1


def test_partition_batches_splits_candidates_deterministically() -> None:
    candidates = [
        FinancialCandidate("100", "A AS", "", "2024"),
        FinancialCandidate("200", "B AS", "", "2024"),
        FinancialCandidate("300", "C AS", "", "2024"),
    ]

    assert partition_batches(candidates, batch_size=2) == [
        [
            FinancialCandidate("100", "A AS", "", "2024"),
            FinancialCandidate("200", "B AS", "", "2024"),
        ],
        [FinancialCandidate("300", "C AS", "", "2024")],
    ]


def test_partition_batches_rejects_invalid_batch_size() -> None:
    try:
        partition_batches([], batch_size=0)
    except ValueError as exc:
        assert str(exc) == "batch_size must be at least 1"
    else:
        raise AssertionError("partition_batches should reject batch_size < 1")


def test_aggregate_batch_results_sums_counts_and_statuses() -> None:
    result = aggregate_batch_results(
        candidate_count=5,
        batch_results=[
            FetchBatchResult(
                fetched_count=2,
                skipped_count=1,
                status_counts={"success": 1, "not_found": 1},
            ),
            FetchBatchResult(
                fetched_count=1,
                skipped_count=1,
                status_counts={"success": 1},
            ),
        ],
    )

    assert result == BootstrapResult(
        candidate_count=5,
        batch_count=2,
        fetched_count=3,
        skipped_count=2,
        status_counts={"success": 2, "not_found": 1},
    )


def test_start_cli_parser_accepts_workflow_options() -> None:
    args = build_start_parser().parse_args(
        [
            "--snapshot-date",
            "2026-07-01",
            "--no-companies-key",
            "snapshots/no_companies.parquet",
            "--temporal-address",
            "localhost:7233",
            "--task-queue",
            "norway-financial-test",
            "--batch-size",
            "25",
            "--max-concurrent-batches",
            "3",
        ]
    )

    assert args.snapshot_date == "2026-07-01"
    assert args.no_companies_key == "snapshots/no_companies.parquet"
    assert args.temporal_address == "localhost:7233"
    assert args.task_queue == "norway-financial-test"
    assert args.batch_size == 25
    assert args.max_concurrent_batches == 3


def test_worker_cli_parser_accepts_worker_options() -> None:
    args = build_worker_parser().parse_args(
        [
            "--temporal-address",
            "localhost:7233",
            "--task-queue",
            "norway-financial-test",
            "--max-workers",
            "3",
            "--env-file",
            ".env.test",
        ]
    )

    assert args.temporal_address == "localhost:7233"
    assert args.task_queue == "norway-financial-test"
    assert args.max_workers == 3
    assert args.env_file == ".env.test"


def test_build_worker_registers_workflow_activity_and_executor(monkeypatch) -> None:
    import norway_financial_bootstrap.worker as worker

    FakeWorker.instances = []
    monkeypatch.setattr(worker, "Worker", FakeWorker)

    build_worker(object(), task_queue="norway-financial-test", max_workers=3)

    record = FakeWorker.instances[0]
    assert record["task_queue"] == "norway-financial-test"
    assert record["workflows"] == [NorwayBrregInitialFinancialRawFetchWorkflow]
    assert fetch_batch in record["activities"]
    assert record["max_concurrent_activities"] == 3
    assert isinstance(record["activity_executor"], concurrent.futures.ThreadPoolExecutor)
