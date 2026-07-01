import concurrent.futures
import json
from types import SimpleNamespace

import polars as pl
import pytest

from norway_financial_bootstrap.activities import FetchBatchInput, FetchBatchResult, fetch_batch
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.cli import (
    FIXED_WORKFLOW_ID,
    build_parser as build_start_parser,
    main as cli_main,
    write_candidate_batches,
)
from norway_financial_bootstrap.storage import COMPANY_SNAPSHOT_NO_COMPANIES_KEY
from norway_financial_bootstrap.worker import build_parser as build_worker_parser
from norway_financial_bootstrap.worker import build_worker
from norway_financial_bootstrap.workflows import (
    BootstrapInput,
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
        raw_response = json.dumps(
            [
                {
                    "id": int(candidate.org_number),
                    "journalnr": f"journal-{candidate.org_number}",
                    "regnskapstype": "SELSKAP",
                    "regnskapsperiode": {
                        "fraDato": f"{candidate.last_submitted_accounts_year}-01-01",
                        "tilDato": f"{candidate.last_submitted_accounts_year}-12-31",
                    },
                }
            ],
            separators=(",", ":"),
        )
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
            "raw_response": raw_response,
        }


class StatusClient:
    def __init__(self, statuses: list[str]) -> None:
        self.statuses = statuses

    def fetch_candidate(self, candidate, *, source_run_id, source_line_number, fetched_at):
        status = self.statuses.pop(0)
        raw_response = (
            '[{"id":200,"journalnr":"journal-200","regnskapstype":"SELSKAP"}]'
            if status == "success"
            else "[]"
        )
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
            "raw_response": raw_response,
        }


class FakeStorage:
    def __init__(
        self, completed: set[tuple[str, str]] | set[tuple[str, str, str, str]]
    ) -> None:
        self.completed = completed
        self.candidate_batches: dict[str, list[FinancialCandidate]] = {}
        self.frames: dict[str, pl.DataFrame] = {}
        self.written_candidate_batches: list[
            tuple[str, str, int, list[FinancialCandidate]]
        ] = []
        self.raw_report_writes: dict[tuple[str, str, str, str], dict] = {}

    def existing_raw_report_ids(self) -> set[tuple[str, str, str, str]]:
        return set(self.completed)

    def write_candidate_batch(
        self,
        source_run_id: str,
        attempt_id: str,
        batch_index: int,
        candidates: list[FinancialCandidate],
    ) -> str:
        key = (
            "norway_brreg/finance/bootstrap_runs/"
            f"run={source_run_id}/attempt={attempt_id}/candidates/batch={batch_index:06d}.parquet"
        )
        self.candidate_batches[key] = list(candidates)
        self.written_candidate_batches.append(
            (source_run_id, attempt_id, batch_index, list(candidates))
        )
        return key

    def read_candidate_batch(self, key: str) -> list[FinancialCandidate]:
        return list(self.candidate_batches[key])

    def read_parquet(self, key: str) -> pl.DataFrame:
        return self.frames[key]

    def raw_report_exists(
        self, org_number: str, accounts_year: str, report_type: str, report_id: str
    ) -> bool:
        return (org_number, accounts_year, report_type, report_id) in self.completed

    def write_raw_report(
        self,
        *,
        org_number: str,
        accounts_year: str,
        report_type: str,
        report_id: str,
        report: dict,
    ) -> str:
        key = (org_number, accounts_year, report_type, report_id)
        self.raw_report_writes[key] = report
        self.completed.add(key)
        return f"raw_reports/{org_number}/{accounts_year}/{report_type}/{report_id}.json"


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


def test_fetch_batch_writes_missing_raw_reports_and_skips_existing_report_ids() -> None:
    client = FakeClient()
    storage = FakeStorage(completed={("100", "2024", "SELSKAP", "100")})
    storage.candidate_batches["batches/000001.parquet"] = [
        FinancialCandidate("100", "EXISTING AS", "", "2024"),
        FinancialCandidate("200", "MISSING AS", "", "2024"),
    ]

    result = fetch_batch(
        FetchBatchInput(
            source_run_id="run-1",
            fetched_at="2026-07-01T00:00:00.000Z",
            candidate_batch_key="batches/000001.parquet",
        ),
        storage=storage,
        client=client,
    )

    assert client.fetched == ["100", "200"]
    assert result.fetched_count == 1
    assert result.skipped_count == 1
    assert set(storage.raw_report_writes) == {("200", "2024", "SELSKAP", "200")}


def test_fetch_batch_tracks_status_counts_and_writes_one_row_frame_per_missing_candidate() -> None:
    storage = FakeStorage(completed=set())
    storage.candidate_batches["batches/000001.parquet"] = [
        FinancialCandidate("100", "SUCCESS AS", "", "2024"),
        FinancialCandidate("200", "MISSING AS", "", "2024"),
    ]

    result = fetch_batch(
        FetchBatchInput(
            source_run_id="run-1",
            fetched_at="2026-07-01T00:00:00.000Z",
            candidate_batch_key="batches/000001.parquet",
        ),
        storage=storage,
        client=StatusClient(["success", "not_found"]),
    )

    assert result.status_counts == {"success": 1, "not_found": 1}
    assert result.fetched_count == 1
    assert storage.raw_report_writes[("100", "2024", "SELSKAP", "200")]["id"] == 200


def test_fetch_batch_raises_without_persisting_retryable_fetch_statuses() -> None:
    storage = FakeStorage(completed=set())
    storage.candidate_batches["batches/000001.parquet"] = [
        FinancialCandidate("100", "SUCCESS AS", "", "2024"),
        FinancialCandidate("200", "SERVER ERROR AS", "", "2024"),
    ]

    with pytest.raises(RuntimeError, match="server_error"):
        fetch_batch(
            FetchBatchInput(
                source_run_id="run-1",
                fetched_at="2026-07-01T00:00:00.000Z",
                candidate_batch_key="batches/000001.parquet",
            ),
            storage=storage,
            client=StatusClient(["success", "server_error"]),
        )

    assert set(storage.raw_report_writes) == {("100", "2024", "SELSKAP", "200")}


def test_write_candidate_batches_returns_batch_keys_and_persists_candidates() -> None:
    storage = FakeStorage(completed=set())
    candidates = [
        FinancialCandidate("100", "A AS", "", "2024"),
        FinancialCandidate("200", "B AS", "", "2024"),
        FinancialCandidate("300", "C AS", "", "2024"),
    ]

    batch_keys = write_candidate_batches(
        storage=storage,
        source_run_id="run-1",
        attempt_id="attempt-a",
        candidates=candidates,
        batch_size=2,
    )

    assert batch_keys == [
        "norway_brreg/finance/bootstrap_runs/run=run-1/attempt=attempt-a/candidates/batch=000000.parquet",
        "norway_brreg/finance/bootstrap_runs/run=run-1/attempt=attempt-a/candidates/batch=000001.parquet",
    ]
    assert storage.read_candidate_batch(batch_keys[0]) == candidates[:2]
    assert storage.read_candidate_batch(batch_keys[1]) == candidates[2:]


def test_write_candidate_batches_uses_attempt_id_to_keep_keys_disjoint() -> None:
    storage = FakeStorage(completed=set())
    candidates = [FinancialCandidate("100", "A AS", "", "2024")]

    first_batch_keys = write_candidate_batches(
        storage=storage,
        source_run_id="run-1",
        attempt_id="attempt-a",
        candidates=candidates,
        batch_size=1,
    )
    second_batch_keys = write_candidate_batches(
        storage=storage,
        source_run_id="run-1",
        attempt_id="attempt-b",
        candidates=candidates,
        batch_size=1,
    )

    assert first_batch_keys == [
        "norway_brreg/finance/bootstrap_runs/run=run-1/attempt=attempt-a/candidates/batch=000000.parquet"
    ]
    assert second_batch_keys == [
        "norway_brreg/finance/bootstrap_runs/run=run-1/attempt=attempt-b/candidates/batch=000000.parquet"
    ]
    assert first_batch_keys[0] != second_batch_keys[0]
    assert set(storage.candidate_batches) == {first_batch_keys[0], second_batch_keys[0]}


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


def test_bootstrap_input_uses_batch_keys_not_embedded_candidates() -> None:
    workflow_input = BootstrapInput(
        source_run_id="run-1",
        fetched_at="2026-07-01T00:00:00.000Z",
        candidate_count=2,
        batch_keys=["batch-1", "batch-2"],
        max_concurrent_batches=3,
    )

    assert workflow_input.batch_keys == ["batch-1", "batch-2"]
    assert workflow_input.candidate_count == 2
    assert not hasattr(workflow_input, "candidates")


def test_start_cli_parser_accepts_only_temporal_and_s3_endpoint_options() -> None:
    args = build_start_parser().parse_args(
        [
            "--temporal-address",
            "localhost:7233",
            "--s3-endpoint",
            "http://localhost:9000",
        ]
    )

    assert args.temporal_address == "localhost:7233"
    assert args.s3_endpoint == "http://localhost:9000"
    assert not hasattr(args, "task_queue")
    assert not hasattr(args, "batch_size")
    assert not hasattr(args, "max_concurrent_batches")


def test_cli_main_writes_candidate_batches_and_starts_workflow_with_batch_keys(
    monkeypatch, capsys
) -> None:
    import norway_financial_bootstrap.cli as cli

    storage = FakeStorage(completed=set())
    storage.frames[COMPANY_SNAPSHOT_NO_COMPANIES_KEY] = pl.DataFrame(
        [
            {
                "org_number": "100",
                "name": "EXISTING AS",
                "primary_website_url": "",
                "is_active": True,
                "last_submitted_accounts_year": "2024",
            },
            {
                "org_number": "200",
                "name": "MISSING AS",
                "primary_website_url": "",
                "is_active": True,
                "last_submitted_accounts_year": "2024",
            },
            {
                "org_number": "300",
                "name": "MISSING TWO AS",
                "primary_website_url": "",
                "is_active": True,
                "last_submitted_accounts_year": "2024",
            },
        ]
    )
    captured: dict[str, object] = {}

    def fake_storage_from_env() -> FakeStorage:
        return storage

    async def fake_start_workflow(
        *,
        temporal_address: str,
        task_queue: str,
        workflow_id: str,
        input: BootstrapInput,
    ) -> str:
        captured["temporal_address"] = temporal_address
        captured["task_queue"] = task_queue
        captured["workflow_id"] = workflow_id
        captured["input"] = input
        return workflow_id

    monkeypatch.setattr(cli, "storage_from_env", fake_storage_from_env)
    monkeypatch.setattr(cli, "start_workflow", fake_start_workflow)
    monkeypatch.setattr(cli, "_generate_attempt_id", lambda: "attempt-a")
    monkeypatch.setattr(cli, "_utc_now_iso", lambda: "2026-07-01T00:00:00.000Z")

    exit_code = cli_main(
        [
            "--temporal-address",
            "localhost:7233",
            "--s3-endpoint",
            "http://localhost:9000",
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out == f"{FIXED_WORKFLOW_ID}\n"
    assert captured["temporal_address"] == "localhost:7233"
    assert captured["task_queue"] == "norway-financial-bootstrap"
    assert captured["workflow_id"] == FIXED_WORKFLOW_ID
    assert captured["input"] == BootstrapInput(
        source_run_id=FIXED_WORKFLOW_ID,
        fetched_at="2026-07-01T00:00:00.000Z",
        candidate_count=3,
        batch_keys=[
            "norway_brreg/finance/bootstrap_runs/"
            f"run={FIXED_WORKFLOW_ID}/attempt=attempt-a/candidates/batch=000000.parquet",
        ],
        max_concurrent_batches=4,
    )
    assert storage.written_candidate_batches == [
        (
            FIXED_WORKFLOW_ID,
            "attempt-a",
            0,
            [
                FinancialCandidate("100", "EXISTING AS", "", "2024"),
                FinancialCandidate("200", "MISSING AS", "", "2024"),
                FinancialCandidate("300", "MISSING TWO AS", "", "2024"),
            ],
        ),
    ]


def test_start_workflow_does_not_attach_to_existing_workflow_id(monkeypatch) -> None:
    import asyncio

    import norway_financial_bootstrap.cli as cli

    captured: dict[str, object] = {}

    class FakeTemporalClient:
        async def start_workflow(self, workflow, input, **kwargs):
            captured["workflow"] = workflow
            captured["input"] = input
            captured["kwargs"] = kwargs
            return SimpleNamespace(id=kwargs["id"])

    async def fake_connect(address: str) -> FakeTemporalClient:
        captured["address"] = address
        return FakeTemporalClient()

    monkeypatch.setattr(cli.Client, "connect", fake_connect)

    workflow_id = asyncio.run(
        cli.start_workflow(
            temporal_address="localhost:7233",
            task_queue="norway-financial-test",
            workflow_id="run-1",
            input=BootstrapInput(
                source_run_id="run-1",
                fetched_at="2026-07-01T00:00:00.000Z",
                candidate_count=1,
                batch_keys=["batch-1"],
                max_concurrent_batches=3,
            ),
        )
    )

    assert workflow_id == "run-1"
    assert captured["address"] == "localhost:7233"
    assert captured["kwargs"] == {
        "id": "run-1",
        "task_queue": "norway-financial-test",
    }


def test_worker_cli_parser_accepts_worker_options() -> None:
    args = build_worker_parser().parse_args(
        [
            "--temporal-address",
            "localhost:7233",
            "--s3-endpoint",
            "http://localhost:9000",
        ]
    )

    assert args.temporal_address == "localhost:7233"
    assert args.s3_endpoint == "http://localhost:9000"
    assert not hasattr(args, "task_queue")
    assert not hasattr(args, "max_workers")
    assert not hasattr(args, "env_file")


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
