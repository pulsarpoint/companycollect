import asyncio
import concurrent.futures
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

from norway_financial_bootstrap.activities import (
    CandidateMarkerStatusInput,
    FetchAndStoreCandidateInput,
    GetCandidateInput,
    MarkerWriteInput,
    _heartbeating_sleep,
    candidate_marker_status,
    candidate_marker_status_with_dependencies,
    fetch_and_store_candidate,
    fetch_and_store_candidate_with_dependencies,
    get_candidate,
    get_candidate_with_dependencies,
    write_done_marker,
    write_done_marker_with_dependencies,
    write_failed_marker,
    write_failed_marker_with_dependencies,
)
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.cli import (
    FIXED_WORKFLOW_ID,
    build_parser as build_start_parser,
    main as cli_main,
    start_slot_workflows,
)
from norway_financial_bootstrap.worker import build_parser as build_worker_parser
from norway_financial_bootstrap.worker import build_worker, validate_s3_environment
from norway_financial_bootstrap.workflows import (
    DEFAULT_SLOT_COUNT,
    BootstrapSlotInput,
    BootstrapSlotResult,
    NorwayBrregFinancialBootstrapSlotWorkflow,
    slot_workflow_id,
)
from norway_financial_bootstrap.types import CandidateMarkerStatus


class FakeClickHouseGateway:
    def __init__(self, candidate: FinancialCandidate | None = None) -> None:
        self.candidate = candidate
        self.calls: list[tuple[str, int, int]] = []

    def get_candidate(self, run_id: str, slot_id: int, slot_index: int):
        self.calls.append((run_id, slot_id, slot_index))
        return self.candidate


class FakeStorage:
    def __init__(self) -> None:
        self.status = CandidateMarkerStatus.MISSING
        self.raw_writes: list[tuple[str, dict]] = []
        self.done_markers: list[dict] = []
        self.failed_markers: list[dict] = []

    def candidate_marker_status(self, org_number: str) -> CandidateMarkerStatus:
        return self.status

    def write_raw_report(self, *, org_number: str, report: dict) -> str:
        key = f"raw/{org_number}/{report['id']}.json"
        self.raw_writes.append((org_number, report))
        return key

    def write_done_marker(self, **kwargs) -> str:
        self.done_markers.append(kwargs)
        return f"raw/{kwargs['org_number']}/status/done.json"

    def write_failed_marker(self, **kwargs) -> str:
        self.failed_markers.append(kwargs)
        return f"raw/{kwargs['org_number']}/status/failed.json"


class FakeBrregClient:
    def __init__(self, row: dict) -> None:
        self.row = row
        self.calls = []

    def fetch_candidate(
        self, candidate, *, source_run_id, source_line_number, fetched_at
    ):
        self.calls.append((candidate, source_run_id, source_line_number, fetched_at))
        return self.row


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


def test_get_candidate_activity_registers_single_temporal_input_argument() -> None:
    activity_definition = get_candidate.__temporal_activity_definition

    assert activity_definition.arg_types == [GetCandidateInput]


def test_fetch_activity_registers_single_temporal_input_argument() -> None:
    activity_definition = fetch_and_store_candidate.__temporal_activity_definition

    assert activity_definition.arg_types == [FetchAndStoreCandidateInput]


def test_get_candidate_reads_candidate_by_slot_index(monkeypatch) -> None:
    import norway_financial_bootstrap.activities as activities

    gateway = FakeClickHouseGateway(FinancialCandidate("923609016"))
    monkeypatch.setattr(
        activities,
        "get_candidate_from_clickhouse",
        lambda clickhouse, *, run_id, slot_id, slot_index: clickhouse.get_candidate(
            run_id, slot_id, slot_index
        ),
    )

    candidate = get_candidate_with_dependencies(
        GetCandidateInput(run_id="run-1", slot_id=2, slot_index=9),
        clickhouse=gateway,
    )

    assert candidate == FinancialCandidate("923609016")
    assert gateway.calls == [("run-1", 2, 9)]


def test_marker_status_reads_s3_status() -> None:
    storage = FakeStorage()
    storage.status = CandidateMarkerStatus.DONE

    assert (
        candidate_marker_status_with_dependencies(
            CandidateMarkerStatusInput(org_number="923609016"), storage=storage
        )
        == CandidateMarkerStatus.DONE
    )


def test_fetch_and_store_candidate_writes_raw_reports_and_returns_result() -> None:
    row = {
        "org_number": "923609016",
        "fetch_status": "success",
        "fetched_at": "2026-07-01T18:00:00Z",
        "raw_response": json.dumps(
            [
                {
                    "id": 5667197,
                    "regnskapstype": "SELSKAP",
                    "regnskapsperiode": {"tilDato": "2024-12-31"},
                }
            ]
        ),
    }
    storage = FakeStorage()
    client = FakeBrregClient(row)

    result = fetch_and_store_candidate_with_dependencies(
        FetchAndStoreCandidateInput(
            run_id="run-1",
            org_number="923609016",
            fetched_at="2026-07-01T18:00:00Z",
        ),
        storage=storage,
        client=client,
    )

    assert result.org_number == "923609016"
    assert result.fetch_status == "success"
    assert result.report_count == 1
    assert result.raw_report_keys == ["raw/923609016/5667197.json"]
    assert client.calls[0][0] == FinancialCandidate("923609016")


def test_fetch_and_store_candidate_raises_for_retryable_status() -> None:
    storage = FakeStorage()
    client = FakeBrregClient(
        {
            "org_number": "923609016",
            "fetch_status": "server_error",
            "fetched_at": "2026-07-01T18:00:00Z",
            "raw_response": "[]",
        }
    )

    with pytest.raises(ApplicationError) as exc_info:
        fetch_and_store_candidate_with_dependencies(
            FetchAndStoreCandidateInput(
                run_id="run-1",
                org_number="923609016",
                fetched_at="2026-07-01T18:00:00Z",
            ),
            storage=storage,
            client=client,
        )

    assert exc_info.value.type == "BrregFinancialFetchFailed"
    assert exc_info.value.details == ("server_error",)
    assert storage.raw_writes == []


def test_write_done_marker_persists_result() -> None:
    storage = FakeStorage()

    key = write_done_marker_with_dependencies(
        MarkerWriteInput(
            org_number="923609016",
            fetch_status="success",
            report_count=1,
            raw_report_keys=["raw.json"],
            timestamp="2026-07-01T18:00:00Z",
            error_type="",
            error_message="",
        ),
        storage=storage,
    )

    assert key.endswith("/status/done.json")
    assert storage.done_markers[0]["report_count"] == 1


def test_write_failed_marker_persists_error() -> None:
    storage = FakeStorage()

    key = write_failed_marker_with_dependencies(
        MarkerWriteInput(
            org_number="923609016",
            fetch_status="server_error",
            report_count=0,
            raw_report_keys=[],
            timestamp="2026-07-01T18:00:00Z",
            error_type="RuntimeError",
            error_message="failed",
        ),
        storage=storage,
    )

    assert key.endswith("/status/failed.json")
    assert storage.failed_markers[0]["error_type"] == "RuntimeError"
    assert storage.failed_markers[0]["fetch_status"] == "server_error"


def test_activity_retry_sleep_heartbeats_during_backoff(monkeypatch) -> None:
    import norway_financial_bootstrap.activities as activities

    heartbeats: list[object] = []
    sleeps: list[float] = []
    monkeypatch.setattr(activities, "_safe_heartbeat", heartbeats.append)

    _heartbeating_sleep(125, sleep=sleeps.append)

    assert sleeps == [60, 60, 5]
    assert heartbeats == [
        {"retry_sleep_seconds_remaining": 125},
        {"retry_sleep_seconds_remaining": 65},
        {"retry_sleep_seconds_remaining": 5},
        {"retry_sleep_seconds_remaining": 0},
    ]


def test_slot_workflow_id_is_stable_per_slot() -> None:
    assert slot_workflow_id("norway-brreg-finance-historical-bootstrap", 2) == (
        "norway-brreg-finance-historical-bootstrap-slot-2"
    )


def test_default_slot_count_is_four() -> None:
    assert DEFAULT_SLOT_COUNT == 4


def test_bootstrap_slot_input_starts_at_index_zero() -> None:
    assert (
        BootstrapSlotInput(
            run_id="run-1",
            slot_id=0,
            slot_index=0,
            slot_count=4,
            fetched_at="2026-07-01T18:00:00Z",
        ).slot_index
        == 0
    )


def test_slot_workflow_completes_when_candidate_is_missing(monkeypatch) -> None:
    import norway_financial_bootstrap.workflows as workflows

    async def fake_execute_activity(activity_fn, input, **_kwargs):
        assert activity_fn is workflows.get_candidate
        assert input.slot_index == 3
        return None

    monkeypatch.setattr(workflows.workflow, "execute_activity", fake_execute_activity)

    result = asyncio.run(
        NorwayBrregFinancialBootstrapSlotWorkflow().run(
            BootstrapSlotInput(
                run_id="run-1",
                slot_id=1,
                slot_index=3,
                slot_count=4,
                fetched_at="2026-07-01T18:00:00Z",
            )
        )
    )

    assert result == BootstrapSlotResult(
        run_id="run-1",
        slot_id=1,
        slot_index=3,
        completed=True,
        org_number=None,
    )


def test_slot_workflow_skips_done_marker_and_continues_as_new(monkeypatch) -> None:
    import norway_financial_bootstrap.workflows as workflows

    class ContinueAsNew(Exception):
        def __init__(self, input):
            self.input = input

    async def fake_execute_activity(activity_fn, input, **_kwargs):
        if activity_fn is workflows.get_candidate:
            return FinancialCandidate("923609016")
        if activity_fn is workflows.candidate_marker_status:
            return CandidateMarkerStatus.DONE
        raise AssertionError(f"unexpected activity: {activity_fn}")

    def fake_continue_as_new(input):
        raise ContinueAsNew(input)

    monkeypatch.setattr(workflows.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(workflows.workflow, "continue_as_new", fake_continue_as_new)

    try:
        asyncio.run(
            NorwayBrregFinancialBootstrapSlotWorkflow().run(
                BootstrapSlotInput(
                    run_id="run-1",
                    slot_id=1,
                    slot_index=3,
                    slot_count=4,
                    fetched_at="2026-07-01T18:00:00Z",
                )
            )
        )
    except ContinueAsNew as exc:
        assert exc.input.slot_index == 4
    else:
        raise AssertionError("workflow should continue as new")


def test_slot_workflow_fetches_missing_marker_writes_done_and_continues(
    monkeypatch,
) -> None:
    import norway_financial_bootstrap.workflows as workflows
    from norway_financial_bootstrap.activities import FetchAndStoreCandidateResult

    events: list[str] = []

    class ContinueAsNew(Exception):
        def __init__(self, input):
            self.input = input

    def fake_workflow_now():
        from datetime import UTC, datetime

        return datetime(2026, 7, 1, 18, 1, tzinfo=UTC)

    async def fake_execute_activity(activity_fn, input, **_kwargs):
        if activity_fn is workflows.get_candidate:
            events.append("get")
            return FinancialCandidate("923609016")
        if activity_fn is workflows.candidate_marker_status:
            events.append("status")
            return CandidateMarkerStatus.MISSING
        if activity_fn is workflows.fetch_and_store_candidate:
            events.append("fetch")
            return FetchAndStoreCandidateResult(
                org_number=input.org_number,
                fetch_status="success",
                report_count=1,
                raw_report_keys=["raw.json"],
                fetched_at=input.fetched_at,
            )
        if activity_fn is workflows.write_done_marker:
            events.append("done")
            return "done.json"
        raise AssertionError(f"unexpected activity: {activity_fn}")

    def fake_continue_as_new(input):
        raise ContinueAsNew(input)

    monkeypatch.setattr(workflows.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(workflows.workflow, "continue_as_new", fake_continue_as_new)
    monkeypatch.setattr(workflows.workflow, "now", fake_workflow_now)

    try:
        asyncio.run(
            NorwayBrregFinancialBootstrapSlotWorkflow().run(
                BootstrapSlotInput(
                    run_id="run-1",
                    slot_id=1,
                    slot_index=3,
                    slot_count=4,
                    fetched_at="2026-07-01T18:00:00Z",
                )
            )
        )
    except ContinueAsNew as exc:
        assert exc.input.slot_index == 4
    else:
        raise AssertionError("workflow should continue as new")

    assert events == ["get", "status", "fetch", "done"]


def test_slot_workflow_writes_failed_marker_with_activity_root_cause(
    monkeypatch,
) -> None:
    import norway_financial_bootstrap.workflows as workflows

    events: list[tuple[str, object]] = []

    class ContinueAsNew(Exception):
        def __init__(self, input):
            self.input = input

    def fake_workflow_now():
        from datetime import UTC, datetime

        return datetime(2026, 7, 1, 18, 2, tzinfo=UTC)

    async def fake_execute_activity(activity_fn, input, **_kwargs):
        if activity_fn is workflows.get_candidate:
            return FinancialCandidate("923609016")
        if activity_fn is workflows.candidate_marker_status:
            return CandidateMarkerStatus.MISSING
        if activity_fn is workflows.fetch_and_store_candidate:
            app_error = ApplicationError(
                "BRREG financial fetch failed for 923609016: server_error",
                "server_error",
                type="BrregFinancialFetchFailed",
            )
            raise activity_error("Activity task failed") from app_error
        if activity_fn is workflows.write_failed_marker:
            events.append(("failed", input))
            return "failed.json"
        raise AssertionError(f"unexpected activity: {activity_fn}")

    def fake_continue_as_new(input):
        raise ContinueAsNew(input)

    monkeypatch.setattr(workflows.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(workflows.workflow, "continue_as_new", fake_continue_as_new)
    monkeypatch.setattr(workflows.workflow, "now", fake_workflow_now)

    try:
        asyncio.run(
            NorwayBrregFinancialBootstrapSlotWorkflow().run(
                BootstrapSlotInput(
                    run_id="run-1",
                    slot_id=1,
                    slot_index=3,
                    slot_count=4,
                    fetched_at="2026-07-01T18:00:00Z",
                )
            )
        )
    except ContinueAsNew as exc:
        assert exc.input.slot_index == 4
    else:
        raise AssertionError("workflow should continue as new")

    marker = events[0][1]
    assert marker.fetch_status == "server_error"
    assert marker.error_type == "BrregFinancialFetchFailed"
    assert (
        marker.error_message
        == "BRREG financial fetch failed for 923609016: server_error"
    )
    assert marker.timestamp == "2026-07-01T18:02:00.000Z"


def test_slot_workflow_does_not_write_failed_marker_on_cancellation(
    monkeypatch,
) -> None:
    import norway_financial_bootstrap.workflows as workflows

    async def fake_execute_activity(activity_fn, input, **_kwargs):
        if activity_fn is workflows.get_candidate:
            return FinancialCandidate("923609016")
        if activity_fn is workflows.candidate_marker_status:
            return CandidateMarkerStatus.MISSING
        if activity_fn is workflows.fetch_and_store_candidate:
            raise activity_error("Activity task cancelled") from CancelledError(
                "cancelled"
            )
        if activity_fn is workflows.write_failed_marker:
            raise AssertionError("cancelled activities must not write failed markers")
        raise AssertionError(f"unexpected activity: {activity_fn}")

    monkeypatch.setattr(workflows.workflow, "execute_activity", fake_execute_activity)
    monkeypatch.setattr(
        workflows.workflow,
        "now",
        lambda: datetime(2026, 7, 1, 18, 2, tzinfo=UTC),
    )

    with pytest.raises(ActivityError):
        asyncio.run(
            NorwayBrregFinancialBootstrapSlotWorkflow().run(
                BootstrapSlotInput(
                    run_id="run-1",
                    slot_id=1,
                    slot_index=3,
                    slot_count=4,
                    fetched_at="2026-07-01T18:00:00Z",
                )
            )
        )


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


class FakeTemporalClient:
    def __init__(self) -> None:
        self.started = []

    async def start_workflow(self, workflow, input, id, task_queue):
        self.started.append((workflow, input, id, task_queue))
        return SimpleNamespace(id=id)


class FakeAlreadyStartedTemporalClient:
    def __init__(self) -> None:
        self.started = []

    async def start_workflow(self, workflow, input, id, task_queue):
        self.started.append((workflow, input, id, task_queue))
        raise workflows_already_started_error()


def workflows_already_started_error() -> Exception:
    from temporalio.exceptions import WorkflowAlreadyStartedError

    return WorkflowAlreadyStartedError(
        "existing", "NorwayBrregFinancialBootstrapSlotWorkflow"
    )


def activity_error(message: str) -> ActivityError:
    return ActivityError(
        message,
        scheduled_event_id=1,
        started_event_id=2,
        identity="test-worker",
        activity_type="fetch_and_store_candidate",
        activity_id="activity-1",
        retry_state=None,
    )


def test_start_slot_workflows_starts_one_workflow_per_slot() -> None:
    client = FakeTemporalClient()

    ids = asyncio.run(
        start_slot_workflows(
            client=client,
            run_id=FIXED_WORKFLOW_ID,
            fetched_at="2026-07-01T18:00:00Z",
            slot_count=DEFAULT_SLOT_COUNT,
            task_queue="test-queue",
        )
    )

    assert ids == [
        "norway-brreg-finance-historical-bootstrap-slot-0",
        "norway-brreg-finance-historical-bootstrap-slot-1",
        "norway-brreg-finance-historical-bootstrap-slot-2",
        "norway-brreg-finance-historical-bootstrap-slot-3",
    ]
    assert [call[1].slot_index for call in client.started] == [0, 0, 0, 0]


def test_start_slot_workflows_reuses_already_started_slot_ids() -> None:
    client = FakeAlreadyStartedTemporalClient()

    ids = asyncio.run(
        start_slot_workflows(
            client=client,
            run_id=FIXED_WORKFLOW_ID,
            fetched_at="2026-07-01T18:00:00Z",
            slot_count=2,
            task_queue="test-queue",
        )
    )

    assert ids == [
        "norway-brreg-finance-historical-bootstrap-slot-0",
        "norway-brreg-finance-historical-bootstrap-slot-1",
    ]


def test_cli_main_prepares_candidate_table_and_starts_slot_workflows(
    monkeypatch, capsys
) -> None:
    import norway_financial_bootstrap.cli as cli

    captured: dict[str, object] = {}

    def fake_clickhouse_from_env():
        return object()

    def fake_prepare_candidate_table(client, *, run_id, slot_count):
        captured["clickhouse_client"] = client
        captured["run_id"] = run_id
        captured["slot_count"] = slot_count
        return SimpleNamespace(
            run_id=run_id,
            slot_count=slot_count,
            existing_count=0,
            candidate_count=10,
            stored_slot_count=4,
            inserted=True,
        )

    async def fake_connect_and_start_slot_workflows(
        *,
        temporal_address: str,
        run_id: str,
        fetched_at: str,
        slot_count: int,
        task_queue: str,
    ) -> list[str]:
        captured["temporal_address"] = temporal_address
        captured["workflow_run_id"] = run_id
        captured["fetched_at"] = fetched_at
        captured["workflow_slot_count"] = slot_count
        captured["task_queue"] = task_queue
        return [slot_workflow_id(run_id, slot_id) for slot_id in range(slot_count)]

    monkeypatch.setattr(cli, "clickhouse_from_env", fake_clickhouse_from_env)
    monkeypatch.setattr(cli, "prepare_candidate_table", fake_prepare_candidate_table)
    monkeypatch.setattr(
        cli,
        "_connect_and_start_slot_workflows",
        fake_connect_and_start_slot_workflows,
    )
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
    assert capsys.readouterr().out == (
        "norway-brreg-finance-historical-bootstrap-slot-0\n"
        "norway-brreg-finance-historical-bootstrap-slot-1\n"
        "norway-brreg-finance-historical-bootstrap-slot-2\n"
        "norway-brreg-finance-historical-bootstrap-slot-3\n"
    )
    assert captured["run_id"] == FIXED_WORKFLOW_ID
    assert captured["slot_count"] == DEFAULT_SLOT_COUNT
    assert captured["temporal_address"] == "localhost:7233"
    assert captured["task_queue"] == "norway-financial-bootstrap"


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


def test_worker_validates_required_s3_environment(monkeypatch) -> None:
    for name in (
        "CORPSCOUT_S3_ENDPOINT",
        "CORPSCOUT_S3_ACCESS_KEY",
        "CORPSCOUT_S3_SECRET_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError) as exc_info:
        validate_s3_environment()

    assert "CORPSCOUT_S3_ENDPOINT" in str(exc_info.value)
    assert "CORPSCOUT_S3_ACCESS_KEY" in str(exc_info.value)
    assert "CORPSCOUT_S3_SECRET_KEY" in str(exc_info.value)


def test_build_worker_registers_slot_workflow_activities_and_executor(
    monkeypatch,
) -> None:
    import norway_financial_bootstrap.worker as worker

    FakeWorker.instances = []
    monkeypatch.setattr(worker, "Worker", FakeWorker)

    build_worker(object(), task_queue="norway-financial-test", max_workers=3)

    record = FakeWorker.instances[0]
    assert record["task_queue"] == "norway-financial-test"
    assert record["workflows"] == [NorwayBrregFinancialBootstrapSlotWorkflow]
    assert set(record["activities"]) == {
        get_candidate,
        candidate_marker_status,
        fetch_and_store_candidate,
        write_done_marker,
        write_failed_marker,
    }
    assert record["max_concurrent_activities"] == 3
    assert isinstance(
        record["activity_executor"], concurrent.futures.ThreadPoolExecutor
    )
