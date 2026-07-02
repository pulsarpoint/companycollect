from norway_financial_bootstrap.paths import DEFAULT_BUCKET, RAW_REPORT_PREFIX
from norway_financial_bootstrap.status import (
    S3Summary,
    SlotSummary,
    TaskQueuePollers,
    classify_status,
    s3_key_kind,
)


def test_s3_key_kind_classifies_fixed_financial_storage_keys() -> None:
    assert (
        s3_key_kind(
            "norway_brreg/finance/raw_reports/org=811685852/"
            "year=2024/type=SELSKAP/id=6697842.json"
        )
        == "raw_report"
    )
    assert (
        s3_key_kind("norway_brreg/finance/raw_reports/org=811685852/status/done.json")
        == "done_marker"
    )
    assert (
        s3_key_kind(
            "norway_brreg/finance/raw_reports/org=811685852/status/failed.json"
        )
        == "failed_marker"
    )
    assert s3_key_kind("norway_brreg/finance/raw_reports/readme.txt") == "other"


def test_classify_status_reports_missing_slot_workflows() -> None:
    assert (
        classify_status(
            slots=[],
            pollers=TaskQueuePollers(
                workflow_poller_count=1,
                activity_poller_count=1,
            ),
            s3_before=_s3_summary(done=0, raw=0, failed=0),
            s3_after=None,
        )
        == "NO_RUNNING_SLOT_WORKFLOWS"
    )


def test_classify_status_reports_missing_worker_pollers() -> None:
    assert (
        classify_status(
            slots=[_slot(status="RUNNING")],
            pollers=TaskQueuePollers(
                workflow_poller_count=1,
                activity_poller_count=0,
            ),
            s3_before=_s3_summary(done=1, raw=1, failed=0),
            s3_after=None,
        )
        == "NO_WORKER_POLLERS"
    )


def test_classify_status_reports_fetch_backoff_when_s3_does_not_move() -> None:
    before = _s3_summary(done=10, raw=20, failed=1)

    assert (
        classify_status(
            slots=[
                _slot(
                    status="RUNNING",
                    pending_activities=["fetch_and_store_candidate"],
                )
            ],
            pollers=TaskQueuePollers(
                workflow_poller_count=1,
                activity_poller_count=1,
            ),
            s3_before=before,
            s3_after=before,
        )
        == "RUNNING_FETCH_BACKOFF_OR_STALLED"
    )


def test_classify_status_reports_running_when_s3_progresses() -> None:
    assert (
        classify_status(
            slots=[
                _slot(
                    status="RUNNING",
                    pending_activities=["fetch_and_store_candidate"],
                )
            ],
            pollers=TaskQueuePollers(
                workflow_poller_count=1,
                activity_poller_count=1,
            ),
            s3_before=_s3_summary(done=10, raw=20, failed=1),
            s3_after=_s3_summary(done=11, raw=20, failed=1),
        )
        == "RUNNING"
    )


def _slot(
    *,
    status: str,
    pending_activities: list[str] | None = None,
    current_failure_count: int = 0,
) -> SlotSummary:
    return SlotSummary(
        workflow_id="norway-brreg-finance-historical-bootstrap-slot-0",
        run_id="run-1",
        status=status,
        workflow_type="NorwayBrregFinancialBootstrapSlotWorkflow",
        history_length=10,
        pending_activities=pending_activities or [],
        latest_event_type="ACTIVITY_TASK_SCHEDULED",
        current_failure_count=current_failure_count,
    )


def _s3_summary(*, done: int, raw: int, failed: int) -> S3Summary:
    return S3Summary(
        bucket=DEFAULT_BUCKET,
        prefix=RAW_REPORT_PREFIX,
        total_count=done + raw + failed,
        raw_report_count=raw,
        done_marker_count=done,
        failed_marker_count=failed,
        other_count=0,
        latest_key=None,
        latest_modified=None,
        failed_marker_keys=[],
    )
