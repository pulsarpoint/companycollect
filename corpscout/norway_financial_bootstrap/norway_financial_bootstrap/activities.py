import json
import os
from dataclasses import dataclass
from typing import Any

from temporalio import activity

from norway_financial_bootstrap import fetch_status as financial_fetches
from norway_financial_bootstrap.brreg_client import BrregFinancialClient
from norway_financial_bootstrap.storage import NorwayFinancialBootstrapStorage

HEARTBEAT_EVERY_ROWS = 25


@dataclass(frozen=True)
class FetchBatchInput:
    source_run_id: str
    fetched_at: str
    candidate_batch_key: str


@dataclass(frozen=True)
class FetchBatchResult:
    fetched_count: int
    skipped_count: int
    status_counts: dict[str, int]


def storage_from_env(
    *, endpoint_url: str | None = None
) -> NorwayFinancialBootstrapStorage:
    kwargs: dict[str, str | None] = {
        "endpoint_url": endpoint_url or os.environ.get("CORPSCOUT_S3_ENDPOINT"),
        "access_key": os.environ.get("CORPSCOUT_S3_ACCESS_KEY"),
        "secret_key": os.environ.get("CORPSCOUT_S3_SECRET_KEY"),
    }
    return NorwayFinancialBootstrapStorage(**kwargs)


@activity.defn
def fetch_batch(
    input: FetchBatchInput,
    storage: Any | None = None,
    client: BrregFinancialClient | None = None,
) -> FetchBatchResult:
    storage = storage if storage is not None else storage_from_env()
    client = client if client is not None else BrregFinancialClient()
    candidates = storage.read_candidate_batch(input.candidate_batch_key)

    completed_report_ids = storage.existing_raw_report_ids()
    fetched_count = 0
    skipped_count = 0
    status_counts: dict[str, int] = {}
    retryable_status_counts: dict[str, int] = {}
    processed_count = 0

    for line_number, candidate in enumerate(candidates, start=1):
        processed_count += 1
        row = client.fetch_candidate(
            candidate,
            source_run_id=input.source_run_id,
            source_line_number=line_number,
            fetched_at=input.fetched_at,
        )
        status = str(row.get("fetch_status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        if financial_fetches.financial_fetch_status_requires_failure(status):
            retryable_status_counts[status] = retryable_status_counts.get(status, 0) + 1
            _heartbeat_if_due(processed_count)
            continue

        if status == financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS:
            for report in _reports_from_success_row(row):
                report_id = _required_report_value(report, "id")
                report_type = _required_report_value(report, "regnskapstype")
                report_key = (
                    candidate.org_number,
                    candidate.last_submitted_accounts_year,
                    report_type,
                    report_id,
                )
                if report_key in completed_report_ids:
                    skipped_count += 1
                    continue
                storage.write_raw_report(
                    org_number=candidate.org_number,
                    accounts_year=candidate.last_submitted_accounts_year,
                    report_type=report_type,
                    report_id=report_id,
                    report=report,
                )
                completed_report_ids.add(report_key)
                fetched_count += 1

        _heartbeat_if_due(processed_count)

    _safe_heartbeat(
        {
            "fetched_count": fetched_count,
            "skipped_count": skipped_count,
            "status_counts": status_counts,
        }
    )
    if retryable_status_counts:
        raise RuntimeError(
            "Norway financial bootstrap fetch batch has retryable failures: "
            + ", ".join(
                f"{status}={count}"
                for status, count in sorted(retryable_status_counts.items())
            )
        )
    return FetchBatchResult(
        fetched_count=fetched_count,
        skipped_count=skipped_count,
        status_counts=status_counts,
    )


def _reports_from_success_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    payload = json.loads(str(row.get("raw_response") or "[]"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise RuntimeError("BRREG financial success row does not contain a list of reports")
    return payload


def _required_report_value(report: dict[str, Any], key: str) -> str:
    value = report.get(key)
    if value is None or str(value) == "":
        raise RuntimeError(f"BRREG financial report is missing required field {key!r}")
    return str(value)


def _heartbeat_if_due(processed_count: int) -> None:
    if processed_count > 0 and processed_count % HEARTBEAT_EVERY_ROWS == 0:
        _safe_heartbeat(processed_count)


def _safe_heartbeat(details: Any) -> None:
    try:
        activity.heartbeat(details)
    except RuntimeError:
        return
