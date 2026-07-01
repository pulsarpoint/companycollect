import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from temporalio import activity

from norway_financial_bootstrap import fetch_status as financial_fetches
from norway_financial_bootstrap.brreg_client import BrregFinancialClient
from norway_financial_bootstrap.candidates import FinancialCandidate
from norway_financial_bootstrap.clickhouse import (
    clickhouse_from_env,
    get_candidate_from_clickhouse,
)
from norway_financial_bootstrap.storage import NorwayFinancialBootstrapStorage
from norway_financial_bootstrap.types import CandidateMarkerStatus

HEARTBEAT_SLEEP_INTERVAL_SECONDS = 60


@dataclass(frozen=True)
class GetCandidateInput:
    run_id: str
    slot_id: int
    slot_index: int


@dataclass(frozen=True)
class CandidateMarkerStatusInput:
    org_number: str


@dataclass(frozen=True)
class FetchAndStoreCandidateInput:
    run_id: str
    org_number: str
    fetched_at: str


@dataclass(frozen=True)
class FetchAndStoreCandidateResult:
    org_number: str
    fetch_status: str
    report_count: int
    raw_report_keys: list[str]
    fetched_at: str


@dataclass(frozen=True)
class MarkerWriteInput:
    org_number: str
    fetch_status: str
    report_count: int
    raw_report_keys: list[str]
    timestamp: str
    error_type: str
    error_message: str


def storage_from_env(
    *, endpoint_url: str | None = None
) -> NorwayFinancialBootstrapStorage:
    return NorwayFinancialBootstrapStorage(
        endpoint_url=endpoint_url or os.environ.get("CORPSCOUT_S3_ENDPOINT"),
        access_key=os.environ.get("CORPSCOUT_S3_ACCESS_KEY"),
        secret_key=os.environ.get("CORPSCOUT_S3_SECRET_KEY"),
    )


@activity.defn
def get_candidate(input: GetCandidateInput) -> FinancialCandidate | None:
    return get_candidate_with_dependencies(input, clickhouse=clickhouse_from_env())


def get_candidate_with_dependencies(
    input: GetCandidateInput, *, clickhouse: Any
) -> FinancialCandidate | None:
    return get_candidate_from_clickhouse(
        clickhouse,
        run_id=input.run_id,
        slot_id=input.slot_id,
        slot_index=input.slot_index,
    )


@activity.defn
def candidate_marker_status(input: CandidateMarkerStatusInput) -> CandidateMarkerStatus:
    return candidate_marker_status_with_dependencies(input, storage=storage_from_env())


def candidate_marker_status_with_dependencies(
    input: CandidateMarkerStatusInput, *, storage: Any
) -> CandidateMarkerStatus:
    return storage.candidate_marker_status(input.org_number)


@activity.defn
def fetch_and_store_candidate(
    input: FetchAndStoreCandidateInput,
) -> FetchAndStoreCandidateResult:
    # Keep this Temporal activity signature to exactly one argument. The Python
    # SDK only applies type-hint decoding when registered argument count matches
    # payload count; dependency injection belongs in the helper below.
    return fetch_and_store_candidate_with_dependencies(
        input,
        storage=storage_from_env(),
        client=BrregFinancialClient(sleep=_heartbeating_sleep),
    )


def fetch_and_store_candidate_with_dependencies(
    input: FetchAndStoreCandidateInput,
    *,
    storage: Any,
    client: BrregFinancialClient,
) -> FetchAndStoreCandidateResult:
    candidate = FinancialCandidate(input.org_number)
    row = client.fetch_candidate(
        candidate,
        source_run_id=input.run_id,
        source_line_number=1,
        fetched_at=input.fetched_at,
    )
    status = str(row.get("fetch_status") or "unknown")
    if financial_fetches.financial_fetch_status_requires_failure(status):
        raise RuntimeError(f"BRREG financial fetch failed for {input.org_number}: {status}")

    raw_report_keys: list[str] = []
    if status == financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS:
        for report in _reports_from_success_row(row):
            raw_report_keys.append(
                storage.write_raw_report(org_number=input.org_number, report=report)
            )

    return FetchAndStoreCandidateResult(
        org_number=input.org_number,
        fetch_status=status,
        report_count=len(raw_report_keys),
        raw_report_keys=raw_report_keys,
        fetched_at=input.fetched_at,
    )


@activity.defn
def write_done_marker(input: MarkerWriteInput) -> str:
    return write_done_marker_with_dependencies(input, storage=storage_from_env())


def write_done_marker_with_dependencies(input: MarkerWriteInput, *, storage: Any) -> str:
    return storage.write_done_marker(
        org_number=input.org_number,
        fetch_status=input.fetch_status,
        report_count=input.report_count,
        raw_report_keys=input.raw_report_keys,
        completed_at=input.timestamp,
    )


@activity.defn
def write_failed_marker(input: MarkerWriteInput) -> str:
    return write_failed_marker_with_dependencies(input, storage=storage_from_env())


def write_failed_marker_with_dependencies(input: MarkerWriteInput, *, storage: Any) -> str:
    return storage.write_failed_marker(
        org_number=input.org_number,
        error_type=input.error_type,
        error_message=input.error_message,
        failed_at=input.timestamp,
    )


def _reports_from_success_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    payload = json.loads(str(row.get("raw_response") or "[]"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise RuntimeError("BRREG financial success row does not contain a list of reports")
    return payload


def _heartbeating_sleep(
    total_seconds: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    remaining_seconds = max(float(total_seconds), 0.0)
    _safe_heartbeat({"retry_sleep_seconds_remaining": _display_seconds(remaining_seconds)})
    while remaining_seconds > 0:
        sleep_seconds = min(HEARTBEAT_SLEEP_INTERVAL_SECONDS, remaining_seconds)
        sleep(sleep_seconds)
        remaining_seconds = max(remaining_seconds - sleep_seconds, 0.0)
        _safe_heartbeat(
            {"retry_sleep_seconds_remaining": _display_seconds(remaining_seconds)}
        )


def _display_seconds(value: float) -> int | float:
    return int(value) if value.is_integer() else value


def _safe_heartbeat(details: Any) -> None:
    try:
        activity.heartbeat(details)
    except RuntimeError:
        return
