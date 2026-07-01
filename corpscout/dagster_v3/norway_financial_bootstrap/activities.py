import os
from dataclasses import dataclass
from typing import Any

import polars as pl
from temporalio import activity

from dagster_v3.defs.norway_brreg import financial_fetches
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


def storage_from_env() -> NorwayFinancialBootstrapStorage:
    kwargs: dict[str, str | None] = {
        "endpoint_url": os.environ.get("CORPSCOUT_S3_ENDPOINT"),
        "access_key": os.environ.get("CORPSCOUT_S3_ACCESS_KEY"),
        "secret_key": os.environ.get("CORPSCOUT_S3_SECRET_KEY"),
    }
    if "CORPSCOUT_S3_BUCKET" in os.environ:
        kwargs["bucket"] = os.environ["CORPSCOUT_S3_BUCKET"]
    region_name = os.environ.get("CORPSCOUT_S3_REGION") or os.environ.get(
        "CORPSCOUT_S3_REGION_NAME"
    )
    if region_name is not None:
        kwargs["region_name"] = region_name
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

    completed_org_years = storage.existing_raw_fetch_org_years()
    fetched_count = 0
    skipped_count = 0
    status_counts: dict[str, int] = {}
    retryable_status_counts: dict[str, int] = {}
    processed_count = 0

    for line_number, candidate in enumerate(candidates, start=1):
        processed_count += 1
        org_year = (candidate.org_number, candidate.last_submitted_accounts_year)
        if org_year in completed_org_years:
            skipped_count += 1
            _heartbeat_if_due(processed_count)
            continue

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

        storage.write_raw_fetch(
            candidate.org_number,
            candidate.last_submitted_accounts_year,
            pl.DataFrame([row]),
        )
        completed_org_years.add(org_year)
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


def _heartbeat_if_due(processed_count: int) -> None:
    if processed_count > 0 and processed_count % HEARTBEAT_EVERY_ROWS == 0:
        _safe_heartbeat(processed_count)


def _safe_heartbeat(details: Any) -> None:
    try:
        activity.heartbeat(details)
    except RuntimeError:
        return
