from __future__ import annotations

from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.constants import (
    GROUP_NAME,
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    BOOTSTRAP_FETCH_PREFIX,
    NorwayBrregFinancialParquetStorageResource,
    latest_fetch_rows_per_org,
)

NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT = 64
NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        f"bucket_{bucket_index:02d}"
        for bucket_index in range(NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT)
    ]
)
NORWAY_BRREG_FINANCIAL_BOOTSTRAP_POOL = "norway_brreg_financial_api"
BOOTSTRAP_CHUNK_SIZE = 250
BOOTSTRAP_LEGACY_ERROR_MESSAGE = "converted from Temporal bootstrap done marker"

# The cityHash64 bucket expression is a stable contract: the same org must land
# in the same partition on every run, so never change it without a full
# re-bucketing of the chunk parquet layout.
BOOTSTRAP_CANDIDATES_SQL = """
SELECT
    toString(org_number) AS org_number,
    name,
    primary_website_url,
    last_submitted_accounts_year
FROM no_companies
WHERE is_active
  AND last_submitted_accounts_year IS NOT NULL
  AND cityHash64(toString(org_number)) %% %(bucket_count)s = %(bucket_index)s
ORDER BY org_number
"""


@dg.asset(
    name="norway_brreg_financial_bootstrap_fetches_parquet",
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "clickhouse", "brreg"},
    partitions_def=NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=NORWAY_BRREG_FINANCIAL_BOOTSTRAP_POOL,
    description=(
        "Backfills Norway Brreg raw financial fetches for one org-number hash bucket "
        "into chunked parquet, converting already-completed Temporal bootstrap "
        "orgs from their legacy S3 JSON without re-calling BRREG."
    ),
)
def norway_brreg_financial_bootstrap_fetches_parquet(
    context,
    clickhouse: ClickhouseResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    bucket_key = context.partition_key
    bucket_index = _bucket_index_from_partition_key(bucket_key)

    with clickhouse.get_connection() as client:
        candidates = _bootstrap_candidates(client, bucket_index)

    existing_frame = norway_brreg_financial_storage.read_bootstrap_bucket_fetches(bucket_key)
    done_orgs = _source_outcome_orgs(existing_frame)
    pending = [
        candidate for candidate in candidates if candidate["org_number"] not in done_orgs
    ]
    next_chunk_index = _next_chunk_index(
        norway_brreg_financial_storage.list_bootstrap_chunk_keys(bucket_key)
    )
    context.log.info(
        "Starting Norway Brreg financial bootstrap bucket: bucket=%s candidates=%d "
        "already_done=%d pending=%d next_chunk_index=%d",
        bucket_key,
        len(candidates),
        len(candidates) - len(pending),
        len(pending),
        next_chunk_index,
    )

    converted_count = 0
    fetched_count = 0
    chunk_keys: list[str] = []
    status_counts: dict[str, int] = {}
    for chunk_start in range(0, len(pending), BOOTSTRAP_CHUNK_SIZE):
        chunk_candidates = pending[chunk_start : chunk_start + BOOTSTRAP_CHUNK_SIZE]
        rows: list[dict[str, Any]] = []
        fetch_candidates: list[dict[str, Any]] = []
        for candidate in chunk_candidates:
            marker = norway_brreg_financial_storage.read_legacy_bootstrap_done_marker(
                candidate["org_number"]
            )
            if marker is None:
                fetch_candidates.append(candidate)
                continue
            rows.append(
                _converted_row_from_legacy_marker(
                    marker=marker,
                    candidate=candidate,
                    storage=norway_brreg_financial_storage,
                    source_run_id=context.run_id,
                )
            )
            converted_count += 1
        if fetch_candidates:
            rows.extend(
                financial_fetches.fetch_financial_rows_for_orgs(
                    orgs=fetch_candidates,
                    source_run_id=context.run_id,
                    log=context.log.info,
                )
            )
            fetched_count += len(fetch_candidates)

        chunk_frame = financial_fetches.financial_fetches_frame(rows)
        chunk_key = norway_brreg_financial_storage.write_bootstrap_chunk(
            bucket_key,
            next_chunk_index,
            chunk_frame,
        )
        chunk_keys.append(chunk_key)
        next_chunk_index += 1
        for row in rows:
            fetch_status = _string(row.get("fetch_status"))
            status_counts[fetch_status] = status_counts.get(fetch_status, 0) + 1
        context.log.info(
            "Wrote Norway Brreg financial bootstrap chunk: bucket=%s chunk_key=%s "
            "rows=%d converted_total=%d fetched_total=%d",
            bucket_key,
            chunk_key,
            chunk_frame.height,
            converted_count,
            fetched_count,
        )

    retryable_statuses = sorted(
        status
        for status in status_counts
        if financial_fetches.financial_fetch_status_requires_failure(status)
    )
    metadata = {
        "bucket_key": bucket_key,
        "candidate_count": len(candidates),
        "already_done_count": len(candidates) - len(pending),
        "converted_count": converted_count,
        "fetched_count": fetched_count,
        "chunk_count": len(chunk_keys),
        "status_counts": status_counts,
        "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
        "s3_prefix": BOOTSTRAP_FETCH_PREFIX,
    }
    context.log.info(
        "Completed Norway Brreg financial bootstrap bucket: bucket=%s metadata=%s",
        bucket_key,
        metadata,
    )
    if retryable_statuses:
        raise RuntimeError(
            "Norway Brreg financial bootstrap bucket contains retryable statuses "
            f"after persisting parquet chunks: bucket={bucket_key} "
            f"statuses={retryable_statuses}"
        )
    return dg.MaterializeResult(metadata=metadata)


def _bootstrap_candidates(client: Any, bucket_index: int) -> list[dict[str, Any]]:
    if not 0 <= bucket_index < NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT:
        raise ValueError(
            f"Norway bootstrap bucket index out of range: {bucket_index}"
        )
    rows = client.execute(
        BOOTSTRAP_CANDIDATES_SQL,
        {
            "bucket_count": NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT,
            "bucket_index": bucket_index,
        },
    )
    return [
        {
            "org_number": _string(org_number),
            "legal_name": _string(name),
            "website": _string(website),
            "last_submitted_accounts_year": _string(accounts_year),
        }
        for org_number, name, website, accounts_year in rows
        if _string(accounts_year) != ""
    ]


def _next_chunk_index(chunk_keys: list[str]) -> int:
    indexes = []
    for key in chunk_keys:
        stem = key.rsplit("chunk=", 1)[-1].removesuffix(".parquet")
        if not stem.isdigit():
            raise RuntimeError(f"Invalid Norway bootstrap chunk key: {key}")
        indexes.append(int(stem))
    return max(indexes) + 1 if indexes else 0


def _bucket_index_from_partition_key(partition_key: str) -> int:
    prefix, _, suffix = partition_key.partition("_")
    if prefix != "bucket" or not suffix.isdigit():
        raise ValueError(
            f"Invalid Norway bootstrap partition key: {partition_key!r}"
        )
    return int(suffix)


def _source_outcome_orgs(existing_frame: Any) -> set[str]:
    latest = latest_fetch_rows_per_org(existing_frame)
    if latest.is_empty():
        return set()
    return {
        _string(row["org_number"])
        for row in latest.select(["org_number", "fetch_status"]).to_dicts()
        if _string(row["fetch_status"]) in financial_fetches.SOURCE_OUTCOME_FETCH_STATUSES
    }


def _converted_row_from_legacy_marker(
    *,
    marker: dict[str, Any],
    candidate: dict[str, Any],
    storage: NorwayBrregFinancialParquetStorageResource,
    source_run_id: str,
) -> dict[str, Any]:
    org_number = candidate["org_number"]
    fetch_status = _string(marker.get("fetch_status"))
    completed_at = _string(marker.get("completed_at"))
    source_url = f"{financial_fetches.BRREG_REGNSKAP_BASE_URL}/{org_number}"
    raw_report_keys = [
        _string(key) for key in marker.get("raw_report_keys") or [] if _string(key)
    ]
    if fetch_status == financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS:
        return financial_fetches.financial_fetch_success_row(
            org=candidate,
            source_url=source_url,
            payload=storage.read_legacy_bootstrap_raw_reports(raw_report_keys),
            source_run_id=source_run_id,
            source_line_number=1,
            status_code=200,
            fetched_at=completed_at,
            attempt_count=1,
        )
    if fetch_status not in financial_fetches.SOURCE_OUTCOME_FETCH_STATUSES:
        raise RuntimeError(
            "Legacy Norway bootstrap done marker has a non-terminal fetch_status: "
            f"org={org_number} fetch_status={fetch_status!r}"
        )
    return financial_fetches.financial_fetch_failure_row(
        org=candidate,
        source_url=source_url,
        source_run_id=source_run_id,
        source_line_number=1,
        status_code=(
            404
            if fetch_status == financial_fetches.FINANCIAL_FETCH_STATUS_NOT_FOUND
            else None
        ),
        fetch_status=fetch_status,
        error_type="LegacyBootstrapMarker",
        error_message=BOOTSTRAP_LEGACY_ERROR_MESSAGE,
        fetched_at=completed_at,
        attempt_count=1,
        raw_response="",
    )


def _string(value: Any) -> str:
    return "" if value is None else str(value)
