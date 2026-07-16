import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.denmark_cvr.filters import DATACVR_RESULT_LIMIT
from dagster_v3.defs.denmark_cvr.partitions import (
    DENMARK_CVR_MONTHLY_PARTITIONS,
    month_date_range,
)
from dagster_v3.defs.denmark_cvr.resources import (
    DATACVR_ENTITY_TYPE,
    DenmarkCvrSearchResource,
    DenmarkCvrValidationError,
)

DENMARK_CVR_BUCKET = "source-denmark-cvr"


@dataclass(frozen=True)
class DenmarkCvrMonthSummary:
    result_key: str
    object_prefix: str
    partition_key: str
    start_date: date
    end_date: date
    generic_advertised_count: int
    filtered_advertised_count: int
    downloaded_entity_count: int
    missing_entity_count: int
    is_complete: bool
    query_count: int
    page_count: int
    stored_file_count: int
    downloaded_size_bytes: int
    stored_size_bytes: int


def result_object_key(
    partition_key: str,
    run_id: str,
    *,
    is_complete: bool,
) -> str:
    _validate_object_scope(partition_key, run_id)
    filename = "companies.json" if is_complete else "companies_incomplete.json"
    return f"denmark_cvr/search/month={partition_key}/run_id={run_id}/{filename}"


def invalid_response_object_key(
    partition_key: str,
    run_id: str,
    filter_id: str,
    page_index: int,
) -> str:
    _validate_object_scope(partition_key, run_id)
    if page_index < 0:
        raise ValueError("DataCVR page index must not be negative")
    if filter_id == "" or any(
        not (character.isalnum() or character in {"-", "_", "."})
        for character in filter_id
    ):
        raise ValueError("DataCVR filter ID contains unsafe object-key characters")
    return (
        f"denmark_cvr/search/month={partition_key}/run_id={run_id}/"
        f"invalid/filter={filter_id}/page={page_index:06d}.invalid.json"
    )


def write_denmark_cvr_month(
    *,
    object_store: ObjectStoreResource,
    search: DenmarkCvrSearchResource,
    partition_key: str,
    run_id: str,
    retrieved_at: datetime,
    log_info: Callable[..., object] | None = None,
    log_warning: Callable[..., object] | None = None,
) -> DenmarkCvrMonthSummary:
    _validate_object_scope(partition_key, run_id)
    if retrieved_at.utcoffset() is None:
        raise ValueError("DataCVR retrieval timestamp must include a timezone")
    start_date, end_date = month_date_range(partition_key)
    object_store.ensure_bucket(DENMARK_CVR_BUCKET)
    prefix = f"denmark_cvr/search/month={partition_key}/run_id={run_id}/"
    if log_info is not None:
        log_info(
            "Starting DataCVR monthly company download: partition=%s "
            "start_date=%s end_date=%s bucket=%s prefix=%s",
            partition_key,
            start_date,
            end_date,
            DENMARK_CVR_BUCKET,
            prefix,
        )

    try:
        download = search.download_month(
            start_date=start_date,
            end_date=end_date,
            log_info=log_info,
        )
    except DenmarkCvrValidationError as exc:
        invalid_key = invalid_response_object_key(
            partition_key,
            run_id,
            exc.filter_id,
            exc.page_index,
        )
        object_store.write_bytes(
            invalid_key,
            exc.raw_body.encode("utf-8"),
            bucket=DENMARK_CVR_BUCKET,
        )
        if log_warning is not None:
            log_warning(
                "DataCVR monthly company response failed validation: partition=%s "
                "filter=%s page=%s invalid_object_key=%s",
                partition_key,
                exc.filter_id,
                exc.page_index,
                invalid_key,
            )
        raise

    missing_entity_count = max(
        download.generic_advertised_count - download.downloaded_entity_count,
        0,
    )
    body = json.dumps(
        {
            "schema_version": 1,
            "source": "denmark_cvr",
            "source_url": search.search_base_url,
            "entity_type": DATACVR_ENTITY_TYPE,
            "partition_key": partition_key,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "retrieved_at": retrieved_at.isoformat(),
            "run_id": run_id,
            "result_limit": DATACVR_RESULT_LIMIT,
            "is_complete": download.is_complete,
            "generic_advertised_count": download.generic_advertised_count,
            "filtered_advertised_count": download.filtered_advertised_count,
            "downloaded_entity_count": download.downloaded_entity_count,
            "missing_entity_count": missing_entity_count,
            "filtered_count_difference": (
                download.filtered_advertised_count - download.generic_advertised_count
            ),
            "downloaded_count_difference": (
                download.downloaded_entity_count - download.generic_advertised_count
            ),
            "query_count": len(download.query_downloads),
            "page_count": download.page_count,
            "downloaded_size_bytes": download.downloaded_size_bytes,
            "queries": [
                {
                    "filter_id": query.query_filter.filter_id,
                    "region": query.query_filter.region,
                    "municipality": query.query_filter.municipality,
                    "advertised_count": query.advertised_count,
                    "downloaded_count": query.downloaded_entity_count,
                    "page_count": query.page_count,
                    "is_complete": query.is_complete,
                }
                for query in download.query_downloads
            ],
            "enheder": list(download.entities),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    key = result_object_key(
        partition_key,
        run_id,
        is_complete=download.is_complete,
    )
    object_store.write_json(key, body, bucket=DENMARK_CVR_BUCKET)
    stored_size_bytes = len(body.encode("utf-8"))
    summary = DenmarkCvrMonthSummary(
        result_key=key,
        object_prefix=prefix,
        partition_key=partition_key,
        start_date=start_date,
        end_date=end_date,
        generic_advertised_count=download.generic_advertised_count,
        filtered_advertised_count=download.filtered_advertised_count,
        downloaded_entity_count=download.downloaded_entity_count,
        missing_entity_count=missing_entity_count,
        is_complete=download.is_complete,
        query_count=len(download.query_downloads),
        page_count=download.page_count,
        stored_file_count=1,
        downloaded_size_bytes=download.downloaded_size_bytes,
        stored_size_bytes=stored_size_bytes,
    )
    if summary.is_complete:
        if log_info is not None:
            log_info(
                "DataCVR monthly company download complete: partition=%s "
                "queries=%s pages=%s advertised=%s downloaded=%s "
                "downloaded_bytes=%s stored_bytes=%s result_key=%s",
                summary.partition_key,
                summary.query_count,
                summary.page_count,
                summary.generic_advertised_count,
                summary.downloaded_entity_count,
                summary.downloaded_size_bytes,
                summary.stored_size_bytes,
                summary.result_key,
            )
    elif log_warning is not None:
        log_warning(
            "DataCVR monthly company download incomplete: partition=%s "
            "generic_advertised=%s filtered_advertised=%s downloaded=%s "
            "missing=%s queries=%s pages=%s result_key=%s",
            summary.partition_key,
            summary.generic_advertised_count,
            summary.filtered_advertised_count,
            summary.downloaded_entity_count,
            summary.missing_entity_count,
            summary.query_count,
            summary.page_count,
            summary.result_key,
        )
    return summary


@dg.asset(
    group_name="denmark_cvr",
    kinds={"python", "browser", "json", "s3"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "layer": "raw",
    },
    partitions_def=DENMARK_CVR_MONTHLY_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="denmark_cvr_search",
    description=(
        "Captures one merged DataCVR company JSON object per completed month. "
        "Months above the 3,000-result ceiling use fixed region and municipality "
        "filters; count mismatches are stored and materialized as incomplete."
    ),
)
def denmark_cvr_search_results_s3(
    context: dg.AssetExecutionContext,
    denmark_cvr_search: DenmarkCvrSearchResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    summary = write_denmark_cvr_month(
        object_store=object_store,
        search=denmark_cvr_search,
        partition_key=context.partition_key,
        run_id=context.run.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
        log_warning=context.log.warning,
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": DENMARK_CVR_BUCKET,
            "s3_prefix": summary.object_prefix,
            "result_key": summary.result_key,
            "source_url": denmark_cvr_search.search_base_url,
            "partition_key": summary.partition_key,
            "start_date": summary.start_date.isoformat(),
            "end_date": summary.end_date.isoformat(),
            "is_complete": summary.is_complete,
            "generic_advertised_count": summary.generic_advertised_count,
            "filtered_advertised_count": summary.filtered_advertised_count,
            "downloaded_entity_count": summary.downloaded_entity_count,
            "missing_entity_count": summary.missing_entity_count,
            "query_count": summary.query_count,
            "page_count": summary.page_count,
            "stored_file_count": summary.stored_file_count,
            "downloaded_size_bytes": summary.downloaded_size_bytes,
            "stored_size_bytes": summary.stored_size_bytes,
        }
    )


defs = dg.Definitions(
    assets=[denmark_cvr_search_results_s3],
    resources={"denmark_cvr_search": DenmarkCvrSearchResource()},
)


def _validate_object_scope(partition_key: str, run_id: str) -> None:
    month_date_range(partition_key)
    if run_id == "" or any(
        not (character.isalnum() or character in {"-", "_", "."})
        for character in run_id
    ):
        raise ValueError("DataCVR run ID contains unsafe object-key characters")
