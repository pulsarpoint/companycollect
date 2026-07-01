from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.constants import (
    GROUP_NAME,
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
    NorwayBrregEntityParquetStorageResource,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)

FINANCIAL_FETCHED_AT_DTYPE = financial_fetches.FINANCIAL_FETCHED_AT_DTYPE
_polars_type_for_fetch_column = financial_fetches.polars_type_for_financial_fetch_column


FINANCIAL_FETCH_PARQUET_KINDS = {"python", "s3", "parquet", "brreg"}
FINANCIAL_FETCH_CANDIDATE_SCHEMA = {
    "org_number": pl.Utf8,
    "legal_name": pl.Utf8,
    "website": pl.Utf8,
    "last_submitted_accounts_year": pl.Utf8,
}
FINANCIAL_FETCHES_PARQUET_SCHEMA = financial_fetches.financial_fetches_parquet_schema()
NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-06-01",
    end_offset=1,
)


@dg.asset(
    name="norway_brreg_financial_fetches_snapshot_parquet",
    group_name=GROUP_NAME,
    kinds=FINANCIAL_FETCH_PARQUET_KINDS,
    description=(
        "Inventories externally bootstrapped Norway Brreg historical raw financial fetch parquet."
    ),
)
def norway_brreg_financial_fetches_snapshot_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    context.log.info("Inventorying Norway Brreg historical raw financial fetches")
    fetch_frame = norway_brreg_financial_storage.read_historical_raw_fetches()
    status_counts = _status_counts(fetch_frame.to_dicts())
    context.log.info(
        "Completed Norway Brreg historical raw financial fetch inventory: "
        "row_count=%d status_counts=%s",
        fetch_frame.height,
        status_counts,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": fetch_frame.height,
            "status_counts": status_counts,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
        }
    )


@dg.asset(
    name="norway_brreg_financial_fetches_updates_parquet",
    deps=[dg.AssetKey("norway_brreg_entity_updates_no_companies_parquet")],
    group_name=GROUP_NAME,
    kinds=FINANCIAL_FETCH_PARQUET_KINDS,
    partitions_def=NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
    description=(
        "Fetches Norway Brreg annual-account outcomes for one normalized update "
        "no_companies parquet partition and stores raw plus aggregate parquet diagnostics."
    ),
)
def norway_brreg_financial_fetches_updates_parquet(
    context,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    context.log.info(
        "Starting Norway Brreg update financial parquet fetches: partition_date=%s",
        partition_date,
    )
    no_companies = norway_brreg_entity_storage.read_normalized_update_table(
        partition_date,
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
    )
    candidates = financial_fetches.financial_fetch_candidates_from_no_companies(
        no_companies
    )
    candidate_frame = _candidate_frame(candidates)
    candidate_key = norway_brreg_financial_storage.write_update_financial_candidates(
        partition_date,
        candidate_frame,
    )
    context.log.info(
        "Prepared Norway Brreg update financial candidates: partition_date=%s "
        "candidate_count=%d candidate_s3_key=%s",
        partition_date,
        len(candidates),
        candidate_key,
    )

    rows: list[dict[str, Any]] = []
    fetched_count = 0
    for candidate in candidates:
        org_number = _candidate_org_number(candidate)
        accounts_year = _candidate_accounts_year(candidate)
        fetched_rows = financial_fetches.fetch_financial_rows_for_orgs(
            orgs=[candidate],
            source_run_id=context.op_execution_context.run_id,
            log=context.log.info,
        )
        raw_frame = _financial_fetches_frame(fetched_rows)
        norway_brreg_financial_storage.write_raw_fetch(
            org_number,
            accounts_year,
            raw_frame,
            overwrite=True,
        )
        rows.extend(fetched_rows)
        fetched_count += 1

    aggregate_frame = _financial_fetches_frame(rows)
    output_key = norway_brreg_financial_storage.write_update_fetches(
        partition_date,
        aggregate_frame,
    )
    status_counts = _status_counts(rows)
    context.log.info(
        "Completed Norway Brreg update financial parquet fetches: partition_date=%s "
        "candidate_count=%d reused_count=%d fetched_count=%d row_count=%d "
        "status_counts=%s s3_key=%s candidate_s3_key=%s",
        partition_date,
        len(candidates),
        0,
        fetched_count,
        aggregate_frame.height,
        status_counts,
        output_key,
        candidate_key,
    )
    _raise_for_retryable_fetch_statuses(rows)
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "candidate_count": len(candidates),
            "reused_count": 0,
            "fetched_count": fetched_count,
            "row_count": aggregate_frame.height,
            "status_counts": status_counts,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "s3_key": output_key,
            "candidate_s3_key": candidate_key,
        }
    )


def _candidate_frame(candidates: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(candidates, schema=FINANCIAL_FETCH_CANDIDATE_SCHEMA)


def _financial_fetches_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=FINANCIAL_FETCHES_PARQUET_SCHEMA)

    frame = pl.DataFrame(rows)
    return frame.select(
        [
            _fetch_column_expression(frame, column_name, data_type)
            for column_name, data_type in FINANCIAL_FETCHES_PARQUET_SCHEMA.items()
        ]
    )


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        fetch_status = _string(row.get("fetch_status"))
        counts[fetch_status] = counts.get(fetch_status, 0) + 1
    return counts


def _raise_for_retryable_fetch_statuses(rows: list[dict[str, Any]]) -> None:
    retryable_statuses = sorted(
        {
            _string(row.get("fetch_status"))
            for row in rows
            if financial_fetches.financial_fetch_status_requires_failure(
                _string(row.get("fetch_status"))
            )
        }
    )
    if retryable_statuses:
        raise RuntimeError(
            "Norway Brreg financial fetches contain retryable statuses after "
            f"persisting parquet diagnostics: {retryable_statuses}"
        )


def _candidate_org_number(candidate: dict[str, Any]) -> str:
    return _string(candidate.get("org_number"))


def _candidate_accounts_year(candidate: dict[str, Any]) -> str:
    return _string(candidate.get("last_submitted_accounts_year"))


def _string(value: Any) -> str:
    return "" if value is None else str(value)


def _fetch_column_expression(
    frame: pl.DataFrame,
    column_name: str,
    data_type: pl.DataType,
) -> pl.Expr:
    if column_name not in frame.columns:
        return pl.lit(None, dtype=data_type).alias(column_name)
    if data_type == FINANCIAL_FETCHED_AT_DTYPE and frame.schema[column_name] == pl.Utf8:
        return (
            pl.col(column_name)
            .str.to_datetime(time_unit="ms", time_zone="UTC", strict=False)
            .alias(column_name)
        )
    return pl.col(column_name).cast(data_type, strict=False).alias(column_name)
