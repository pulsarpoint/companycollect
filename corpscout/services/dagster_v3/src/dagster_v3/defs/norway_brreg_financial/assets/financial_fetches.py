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
    NorwayBrregFinancialParquetStorageResource,
    financial_update_response_partition_prefix,
)
from dagster_v3.defs.norway_brreg_financial.response_pipeline import (
    materialize_response_json_partition,
    verified_response_index_frame,
)

FINANCIAL_FETCHED_AT_DTYPE = financial_fetches.FINANCIAL_FETCHED_AT_DTYPE
FINANCIAL_FETCHES_PARQUET_SCHEMA = financial_fetches.financial_fetches_parquet_schema()
NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-06-01",
    end_offset=1,
)

UPDATE_CANDIDATES_SQL = """
SELECT
    toString(company.org_number) AS org_number,
    company.name,
    company.primary_website_url,
    company.last_submitted_accounts_year
FROM no_companies AS company
LEFT JOIN
(
    SELECT
        org_number,
        max(fiscal_year) AS latest_financial_year
    FROM no_financial_statements
    GROUP BY org_number
) AS financials USING (org_number)
WHERE company.is_active
  AND notEmpty(company.last_submitted_accounts_year)
  AND toInt32OrZero(company.last_submitted_accounts_year)
      > coalesce(financials.latest_financial_year, 0)
ORDER BY company.org_number
"""


@dg.asset(
    name="norway_brreg_financial_responses_updates_json",
    group_name=GROUP_NAME,
    kinds={"python", "s3", "json", "clickhouse", "brreg"},
    partitions_def=NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="norway_brreg_financial_api",
    description=(
        "Reads active companies with an uncovered latest accounts year from "
        "canonical ClickHouse tables, then downloads only missing Norway BRREG "
        "responses as immutable JSON and checkpoints every batch."
    ),
)
def norway_brreg_financial_responses_updates_json(
    context,
    clickhouse: ClickhouseResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    with clickhouse.get_connection() as client:
        candidates = _update_candidates(client)
    metadata = materialize_response_json_partition(
        candidates=candidates,
        partition_prefix=financial_update_response_partition_prefix(partition_date),
        source_run_id=context.op_execution_context.run_id,
        storage=norway_brreg_financial_storage,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            **metadata,
        }
    )


def _update_candidates(client: Any) -> list[dict[str, str]]:
    rows = client.execute(UPDATE_CANDIDATES_SQL)
    return [
        {
            "org_number": _string(org_number),
            "legal_name": _string(name),
            "website": _string(website),
            "last_submitted_accounts_year": _string(accounts_year),
        }
        for org_number, name, website, accounts_year in rows
    ]


def _string(value: Any) -> str:
    return "" if value is None else str(value)


@dg.asset(
    name="norway_brreg_financial_responses_updates_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_responses_updates_json")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
    partitions_def=NORWAY_BRREG_FINANCIAL_UPDATE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Builds a metadata-only Parquet index for one verified update JSON "
        "response partition."
    ),
)
def norway_brreg_financial_responses_updates_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    partition_date = context.partition_key
    partition_prefix = financial_update_response_partition_prefix(partition_date)
    frame, metadata = verified_response_index_frame(
        partition_prefix=partition_prefix,
        storage=norway_brreg_financial_storage,
    )
    output_key = norway_brreg_financial_storage.write_update_response_index(
        partition_date,
        frame,
    )
    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "s3_key": output_key,
            **metadata,
        }
    )
