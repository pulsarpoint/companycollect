from __future__ import annotations

from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.norway_brreg_financial.constants import (
    GROUP_NAME,
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    financial_bootstrap_response_partition_prefix,
)
from dagster_v3.defs.norway_brreg_financial.response_pipeline import (
    materialize_response_json_partition,
    verified_response_index_frame,
)

NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT = 64
NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        f"bucket_{bucket_index:02d}"
        for bucket_index in range(NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT)
    ]
)
NORWAY_BRREG_FINANCIAL_BOOTSTRAP_POOL = "norway_brreg_financial_api"

# This expression is the stable partition contract for the raw response archive.
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
    name="norway_brreg_financial_bootstrap_responses_json",
    group_name=GROUP_NAME,
    kinds={"python", "s3", "json", "clickhouse", "brreg"},
    partitions_def=NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=NORWAY_BRREG_FINANCIAL_BOOTSTRAP_POOL,
    description=(
        "Downloads only missing Norway BRREG financial responses as immutable, "
        "partition-scoped JSON objects and checkpoints every batch."
    ),
)
def norway_brreg_financial_bootstrap_responses_json(
    context,
    clickhouse: ClickhouseResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    bucket_key = context.partition_key
    bucket_index = _bucket_index_from_partition_key(bucket_key)
    with clickhouse.get_connection() as client:
        candidates = _bootstrap_candidates(client, bucket_index)

    metadata = materialize_response_json_partition(
        candidates=candidates,
        partition_prefix=financial_bootstrap_response_partition_prefix(bucket_key),
        source_run_id=context.op_execution_context.run_id,
        storage=norway_brreg_financial_storage,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket_key": bucket_key,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            **metadata,
        }
    )


@dg.asset(
    name="norway_brreg_financial_bootstrap_responses_parquet",
    deps=[dg.AssetKey("norway_brreg_financial_bootstrap_responses_json")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "parquet", "brreg"},
    partitions_def=NORWAY_BRREG_FINANCIAL_BOOTSTRAP_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Builds a metadata-only Parquet index for one verified bootstrap JSON "
        "response partition."
    ),
)
def norway_brreg_financial_bootstrap_responses_parquet(
    context,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    bucket_key = context.partition_key
    partition_prefix = financial_bootstrap_response_partition_prefix(bucket_key)
    frame, metadata = verified_response_index_frame(
        partition_prefix=partition_prefix,
        storage=norway_brreg_financial_storage,
    )
    output_key = norway_brreg_financial_storage.write_bootstrap_response_index(
        bucket_key,
        frame,
    )
    return dg.MaterializeResult(
        metadata={
            "bucket_key": bucket_key,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "s3_key": output_key,
            **metadata,
        }
    )


def _bootstrap_candidates(client: Any, bucket_index: int) -> list[dict[str, Any]]:
    if not 0 <= bucket_index < NORWAY_BRREG_FINANCIAL_BOOTSTRAP_BUCKET_COUNT:
        raise ValueError(f"Norway bootstrap bucket index out of range: {bucket_index}")
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


def _bucket_index_from_partition_key(partition_key: str) -> int:
    prefix, _, suffix = partition_key.partition("_")
    if prefix != "bucket" or not suffix.isdigit():
        raise ValueError(f"Invalid Norway bootstrap partition key: {partition_key!r}")
    return int(suffix)


def _string(value: Any) -> str:
    return "" if value is None else str(value)
