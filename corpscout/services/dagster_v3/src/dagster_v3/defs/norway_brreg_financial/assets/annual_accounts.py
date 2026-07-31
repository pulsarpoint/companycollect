from datetime import datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource
from dagster_v3.defs.norway_brreg_financial.annual_account_pipeline import (
    download_annual_account_pdfs,
    materialize_annual_account_documents,
    remove_processed_annual_account_pdfs,
)
from dagster_v3.defs.norway_brreg_financial.constants import (
    GROUP_NAME,
    NORWAY_BRREG_FINANCIAL_BUCKET,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)

NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNK_COUNT = 64
NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNKS = dg.StaticPartitionsDefinition(
    [
        f"bucket_{chunk_index:02d}"
        for chunk_index in range(NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNK_COUNT)
    ]
)
NORWAY_BRREG_ANNUAL_ACCOUNT_YEARS = dg.TimeWindowPartitionsDefinition(
    start=datetime(2011, 1, 1),
    fmt="%Y",
    cron_schedule="0 0 1 1 *",
    timezone="UTC",
)
NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS = dg.MultiPartitionsDefinition(
    {
        "year": NORWAY_BRREG_ANNUAL_ACCOUNT_YEARS,
        "chunk": NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNKS,
    }
)


class NorwayBrregAnnualAccountDocumentConfig(dg.Config):
    max_documents_per_run: int = 25


ANNUAL_ACCOUNT_CANDIDATES_SQL = """
SELECT
    toString(org_number) AS org_number,
    name
FROM no_companies
WHERE notEmpty(org_number)
  AND toInt32OrZero(last_submitted_accounts_year) >= %(filing_year)s
  AND coalesce(
        toYear(incorporation_date),
        toYear(registration_date),
        toUInt16(0)
      ) <= %(filing_year)s
  AND cityHash64(toString(org_number)) %% %(chunk_count)s = %(chunk_index)s
ORDER BY org_number
"""


@dg.asset(
    name="norway_brreg_annual_account_pdfs",
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "brreg", "pdf", "s3"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="norway_brreg_annual_account_api",
    description=(
        "For one accounting year and stable company bucket, downloads only "
        "unprocessed BRREG annual-account PDFs to S3 and writes a Parquet catalog. "
        "Exhausted document-specific backend failures are recorded as durable S3 "
        "markers and skipped without failing the partition. This asset performs no "
        "PDF parsing or OCR."
    ),
)
def norway_brreg_annual_account_pdfs(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    norway_brreg_api: NorwayBrregApiResource,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    filing_year, chunk_key = _partition_values(context.partition_key)
    chunk_index = _chunk_index(chunk_key)
    with clickhouse.get_connection() as client:
        candidates = _annual_account_candidates(
            client,
            filing_year=filing_year,
            chunk_index=chunk_index,
        )
    metadata = download_annual_account_pdfs(
        candidates=candidates,
        filing_year=filing_year,
        chunk_key=chunk_key,
        source_run_id=context.op_execution_context.run_id,
        api=norway_brreg_api,
        storage=norway_brreg_financial_storage,
        log=context.log.info,
        log_warning=context.log.warning,
    )
    return dg.MaterializeResult(
        metadata={
            "filing_year": filing_year,
            "chunk_key": chunk_key,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            **metadata,
        }
    )


@dg.asset(
    name="norway_brreg_annual_account_documents_json",
    deps=[dg.AssetKey("norway_brreg_annual_account_pdfs")],
    group_name=GROUP_NAME,
    kinds={"python", "pdf", "ocr", "json", "s3"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="norway_brreg_annual_account_ocr",
    description=(
        "Reads staged BRREG annual-account PDFs from S3, extracts native text or "
        "PyMuPDF/Tesseract OCR, and writes one immutable JSON object per document. "
        "Each materialization processes a bounded, resumable document batch."
    ),
)
def norway_brreg_annual_account_documents_json(
    context: AssetExecutionContext,
    config: NorwayBrregAnnualAccountDocumentConfig,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    _require_upstream_partition_materializations(
        context,
        upstream_asset_keys=(dg.AssetKey("norway_brreg_annual_account_pdfs"),),
    )
    filing_year, chunk_key = _partition_values(context.partition_key)
    metadata = materialize_annual_account_documents(
        filing_year=filing_year,
        chunk_key=chunk_key,
        source_run_id=context.op_execution_context.run_id,
        max_documents=config.max_documents_per_run,
        storage=norway_brreg_financial_storage,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "filing_year": filing_year,
            "chunk_key": chunk_key,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            "partition_complete": metadata["remaining_count"] == 0,
            **metadata,
        }
    )


@dg.asset(
    name="norway_brreg_annual_account_pdf_cleanup",
    deps=[dg.AssetKey("norway_brreg_annual_account_documents_json")],
    group_name=GROUP_NAME,
    kinds={"python", "pdf", "s3"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    description=(
        "Verifies every staged PDF has matching processed JSON, then removes only "
        "those verified PDFs from S3."
    ),
)
def norway_brreg_annual_account_pdf_cleanup(
    context: AssetExecutionContext,
    norway_brreg_financial_storage: NorwayBrregFinancialParquetStorageResource,
) -> dg.MaterializeResult:
    _require_upstream_partition_materializations(
        context,
        upstream_asset_keys=(
            dg.AssetKey("norway_brreg_annual_account_documents_json"),
        ),
    )
    filing_year, chunk_key = _partition_values(context.partition_key)
    metadata = remove_processed_annual_account_pdfs(
        filing_year=filing_year,
        chunk_key=chunk_key,
        storage=norway_brreg_financial_storage,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "filing_year": filing_year,
            "chunk_key": chunk_key,
            "s3_bucket": NORWAY_BRREG_FINANCIAL_BUCKET,
            **metadata,
        }
    )


def _partition_values(partition_key: str) -> tuple[int, str]:
    if not isinstance(partition_key, dg.MultiPartitionKey):
        raise ValueError(
            "Norway annual-account asset requires a year × chunk partition key"
        )
    dimensions = partition_key.keys_by_dimension
    return int(dimensions["year"]), dimensions["chunk"]


def _chunk_index(chunk_key: str) -> int:
    prefix, separator, suffix = chunk_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid Norway annual-account chunk key: {chunk_key!r}")
    chunk_index = int(suffix)
    if not 0 <= chunk_index < NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNK_COUNT:
        raise ValueError(f"Norway annual-account chunk is out of range: {chunk_key}")
    return chunk_index


def _annual_account_candidates(
    client: Any,
    *,
    filing_year: int,
    chunk_index: int,
) -> list[dict[str, str]]:
    rows = client.execute(
        ANNUAL_ACCOUNT_CANDIDATES_SQL,
        {
            "filing_year": filing_year,
            "chunk_count": NORWAY_BRREG_ANNUAL_ACCOUNT_CHUNK_COUNT,
            "chunk_index": chunk_index,
        },
    )
    return [
        {
            "org_number": str(org_number).strip(),
            "legal_name": "" if name is None else str(name).strip(),
        }
        for org_number, name in rows
    ]


def _require_upstream_partition_materializations(
    context: AssetExecutionContext,
    *,
    upstream_asset_keys: tuple[dg.AssetKey, ...],
) -> None:
    missing_asset_keys = [
        asset_key
        for asset_key in upstream_asset_keys
        if not context.instance.fetch_materializations(
            dg.AssetRecordsFilter(
                asset_key=asset_key,
                asset_partitions=[context.partition_key],
            ),
            limit=1,
        ).records
    ]
    if not missing_asset_keys:
        return

    missing_assets = ", ".join(
        asset_key.to_user_string() for asset_key in missing_asset_keys
    )
    raise dg.Failure(
        description=(
            f"Partition {context.partition_key} has no materialization for upstream "
            f"asset(s): {missing_assets}; refusing downstream materialization"
        ),
        metadata={
            "partition": context.partition_key,
            "missing_upstream_assets": [
                asset_key.to_user_string() for asset_key in missing_asset_keys
            ],
        },
        allow_retries=False,
    )
