from datetime import datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource
from dagster_v3.defs.norway_brreg_financial.annual_account_pipeline import (
    materialize_annual_account_documents,
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
    name="norway_brreg_annual_account_documents_json",
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "brreg", "pdf", "ocr", "json", "s3"},
    partitions_def=NORWAY_BRREG_ANNUAL_ACCOUNT_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="norway_brreg_annual_account_ocr",
    description=(
        "For one accounting year and stable company bucket, downloads each BRREG "
        "annual-account PDF, extracts native text or PyMuPDF/Tesseract OCR into one "
        "immutable JSON object per document, and retains no local PDF."
    ),
)
def norway_brreg_annual_account_documents_json(
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
    metadata = materialize_annual_account_documents(
        candidates=candidates,
        filing_year=filing_year,
        chunk_key=chunk_key,
        source_run_id=context.op_execution_context.run_id,
        api=norway_brreg_api,
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
