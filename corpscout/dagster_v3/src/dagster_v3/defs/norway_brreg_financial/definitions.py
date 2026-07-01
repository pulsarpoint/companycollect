import dagster as dg

from dagster_v3.defs.norway_brreg_financial.assets import (
    norway_brreg_financial_fetches_snapshot_parquet,
    norway_brreg_financial_fetches_updates_parquet,
    norway_brreg_financial_snapshot_job,
    norway_brreg_financial_updates_job,
    norway_brreg_financial_statements_snapshot_clickhouse,
    norway_brreg_financial_statements_snapshot_parquet,
    norway_brreg_financial_statements_snapshot_usd_parquet,
    norway_brreg_financial_statements_updates_clickhouse,
    norway_brreg_financial_statements_updates_parquet,
    norway_brreg_financial_statements_updates_usd_parquet,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)


defs = dg.Definitions(
    assets=[
        norway_brreg_financial_fetches_snapshot_parquet,
        norway_brreg_financial_fetches_updates_parquet,
        norway_brreg_financial_statements_snapshot_parquet,
        norway_brreg_financial_statements_updates_parquet,
        norway_brreg_financial_statements_snapshot_usd_parquet,
        norway_brreg_financial_statements_updates_usd_parquet,
        norway_brreg_financial_statements_snapshot_clickhouse,
        norway_brreg_financial_statements_updates_clickhouse,
    ],
    jobs=[
        norway_brreg_financial_snapshot_job,
        norway_brreg_financial_updates_job,
    ],
    resources={
        "norway_brreg_financial_storage": NorwayBrregFinancialParquetStorageResource(),
    },
)
