import dagster as dg

from dagster_v3.defs.norway_brreg.assets import (
    norway_brreg_entities_full_snapshot_job,
    norway_brreg_entities_snapshot_clickhouse,
    norway_brreg_entities_snapshot_normalized_parquets,
    norway_brreg_entities_snapshot_s3,
    norway_brreg_entity_updates_job,
    norway_brreg_entity_updates_clickhouse,
    norway_brreg_entity_updates_normalized_parquets,
    norway_brreg_entity_updates_s3,
    norway_brreg_entity_updates_schedule,
    norway_brreg_financial_fetches_snapshot_parquet,
    norway_brreg_financial_fetches_updates_parquet,
    norway_brreg_financial_snapshot_job,
    norway_brreg_financial_statements_snapshot_clickhouse,
    norway_brreg_financial_statements_snapshot_parquet,
    norway_brreg_financial_statements_snapshot_usd_parquet,
    norway_brreg_financial_statements_updates_clickhouse,
    norway_brreg_financial_statements_updates_parquet,
    norway_brreg_financial_statements_updates_usd_parquet,
    norway_brreg_translation_trigger,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    NorwayBrregEntityParquetStorageResource,
)
from dagster_v3.defs.norway_brreg.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
)
from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource


defs = dg.Definitions(
    assets=[
        norway_brreg_entities_snapshot_s3,
        norway_brreg_entity_updates_s3,
        norway_brreg_entities_snapshot_normalized_parquets,
        norway_brreg_entity_updates_normalized_parquets,
        norway_brreg_financial_fetches_snapshot_parquet,
        norway_brreg_financial_fetches_updates_parquet,
        norway_brreg_financial_statements_snapshot_parquet,
        norway_brreg_financial_statements_updates_parquet,
        norway_brreg_financial_statements_snapshot_usd_parquet,
        norway_brreg_financial_statements_updates_usd_parquet,
        norway_brreg_entities_snapshot_clickhouse,
        norway_brreg_entity_updates_clickhouse,
        norway_brreg_financial_statements_snapshot_clickhouse,
        norway_brreg_financial_statements_updates_clickhouse,
        norway_brreg_translation_trigger,
    ],
    jobs=[
        norway_brreg_entities_full_snapshot_job,
        norway_brreg_financial_snapshot_job,
        norway_brreg_entity_updates_job,
    ],
    schedules=[
        norway_brreg_entity_updates_schedule,
    ],
    resources={
        "norway_brreg_api": NorwayBrregApiResource(),
        "norway_brreg_entity_storage": NorwayBrregEntityParquetStorageResource(),
        "norway_brreg_financial_storage": NorwayBrregFinancialParquetStorageResource(),
    },
)
