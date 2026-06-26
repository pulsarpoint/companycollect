import dagster as dg

from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_DUCKDB_PATH,
    NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH,
    norway_brreg_clickhouse_companies,
    norway_brreg_clickhouse_financial_statements,
    norway_brreg_entities_duckdb_asset,
    norway_brreg_financial_fetches_duckdb_asset,
    norway_brreg_financial_statements_duckdb_asset,
    norway_brreg_refresh_job,
    norway_brreg_refresh_schedule,
    norway_brreg_translation_completion_job,
    norway_brreg_translation_queue,
    norway_brreg_translation_workflow_status,
    norway_brreg_translations_applied,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.norway_brreg.sensors import norway_brreg_translation_completion_sensor


defs = dg.Definitions(
    assets=[
        norway_brreg_entities_duckdb_asset,
        norway_brreg_financial_fetches_duckdb_asset,
        norway_brreg_financial_statements_duckdb_asset,
        norway_brreg_translation_queue,
        norway_brreg_translation_workflow_status,
        norway_brreg_translations_applied,
        norway_brreg_clickhouse_companies,
        norway_brreg_clickhouse_financial_statements,
    ],
    jobs=[norway_brreg_translation_completion_job, norway_brreg_refresh_job],
    schedules=[norway_brreg_refresh_schedule],
    sensors=[norway_brreg_translation_completion_sensor],
    resources={
        "norway_brreg_duckdb": duckdb_resource(NORWAY_BRREG_DUCKDB_PATH),
        "norway_brreg_translation_queue_duckdb": duckdb_resource(
            NORWAY_BRREG_TRANSLATION_QUEUE_DUCKDB_PATH
        ),
    },
)
