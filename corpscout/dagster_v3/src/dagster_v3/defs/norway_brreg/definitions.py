import dagster as dg

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.norway_brreg.assets import (
    NORWAY_BRREG_DUCKDB_PATH,
    norway_brreg_entities_duckdb_asset,
    norway_brreg_financial_fetches_duckdb_asset,
    norway_brreg_financial_statements_duckdb_asset,
    norway_brreg_refresh_job,
    norway_brreg_refresh_schedule,
    norway_brreg_translation_trigger,
)


defs = dg.Definitions(
    assets=[
        norway_brreg_entities_duckdb_asset,
        norway_brreg_financial_fetches_duckdb_asset,
        norway_brreg_financial_statements_duckdb_asset,
        norway_brreg_translation_trigger,
    ],
    jobs=[norway_brreg_refresh_job],
    schedules=[norway_brreg_refresh_schedule],
    resources={
        "norway_brreg_duckdb": duckdb_resource(NORWAY_BRREG_DUCKDB_PATH),
    },
)
