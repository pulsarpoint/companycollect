import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland_prhytj.assets import (
    code_lists,
    company_explorer_cache,
    industry_nace_mappings,
    prh_ytj_open_data_api,
    normalized_tables,
    raw_snapshot,
)
from dagster_corpscout.sources.finland_prhytj.checks import (
    company_explorer_cache_matches_view,
    industry_nace_mappings_rows_present,
)
from dagster_corpscout.sources.finland_prhytj.schedules import (
    pipeline_job,
    pull_job,
    pull_schedule,
    transform_latest_job,
)

defs = dg.Definitions(
    assets=[
        prh_ytj_open_data_api,
        raw_snapshot,
        normalized_tables,
        code_lists,
        industry_nace_mappings,
        company_explorer_cache,
    ],
    asset_checks=[industry_nace_mappings_rows_present, company_explorer_cache_matches_view],
    jobs=[pull_job, pipeline_job, transform_latest_job],
    schedules=[pull_schedule],
    resources={
        "rustfs": RustFSResource(
            endpoint_url=dg.EnvVar("CORPSCOUT_S3_ENDPOINT"),
            access_key=dg.EnvVar("CORPSCOUT_S3_ACCESS_KEY"),
            secret_key=dg.EnvVar("CORPSCOUT_S3_SECRET_KEY"),
        ),
        "clickhouse": ClickHouseResource(
            host=dg.EnvVar("CLICKHOUSE_HOST"),
            port=dg.EnvVar("CLICKHOUSE_PORT"),
            username=dg.EnvVar("CLICKHOUSE_USER"),
            password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
            database=dg.EnvVar("CLICKHOUSE_DATABASE"),
            secure=dg.EnvVar("CLICKHOUSE_SECURE"),
        ),
    },
)
