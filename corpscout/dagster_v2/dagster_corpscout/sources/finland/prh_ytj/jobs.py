import dagster as dg

from dagster_corpscout.sources.finland.prh_ytj.assets import (
    code_lists,
    company_explorer_cache,
    industry_nace_mappings,
    normalized_tables,
    raw_snapshot,
)

pull_job = dg.define_asset_job(
    name="finland_prhytj_pull",
    selection=[raw_snapshot],
)

pipeline_job = dg.define_asset_job(
    name="finland_prhytj_pipeline",
    selection=[
        raw_snapshot,
        normalized_tables,
        code_lists,
        industry_nace_mappings,
        company_explorer_cache,
    ],
)

transform_latest_job = dg.define_asset_job(
    name="finland_prhytj_transform_latest",
    selection=[
        normalized_tables,
        code_lists,
        industry_nace_mappings,
        company_explorer_cache,
    ],
)
