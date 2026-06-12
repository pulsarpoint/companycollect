from dataclasses import dataclass
from typing import Any

from dagster_corpscout.sources.finland.prh_ytj.assets import (
    code_lists,
    company_explorer_cache,
    industry_nace_mappings,
    normalized_tables,
    raw_snapshot,
    source_system,
)
from dagster_corpscout.sources.finland.prh_ytj.checks import (
    company_explorer_cache_matches_view,
    industry_nace_mappings_rows_present,
)
from dagster_corpscout.sources.finland.prh_ytj.jobs import (
    pipeline_job,
    pull_job,
    transform_latest_job,
)
from dagster_corpscout.sources.finland.prh_ytj.schedules import pull_schedule


@dataclass(frozen=True)
class SourceBundle:
    assets: list[Any]
    asset_checks: list[Any]
    jobs: list[Any]
    schedules: list[Any]


source_bundle = SourceBundle(
    assets=[
        source_system,
        raw_snapshot,
        normalized_tables,
        code_lists,
        industry_nace_mappings,
        company_explorer_cache,
    ],
    asset_checks=[
        industry_nace_mappings_rows_present,
        company_explorer_cache_matches_view,
    ],
    jobs=[pull_job, pipeline_job, transform_latest_job],
    schedules=[pull_schedule],
)

__all__ = ["SourceBundle", "source_bundle"]
