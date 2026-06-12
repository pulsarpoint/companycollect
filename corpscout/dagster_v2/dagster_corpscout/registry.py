"""Aggregates source bundles into the lists consumed by definitions.py."""

from dagster_corpscout.source_bundle import SourceBundle
from dagster_corpscout.sources.finland.prh_xbrl import source_bundle as finland_prh_xbrl
from dagster_corpscout.sources.finland.prh_ytj import source_bundle as finland_prh_ytj

source_modules: tuple[str, ...] = (
    "dagster_corpscout.sources.finland.prh_ytj",
    "dagster_corpscout.sources.finland.prh_xbrl",
)
source_bundles: list[SourceBundle] = [finland_prh_ytj, finland_prh_xbrl]

all_assets = [asset for bundle in source_bundles for asset in bundle.assets]
all_asset_checks = [check for bundle in source_bundles for check in bundle.asset_checks]
all_jobs = [job for bundle in source_bundles for job in bundle.jobs]
all_schedules = [schedule for bundle in source_bundles for schedule in bundle.schedules]
