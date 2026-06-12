from dagster_corpscout.sources.finland.prh_ytj import source_bundle as finland_prh_ytj

source_modules = ("dagster_corpscout.sources.finland.prh_ytj",)
source_bundles = [finland_prh_ytj]

all_assets = [asset for bundle in source_bundles for asset in bundle.assets]
all_asset_checks = [check for bundle in source_bundles for check in bundle.asset_checks]
all_jobs = [job for bundle in source_bundles for job in bundle.jobs]
all_schedules = [schedule for bundle in source_bundles for schedule in bundle.schedules]
