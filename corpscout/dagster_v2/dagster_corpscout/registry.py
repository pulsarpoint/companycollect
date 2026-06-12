"""Aggregates source bundles into the lists consumed by definitions.py."""

source_modules: tuple[str, ...] = ()
source_bundles: list = []

all_assets = [asset for bundle in source_bundles for asset in bundle.assets]
all_asset_checks = [check for bundle in source_bundles for check in bundle.asset_checks]
all_jobs = [job for bundle in source_bundles for job in bundle.jobs]
all_schedules = [schedule for bundle in source_bundles for schedule in bundle.schedules]
