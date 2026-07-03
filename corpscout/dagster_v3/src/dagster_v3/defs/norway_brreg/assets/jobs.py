"""Norway Brreg asset jobs and schedules.

The manual full-snapshot job drives entities → parquet → ClickHouse publish →
translation loader (``norway_brreg_translation_load`` scans for untranslated
text and enqueues it to the Go translator service). The daily updates job owns
the recurring schedule.
"""

import dagster as dg

# Manual full entity snapshot. The translation loader runs after the
# parquet-backed snapshot publish lands corpscout.no_companies in ClickHouse.
norway_brreg_entities_full_snapshot_job = dg.define_asset_job(
    "norway_brreg_entities_full_snapshot_job",
    selection=dg.AssetSelection.assets(
        "norway_brreg_translation_load"
    ).upstream().required_multi_asset_neighbors(),
)

norway_brreg_entity_updates_job = dg.define_asset_job(
    "norway_brreg_entity_updates_job",
    selection=dg.AssetSelection.assets(
        "norway_brreg_entity_updates_clickhouse"
    ).upstream().required_multi_asset_neighbors(),
)
norway_brreg_entity_updates_schedule = dg.build_schedule_from_partitioned_job(
    norway_brreg_entity_updates_job,
    name="norway_brreg_entity_updates_schedule",
)
