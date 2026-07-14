"""Norway Brreg asset jobs and schedules.

The manual full-snapshot job drives entities → parquet → ClickHouse publish →
translation loader (``norway_brreg_translation_load`` scans for untranslated
text and enqueues it to the Go translator service) and the canonical-contacts
derivation (``norway_brreg_clickhouse_canonical_contacts`` reshapes
corpscout.no_websites into the canonical contacts/domains pair). The daily
updates job owns the recurring schedule.
"""

import dagster as dg

# Manual full entity snapshot. The translation loader and canonical-contacts
# derivation run after the parquet-backed snapshot publish lands
# corpscout.no_companies/no_websites in ClickHouse. The selection is an
# explicit union (snapshot chain + loader + derivation) rather than
# loader.upstream(): both downstream assets are also downstream of the daily
# updates asset, and upstream() from either would drag the partitioned
# updates chain into this manual job.
norway_brreg_entities_full_snapshot_job = dg.define_asset_job(
    "norway_brreg_entities_full_snapshot_job",
    selection=(
        dg.AssetSelection.assets("norway_brreg_entities_snapshot_clickhouse").upstream()
        | dg.AssetSelection.assets("norway_brreg_translation_load")
        | dg.AssetSelection.assets("norway_brreg_clickhouse_canonical_contacts")
    ).required_multi_asset_neighbors(),
)

# Daily updates land rows in ClickHouse too, so the translation loader and
# canonical-contacts derivation run at the end of this job as well
# (unpartitioned assets appended to the partitioned chain).
norway_brreg_entity_updates_job = dg.define_asset_job(
    "norway_brreg_entity_updates_job",
    selection=(
        dg.AssetSelection.assets("norway_brreg_entity_updates_clickhouse").upstream()
        | dg.AssetSelection.assets("norway_brreg_translation_load")
        | dg.AssetSelection.assets("norway_brreg_clickhouse_canonical_contacts")
    ).required_multi_asset_neighbors(),
)
norway_brreg_entity_updates_schedule = dg.build_schedule_from_partitioned_job(
    norway_brreg_entity_updates_job,
    name="norway_brreg_entity_updates_schedule",
)
