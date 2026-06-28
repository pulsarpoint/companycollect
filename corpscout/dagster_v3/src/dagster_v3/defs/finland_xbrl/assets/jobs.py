import dagster as dg

from dagster_v3.defs.finland_xbrl.assets.common import BACKFILL_PARTITIONS, DAILY_PARTITIONS

finland_xbrl_backfill_job = dg.define_asset_job(
    "finland_xbrl_backfill_job",
    selection=dg.AssetSelection.assets(
        "finland_xbrl_company_seed_duckdb",
        "finland_xbrl_financial_reports_backfill_duckdb",
        "finland_xbrl_raw_xml_documents_backfill",
        "finland_xbrl_parse_backfill",
    ),
    partitions_def=BACKFILL_PARTITIONS,
)
finland_xbrl_incremental_job = dg.define_asset_job(
    "finland_xbrl_incremental_job",
    selection=dg.AssetSelection.assets(
        "finland_xbrl_company_seed_duckdb",
        "finland_xbrl_financial_reports_incremental_duckdb",
        "finland_xbrl_raw_xml_documents_incremental",
        "finland_xbrl_parse_incremental",
    ),
    partitions_def=DAILY_PARTITIONS,
)
finland_xbrl_incremental_schedule = dg.build_schedule_from_partitioned_job(
    finland_xbrl_incremental_job,
    name="finland_xbrl_incremental_schedule",
)
