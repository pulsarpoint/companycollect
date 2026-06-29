import dagster as dg

from dagster_v3.defs.finland_xbrl.assets.common import BACKFILL_PARTITIONS, DAILY_PARTITIONS

finland_xbrl_reference_refresh_job = dg.define_asset_job(
    "finland_xbrl_reference_refresh_job",
    selection=dg.AssetSelection.assets(
        "finland_ytj_all_companies_duckdb",
        "finland_xbrl_eligible_companies",
    ),
)
finland_xbrl_historical_backfill_job = dg.define_asset_job(
    "finland_xbrl_historical_backfill_job",
    selection=dg.AssetSelection.assets(
        "finland_xbrl_financial_reports_backfill",
        "finland_xbrl_raw_xml_documents_backfill",
        "finland_xbrl_parse_backfill",
    ),
    partitions_def=BACKFILL_PARTITIONS,
)
finland_xbrl_incremental_job = dg.define_asset_job(
    "finland_xbrl_incremental_job",
    selection=dg.AssetSelection.assets(
        "finland_xbrl_financial_reports_incremental",
        "finland_xbrl_raw_xml_documents_incremental",
        "finland_xbrl_parse_incremental",
    ),
    partitions_def=DAILY_PARTITIONS,
)
finland_xbrl_publish_job = dg.define_asset_job(
    "finland_xbrl_publish_job",
    selection=dg.AssetSelection.assets(
        "finland_xbrl_eligible_companies",
        "fi_prh_xbrl_financial_metrics",
        "finland_xbrl_financial_metrics_clickhouse",
    ),
)
finland_xbrl_incremental_schedule = dg.build_schedule_from_partitioned_job(
    finland_xbrl_incremental_job,
    name="finland_xbrl_incremental_schedule",
)
