import dagster as dg

from dagster_v3.defs.finland_xbrl.assets.common import BACKFILL_PARTITIONS, DAILY_PARTITIONS
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import XML_SNAPSHOT_PARTITIONS

finland_xbrl_data_snapshot_job = dg.define_asset_job(
    "finland_xbrl_data_snapshot_job",
    selection=dg.AssetSelection.assets(
        "data_snapshot",
        "data_snapshot_duckdb",
        "data_snapshot_duckdb_ch",
    ),
)
finland_xbrl_xml_snapshot_job = dg.define_asset_job(
    "finland_xbrl_xml_snapshot_job",
    selection=dg.AssetSelection.assets("data_snapshot_xml", "data_snapshot_xml_duckdb"),
    partitions_def=XML_SNAPSHOT_PARTITIONS,
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
        "data_daily",
        "data_daily_duckdb",
        "data_daily_duckdb_ch",
        "data_daily_xml",
        "data_daily_xml_duckdb",
        "finland_xbrl_financial_reports_incremental",
        "finland_xbrl_raw_xml_documents_incremental",
        "finland_xbrl_parse_incremental",
    ),
    partitions_def=DAILY_PARTITIONS,
)
finland_xbrl_publish_job = dg.define_asset_job(
    "finland_xbrl_publish_job",
    selection=dg.AssetSelection.assets(
        "fi_prh_xbrl_financial_metrics",
        "fi_prh_xbrl_financial_metrics_usd",
        "finland_xbrl_financial_metrics_clickhouse",
    ),
)
finland_xbrl_incremental_schedule = dg.build_schedule_from_partitioned_job(
    finland_xbrl_incremental_job,
    name="finland_xbrl_incremental_schedule",
)
