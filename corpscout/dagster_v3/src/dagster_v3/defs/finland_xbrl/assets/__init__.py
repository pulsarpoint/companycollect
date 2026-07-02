import dagster as dg

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.common import (
    BACKFILL_PARTITIONS,
    DAILY_PARTITIONS,
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
    DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
    DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    XBRL_BASE_URL,
    XBRL_BUCKET,
    XBRL_DLT_DATASET_NAME,
    XBRL_DLT_FINANCIAL_REPORTS_TABLE,
    XBRL_TIMEOUT_SECONDS,
)
from dagster_v3.defs.finland_xbrl.assets.financial_reports import (
    XbrlFinancialReportsConfig,
    finland_xbrl_financial_reports_backfill,
    finland_xbrl_financial_reports_incremental,
    materialize_financial_reports_window,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot import (
    FINANCIAL_DATA_S3_SNAPSHOT_KEY,
    FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END,
    FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START,
    build_financial_data_snapshot_csv,
    data_snapshot,
    write_financial_data_snapshot_csv,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_duckdb_ch import (
    DATA_SNAPSHOT_CLICKHOUSE_TABLE,
    data_snapshot_duckdb_ch,
    export_data_snapshot_duckdb_to_clickhouse,
)
from dagster_v3.defs.finland_xbrl.assets.financial_metrics import (
    build_financial_metric_rows,
    build_financial_metric_usd_rows,
    finland_xbrl_financial_metrics,
    finland_xbrl_financial_metrics_clickhouse,
    finland_xbrl_financial_metrics_usd,
)
from dagster_v3.defs.finland_xbrl.assets.jobs import (
    finland_xbrl_data_snapshot_job,
    finland_xbrl_historical_backfill_job,
    finland_xbrl_incremental_job,
    finland_xbrl_incremental_schedule,
    finland_xbrl_publish_job,
)
from dagster_v3.defs.finland_xbrl.assets.parse import (
    XbrlParsedConfig,
    XbrlParseRunResult,
    build_concept_profile_rows,
    build_parse_quality_row,
    documents_in_registration_window,
    documents_missing_registration_date,
    finland_xbrl_parse_backfill,
    finland_xbrl_parse_incremental,
    parse_xbrl_documents,
    run_finland_xbrl_parse,
)
from dagster_v3.defs.finland_xbrl.assets.raw_xml_documents import (
    RawXmlDownloadResult,
    XbrlRawConfig,
    document_object_key,
    download_finland_xbrl_raw_xml_documents,
    finland_xbrl_raw_xml_documents,
    finland_xbrl_raw_xml_documents_backfill,
    finland_xbrl_raw_xml_documents_incremental,
    finland_xbrl_xml_documents,
    financial_report_rows_in_registration_window,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_duckdb import (
    FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH,
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA,
    FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE,
    data_snapshot_duckdb,
    materialize_data_snapshot_duckdb,
)
from dagster_v3.defs.finland_xbrl.resources import (
    XbrlApiResource,
    XbrlParquetStorageResource,
)

__all__ = [
    "BACKFILL_PARTITIONS",
    "DAILY_PARTITIONS",
    "DEFAULT_XBRL_REQUEST_DELAY_SECONDS",
    "DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS",
    "DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS",
    "DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS",
    "DATA_SNAPSHOT_CLICKHOUSE_TABLE",
    "RawXmlDownloadResult",
    "XBRL_BASE_URL",
    "XBRL_BUCKET",
    "XBRL_DLT_DATASET_NAME",
    "XBRL_DLT_FINANCIAL_REPORTS_TABLE",
    "XBRL_TIMEOUT_SECONDS",
    "XbrlFinancialReportsConfig",
    "XbrlParquetStorageResource",
    "XbrlParsedConfig",
    "XbrlParseRunResult",
    "XbrlRawConfig",
    "FINANCIAL_DATA_S3_SNAPSHOT_KEY",
    "FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END",
    "FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START",
    "FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH",
    "FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA",
    "FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE",
    "build_concept_profile_rows",
    "build_financial_data_snapshot_csv",
    "build_financial_metric_rows",
    "build_financial_metric_usd_rows",
    "build_parse_quality_row",
    "data_snapshot",
    "data_snapshot_duckdb",
    "data_snapshot_duckdb_ch",
    "defs",
    "document_object_key",
    "documents_in_registration_window",
    "documents_missing_registration_date",
    "download_finland_xbrl_raw_xml_documents",
    "export_data_snapshot_duckdb_to_clickhouse",
    "finland_xbrl_historical_backfill_job",
    "finland_xbrl_financial_metrics",
    "finland_xbrl_financial_metrics_clickhouse",
    "finland_xbrl_financial_metrics_usd",
    "finland_xbrl_financial_reports_backfill",
    "finland_xbrl_financial_reports_incremental",
    "finland_xbrl_data_snapshot_job",
    "finland_xbrl_incremental_job",
    "finland_xbrl_incremental_schedule",
    "finland_xbrl_parse_backfill",
    "finland_xbrl_parse_incremental",
    "finland_xbrl_publish_job",
    "finland_xbrl_raw_xml_documents",
    "finland_xbrl_raw_xml_documents_backfill",
    "finland_xbrl_raw_xml_documents_incremental",
    "finland_xbrl_xml_documents",
    "financial_report_rows_in_registration_window",
    "materialize_financial_reports_window",
    "materialize_data_snapshot_duckdb",
    "parse_xbrl_documents",
    "run_finland_xbrl_parse",
    "tables",
    "write_financial_data_snapshot_csv",
]

defs = dg.Definitions(
    assets=[
        data_snapshot,
        data_snapshot_duckdb,
        data_snapshot_duckdb_ch,
        finland_xbrl_financial_reports_backfill,
        finland_xbrl_financial_reports_incremental,
        finland_xbrl_raw_xml_documents_backfill,
        finland_xbrl_raw_xml_documents_incremental,
        finland_xbrl_raw_xml_documents,
        finland_xbrl_xml_documents,
        finland_xbrl_parse_backfill,
        finland_xbrl_parse_incremental,
        finland_xbrl_financial_metrics,
        finland_xbrl_financial_metrics_usd,
        finland_xbrl_financial_metrics_clickhouse,
    ],
    jobs=[
        finland_xbrl_data_snapshot_job,
        finland_xbrl_historical_backfill_job,
        finland_xbrl_incremental_job,
        finland_xbrl_publish_job,
    ],
    schedules=[finland_xbrl_incremental_schedule],
    resources={
        "xbrl_api": XbrlApiResource(),
        "xbrl_parquet_storage": XbrlParquetStorageResource(),
        "object_store": ObjectStoreResource(),
        "xbrl_financial_data_snapshot_duckdb": duckdb_resource(
            FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH
        ),
    },
)
