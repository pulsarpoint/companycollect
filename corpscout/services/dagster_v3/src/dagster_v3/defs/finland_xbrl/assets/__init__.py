import dagster as dg

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_xbrl import tables
from dagster_v3.defs.finland_xbrl.assets.common import (
    DAILY_PARTITIONS,
    DEFAULT_XBRL_REQUEST_DELAY_SECONDS,
    DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS,
    DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS,
    DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS,
    XBRL_BASE_URL,
    XBRL_BUCKET,
    XBRL_TIMEOUT_SECONDS,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot import (
    FINANCIAL_DATA_S3_SNAPSHOT_KEY,
    FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END,
    FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START,
    build_financial_data_snapshot_csv,
    data_snapshot,
    write_financial_data_snapshot_csv,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily import (
    FINANCIAL_DATA_DAILY_KEY_PREFIX,
    data_daily,
    financial_data_daily_key,
    write_financial_data_daily_csv,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily_duckdb import (
    FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE,
    FINLAND_XBRL_FINANCIAL_DATA_DAILY_DUCKDB_PATH,
    data_daily_duckdb,
    materialize_data_daily_duckdb,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily_duckdb_ch import (
    data_daily_duckdb_ch,
    export_data_daily_duckdb_to_clickhouse,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily_xml import (
    data_daily_xml,
    materialize_data_daily_xml,
)
from dagster_v3.defs.finland_xbrl.assets.data_daily_xml_duckdb import (
    data_daily_xml_duckdb,
    data_daily_xml_unified_duckdb,
    materialize_data_daily_xml_duckdb,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_duckdb_ch import (
    DATA_SNAPSHOT_CLICKHOUSE_TABLE,
    data_snapshot_duckdb_ch,
    export_data_snapshot_duckdb_to_clickhouse,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
    XML_SNAPSHOT_PARTITIONS,
    data_snapshot_xml,
    download_finland_xbrl_snapshot_xml_partition,
    fetch_xml_snapshot_report_rows,
    xml_snapshot_document_key,
    xml_snapshot_manifest_key,
    xml_snapshot_partition_prefix,
    xml_snapshot_success_key,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    FINLAND_XBRL_XML_DAILY_PARSE_DUCKDB_PATH,
    FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH,
    data_snapshot_xml_duckdb,
    data_snapshot_xml_unified_duckdb,
    list_xml_parse_duckdb_paths,
    list_xml_unified_duckdb_paths,
    materialize_data_snapshot_xml_duckdb,
    read_xml_parse_duckdb_rows,
    read_xml_snapshot_manifest_rows,
    read_xml_unified_duckdb_rows,
    run_finland_xbrl_parse,
    xml_daily_parse_duckdb_path,
    xml_daily_parse_temp_dir,
    xml_daily_unified_duckdb_path,
    xml_daily_unified_parse_temp_dir,
    xml_snapshot_parse_duckdb_path,
    xml_snapshot_parse_temp_dir,
    xml_snapshot_unified_duckdb_path,
    xml_snapshot_unified_parse_temp_dir,
)
from dagster_v3.defs.finland_xbrl.assets.financial_metrics import (
    build_finland_financial_metrics_insert_sql,
    build_financial_metric_rows,
    build_financial_metric_usd_rows,
)
from dagster_v3.defs.finland_xbrl.assets.financial_publish import (
    fi_financial_metrics_ch,
    fi_xbrl_parsed_clickhouse,
    fi_xbrl_taxonomy_codes_ch,
)
from dagster_v3.defs.finland_xbrl.assets.jobs import (
    finland_xbrl_data_snapshot_job,
    finland_xbrl_incremental_job,
    finland_xbrl_incremental_schedule,
    finland_xbrl_publish_job,
    finland_xbrl_xml_snapshot_job,
)
from dagster_v3.defs.finland_xbrl.assets.parity import (
    FINLAND_EXPLAINED_RULES,
    build_finland_parity_results,
    fi_xbrl_parity,
)
from dagster_v3.defs.finland_xbrl.assets.unified_publish import (
    fi_xbrl_unified_clickhouse,
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
    "DAILY_PARTITIONS",
    "DEFAULT_XBRL_REQUEST_DELAY_SECONDS",
    "DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS",
    "DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS",
    "DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS",
    "DATA_SNAPSHOT_CLICKHOUSE_TABLE",
    "FINLAND_EXPLAINED_RULES",
    "XBRL_BASE_URL",
    "XBRL_BUCKET",
    "XBRL_TIMEOUT_SECONDS",
    "XbrlParquetStorageResource",
    "XML_SNAPSHOT_PARTITIONS",
    "FINANCIAL_DATA_S3_SNAPSHOT_KEY",
    "FINANCIAL_DATA_DAILY_KEY_PREFIX",
    "FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_END",
    "FINANCIAL_DATA_S3_SNAPSHOT_REGISTERED_DATE_START",
    "FINLAND_XBRL_DAILY_CSV_DUCKDB_TABLE",
    "FINLAND_XBRL_FINANCIAL_DATA_DAILY_DUCKDB_PATH",
    "FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH",
    "FINLAND_XBRL_XML_DAILY_PARSE_DUCKDB_PATH",
    "FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH",
    "FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_SCHEMA",
    "FINLAND_XBRL_SNAPSHOT_CSV_DUCKDB_TABLE",
    "build_financial_data_snapshot_csv",
    "build_finland_financial_metrics_insert_sql",
    "build_finland_parity_results",
    "build_financial_metric_rows",
    "build_financial_metric_usd_rows",
    "data_snapshot",
    "data_daily",
    "data_daily_duckdb",
    "data_daily_duckdb_ch",
    "data_daily_xml",
    "data_daily_xml_duckdb",
    "data_daily_xml_unified_duckdb",
    "data_snapshot_duckdb",
    "data_snapshot_duckdb_ch",
    "data_snapshot_xml",
    "data_snapshot_xml_duckdb",
    "data_snapshot_xml_unified_duckdb",
    "defs",
    "download_finland_xbrl_snapshot_xml_partition",
    "export_data_daily_duckdb_to_clickhouse",
    "export_data_snapshot_duckdb_to_clickhouse",
    "fetch_xml_snapshot_report_rows",
    "fi_financial_metrics_ch",
    "fi_xbrl_parity",
    "fi_xbrl_parsed_clickhouse",
    "fi_xbrl_taxonomy_codes_ch",
    "fi_xbrl_unified_clickhouse",
    "finland_xbrl_data_snapshot_job",
    "finland_xbrl_incremental_job",
    "finland_xbrl_incremental_schedule",
    "finland_xbrl_publish_job",
    "finland_xbrl_xml_snapshot_job",
    "financial_data_daily_key",
    "list_xml_parse_duckdb_paths",
    "list_xml_unified_duckdb_paths",
    "materialize_data_daily_xml",
    "materialize_data_daily_xml_duckdb",
    "materialize_data_snapshot_xml_duckdb",
    "materialize_data_daily_duckdb",
    "materialize_data_snapshot_duckdb",
    "read_xml_snapshot_manifest_rows",
    "read_xml_parse_duckdb_rows",
    "read_xml_unified_duckdb_rows",
    "run_finland_xbrl_parse",
    "tables",
    "write_financial_data_snapshot_csv",
    "write_financial_data_daily_csv",
    "xml_daily_parse_duckdb_path",
    "xml_daily_parse_temp_dir",
    "xml_daily_unified_duckdb_path",
    "xml_daily_unified_parse_temp_dir",
    "xml_snapshot_document_key",
    "xml_snapshot_manifest_key",
    "xml_snapshot_parse_duckdb_path",
    "xml_snapshot_parse_temp_dir",
    "xml_snapshot_partition_prefix",
    "xml_snapshot_success_key",
    "xml_snapshot_unified_duckdb_path",
    "xml_snapshot_unified_parse_temp_dir",
]

defs = dg.Definitions(
    assets=[
        data_snapshot,
        data_daily,
        data_daily_duckdb,
        data_daily_duckdb_ch,
        data_daily_xml,
        data_daily_xml_duckdb,
        data_daily_xml_unified_duckdb,
        data_snapshot_duckdb,
        data_snapshot_duckdb_ch,
        data_snapshot_xml,
        data_snapshot_xml_duckdb,
        data_snapshot_xml_unified_duckdb,
        fi_xbrl_parsed_clickhouse,
        fi_xbrl_taxonomy_codes_ch,
        fi_financial_metrics_ch,
        fi_xbrl_unified_clickhouse,
        fi_xbrl_parity,
    ],
    jobs=[
        finland_xbrl_data_snapshot_job,
        finland_xbrl_incremental_job,
        finland_xbrl_publish_job,
        finland_xbrl_xml_snapshot_job,
    ],
    schedules=[finland_xbrl_incremental_schedule],
    resources={
        "xbrl_api": XbrlApiResource(),
        "object_store": ObjectStoreResource(),
        "xbrl_financial_data_snapshot_duckdb": duckdb_resource(
            FINLAND_XBRL_FINANCIAL_DATA_SNAPSHOT_DUCKDB_PATH
        ),
        "xbrl_financial_data_daily_duckdb": duckdb_resource(
            FINLAND_XBRL_FINANCIAL_DATA_DAILY_DUCKDB_PATH
        ),
    },
)
