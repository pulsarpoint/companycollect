from dagster_v3.defs.common import ats_tables

SOURCE_SLUG = "sweden_greenhouse"
PROVIDER = "greenhouse"
GROUP_NAME = SOURCE_SLUG
S3_BUCKET = "source-sweden-greenhouse"
DUCKDB_FILE_NAME = "sweden_greenhouse_source.duckdb"
DUCKDB_SCHEMA = SOURCE_SLUG
CLICKHOUSE_DATABASE = "corpscout"

BOARDS_TABLE = "se_greenhouse_boards"
BOARD_COMPANY_LINKS_TABLE = "se_greenhouse_board_company_links"
BOARD_SNAPSHOTS_TABLE = "se_greenhouse_board_snapshots"
VERSIONS_TABLE = "se_greenhouse_job_ad_versions"
EVENTS_TABLE = "se_greenhouse_job_ad_events"
CURRENT_TABLE = "se_greenhouse_job_ad_current"
LOCATIONS_TABLE = "se_greenhouse_job_ad_location_versions"
COMPENSATIONS_TABLE = "se_greenhouse_job_ad_compensation_versions"

CLICKHOUSE_TABLES = (
    BOARDS_TABLE,
    BOARD_COMPANY_LINKS_TABLE,
    BOARD_SNAPSHOTS_TABLE,
    VERSIONS_TABLE,
    EVENTS_TABLE,
    CURRENT_TABLE,
    LOCATIONS_TABLE,
    COMPENSATIONS_TABLE,
)
TABLE_COLUMNS = {
    BOARDS_TABLE: ats_tables.BOARDS_COLUMNS,
    BOARD_COMPANY_LINKS_TABLE: ats_tables.BOARD_COMPANY_LINKS_COLUMNS,
    BOARD_SNAPSHOTS_TABLE: ats_tables.BOARD_SNAPSHOTS_COLUMNS,
    VERSIONS_TABLE: ats_tables.VERSIONS_COLUMNS,
    EVENTS_TABLE: ats_tables.EVENTS_COLUMNS,
    CURRENT_TABLE: ats_tables.JOB_STATE_COLUMNS,
    LOCATIONS_TABLE: ats_tables.LOCATIONS_COLUMNS,
    COMPENSATIONS_TABLE: ats_tables.COMPENSATIONS_COLUMNS,
}
