import dagster as dg

XBRL_BUCKET = "source-finland-prh-xbrl"
# Single-writer pool for every asset touching the finland_xbrl DuckDB files
# (the instance defaults every pool to limit 1).
FINLAND_XBRL_DUCKDB_POOL = "finland_xbrl_duckdb"
XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
XBRL_TIMEOUT_SECONDS = 120
DEFAULT_XBRL_REQUEST_DELAY_SECONDS = 1.0
DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS = 6
DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS = 30.0
DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS = 480.0
DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-06-01",
    end_offset=1,
    hour_offset=6,
    timezone="Europe/Belgrade",
)
