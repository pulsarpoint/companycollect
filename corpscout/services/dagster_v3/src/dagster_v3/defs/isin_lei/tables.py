from pathlib import Path

CLICKHOUSE_DATABASE = "corpscout"
GLEIF_ISIN_LEI_TABLE = "gleif_isin_lei"

# Column order is the contract with migration 000226.
GLEIF_ISIN_LEI_COLUMNS = (
    "isin",
    "lei",
    "source_url",
    "source_file_name",
    "source_run_id",
    "retrieved_at",
)

ISIN_LEI_DUCKDB_PATH = Path("data/isin_lei_source.duckdb")
ISIN_LEI_DUCKDB_SCHEMA = "isin_lei"
ISIN_LEI_RAW_TABLE = "gleif_isin_lei_raw"

# Single-writer pool for every asset that opens the file, readers included.
ISIN_LEI_DUCKDB_POOL = "isin_lei_duckdb"

# 9,097,315 pairs on the 2026-07-31 file. A floor, so growth does not fail the
# load while a truncated download still does.
MIN_ISIN_LEI_ROWS = 5_000_000
