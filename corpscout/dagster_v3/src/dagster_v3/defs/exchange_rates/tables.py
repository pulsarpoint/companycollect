from dagster_v3.defs.duckdb.schema_contract import (
    DuckDBColumnContract,
    DuckDBTableContract,
)

EXCHANGE_RATES_DATABASE = "reference"
EXCHANGE_RATES_TABLE = "exchange_rates"
QUALIFIED_EXCHANGE_RATES_TABLE = f"{EXCHANGE_RATES_DATABASE}.{EXCHANGE_RATES_TABLE}"

EXCHANGE_RATES_COLUMNS = (
    "rate_date",
    "base_currency",
    "quote_currency",
    "rate",
    "source",
    "source_url",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
    "_dlt_load_id",
    "_dlt_id",
)

EXCHANGE_RATES_DUCKDB_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("rate_date", "DATE"),
        DuckDBColumnContract("base_currency", "VARCHAR"),
        DuckDBColumnContract("quote_currency", "VARCHAR"),
        DuckDBColumnContract("rate", "DECIMAL(38, 12)"),
        DuckDBColumnContract("source", "VARCHAR"),
        DuckDBColumnContract("source_url", "VARCHAR"),
        DuckDBColumnContract("source_payload_hash", "VARCHAR"),
        DuckDBColumnContract("source_run_id", "VARCHAR"),
        DuckDBColumnContract("pulled_at", "TIMESTAMP"),
        DuckDBColumnContract("_dlt_load_id", "VARCHAR"),
        DuckDBColumnContract("_dlt_id", "VARCHAR"),
    )
)
EXCHANGE_RATES_DUCKDB_COLUMN_TYPES = EXCHANGE_RATES_DUCKDB_CONTRACT.column_types
