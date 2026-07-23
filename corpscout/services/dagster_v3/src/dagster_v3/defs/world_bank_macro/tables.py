from dagster_v3.defs.duckdb.schema_contract import (
    DuckDBColumnContract,
    DuckDBTableContract,
)

WORLD_BANK_DATABASE = "corpscout"
WORLD_BANK_DUCKDB_SCHEMA = "world_bank_stage"
WORLD_BANK_MACRO_TABLE = "world_bank_macro_observations"
QUALIFIED_WORLD_BANK_MACRO_TABLE = f"{WORLD_BANK_DATABASE}.{WORLD_BANK_MACRO_TABLE}"

WORLD_BANK_MACRO_COLUMNS = (
    "country_code",
    "country_iso3",
    "country_name",
    "region",
    "income_group",
    "indicator_code",
    "indicator_name",
    "year",
    "value",
    "source",
    "source_dataset",
    "source_updated_date",
    "source_url",
    "source_object_key",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
)

WORLD_BANK_DUCKDB_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("country_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("country_iso3", "VARCHAR", nullable=False),
        DuckDBColumnContract("country_name", "VARCHAR", nullable=False),
        DuckDBColumnContract("region", "VARCHAR", nullable=False),
        DuckDBColumnContract("income_group", "VARCHAR", nullable=False),
        DuckDBColumnContract("indicator_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("indicator_name", "VARCHAR", nullable=False),
        DuckDBColumnContract("year", "USMALLINT", nullable=False),
        DuckDBColumnContract("value", "DOUBLE", nullable=False),
        DuckDBColumnContract("source", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_dataset", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_updated_date", "DATE", nullable=False),
        DuckDBColumnContract("source_url", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_object_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_payload_hash", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_run_id", "VARCHAR", nullable=False),
        DuckDBColumnContract("pulled_at", "TIMESTAMP", nullable=False),
    )
)

WORLD_BANK_MACRO_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUALIFIED_WORLD_BANK_MACRO_TABLE}
(
    country_code LowCardinality(String),
    country_iso3 FixedString(3),
    country_name String,
    region LowCardinality(String),
    income_group LowCardinality(String),
    indicator_code LowCardinality(String),
    indicator_name String,
    year UInt16,
    value Float64,
    source LowCardinality(String),
    source_dataset LowCardinality(String),
    source_updated_date Date,
    source_url String,
    source_object_key String,
    source_payload_hash FixedString(64),
    source_run_id String,
    pulled_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (country_code, indicator_code, year)
"""
