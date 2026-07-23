from dagster_v3.defs.duckdb.schema_contract import (
    DuckDBColumnContract,
    DuckDBTableContract,
)

UN_COMTRADE_DATABASE = "corpscout"
UN_COMTRADE_DUCKDB_SCHEMA = "un_comtrade_stage"
UN_COMTRADE_AVAILABILITY_TABLE = "un_comtrade_annual_availability"
UN_COMTRADE_ANNUAL_TOTALS_TABLE = "un_comtrade_annual_totals"

UN_COMTRADE_AVAILABILITY_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("dataset_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("year", "USMALLINT", nullable=False),
        DuckDBColumnContract("reporter_code", "USMALLINT", nullable=False),
        DuckDBColumnContract("reporter_iso", "VARCHAR", nullable=False),
        DuckDBColumnContract("reporter_name", "VARCHAR", nullable=False),
        DuckDBColumnContract("classification_code", "VARCHAR", nullable=False),
        DuckDBColumnContract(
            "classification_search_code",
            "VARCHAR",
            nullable=False,
        ),
        DuckDBColumnContract(
            "is_original_classification",
            "BOOLEAN",
            nullable=False,
        ),
        DuckDBColumnContract("has_extended_flow", "BOOLEAN", nullable=False),
        DuckDBColumnContract("has_extended_partner", "BOOLEAN", nullable=False),
        DuckDBColumnContract("has_extended_partner2", "BOOLEAN", nullable=False),
        DuckDBColumnContract("has_extended_commodity", "BOOLEAN", nullable=False),
        DuckDBColumnContract("has_extended_customs", "BOOLEAN", nullable=False),
        DuckDBColumnContract(
            "has_extended_mode_of_transport",
            "BOOLEAN",
            nullable=False,
        ),
        DuckDBColumnContract("source_total_records", "UBIGINT", nullable=False),
        DuckDBColumnContract("dataset_checksum", "VARCHAR", nullable=False),
        DuckDBColumnContract("first_released_at", "TIMESTAMP", nullable=False),
        DuckDBColumnContract("last_released_at", "TIMESTAMP", nullable=False),
        DuckDBColumnContract("source_url", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_object_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_object_hash", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_run_id", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_line_number", "UBIGINT", nullable=False),
        DuckDBColumnContract("pulled_at", "TIMESTAMP", nullable=False),
    )
)

UN_COMTRADE_ANNUAL_TOTALS_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("year", "USMALLINT", nullable=False),
        DuckDBColumnContract("reporter_code", "USMALLINT", nullable=False),
        DuckDBColumnContract("reporter_iso", "VARCHAR", nullable=False),
        DuckDBColumnContract("reporter_name", "VARCHAR", nullable=False),
        DuckDBColumnContract("flow_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("flow_name", "VARCHAR", nullable=False),
        DuckDBColumnContract("classification_code", "VARCHAR", nullable=False),
        DuckDBColumnContract(
            "classification_search_code",
            "VARCHAR",
            nullable=False,
        ),
        DuckDBColumnContract(
            "is_original_classification",
            "BOOLEAN",
            nullable=False,
        ),
        DuckDBColumnContract(
            "primary_value_usd",
            "DECIMAL(38,3)",
            nullable=False,
        ),
        DuckDBColumnContract("cif_value_usd", "DECIMAL(38,3)"),
        DuckDBColumnContract("fob_value_usd", "DECIMAL(38,3)"),
        DuckDBColumnContract(
            "legacy_estimation_flag",
            "SMALLINT",
            nullable=False,
        ),
        DuckDBColumnContract("is_reported", "BOOLEAN", nullable=False),
        DuckDBColumnContract("is_aggregate", "BOOLEAN", nullable=False),
        DuckDBColumnContract("source_url", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_object_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_object_hash", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_run_id", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_line_number", "UBIGINT", nullable=False),
        DuckDBColumnContract("pulled_at", "TIMESTAMP", nullable=False),
    )
)

UN_COMTRADE_AVAILABILITY_COLUMNS = UN_COMTRADE_AVAILABILITY_CONTRACT.column_names
UN_COMTRADE_ANNUAL_TOTALS_COLUMNS = UN_COMTRADE_ANNUAL_TOTALS_CONTRACT.column_names

UN_COMTRADE_TABLE_CONTRACTS = {
    UN_COMTRADE_AVAILABILITY_TABLE: (
        UN_COMTRADE_AVAILABILITY_COLUMNS,
        UN_COMTRADE_AVAILABILITY_CONTRACT,
    ),
    UN_COMTRADE_ANNUAL_TOTALS_TABLE: (
        UN_COMTRADE_ANNUAL_TOTALS_COLUMNS,
        UN_COMTRADE_ANNUAL_TOTALS_CONTRACT,
    ),
}
