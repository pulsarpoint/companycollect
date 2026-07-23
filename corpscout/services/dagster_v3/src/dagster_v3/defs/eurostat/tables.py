from dagster_v3.defs.duckdb.schema_contract import (
    DuckDBColumnContract,
    DuckDBTableContract,
)

EUROSTAT_DATABASE = "corpscout"
EUROSTAT_DUCKDB_SCHEMA = "eurostat_stage"
EUROSTAT_DATASETS_TABLE = "eurostat_datasets"
EUROSTAT_DIMENSION_VALUES_TABLE = "eurostat_dimension_values"
EUROSTAT_SERIES_TABLE = "eurostat_series"
EUROSTAT_SERIES_DIMENSIONS_TABLE = "eurostat_series_dimensions"
EUROSTAT_OBSERVATIONS_TABLE = "eurostat_observations"

EUROSTAT_DATASETS_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("dataset_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("title", "VARCHAR", nullable=False),
        DuckDBColumnContract("dsd_version", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_observation_count", "UBIGINT", nullable=False),
        DuckDBColumnContract("source_oldest_period", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_latest_period", "VARCHAR", nullable=False),
        DuckDBColumnContract("data_updated_at", "TIMESTAMP", nullable=False),
        DuckDBColumnContract("structure_updated_at", "TIMESTAMP", nullable=False),
        DuckDBColumnContract("source_data_url", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_data_object_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_data_hash", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_structure_url", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_structure_object_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_structure_hash", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_run_id", "VARCHAR", nullable=False),
        DuckDBColumnContract("pulled_at", "TIMESTAMP", nullable=False),
    )
)

EUROSTAT_DIMENSION_VALUES_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("dataset_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("dimension_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("dimension_label", "VARCHAR", nullable=False),
        DuckDBColumnContract("dimension_position", "USMALLINT", nullable=False),
        DuckDBColumnContract("value_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("value_label", "VARCHAR", nullable=False),
        DuckDBColumnContract("value_position", "USMALLINT", nullable=False),
    )
)

EUROSTAT_SERIES_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("dataset_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("series_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("geo_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("frequency", "VARCHAR", nullable=False),
        DuckDBColumnContract("unit_code", "VARCHAR"),
        DuckDBColumnContract("source_line_number", "UBIGINT", nullable=False),
    )
)

EUROSTAT_SERIES_DIMENSIONS_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("dataset_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("series_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("dimension_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("value_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("dimension_position", "USMALLINT", nullable=False),
    )
)

EUROSTAT_OBSERVATIONS_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("dataset_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("geo_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("series_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("time_period", "VARCHAR", nullable=False),
        DuckDBColumnContract("period_start", "DATE", nullable=False),
        DuckDBColumnContract("year", "USMALLINT", nullable=False),
        DuckDBColumnContract("value", "DOUBLE"),
        DuckDBColumnContract("status", "VARCHAR", nullable=False),
    )
)

EUROSTAT_DATASETS_COLUMNS = EUROSTAT_DATASETS_CONTRACT.column_names
EUROSTAT_DIMENSION_VALUES_COLUMNS = EUROSTAT_DIMENSION_VALUES_CONTRACT.column_names
EUROSTAT_SERIES_COLUMNS = EUROSTAT_SERIES_CONTRACT.column_names
EUROSTAT_SERIES_DIMENSIONS_COLUMNS = EUROSTAT_SERIES_DIMENSIONS_CONTRACT.column_names
EUROSTAT_OBSERVATIONS_COLUMNS = EUROSTAT_OBSERVATIONS_CONTRACT.column_names

EUROSTAT_TABLE_CONTRACTS = {
    EUROSTAT_DATASETS_TABLE: (
        EUROSTAT_DATASETS_COLUMNS,
        EUROSTAT_DATASETS_CONTRACT,
    ),
    EUROSTAT_DIMENSION_VALUES_TABLE: (
        EUROSTAT_DIMENSION_VALUES_COLUMNS,
        EUROSTAT_DIMENSION_VALUES_CONTRACT,
    ),
    EUROSTAT_SERIES_TABLE: (
        EUROSTAT_SERIES_COLUMNS,
        EUROSTAT_SERIES_CONTRACT,
    ),
    EUROSTAT_SERIES_DIMENSIONS_TABLE: (
        EUROSTAT_SERIES_DIMENSIONS_COLUMNS,
        EUROSTAT_SERIES_DIMENSIONS_CONTRACT,
    ),
    EUROSTAT_OBSERVATIONS_TABLE: (
        EUROSTAT_OBSERVATIONS_COLUMNS,
        EUROSTAT_OBSERVATIONS_CONTRACT,
    ),
}
