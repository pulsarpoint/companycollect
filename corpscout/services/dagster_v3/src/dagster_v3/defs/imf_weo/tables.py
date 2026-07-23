from dagster_v3.defs.duckdb.schema_contract import (
    DuckDBColumnContract,
    DuckDBTableContract,
)

IMF_WEO_DATABASE = "corpscout"
IMF_WEO_DUCKDB_SCHEMA = "imf_weo_stage"
IMF_WEO_VINTAGES_TABLE = "imf_weo_vintages"
IMF_WEO_SERIES_TABLE = "imf_weo_series"
IMF_WEO_OBSERVATIONS_TABLE = "imf_weo_observations"

IMF_WEO_VINTAGES_COLUMNS = (
    "vintage_date",
    "vintage_label",
    "dataset_id",
    "dataset_version",
    "publication_at",
    "update_at",
    "source_url",
    "source_object_key",
    "source_payload_hash",
    "source_run_id",
    "pulled_at",
)

IMF_WEO_SERIES_COLUMNS = (
    "vintage_date",
    "series_code",
    "country_iso3",
    "country_name",
    "indicator_code",
    "indicator_name",
    "indicator_description",
    "frequency",
    "scale",
    "unit",
    "country_update_date",
    "methodology",
    "methodology_notes",
    "latest_actual_year",
    "historical_data_source",
    "base_year",
    "reporting_year_months",
    "chain_weighted",
    "basis_of_projections",
    "valuation",
    "harmonized_prices",
    "employment_type",
    "government_composition",
    "debt_valuation",
    "debt_instruments",
    "oil_coverage",
    "primary_domestic_currency",
)

IMF_WEO_OBSERVATIONS_COLUMNS = (
    "vintage_date",
    "country_iso3",
    "indicator_code",
    "year",
    "value",
    "value_base",
    "is_estimate",
)

IMF_WEO_VINTAGES_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("vintage_date", "DATE", nullable=False),
        DuckDBColumnContract("vintage_label", "VARCHAR", nullable=False),
        DuckDBColumnContract("dataset_id", "VARCHAR", nullable=False),
        DuckDBColumnContract("dataset_version", "VARCHAR", nullable=False),
        DuckDBColumnContract("publication_at", "TIMESTAMP", nullable=False),
        DuckDBColumnContract("update_at", "TIMESTAMP", nullable=False),
        DuckDBColumnContract("source_url", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_object_key", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_payload_hash", "VARCHAR", nullable=False),
        DuckDBColumnContract("source_run_id", "VARCHAR", nullable=False),
        DuckDBColumnContract("pulled_at", "TIMESTAMP", nullable=False),
    )
)

IMF_WEO_SERIES_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("vintage_date", "DATE", nullable=False),
        DuckDBColumnContract("series_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("country_iso3", "VARCHAR", nullable=False),
        DuckDBColumnContract("country_name", "VARCHAR", nullable=False),
        DuckDBColumnContract("indicator_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("indicator_name", "VARCHAR", nullable=False),
        DuckDBColumnContract("indicator_description", "VARCHAR", nullable=False),
        DuckDBColumnContract("frequency", "VARCHAR", nullable=False),
        DuckDBColumnContract("scale", "VARCHAR"),
        DuckDBColumnContract("unit", "VARCHAR"),
        DuckDBColumnContract("country_update_date", "DATE"),
        DuckDBColumnContract("methodology", "VARCHAR"),
        DuckDBColumnContract("methodology_notes", "VARCHAR"),
        DuckDBColumnContract("latest_actual_year", "USMALLINT"),
        DuckDBColumnContract("historical_data_source", "VARCHAR"),
        DuckDBColumnContract("base_year", "VARCHAR"),
        DuckDBColumnContract("reporting_year_months", "VARCHAR"),
        DuckDBColumnContract("chain_weighted", "VARCHAR"),
        DuckDBColumnContract("basis_of_projections", "VARCHAR"),
        DuckDBColumnContract("valuation", "VARCHAR"),
        DuckDBColumnContract("harmonized_prices", "VARCHAR"),
        DuckDBColumnContract("employment_type", "VARCHAR"),
        DuckDBColumnContract("government_composition", "VARCHAR"),
        DuckDBColumnContract("debt_valuation", "VARCHAR"),
        DuckDBColumnContract("debt_instruments", "VARCHAR"),
        DuckDBColumnContract("oil_coverage", "VARCHAR"),
        DuckDBColumnContract("primary_domestic_currency", "VARCHAR"),
    )
)

IMF_WEO_OBSERVATIONS_CONTRACT = DuckDBTableContract(
    columns=(
        DuckDBColumnContract("vintage_date", "DATE", nullable=False),
        DuckDBColumnContract("country_iso3", "VARCHAR", nullable=False),
        DuckDBColumnContract("indicator_code", "VARCHAR", nullable=False),
        DuckDBColumnContract("year", "USMALLINT", nullable=False),
        DuckDBColumnContract("value", "DOUBLE", nullable=False),
        DuckDBColumnContract("value_base", "DOUBLE", nullable=False),
        DuckDBColumnContract("is_estimate", "BOOLEAN", nullable=False),
    )
)
