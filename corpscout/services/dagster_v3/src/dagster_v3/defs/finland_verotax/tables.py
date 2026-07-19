"""Table and column contracts for the Finland Verohallinto public tax data source.

Column orders here are load-bearing: the DuckDB tables, the ClickHouse migration
(000144), and the export subset must all agree. See docs/finland_verotax-design.md.
"""

from __future__ import annotations

COUNTRY_ISO2 = "FI"
SOURCE_SLUG = "finland_verotax"

# DuckDB file stem must differ from the dlt/DuckDB dataset (schema) name.
DLT_DATASET_NAME = "finland_verotax"
DUCKDB_FILE_NAME = "finland_verotax_source.duckdb"

FINLAND_VEROTAX_DATABASE = "corpscout"
FI_TAX_RECORDS_TABLE = "fi_tax_records"
QUALIFIED_FI_TAX_RECORDS_TABLE = f"{FINLAND_VEROTAX_DATABASE}.{FI_TAX_RECORDS_TABLE}"

TAX_RECORDS_TABLE = "tax_records"

SOURCE_PAGE_URL = "https://www.vero.fi/tietoa-verohallinnosta/tilastot/avoin_dat/"
VERO_BASE_URL = "https://www.vero.fi"

# Tax years ingested. Extend when Vero publishes a new November snapshot; the
# runtime page discovery logs any year it finds that is missing from this tuple.
EXPECTED_YEARS = (2020, 2021, 2022, 2023, 2024)

# Fallback year -> URL map used when the vero.fi page scrape fails. Filenames
# rotate per year with no stable pattern, so runtime discovery is the primary path.
FALLBACK_YEAR_SOURCES = {
    2020: (
        f"{VERO_BASE_URL}/contentassets/a7b04897ccd44feda446bd1894c1fd74/"
        "verohallinto_yhteisojen-tuloverotuksen-julkiset-tiedot-2020.csv"
    ),
    2021: (
        f"{VERO_BASE_URL}/contentassets/a7b04897ccd44feda446bd1894c1fd74/"
        "verohallinto_yhteisojen-tuloverotuksen-julkiset-tiedot-2021.csv"
    ),
    2022: (
        f"{VERO_BASE_URL}/contentassets/a7b04897ccd44feda446bd1894c1fd74/"
        "verohallinto_yhteisojen-tuloverotuksen-julkiset-tiedot-2022.csv"
    ),
    2023: (
        f"{VERO_BASE_URL}/contentassets/a7b04897ccd44feda446bd1894c1fd74/"
        "yhteiso-tuloverotus-julk-2023.csv"
    ),
    2024: (
        f"{VERO_BASE_URL}/contentassets/a7b04897ccd44feda446bd1894c1fd74/"
        "yhteis%C3%B6_tuloverotus_julk_2024.csv"
    ),
}


def raw_table_for_year(year: int) -> str:
    return f"raw_tax_records_{year}"


# Positional column names for the semicolon-delimited Latin-1 CSVs. The bilingual
# FI|SV header row is skipped; 2020-2022 files carry a 9th "Ennakot yhteensä"
# (total prepayments) column that 2023+ files dropped.
RAW_COLUMNS_8 = (
    "tax_year",
    "business_id",
    "taxpayer_name",
    "municipality_raw",
    "taxable_income",
    "taxes_total",
    "tax_refund",
    "residual_tax",
)
RAW_COLUMNS_9 = (
    "tax_year",
    "business_id",
    "taxpayer_name",
    "municipality_raw",
    "taxable_income",
    "taxes_total",
    "prepayments_total",
    "tax_refund",
    "residual_tax",
)

# canonical metric name -> raw column. taxable_income is a TAX BASE metric —
# never map it onto profit_loss (loss carryforwards / group contributions).
TAX_METRIC_NAMES = (
    "taxable_income",
    "taxes_total",
    "prepayments_total",
    "tax_refund",
    "residual_tax",
)

_METRIC_AMOUNT_COLUMNS = tuple(
    col
    for metric in TAX_METRIC_NAMES
    for col in (f"{metric}_amount_original", f"{metric}_amount_usd")
)

# Column order must match the DuckDB tax_records table and the 000144 migration.
FI_TAX_RECORDS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "business_id",
    "taxpayer_name",
    "municipality_code",
    "municipality_name",
    "tax_year",
    "period_end_date",
    "currency",
    *_METRIC_AMOUNT_COLUMNS,
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "source_url",
    "resolved_at",
    "raw_tax_record",
    "source_payload_hash",
)

CLICKHOUSE_EXCLUDED_COLUMNS = frozenset({"raw_tax_record", "source_payload_hash"})

FI_TAX_RECORDS_EXPORT_COLUMNS = tuple(
    column
    for column in FI_TAX_RECORDS_COLUMNS
    if column not in CLICKHOUSE_EXCLUDED_COLUMNS
)
