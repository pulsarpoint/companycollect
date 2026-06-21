DLT_DATASET_NAME = "uk_companies_house"
COMPANIES_RAW_TABLE = "basic_company_data_raw"  # DuckDB staging (normalize_names read_csv)
COMPANIES_TABLE = "companies"  # DuckDB normalized
INDUSTRIES_RAW_TABLE = "industries"  # DuckDB normalized

UK_DATABASE = "corpscout"

# Free monthly bulk; the dated filename is resolved from the index page.
DOWNLOAD_BASE_URL = "https://download.companieshouse.gov.uk/"
DOWNLOAD_INDEX_URL = "https://download.companieshouse.gov.uk/en_output.html"
BASIC_DATA_FILENAME_RE = r"BasicCompanyDataAsOneFile-\d{4}-\d{2}-01\.zip"

_EXCLUDED = frozenset({"raw_entity", "source_payload_hash"})


def _export_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(c for c in columns if c not in _EXCLUDED)


# --- gb_companies (register) -------------------------------------------------
COMPANIES_TABLE_CH = "gb_companies"
QUALIFIED_COMPANIES_TABLE = f"{UK_DATABASE}.{COMPANIES_TABLE_CH}"
GB_COMPANIES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "company_number",
    "name",
    "company_category",
    "company_status",
    "is_active",
    "incorporation_date",
    "dissolution_date",
    "address",
    "address_line_2",
    "postal_code",
    "city",
    "county",
    "country",
    "country_of_origin",
    "source_url",
    "raw_entity",
)
GB_COMPANIES_EXPORT_COLUMNS = _export_columns(GB_COMPANIES_COLUMNS)

# --- gb_industries (SIC → unified NACE) --------------------------------------
INDUSTRIES_TABLE_CH = "gb_industries"
QUALIFIED_INDUSTRIES_TABLE = f"{UK_DATABASE}.{INDUSTRIES_TABLE_CH}"
GB_INDUSTRIES_COLUMNS = (
    "company_number",
    "source_industry_code",
    "source_industry_code_set",
    "description_original",
    "description_language",
    "description_en",
    "description_translated_at",
    "description_translation_provider",
    "description_translation_model",
    "nace_revision",
    "nace_code",
    "nace_normalized_code",
    "nace_mapping_method",
    "nace_mapping_status",
    "is_primary",
    "source_system",
    "source_run_id",
    "source_record_id",
    "resolved_at",
)
GB_INDUSTRIES_EXPORT_COLUMNS = _export_columns(GB_INDUSTRIES_COLUMNS)
