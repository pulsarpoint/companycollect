from dagster_v3.defs.common.wikidata_registry_seed import WikidataRegistrySeedSpec

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

# Wikidata P2622 = UK Companies House company number (GB). Aggregated by
# defs/wikidata/registry_seed.py; see WikidataRegistrySeedSpec for why this lives here
# instead of a central list in defs/wikidata/.
WIKIDATA_REGISTRY_SEED_SPEC = WikidataRegistrySeedSpec(
    property_id="P2622",
    country_iso2="GB",
    spine_asset_key="uk_companies_house_clickhouse_companies",
)


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

# --- gb_financial_metrics (XBRL accounts → metrics, GBP + USD) ----------------
# Companies House Accounts Data Product (daily iXBRL archives). The dated filename
# is resolved from the accounts index page.
ACCOUNTS_INDEX_URL = "https://download.companieshouse.gov.uk/en_accountsdata.html"
ACCOUNTS_FILENAME_RE = r"Accounts_Bulk_Data-\d{4}-\d{2}-\d{2}\.zip"
FINANCIAL_METRICS_TABLE = "financial_metrics"  # DuckDB normalized
FINANCIAL_METRICS_TABLE_CH = "gb_financial_metrics"
QUALIFIED_FINANCIAL_METRICS_TABLE = f"{UK_DATABASE}.{FINANCIAL_METRICS_TABLE_CH}"

# Canonical metric -> FRC core taxonomy concept local-names (first match wins).
# Balance-sheet items are broadly tagged (even by micro-entities); turnover/profit
# only by companies that file a P&L (medium+), so those are often NULL.
UK_METRIC_CONCEPTS: dict[str, tuple[str, ...]] = {
    "revenue": ("TurnoverRevenue", "Turnover", "Revenue"),
    "gross_profit": ("GrossProfitLoss",),
    "operating_profit": ("OperatingProfitLoss",),
    "pretax_result": ("ProfitLossOnOrdinaryActivitiesBeforeTax", "ProfitLossBeforeTax"),
    "net_result": ("ProfitLoss",),
    "total_assets": ("Assets",),
    "fixed_assets": ("FixedAssets",),
    "current_assets": ("CurrentAssets",),
    "cash": ("CashBankOnHand", "CashBankInHand"),
    "net_assets": (
        "NetAssetsLiabilities",
        "NetAssetsLiabilitiesIncludingPensionAssetLiability",
    ),
    "equity": ("Equity", "TotalShareholdersFunds", "ShareholdersFunds", "CapitalEmployed"),
}
UK_FINANCIAL_METRIC_NAMES = tuple(UK_METRIC_CONCEPTS)

_METRIC_AMOUNT_COLUMNS = tuple(
    col
    for metric in UK_FINANCIAL_METRIC_NAMES
    for col in (f"{metric}_amount_original", f"{metric}_amount_usd")
)
GB_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "company_number",
    "period_end_date",
    "fiscal_year",
    "currency",
    *_METRIC_AMOUNT_COLUMNS,
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "resolved_at",
)
GB_FINANCIAL_METRICS_EXPORT_COLUMNS = _export_columns(GB_FINANCIAL_METRICS_COLUMNS)
