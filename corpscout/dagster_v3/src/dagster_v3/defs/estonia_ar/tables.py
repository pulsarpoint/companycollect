from typing import Any

DLT_DATASET_NAME = "estonia_ar"
ENTITIES_TABLE = "entities"

# DuckDB entities (dlt) schema. Column ORDER defines the ClickHouse export order
# (minus CLICKHOUSE_EXCLUDED_COLUMNS) and must match migration 000024.
ESTONIA_AR_ENTITIES_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "reg_code": {"data_type": "text", "nullable": False},
    "name": {"data_type": "text"},
    "vat_id": {"data_type": "text"},
    "legal_form_original": {"data_type": "text"},
    "legal_form_en": {"data_type": "text"},
    "legal_form_subtype_original": {"data_type": "text"},
    "legal_form_subtype_en": {"data_type": "text"},
    "status_code": {"data_type": "text"},
    "status_original": {"data_type": "text"},
    "status_en": {"data_type": "text"},
    "is_active": {"data_type": "bool"},
    "first_entry_date": {"data_type": "date"},
    "address": {"data_type": "text"},
    "ehak_code": {"data_type": "text"},
    "location": {"data_type": "text"},
    "postal_code": {"data_type": "text"},
    "address_id": {"data_type": "text"},
    "company_url": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "raw_entity": {"data_type": "text"},
}


def copy_dlt_columns(columns: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {name: dict(spec) for name, spec in columns.items()}


# ClickHouse export target (schema owned by clickhouse/migrations/000024_corpscout_ee_companies).
ESTONIA_AR_DATABASE = "corpscout"
EE_COMPANIES_TABLE = "ee_companies"
QUALIFIED_EE_COMPANIES_TABLE = f"{ESTONIA_AR_DATABASE}.{EE_COMPANIES_TABLE}"

EE_COMPANIES_COLUMNS = tuple(ESTONIA_AR_ENTITIES_COLUMNS)


# Columns kept in DuckDB staging but NOT exported to ClickHouse (raw source JSON +
# incompressible per-row hash; nothing queries them). See dagster_v3/CLAUDE.md.
# For this fresh module they are simply absent from the migration DDL.
CLICKHOUSE_EXCLUDED_COLUMNS = frozenset(
    {"raw_entity", "raw_financial_record", "source_payload_hash"}
)


def _export_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(c for c in columns if c not in CLICKHOUSE_EXCLUDED_COLUMNS)


EE_COMPANIES_EXPORT_COLUMNS = _export_columns(EE_COMPANIES_COLUMNS)


# --- Financials (Phase 2) ----------------------------------------------------
# 9 files: report-general (statement spine) + one Key Indicators (EAV/long) file
# per report year. Snapshot filenames carry a monthly cumulative datestamp that
# rotates; resolved live from the dataset index. Refresh EE_FINANCIAL_DATESTAMP /
# EE_FINANCIAL_SUFFIX when the snapshot rotates (~monthly).
EE_FINANCIAL_BASE_URL = "https://avaandmed.ariregister.rik.ee/sites/default/files"
EE_FINANCIAL_DATESTAMP = "31052026"
EE_FINANCIAL_SUFFIX = "_0"
EE_FINANCIAL_YEARS = (2019, 2020, 2021, 2022, 2023, 2024, 2025)

REPORT_GENERAL_RAW_TABLE = "report_general_raw"
REPORT_GENERAL_URL = (
    f"{EE_FINANCIAL_BASE_URL}/1.aruannete_yldandmed_kuni_"
    f"{EE_FINANCIAL_DATESTAMP}{EE_FINANCIAL_SUFFIX}.zip"
)


def key_indicators_raw_table(year: int) -> str:
    return f"key_indicators_{year}_raw"


def key_indicators_url(year: int) -> str:
    return (
        f"{EE_FINANCIAL_BASE_URL}/4.{year}_aruannete_elemendid_kuni_"
        f"{EE_FINANCIAL_DATESTAMP}{EE_FINANCIAL_SUFFIX}.zip"
    )


# raw DuckDB staging table -> download URL (report-general + one per year).
EE_FINANCIAL_RAW_SOURCES: dict[str, str] = {
    REPORT_GENERAL_RAW_TABLE: REPORT_GENERAL_URL,
    **{
        key_indicators_raw_table(year): key_indicators_url(year)
        for year in EE_FINANCIAL_YEARS
    },
}
KEY_INDICATORS_RAW_TABLES = tuple(
    key_indicators_raw_table(year) for year in EE_FINANCIAL_YEARS
)

# canonical metric -> Estonian XBRL element name (`elemendi_nimetus`). gross_profit
# has no Estonian element and stays NULL. The pivot conditional-aggregates these.
EE_FINANCIAL_METRIC_ELEMENTS: dict[str, str | None] = {
    "revenue": "Revenue",
    "gross_profit": None,
    "pretax_result": "TotalProfitLossBeforeTax",
    "net_result": "TotalAnnualPeriodProfitLoss",
    "total_assets": "Assets",
    "current_assets": "CurrentAssets",
    "non_current_assets": "NonCurrentAssets",
    "equity": "Equity",
    "current_liabilities": "CurrentLiabilities",
    "non_current_liabilities": "NonCurrentLiabilities",
}
FINANCIAL_METRIC_NAMES = tuple(EE_FINANCIAL_METRIC_ELEMENTS)

# DuckDB wide pivot table + ClickHouse export target (schema owned by migration 000025).
FINANCIAL_STATEMENTS_WIDE_TABLE = "financial_statements"
EE_FINANCIAL_STATEMENTS_TABLE = "ee_financial_statements"
QUALIFIED_EE_FINANCIAL_STATEMENTS_TABLE = (
    f"{ESTONIA_AR_DATABASE}.{EE_FINANCIAL_STATEMENTS_TABLE}"
)

# Envelope columns of the wide pivot, in migration 000025 order. The translatable
# per-report enum is the report size category (valitud aruanne kategooria).
FINANCIAL_STATEMENT_METADATA_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_line_number",
    "source_record_id",
    "source_payload_hash",
    "report_id",
    "reg_code",
    "fiscal_year",
    "period_start_date",
    "period_end_date",
    "submitted_date",
    "is_consolidated",
    "is_audited",
    "report_category_original",
    "report_category_en",
    "currency",
)
EE_FINANCIAL_STATEMENTS_COLUMNS = (
    FINANCIAL_STATEMENT_METADATA_COLUMNS
    + FINANCIAL_METRIC_NAMES
    + ("source_url", "raw_financial_record")
)

# DuckDB metrics table + ClickHouse export target (schema owned by migration 000026).
FINANCIAL_METRICS_WIDE_TABLE = "financial_metrics"
EE_FINANCIAL_METRICS_TABLE = "ee_financial_metrics"
QUALIFIED_EE_FINANCIAL_METRICS_TABLE = (
    f"{ESTONIA_AR_DATABASE}.{EE_FINANCIAL_METRICS_TABLE}"
)

_METRIC_AMOUNT_COLUMNS = tuple(
    col
    for metric in FINANCIAL_METRIC_NAMES
    for col in (f"{metric}_amount_original", f"{metric}_amount_usd")
)
EE_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "report_id",
    "reg_code",
    "fiscal_year",
    "period_start_date",
    "period_end_date",
    "currency",
    *_METRIC_AMOUNT_COLUMNS,
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "resolved_at",
)

EE_FINANCIAL_STATEMENTS_EXPORT_COLUMNS = _export_columns(EE_FINANCIAL_STATEMENTS_COLUMNS)
EE_FINANCIAL_METRICS_EXPORT_COLUMNS = _export_columns(EE_FINANCIAL_METRICS_COLUMNS)


# --- Company contacts (websites / email / phone — §8b mandatory) -------------
# From the richer `yldandmed` general-data JSON (the register CSV has none). Each
# company has a `sidevahendid` array of {liik, sisu} contacts. Normalized: one row
# per contact. Website (liik='WWW') is the domain-discovery signal.
GENERAL_DATA_RAW_TABLE = "company_contacts"  # DuckDB staging (already normalized)
GENERAL_DATA_URL = (
    f"{EE_FINANCIAL_BASE_URL}/avaandmed/ettevotja_rekvisiidid__yldandmed.json.zip"
)

EE_COMPANY_CONTACTS_TABLE = "ee_company_contacts"
QUALIFIED_EE_COMPANY_CONTACTS_TABLE = (
    f"{ESTONIA_AR_DATABASE}.{EE_COMPANY_CONTACTS_TABLE}"
)
EE_COMPANY_CONTACTS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "reg_code",
    "contact_type",
    "contact_type_en",
    "contact_value",
    "is_current",
    "end_date",
    "source_url",
    # Domain signal derived at build time: root_domain(website) for WWW rows, and
    # the email suffix for EMAIL rows whose suffix is unique to one company.
    # `domain_source` is 'website' | 'email' | '' (empty when no domain derived).
    "domain",
    "domain_source",
)
EE_COMPANY_CONTACTS_EXPORT_COLUMNS = _export_columns(EE_COMPANY_CONTACTS_COLUMNS)
# Columns added by migration 000028 (ALTER) on top of the 000027 base table.
EE_COMPANY_CONTACTS_DOMAIN_COLUMNS = ("domain", "domain_source")


# --- Company domains (website OR email-derived → cross-source graph feeder) ---
# Deduped one row per (reg_code, domain). Generalizes the per-source `*_websites`
# pattern to "domain from website or email"; feeds corpscout.company_website_domains.
COMPANY_DOMAINS_TABLE = "company_domains"  # DuckDB staging
EE_COMPANY_DOMAINS_TABLE = "ee_company_domains"
QUALIFIED_EE_COMPANY_DOMAINS_TABLE = (
    f"{ESTONIA_AR_DATABASE}.{EE_COMPANY_DOMAINS_TABLE}"
)
EE_COMPANY_DOMAINS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "reg_code",
    "domain",
    "domain_source",
    "website_url",
    "website_normalized_url",
    "website_host",
    "is_current",
    "is_primary",
    "resolved_at",
)
EE_COMPANY_DOMAINS_EXPORT_COLUMNS = _export_columns(EE_COMPANY_DOMAINS_COLUMNS)


# --- Industry / NACE (EMTAK → NACE, source-provided) -------------------------
# From `yldandmed.teatatud_tegevusalad` (sibling of `sidevahendid` in the same
# general-data JSON). RIK supplies the NACE code directly (`nace_kood`) alongside
# the national EMTAK code, so no fuzzy mapping is needed. Mirrors no_industries /
# fi_industries so every source connects to the same unified NACE id.
INDUSTRIES_RAW_TABLE = "company_industries"  # DuckDB staging
EE_INDUSTRIES_TABLE = "ee_industries"
QUALIFIED_EE_INDUSTRIES_TABLE = f"{ESTONIA_AR_DATABASE}.{EE_INDUSTRIES_TABLE}"
EE_INDUSTRIES_COLUMNS = (
    "reg_code",
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
EE_INDUSTRIES_EXPORT_COLUMNS = _export_columns(EE_INDUSTRIES_COLUMNS)
