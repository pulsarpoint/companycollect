CZECH_DATABASE = "corpscout"

# CSU RES open data — stable URL, refreshed twice monthly. Free, no credentials.
RES_DATA_URL = "https://opendata.csu.gov.cz/soubory/od/od_org03/res_data.csv"

_EXCLUDED = frozenset({"raw_entity", "source_payload_hash"})


def _export_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(c for c in columns if c not in _EXCLUDED)


# --- cz_companies (register + address) ---------------------------------------
COMPANIES_TABLE_CH = "cz_companies"
QUALIFIED_COMPANIES_TABLE = f"{CZECH_DATABASE}.{COMPANIES_TABLE_CH}"
CZ_COMPANIES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "ico",
    "name",
    "legal_form_code",
    "legal_form_en",
    "is_active",
    "established_date",
    "terminated_date",
    "address",
    "postal_code",
    "city",
    "city_part",
    "street",
    "district_code",
    "size_category",
    "institutional_sector",
    "source_url",
    "raw_entity",
)
CZ_COMPANIES_EXPORT_COLUMNS = _export_columns(CZ_COMPANIES_COLUMNS)

# --- cz_industries (CZ-NACE → unified NACE) ----------------------------------
INDUSTRIES_TABLE_CH = "cz_industries"
QUALIFIED_INDUSTRIES_TABLE = f"{CZECH_DATABASE}.{INDUSTRIES_TABLE_CH}"
CZ_INDUSTRIES_COLUMNS = (
    "ico",
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
CZ_INDUSTRIES_EXPORT_COLUMNS = _export_columns(CZ_INDUSTRIES_COLUMNS)

# --- cz_company_contacts (domains / emails extracted from company names) -----
COMPANY_CONTACTS_TABLE_CH = "cz_company_contacts"
QUALIFIED_COMPANY_CONTACTS_TABLE = f"{CZECH_DATABASE}.{COMPANY_CONTACTS_TABLE_CH}"
CZ_COMPANY_CONTACTS_COLUMNS = (
    "source_slug",
    "source_record_id",
    "ico",
    "contact_type",
    "contact_value",
    "domain",
    "domain_source",
    "confidence",
    "resolved_at",
)
CZ_COMPANY_CONTACTS_EXPORT_COLUMNS = _export_columns(CZ_COMPANY_CONTACTS_COLUMNS)
