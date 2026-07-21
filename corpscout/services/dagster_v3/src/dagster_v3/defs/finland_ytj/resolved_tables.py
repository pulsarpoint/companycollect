from dagster_v3.defs.common.wikidata_registry_seed import WikidataRegistrySeedSpec

FI_COMPANIES_TABLE = "fi_companies"
FI_COMPANY_ADDRESSES_TABLE = "fi_company_addresses"
FI_NAMES_TABLE = "fi_names"
FI_WEBSITES_TABLE = "fi_websites"
FI_INDUSTRIES_TABLE = "fi_industries"

FINLAND_YTJ_RESOLVED_TABLES = (
    FI_COMPANIES_TABLE,
    FI_COMPANY_ADDRESSES_TABLE,
    FI_NAMES_TABLE,
    FI_WEBSITES_TABLE,
    FI_INDUSTRIES_TABLE,
)

AUDIT_COLUMNS = {
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
}

# source_payload_hash is a per-row SHA256 that cannot compress and nothing
# queries; keep it in DuckDB/dbt staging but drop it from the ClickHouse export.
# See dagster_v3/CLAUDE.md. RESOLVED_TABLE_COLUMNS stays the full migration
# contract; RESOLVED_EXPORT_COLUMNS drives the ClickHouse insert.
CLICKHOUSE_EXCLUDED_COLUMNS = frozenset({"source_payload_hash"})

RESOLVED_TABLE_COLUMNS = {
    FI_COMPANIES_TABLE: (
        "business_id",
        "business_id_registration_date",
        "eu_id",
        "vat_id",
        "country_iso2",
        "name",
        "name_normalized",
        "registration_date",
        "end_date",
        "lifecycle_status",
        "is_active",
        "trade_register_status",
        "raw_status_code",
        "last_modified",
        "is_vat_registered",
        "is_employer_registered",
        "is_prepayment_registered",
        "legal_form_code",
        "legal_form_description_original",
        "legal_form_description_language",
        "legal_form_description_en",
        "legal_form_description_translated_at",
        "legal_form_description_translation_provider",
        "legal_form_description_translation_model",
        "primary_website_url",
        "primary_website_host",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    FI_COMPANY_ADDRESSES_TABLE: (
        "country_iso2",
        "source_slug",
        "source_run_id",
        "source_record_id",
        "registry_id",
        "address_type",
        "address_lines",
        "postal_code",
        "city",
        "municipality",
        "municipality_code",
        "country",
        "country_code",
        "registered_on",
        "source_code",
        "source_field",
        "is_current",
        "source_url",
        "source_payload_hash",
        "resolved_at",
    ),
    FI_NAMES_TABLE: (
        "business_id",
        "name",
        "name_type_code",
        "name_type_description_original",
        "name_type_description_en",
        "registration_date",
        "end_date",
        "version",
        "is_current",
        "is_primary",
        "source_code",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    FI_WEBSITES_TABLE: (
        "business_id",
        "website_url",
        "website_normalized_url",
        "website_host",
        "root_domain",
        "website_path",
        "registered_on",
        "ended_on",
        "is_current",
        "is_primary",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    FI_INDUSTRIES_TABLE: (
        "business_id",
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
        "source_payload_hash",
        "resolved_at",
    ),
}

RESOLVED_EXPORT_COLUMNS = {
    table: tuple(
        column for column in columns if column not in CLICKHOUSE_EXCLUDED_COLUMNS
    )
    for table, columns in RESOLVED_TABLE_COLUMNS.items()
}

# Wikidata P12980 = Finnish Business ID / y-tunnus (FI). Aggregated by
# defs/wikidata/registry_seed.py; see WikidataRegistrySeedSpec for why this lives here
# instead of a central list in defs/wikidata/.
WIKIDATA_REGISTRY_SEED_SPEC = WikidataRegistrySeedSpec(
    property_id="P12980",
    country_iso2="FI",
    spine_asset_key="finland_ytj_resolved_clickhouse",
)
