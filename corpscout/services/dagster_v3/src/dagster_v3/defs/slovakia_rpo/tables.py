DLT_DATASET_NAME = "slovakia_rpo"
RPO_RAW_TABLE = "rpo_raw"  # DuckDB staging (flat, extracted from pg_dump JSON)
COMPANIES_TABLE = "companies"  # DuckDB normalized
INDUSTRIES_RAW_TABLE = "industries"  # DuckDB normalized

SLOVAKIA_DATABASE = "corpscout"

# RPO V2 weekly PostgreSQL dump (community mirror, ekosystem.slovensko.digital).
# `rpo2.organizations` carries one JSON `data` blob per legal entity, regenerated
# every Sunday ~04:00. CC BY 4.0, no credentials.
RPO_DUMP_URL = (
    "https://s3.eu-central-1.amazonaws.com/"
    "ekosystem-slovensko-digital-dumps/rpo2.sql.gz"
)

# Flat columns produced by the dump loader (all text); both companies + industries
# build off this raw table. Mirrors the "raw load -> SQL transform" pattern, but the
# extraction (current-version resolution from the nested JSON) happens in Python.
RPO_RAW_COLUMNS = (
    "rpo_id",
    "ico",
    "name",
    "legal_form_code",
    "legal_form_original",
    "established",
    "termination",
    "main_activity_code",
    "main_activity_original",
    "address_street",
    "address_building_number",
    "address_reg_number",
    "postal_code",
    "municipality_code",
    "city",
    "country_code",
    "source_register",
)

_EXCLUDED = frozenset({"raw_entity", "source_payload_hash"})


def _export_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(c for c in columns if c not in _EXCLUDED)


# --- sk_companies (register + address) ---------------------------------------
COMPANIES_TABLE_CH = "sk_companies"
QUALIFIED_COMPANIES_TABLE = f"{SLOVAKIA_DATABASE}.{COMPANIES_TABLE_CH}"
SK_COMPANIES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "ico",
    "name",
    "legal_form_code",
    "legal_form_original",
    "legal_form_en",
    "is_active",
    "established_date",
    "terminated_date",
    "address",
    "postal_code",
    "city",
    "street",
    "municipality_code",
    "country_code",
    "source_register",
    "source_url",
    "raw_entity",
)
SK_COMPANIES_EXPORT_COLUMNS = _export_columns(SK_COMPANIES_COLUMNS)

# --- sk_industries (SK-NACE → unified NACE) ----------------------------------
INDUSTRIES_TABLE_CH = "sk_industries"
QUALIFIED_INDUSTRIES_TABLE = f"{SLOVAKIA_DATABASE}.{INDUSTRIES_TABLE_CH}"
SK_INDUSTRIES_COLUMNS = (
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
SK_INDUSTRIES_EXPORT_COLUMNS = _export_columns(SK_INDUSTRIES_COLUMNS)
