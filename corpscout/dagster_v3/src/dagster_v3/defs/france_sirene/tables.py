DLT_DATASET_NAME = "france_sirene"
UNITE_LEGALE_RAW_TABLE = "unite_legale_raw"  # DuckDB staging (all_varchar read_csv)
COMPANIES_TABLE = "companies"  # DuckDB normalized
INDUSTRIES_RAW_TABLE = "industries"  # DuckDB normalized

FRANCE_SIRENE_DATABASE = "corpscout"

# data.gouv.fr dataset slug; monthly stock resources carry a rotating datestamp in
# their URL → resolved live (see resources.resolve_unite_legale_url).
DATAGOUV_DATASET = "base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret"
DATAGOUV_API = (
    f"https://www.data.gouv.fr/api/1/datasets/{DATAGOUV_DATASET}/"
)
# The legal-units resource URL ends with this; the *Historique variant is excluded.
UNITE_LEGALE_URL_SUFFIX = "stock-stockunitelegale-csv.zip"

# --- fr_companies (register) -------------------------------------------------
EE_EXCLUDED = frozenset({"raw_entity", "source_payload_hash"})


def _export_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(c for c in columns if c not in EE_EXCLUDED)


COMPANIES_TABLE_CH = "fr_companies"
QUALIFIED_COMPANIES_TABLE = f"{FRANCE_SIRENE_DATABASE}.{COMPANIES_TABLE_CH}"
FR_COMPANIES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "siren",
    "name",
    "denomination_original",
    "acronym",
    "legal_form_code",
    "legal_form_en",
    "status_code",
    "status_en",
    "is_active",
    "creation_date",
    "enterprise_category",
    "is_social_solidarity_economy",
    "naf_code",
    "naf_nomenclature",
    "source_url",
    "raw_entity",
)
FR_COMPANIES_EXPORT_COLUMNS = _export_columns(FR_COMPANIES_COLUMNS)

# --- fr_industries (NAF → unified NACE) --------------------------------------
INDUSTRIES_TABLE_CH = "fr_industries"
QUALIFIED_INDUSTRIES_TABLE = f"{FRANCE_SIRENE_DATABASE}.{INDUSTRIES_TABLE_CH}"
FR_INDUSTRIES_COLUMNS = (
    "siren",
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
FR_INDUSTRIES_EXPORT_COLUMNS = _export_columns(FR_INDUSTRIES_COLUMNS)
