from pathlib import Path

from dagster_v3.defs.common.wikidata_registry_seed import WikidataRegistrySeedSpec

DLT_DATASET_NAME = "sweden_company"

SWEDEN_DATABASE = "corpscout"
COMPANIES_TABLE_CH = "se_companies"
COMPANY_ADDRESSES_TABLE_CH = "se_company_addresses"
COMPANY_ADDRESSES_CURRENT_TABLE_CH = "se_company_addresses_current"
INDUSTRIES_TABLE_CH = "se_industries"
COMPANY_PROCEEDING_OBSERVATIONS_TABLE_CH = "se_company_proceeding_observations"
COMPANY_PROCEEDINGS_CURRENT_TABLE_CH = "se_company_proceedings_current"
COMPANY_INDUSTRY_OBSERVATIONS_TABLE_CH = "se_company_industry_observations"
COMPANY_INDUSTRY_CURRENT_TABLE_CH = "se_company_industry_current"
QUALIFIED_COMPANIES_TABLE = f"{SWEDEN_DATABASE}.{COMPANIES_TABLE_CH}"
QUALIFIED_COMPANY_ADDRESSES_TABLE = f"{SWEDEN_DATABASE}.{COMPANY_ADDRESSES_TABLE_CH}"
QUALIFIED_COMPANY_ADDRESSES_CURRENT_TABLE = (
    f"{SWEDEN_DATABASE}.{COMPANY_ADDRESSES_CURRENT_TABLE_CH}"
)
QUALIFIED_INDUSTRIES_TABLE = f"{SWEDEN_DATABASE}.{INDUSTRIES_TABLE_CH}"
QUALIFIED_COMPANY_PROCEEDING_OBSERVATIONS_TABLE = (
    f"{SWEDEN_DATABASE}.{COMPANY_PROCEEDING_OBSERVATIONS_TABLE_CH}"
)
QUALIFIED_COMPANY_PROCEEDINGS_CURRENT_TABLE = (
    f"{SWEDEN_DATABASE}.{COMPANY_PROCEEDINGS_CURRENT_TABLE_CH}"
)
QUALIFIED_COMPANY_INDUSTRY_OBSERVATIONS_TABLE = (
    f"{SWEDEN_DATABASE}.{COMPANY_INDUSTRY_OBSERVATIONS_TABLE_CH}"
)
QUALIFIED_COMPANY_INDUSTRY_CURRENT_TABLE = (
    f"{SWEDEN_DATABASE}.{COMPANY_INDUSTRY_CURRENT_TABLE_CH}"
)
SCB_COMPANIES_TABLE_CH = "se_scb_companies"
BOLAGSVERKET_COMPANIES_TABLE_CH = "se_bolagsverket_companies"
QUALIFIED_SCB_COMPANIES_TABLE = f"{SWEDEN_DATABASE}.{SCB_COMPANIES_TABLE_CH}"
QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE = (
    f"{SWEDEN_DATABASE}.{BOLAGSVERKET_COMPANIES_TABLE_CH}"
)

SWEDEN_COMPANY_DUCKDB_PATH = Path("data/sweden_company_source.duckdb")

# Wikidata P6460 = Bolagsverket organisation number (SE). Aggregated by
# defs/wikidata/registry_seed.py; see WikidataRegistrySeedSpec for why this lives here
# instead of a central list in defs/wikidata/.
WIKIDATA_REGISTRY_SEED_SPEC = WikidataRegistrySeedSpec(
    property_id="P6460",
    country_iso2="SE",
    spine_asset_key="sweden_company_companies_clickhouse",
)

BOLAGSVERKET_SOURCE_COLUMNS = (
    "organisationsidentitet",
    "namnskyddslopnummer",
    "registreringsland",
    "organisationsnamn",
    "organisationsform",
    "avregistreringsdatum",
    "avregistreringsorsak",
    "pagandeAvvecklingsEllerOmstruktureringsforfarande",
    "registreringsdatum",
    "verksamhetsbeskrivning",
    "postadress",
)

SCB_SOURCE_COLUMNS = (
    "ForAndrTyp",
    "COAdress",
    "Foretagsnamn",
    "FtgStat",
    "Gatuadress",
    "JEStat",
    "JurForm",
    "Namn",
    "Ng1",
    "Ng2",
    "Ng3",
    "Ng4",
    "Ng5",
    "PeOrgNr",
    "PostNr",
    "PostOrt",
    "RegDatKtid",
    "Reklamsparrtyp",
    "mCOAdress",
    "mForetagsnamn",
    "mFtgStat",
    "mGatuadress",
    "mJEStat",
    "mJurForm",
    "mNamn",
    "mNg1",
    "mNg2",
    "mNg3",
    "mNg4",
    "mNg5",
    "mPostNr",
    "mPostOrt",
    "mRegDatKtid",
    "mReklamsparrtyp",
)

RAW_PROVENANCE_COLUMNS = (
    "source_run_id",
    "source_line_number",
    "source_record_id",
    "source_payload_hash",
    "source_s3_key",
    "raw_record",
)

SE_COMPANIES_EXPORT_COLUMNS = (
    "company_id",
    "registration_number",
    "bolagsverket_company_id_raw",
    "scb_company_id_raw",
    "legal_name",
    "legal_name_raw",
    "legal_name_registration_date",
    "legal_form_code",
    "status",
    "status_source",
    "status_observed_at",
    "status_conflict",
    "status_reason",
    "incorporation_date",
    "dissolution_date",
    "activity_description",
    "source_run_id",
    "bolagsverket_source_record_id",
    "scb_source_record_id",
    "bolagsverket_source_payload_hash",
    "scb_source_payload_hash",
    "updated_from_raw_at",
)

# The whole SCB register record per company, in the DDL order of migration 000373. Bound
# positionally by the exporter, pinned against the migration by
# tests/test_sweden_company_source_tables.py.
SE_SCB_COMPANIES_EXPORT_COLUMNS = (
    "company_id",
    "company_id_raw",
    "legal_name",
    "alternate_name",
    "legal_form_code",
    "source_status_code",
    "source_secondary_status_code",
    "registration_date",
    "registration_date_raw",
    "ng1_code",
    "ng2_code",
    "ng3_code",
    "ng4_code",
    "ng5_code",
    "care_of",
    "street_address",
    "postal_code",
    "post_town",
    "marketing_block_code",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "observed_at",
)

# The whole Bolagsverket register record per company, in the DDL order of migration 000374.
SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS = (
    "company_id",
    "company_id_raw",
    "name_protection_sequence",
    "registration_country_code",
    "legal_name",
    "legal_name_raw",
    "legal_form_code",
    "registration_date",
    "registration_date_raw",
    "deregistration_date",
    "deregistration_date_raw",
    "deregistration_reason",
    "proceedings_raw",
    "activity_description",
    "postal_address",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "observed_at",
)

SE_COMPANY_ADDRESS_BASE_COLUMNS = (
    "company_id",
    "address_type",
    "source",
    "raw_address",
    "street_address",
    "care_of",
    "postal_code",
    "post_town",
    "country_code",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "updated_from_raw_at",
)

SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS = (
    *SE_COMPANY_ADDRESS_BASE_COLUMNS,
    "has_address",
    "address_fingerprint",
    "observation_fingerprint",
    "observed_at",
)

SE_COMPANY_PROCEEDING_OBSERVATION_COLUMNS = (
    "company_id",
    "source",
    "proceeding_code",
    "effective_date",
    "raw_proceeding",
    "proceeding_identity",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "updated_from_raw_at",
    "has_proceeding",
    "proceeding_fingerprint",
    "observation_fingerprint",
    "observed_at",
)

SE_COMPANY_INDUSTRY_OBSERVATION_COLUMNS = (
    "company_id",
    "source",
    "ng1_code",
    "ng2_code",
    "ng3_code",
    "ng4_code",
    "ng5_code",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "updated_from_raw_at",
    "has_industry",
    "state_fingerprint",
    "observation_fingerprint",
    "observed_at",
)

SE_INDUSTRIES_EXPORT_COLUMNS = (
    "company_id",
    "sequence",
    "is_primary",
    "sni_code",
    "nace_rev2_class_code",
    "source_field",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "updated_from_raw_at",
)
