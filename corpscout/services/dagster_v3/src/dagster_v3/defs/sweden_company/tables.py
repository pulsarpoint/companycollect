from pathlib import Path

from dagster_v3.defs.common.wikidata_registry_seed import WikidataRegistrySeedSpec

DLT_DATASET_NAME = "sweden_company"

SWEDEN_DATABASE = "corpscout"
COMPANIES_TABLE_CH = "se_companies"
COMPANY_ADDRESSES_TABLE_CH = "se_company_addresses"
INDUSTRIES_TABLE_CH = "se_industries"
QUALIFIED_COMPANIES_TABLE = f"{SWEDEN_DATABASE}.{COMPANIES_TABLE_CH}"
QUALIFIED_COMPANY_ADDRESSES_TABLE = (
    f"{SWEDEN_DATABASE}.{COMPANY_ADDRESSES_TABLE_CH}"
)
QUALIFIED_INDUSTRIES_TABLE = f"{SWEDEN_DATABASE}.{INDUSTRIES_TABLE_CH}"

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
    "legal_form_code",
    "status",
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

SE_COMPANY_ADDRESSES_EXPORT_COLUMNS = (
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
