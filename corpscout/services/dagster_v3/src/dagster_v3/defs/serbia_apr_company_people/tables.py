"""DuckDB and ClickHouse contracts for Serbia APR company people."""

COUNTRY = "serbia"
SOURCE_NAME = "serbia_apr_company_people"
GROUP_NAME = SOURCE_NAME

REPRESENTATIVES_SOURCE_SLUG = "apr_webservice"
REPRESENTATIVES_SOURCE_NAME = "serbia_apr_webservice"
BENEFICIAL_OWNERS_SOURCE_SLUG = "apr_beneficial_owners"
BENEFICIAL_OWNERS_SOURCE_NAME = "serbia_apr_beneficial_owners"

CLICKHOUSE_DATABASE = "corpscout"

REPRESENTATIVES_DUCKDB_FILE_NAME = "serbia_apr_representatives_source.duckdb"
REPRESENTATIVES_DUCKDB_SCHEMA = "serbia_apr_representatives"
REPRESENTATIVES_DUCKDB_POOL = "serbia_apr_representatives_duckdb"

BENEFICIAL_OWNERS_DUCKDB_FILE_NAME = "serbia_apr_beneficial_owners_source.duckdb"
BENEFICIAL_OWNERS_DUCKDB_SCHEMA = "serbia_apr_beneficial_owners"
BENEFICIAL_OWNERS_DUCKDB_POOL = "serbia_apr_beneficial_owners_duckdb"

REPRESENTATIVE_OBSERVATIONS_TABLE = "rs_apr_company_representative_observations"
REPRESENTATIVES_CURRENT_TABLE = "rs_apr_company_representatives_current"
BENEFICIAL_OWNER_OBSERVATIONS_TABLE = "rs_apr_company_beneficial_owner_observations"
BENEFICIAL_OWNERS_CURRENT_TABLE = "rs_apr_company_beneficial_owners_current"

REPRESENTATIVE_OBSERVATIONS_DUCKDB_ASSET = (
    "serbia_apr_company_representative_observations_duckdb"
)
REPRESENTATIVES_CURRENT_DUCKDB_ASSET = (
    "serbia_apr_company_representatives_current_duckdb"
)
BENEFICIAL_OWNER_OBSERVATIONS_DUCKDB_ASSET = (
    "serbia_apr_company_beneficial_owner_observations_duckdb"
)
BENEFICIAL_OWNERS_CURRENT_DUCKDB_ASSET = (
    "serbia_apr_company_beneficial_owners_current_duckdb"
)

REPRESENTATIVE_OBSERVATIONS_ASSET = (
    "serbia_apr_company_representative_observations_clickhouse"
)
REPRESENTATIVES_CURRENT_ASSET = "serbia_apr_company_representatives_current_clickhouse"
BENEFICIAL_OWNER_OBSERVATIONS_ASSET = (
    "serbia_apr_company_beneficial_owner_observations_clickhouse"
)
BENEFICIAL_OWNERS_CURRENT_ASSET = (
    "serbia_apr_company_beneficial_owners_current_clickhouse"
)

REPRESENTATIVE_OBSERVATIONS_COLUMNS = (
    "company_id",
    "relationship_uid",
    "source_person_key",
    "party_kind",
    "name",
    "source_relationship_kind",
    "role_code",
    "function_title",
    "represents_independently",
    "representation_method_raw",
    "personal_identifier_kind",
    "personal_identifier_hmac",
    "is_present",
    "source_run_id",
    "source_record_id",
    "source_record_uid",
    "state_fingerprint",
    "source_effective_from",
    "source_effective_to",
    "observed_at",
)

REPRESENTATIVES_CURRENT_COLUMNS = (
    "company_id",
    "relationship_uid",
    "source_person_key",
    "party_kind",
    "name",
    "source_relationship_kind",
    "role_code",
    "function_title",
    "represents_independently",
    "representation_method_raw",
    "personal_identifier_kind",
    "personal_identifier_hmac",
    "is_current",
    "source_run_id",
    "source_record_id",
    "source_record_uid",
    "state_fingerprint",
    "source_effective_from",
    "source_effective_to",
    "observed_at",
    "resolved_at",
)

BENEFICIAL_OWNER_OBSERVATIONS_COLUMNS = (
    "company_id",
    "owner_uid",
    "source_person_key",
    "person_kind",
    "name",
    "personal_identifier_kind",
    "personal_identifier_hmac",
    "personal_identifier_issuing_country_code",
    "birth_date",
    "birth_place",
    "birth_country_code",
    "residence_country_code",
    "stay_country_code",
    "citizenship_country_codes",
    "basis_code",
    "basis_label_raw",
    "ownership_percentage",
    "voting_rights_percentage",
    "acquired_on",
    "registered_on",
    "documents_registered_on",
    "has_supporting_documents",
    "supporting_document_count",
    "has_discrepancy",
    "discrepancy_note",
    "trust_legal_form",
    "trust_name",
    "trust_registered_address",
    "trust_identifier_kind",
    "trust_identifier_value",
    "trust_origin_country_code",
    "trust_relationship_kind",
    "is_present",
    "source_run_id",
    "source_record_id",
    "source_record_uid",
    "state_fingerprint",
    "observed_at",
)

BENEFICIAL_OWNERS_CURRENT_COLUMNS = (
    "company_id",
    "owner_uid",
    "source_person_key",
    "person_kind",
    "name",
    "personal_identifier_kind",
    "personal_identifier_hmac",
    "personal_identifier_issuing_country_code",
    "birth_date",
    "birth_place",
    "birth_country_code",
    "residence_country_code",
    "stay_country_code",
    "citizenship_country_codes",
    "basis_code",
    "basis_label_raw",
    "ownership_percentage",
    "voting_rights_percentage",
    "acquired_on",
    "registered_on",
    "documents_registered_on",
    "has_supporting_documents",
    "supporting_document_count",
    "has_discrepancy",
    "discrepancy_note",
    "trust_legal_form",
    "trust_name",
    "trust_registered_address",
    "trust_identifier_kind",
    "trust_identifier_value",
    "trust_origin_country_code",
    "trust_relationship_kind",
    "is_current",
    "source_run_id",
    "source_record_id",
    "source_record_uid",
    "state_fingerprint",
    "observed_at",
    "resolved_at",
)

REPRESENTATIVE_COLUMNS_BY_TABLE = {
    REPRESENTATIVE_OBSERVATIONS_TABLE: REPRESENTATIVE_OBSERVATIONS_COLUMNS,
    REPRESENTATIVES_CURRENT_TABLE: REPRESENTATIVES_CURRENT_COLUMNS,
}

BENEFICIAL_OWNER_COLUMNS_BY_TABLE = {
    BENEFICIAL_OWNER_OBSERVATIONS_TABLE: BENEFICIAL_OWNER_OBSERVATIONS_COLUMNS,
    BENEFICIAL_OWNERS_CURRENT_TABLE: BENEFICIAL_OWNERS_CURRENT_COLUMNS,
}

CLICKHOUSE_COLUMNS_BY_TABLE = {
    **REPRESENTATIVE_COLUMNS_BY_TABLE,
    **BENEFICIAL_OWNER_COLUMNS_BY_TABLE,
}

CLICKHOUSE_TABLES = tuple(CLICKHOUSE_COLUMNS_BY_TABLE)

REPRESENTATIVE_ASSET_TAGS = {
    "country": COUNTRY,
    "source": REPRESENTATIVES_SOURCE_SLUG,
    "source_name": REPRESENTATIVES_SOURCE_NAME,
    "layer": "clickhouse",
    "personal_data": "true",
}

BENEFICIAL_OWNER_ASSET_TAGS = {
    "country": COUNTRY,
    "source": BENEFICIAL_OWNERS_SOURCE_SLUG,
    "source_name": BENEFICIAL_OWNERS_SOURCE_NAME,
    "layer": "clickhouse",
    "personal_data": "true",
}
