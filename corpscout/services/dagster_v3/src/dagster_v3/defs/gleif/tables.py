GLEIF_LEI_RECORDS_TABLE = "gleif_lei_records"
GLEIF_LEI_NAMES_TABLE = "gleif_lei_names"
GLEIF_LEI_ADDRESSES_TABLE = "gleif_lei_addresses"
GLEIF_LEI_IDENTIFIERS_TABLE = "gleif_lei_identifiers"
GLEIF_LEI_RELATIONSHIPS_TABLE = "gleif_lei_relationships"
GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE = "gleif_lei_relationship_periods"
GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE = "gleif_lei_reporting_exceptions"
GLEIF_LEI_ISSUERS_TABLE = "gleif_lei_issuers"
GLEIF_CODE_LIST_ENTRIES_TABLE = "gleif_code_list_entries"

GLEIF_TABLES = (
    GLEIF_LEI_RECORDS_TABLE,
    GLEIF_LEI_NAMES_TABLE,
    GLEIF_LEI_ADDRESSES_TABLE,
    GLEIF_LEI_IDENTIFIERS_TABLE,
    GLEIF_LEI_RELATIONSHIPS_TABLE,
    GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE,
    GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE,
    GLEIF_LEI_ISSUERS_TABLE,
    GLEIF_CODE_LIST_ENTRIES_TABLE,
)

GLEIF_LEI_RECORDS_COLUMNS = (
    "lei",
    "legal_name",
    "legal_name_language",
    "entity_status",
    "registration_status",
    "jurisdiction",
    "category",
    "subcategory",
    "legal_form_id",
    "legal_form_other",
    "registered_at_id",
    "registered_at_other",
    "registered_as",
    "associated_entity_lei",
    "associated_entity_name",
    "successor_entity_lei",
    "successor_entity_name",
    "creation_date",
    "expiration_date",
    "expiration_reason",
    "initial_registration_date",
    "last_update_date",
    "next_renewal_date",
    "managing_lou",
    "corroboration_level",
    "validated_at_id",
    "validated_at_other",
    "validated_as",
    "conformity_flag",
    "legal_address_country",
    "headquarters_address_country",
    "primary_country_iso2",
    "golden_copy_publish_date",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_LEI_NAMES_COLUMNS = (
    "lei",
    "name_type",
    "name",
    "name_normalized",
    "language",
    "cdf_type",
    "sequence",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_LEI_ADDRESSES_COLUMNS = (
    "lei",
    "address_role",
    "language",
    "address_lines",
    "address_number",
    "address_number_within_building",
    "mail_routing",
    "city",
    "region",
    "country",
    "postal_code",
    "normalized_address",
    "latitude",
    "longitude",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_LEI_IDENTIFIERS_COLUMNS = (
    "lei",
    "identifier_type",
    "identifier_value",
    "identifier_scope",
    "mapping_source",
    "is_primary",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_LEI_RELATIONSHIPS_COLUMNS = (
    "relationship_record_id",
    "start_node_lei",
    "start_node_type",
    "end_node_lei",
    "end_node_type",
    "relationship_type",
    "relationship_status",
    "valid_from",
    "valid_to",
    "initial_registration_date",
    "last_update_date",
    "registration_status",
    "next_renewal_date",
    "managing_lou",
    "corroboration_level",
    "corroboration_documents",
    "corroboration_reference",
    "deleted_at",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_LEI_RELATIONSHIP_PERIODS_COLUMNS = (
    "relationship_record_id",
    "period_type",
    "start_date",
    "end_date",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_LEI_REPORTING_EXCEPTIONS_COLUMNS = (
    "exception_record_id",
    "lei",
    "parent_relationship_type",
    "exception_category",
    "exception_reason",
    "exception_reference",
    "initial_registration_date",
    "last_update_date",
    "registration_status",
    "next_renewal_date",
    "managing_lou",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_LEI_ISSUERS_COLUMNS = (
    "lei",
    "name",
    "marketing_name",
    "website",
    "accreditation_date",
    "jurisdictions",
    "fund_jurisdictions",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_CODE_LIST_ENTRIES_COLUMNS = (
    "code_list",
    "code",
    "label",
    "description",
    "country_iso2",
    "valid_from",
    "valid_to",
    "source_system",
    "source_run_id",
    "retrieved_at",
    "resolved_at",
)

GLEIF_TABLE_COLUMNS = {
    GLEIF_LEI_RECORDS_TABLE: GLEIF_LEI_RECORDS_COLUMNS,
    GLEIF_LEI_NAMES_TABLE: GLEIF_LEI_NAMES_COLUMNS,
    GLEIF_LEI_ADDRESSES_TABLE: GLEIF_LEI_ADDRESSES_COLUMNS,
    GLEIF_LEI_IDENTIFIERS_TABLE: GLEIF_LEI_IDENTIFIERS_COLUMNS,
    GLEIF_LEI_RELATIONSHIPS_TABLE: GLEIF_LEI_RELATIONSHIPS_COLUMNS,
    GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE: GLEIF_LEI_RELATIONSHIP_PERIODS_COLUMNS,
    GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE: GLEIF_LEI_REPORTING_EXCEPTIONS_COLUMNS,
    GLEIF_LEI_ISSUERS_TABLE: GLEIF_LEI_ISSUERS_COLUMNS,
    GLEIF_CODE_LIST_ENTRIES_TABLE: GLEIF_CODE_LIST_ENTRIES_COLUMNS,
}
