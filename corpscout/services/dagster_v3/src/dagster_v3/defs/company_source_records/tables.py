DATABASE = "corpscout"
GROUP_NAME = "company_source_records"

SOURCE_RECORDS_TABLE = "company_source_records"
SOURCE_RECORD_ORIGINS_TABLE = "company_source_record_origins"
SOURCE_RECORD_LINKS_TABLE = "company_source_record_links"
DESCRIPTION_OBSERVATIONS_TABLE = "company_description_observations"
ESEF_DOCUMENT_PEOPLE_TABLE = "esef_document_people"
ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE = "esef_document_business_items"
ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE = "esef_document_group_relationships"

QUALIFIED_SOURCE_RECORDS_TABLE = f"{DATABASE}.{SOURCE_RECORDS_TABLE}"
QUALIFIED_SOURCE_RECORD_ORIGINS_TABLE = (
    f"{DATABASE}.{SOURCE_RECORD_ORIGINS_TABLE}"
)
QUALIFIED_SOURCE_RECORD_LINKS_TABLE = f"{DATABASE}.{SOURCE_RECORD_LINKS_TABLE}"
QUALIFIED_DESCRIPTION_OBSERVATIONS_TABLE = (
    f"{DATABASE}.{DESCRIPTION_OBSERVATIONS_TABLE}"
)
QUALIFIED_ESEF_DOCUMENT_PEOPLE_TABLE = f"{DATABASE}.{ESEF_DOCUMENT_PEOPLE_TABLE}"
QUALIFIED_ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE = (
    f"{DATABASE}.{ESEF_DOCUMENT_BUSINESS_ITEMS_TABLE}"
)
QUALIFIED_ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE = (
    f"{DATABASE}.{ESEF_DOCUMENT_GROUP_RELATIONSHIPS_TABLE}"
)

SOURCE_RECORD_COLUMNS = (
    "source_record_uid",
    "identity_kind",
    "record_kind",
    "content_sha256",
    "schema_version",
    "first_seen_at",
    "last_seen_at",
)

SOURCE_RECORD_ORIGIN_COLUMNS = (
    "source_record_uid",
    "source_slug",
    "source_record_key",
    "source_url",
    "source_object_key",
    "payload_sha256",
    "retrieved_at",
    "source_run_id",
)

SOURCE_RECORD_LINK_COLUMNS = (
    "source_record_uid",
    "country_code",
    "company_id",
    "relationship_kind",
    "match_method",
    "match_confidence",
    "matched_identifier_scheme",
    "matched_identifier_value",
    "source_run_id",
    "linked_at",
)

DESCRIPTION_OBSERVATION_COLUMNS = (
    "observation_uid",
    "source_record_uid",
    "country_code",
    "company_id",
    "description_kind",
    "text_original",
    "language_original",
    "text_en",
    "extraction_method",
    "confidence",
    "evidence_ids",
    "source_field",
    "source_date",
    "model_provider",
    "model_name",
    "prompt_version",
    "source_run_id",
    "extracted_at",
)

ESEF_DOCUMENT_PEOPLE_COLUMNS = (
    "candidate_uid",
    "source_record_uid",
    "source_document_id",
    "country_code",
    "company_id",
    "fiscal_year",
    "name",
    "role",
    "role_category",
    "organization",
    "status",
    "effective_from",
    "effective_to",
    "confidence",
    "evidence_ids",
    "model_provider",
    "model_name",
    "prompt_version",
    "source_run_id",
    "extracted_at",
)

ESEF_DOCUMENT_BUSINESS_ITEM_COLUMNS = (
    "candidate_uid",
    "source_record_uid",
    "source_document_id",
    "country_code",
    "company_id",
    "fiscal_year",
    "item_kind",
    "name",
    "geography_type",
    "confidence",
    "evidence_ids",
    "model_provider",
    "model_name",
    "prompt_version",
    "source_run_id",
    "extracted_at",
)

ESEF_DOCUMENT_GROUP_RELATIONSHIP_COLUMNS = (
    "candidate_uid",
    "source_record_uid",
    "source_document_id",
    "country_code",
    "company_id",
    "fiscal_year",
    "related_company_name",
    "relationship_type",
    "ownership_percentage",
    "jurisdiction",
    "confidence",
    "evidence_ids",
    "model_provider",
    "model_name",
    "prompt_version",
    "source_run_id",
    "extracted_at",
)
