CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.esef_source_documents
(
    source_document_id          String,
    document_type               LowCardinality(String),
    lei                         String,
    entity_name                 String,
    country_iso2                LowCardinality(String),
    company_id                  String,
    period_end                  String,
    fiscal_year                 UInt16,
    package_url                 String,
    report_url                  String,
    viewer_url                  String,
    package_sha256              String,
    package_object_key          String,
    package_size_bytes          UInt64,
    parsed_artifact_object_key  String,
    artifact_schema_version     UInt16,
    parser_name                 LowCardinality(String),
    parser_version              String,
    archive_status              LowCardinality(String),
    extraction_status           LowCardinality(String),
    fact_count                  UInt32,
    text_fact_count             UInt32,
    numeric_fact_count          UInt32,
    contact_candidate_count     UInt32,
    website_candidate_count     UInt32,
    validation_error_count      UInt32,
    validation_warning_count    UInt32,
    source_processed_at         String,
    source_run_id               String,
    extracted_at                String,
    resolved_at                 DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY source_document_id;

CREATE TABLE IF NOT EXISTS corpscout.esef_document_contact_candidates
(
    candidate_id                FixedString(64),
    source_document_id          String,
    package_sha256              String,
    lei                         String,
    country_iso2                LowCardinality(String),
    company_id                  String,
    period_end                  String,
    fiscal_year                 UInt16,
    candidate_kind              LowCardinality(String),
    normalized_value            String,
    country_code                LowCardinality(String),
    registrable_domain          String,
    hosts_json                  String,
    normalized_urls_json        String,
    suggested_roles_json        String,
    evidence_json               String,
    evidence_count              UInt16,
    extractor_versions_json     String,
    source_run_id               String,
    extracted_at                String,
    resolved_at                 DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (source_document_id, candidate_kind, normalized_value, candidate_id);

CREATE TABLE IF NOT EXISTS corpscout.esef_document_company_information
(
    source_document_id                 String,
    package_sha256                     String,
    lei                                String,
    country_iso2                       LowCardinality(String),
    company_id                         String,
    period_end                         String,
    fiscal_year                        UInt16,
    extraction_status                  LowCardinality(String),
    company_description                String,
    description_language               LowCardinality(String),
    description_confidence             Float64,
    description_evidence_ids_json      String,
    people_json                        String,
    products_and_services_json         String,
    customer_markets_json              String,
    operating_geographies_json         String,
    business_segments_json             String,
    material_group_relationships_json  String,
    enrichment_artifact_object_key     String,
    input_artifact_object_key          String,
    model_provider                     LowCardinality(String),
    model_name                         String,
    prompt_version                     String,
    prompt_tokens                      UInt64,
    completion_tokens                  UInt64,
    input_character_count              UInt64,
    source_run_id                      String,
    extracted_at                       String,
    resolved_at                        DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (source_document_id, model_provider, model_name, prompt_version);
