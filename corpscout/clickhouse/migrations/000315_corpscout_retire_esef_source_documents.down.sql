CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.esef_source_documents
(
    source_document_id          String,
    source_record_uid           String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n',
        lowerUTF8(toString(package_sha256))
    )))),
    document_type               LowCardinality(String),
    lei                         String,
    entity_name                 String,
    country_iso2                LowCardinality(String),
    company_id                  String,
    period_end                  String,
    fiscal_year                 UInt16,
    package_url                 String,
    report_url                  String,
    viewer_url                 String,
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
    processed_week              Date,
    resolved_at                 DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY processed_week
ORDER BY (processed_week, source_document_id);
