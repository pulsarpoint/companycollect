CREATE DATABASE IF NOT EXISTS corpscout;

-- ESEF people per filing (spec 2026-09-08 revised 2026-09-09, section 2): one row per
-- (document, provider, model, prompt version) written by the paid people pass. The writer
-- replaces its rows through a stage table + EXCHANGE TABLES, so a plain MergeTree suffices.
-- esef_document_people is projected from this table only (its DDL is unchanged, 000395).
CREATE TABLE IF NOT EXISTS corpscout.esef_document_people_extraction
(
    source_document_id String,
    source_record_uid String DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(toString(package_sha256)))))),
    package_sha256 String,
    lei String,
    period_end String,
    fiscal_year UInt16,
    extraction_status LowCardinality(String),
    people_json String,
    extraction_artifact_object_key String,
    input_artifact_object_key String,
    llm_request_object_key String,
    llm_request_sha256 String,
    llm_response_text String,
    llm_response_sha256 String,
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    prompt_tokens UInt64,
    completion_tokens UInt64,
    input_character_count UInt64,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (source_document_id, model_provider, model_name, prompt_version);
