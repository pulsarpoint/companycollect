CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_vdm_company_observations
(
    company_id String,
    name_protection_sequence Nullable(UInt32),
    identity_type_code LowCardinality(Nullable(String)),
    identity_type_label_original Nullable(String),
    active_status_code LowCardinality(Nullable(String)),
    is_active Nullable(UInt8),
    active_status_producer LowCardinality(Nullable(String)),
    active_status_observed_at DateTime64(3, 'UTC'),
    organisation_registered_on Nullable(Date32),
    introduced_at_scb Nullable(Date32),
    organisation_date_producer LowCardinality(Nullable(String)),
    digital_report_document_count UInt32,
    organisation_found UInt8,
    source_run_id String,
    organisation_object_key String,
    organisation_sha256 FixedString(64),
    organisation_request_id String,
    document_list_object_key String,
    document_list_sha256 FixedString(64),
    document_list_request_id String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYear(observed_at)
ORDER BY (company_id, observed_at, ifNull(name_protection_sequence, 0));

CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_vdm_financial_report_documents
(
    company_id String,
    bolagsverket_document_id String,
    reporting_period_end Nullable(Date32),
    filing_registered_on Nullable(Date32),
    source_file_format LowCardinality(Nullable(String)),
    source_run_id String,
    document_list_object_key String,
    document_list_sha256 FixedString(64),
    document_list_request_id String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, bolagsverket_document_id);
