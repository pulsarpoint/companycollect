CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.esef_document_concept_labels
(
    label_id               FixedString(64),
    source_document_id     String,
    source_record_uid      FixedString(64) DEFAULT lower(hex(SHA256(concat('company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(package_sha256))))),
    package_sha256         FixedString(64),
    lei                    String,
    country_iso2           LowCardinality(String),
    company_id             String,
    period_end             String,
    fiscal_year            UInt16,
    concept_qname          String,
    concept_namespace_uri  String,
    concept_local_name     String,
    is_extension           Bool,
    label_role             LowCardinality(String),
    language               LowCardinality(String),
    label                  String,
    is_report_language     Bool,
    source_run_id          String,
    extracted_at           String,
    resolved_at            DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (
    source_document_id,
    concept_namespace_uri,
    concept_local_name,
    label_role,
    language,
    label_id
);
