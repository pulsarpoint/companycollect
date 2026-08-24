CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.esef_fact_disclosures
(
    disclosure_id       FixedString(64),
    source_document_id  String,
    source_record_uid   FixedString(64),
    source_fact_id      String,
    package_sha256      FixedString(64),
    lei                 String,
    country_iso2        LowCardinality(String),
    company_id          String,
    period_end          String,
    fiscal_year         UInt16,
    concept_qname       String,
    concept_local_name  String,
    language            LowCardinality(String),
    raw_value_sha256    FixedString(64),
    parser_name         LowCardinality(String),
    parser_version      String,
    blocks_json         String,
    plain_text          String,
    block_count         UInt32,
    table_count         UInt32,
    source_run_id       String,
    extracted_at        String,
    processed_week      Date,
    resolved_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY processed_week
ORDER BY (processed_week, source_document_id, source_fact_id, disclosure_id);

INSERT INTO corpscout.esef_fact_disclosures
SELECT
    disclosure_id,
    source_document_id,
    source_record_uid,
    source_fact_id,
    package_sha256,
    lei,
    country_iso2,
    company_id,
    period_end,
    fiscal_year,
    concept_qname,
    concept_local_name,
    language,
    text_sha256,
    parser_name,
    parser_version,
    blocks_json,
    plain_text,
    block_count,
    table_count,
    source_run_id,
    extracted_at,
    processed_week,
    resolved_at
FROM corpscout.esef_disclosures
WHERE disclosure_kind = 'tagged_fact';

DROP TABLE corpscout.esef_disclosures;
