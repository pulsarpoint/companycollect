CREATE DATABASE IF NOT EXISTS corpscout;

-- wikidata_company_source_snapshots removed on 2026-09-03: unused, dropped by hand (development-phase ledger policy).

CREATE TABLE IF NOT EXISTS corpscout.company_source_records
(
    source_record_uid FixedString(64),
    identity_kind LowCardinality(String),
    record_kind LowCardinality(String),
    content_sha256 String,
    schema_version UInt16,
    first_seen_at DateTime64(3, 'UTC'),
    last_seen_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(last_seen_at)
ORDER BY source_record_uid;

CREATE TABLE IF NOT EXISTS corpscout.company_source_record_origins
(
    source_record_uid FixedString(64),
    source_slug LowCardinality(String),
    source_record_key String,
    source_url String,
    source_object_key String,
    payload_sha256 String,
    retrieved_at DateTime64(3, 'UTC'),
    source_run_id String
)
ENGINE = ReplacingMergeTree(retrieved_at)
ORDER BY (
    source_record_uid,
    source_slug,
    source_record_key,
    source_url,
    source_object_key
);

CREATE TABLE IF NOT EXISTS corpscout.company_source_record_links
(
    source_record_uid FixedString(64),
    country_code LowCardinality(String),
    company_id String,
    relationship_kind LowCardinality(String),
    match_method LowCardinality(String),
    match_confidence Float32,
    matched_identifier_scheme LowCardinality(String),
    matched_identifier_value String,
    source_run_id String,
    linked_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(linked_at)
ORDER BY (
    country_code,
    company_id,
    relationship_kind,
    source_record_uid,
    match_method,
    matched_identifier_scheme,
    matched_identifier_value
);

CREATE TABLE IF NOT EXISTS corpscout.company_description_observations
(
    observation_uid FixedString(64),
    source_record_uid FixedString(64),
    country_code LowCardinality(String),
    company_id String,
    description_kind LowCardinality(String),
    text_original String,
    language_original LowCardinality(String),
    text_en Nullable(String),
    extraction_method LowCardinality(String),
    confidence Float32,
    evidence_ids Array(String),
    source_field String,
    source_date Nullable(Date32),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (
    country_code,
    company_id,
    description_kind,
    source_record_uid,
    observation_uid
);

-- 2026-09-09 (migration 000395): these three tables dropped country_code/company_id in
-- favor of lei (the ESEF products carry no country or company id -- see 000395's own
-- comment). This historical file is edited in place per the development-phase ledger policy
-- (an already-applied migration's DDL is corrected here rather than rewound) -- the
-- MATERIALIZED person_profile_hash/person_role_hash columns on esef_document_people are
-- still added by 000289, unaffected by this edit.
CREATE TABLE IF NOT EXISTS corpscout.esef_document_people
(
    candidate_uid FixedString(64),
    source_record_uid FixedString(64),
    source_document_id String,
    lei String,
    fiscal_year UInt16,
    name String,
    role String,
    role_category LowCardinality(String),
    organization String,
    status LowCardinality(String),
    effective_from Nullable(Date32),
    effective_to Nullable(Date32),
    confidence Float32,
    evidence_ids Array(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, fiscal_year, source_record_uid, candidate_uid);

CREATE TABLE IF NOT EXISTS corpscout.esef_document_business_items
(
    candidate_uid FixedString(64),
    source_record_uid FixedString(64),
    source_document_id String,
    lei String,
    fiscal_year UInt16,
    item_kind LowCardinality(String),
    name String,
    geography_type LowCardinality(String),
    confidence Float32,
    evidence_ids Array(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, item_kind, fiscal_year, source_record_uid, candidate_uid);

CREATE TABLE IF NOT EXISTS corpscout.esef_document_group_relationships
(
    candidate_uid FixedString(64),
    source_record_uid FixedString(64),
    source_document_id String,
    lei String,
    fiscal_year UInt16,
    related_company_name String,
    relationship_type LowCardinality(String),
    ownership_percentage Nullable(Float32),
    jurisdiction String,
    confidence Float32,
    evidence_ids Array(String),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (lei, relationship_type, fiscal_year, source_record_uid, candidate_uid);

ALTER TABLE corpscout.esef_source_documents
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(package_sha256)
    )))) AFTER source_document_id;

ALTER TABLE corpscout.esef_document_contact_candidates
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(package_sha256)
    )))) AFTER source_document_id;

ALTER TABLE corpscout.esef_document_company_information
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nfile\nesef_report_package\n', lowerUTF8(package_sha256)
    )))) AFTER source_document_id;

-- se_company_addresses removed on 2026-09-08: dropped by hand in SE address slice 4c
-- (development-phase ledger policy).

ALTER TABLE corpscout.se_industries
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\nsweden_scb\nregistry_company\n',
        source_record_id, '\n', lowerUTF8(source_payload_hash)
    )))) AFTER source_payload_hash;

ALTER TABLE corpscout.se_financial_reports
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n',
        statement_key, '\n', statement_key
    )))) AFTER statement_key;

ALTER TABLE corpscout.se_financial_facts
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n',
        statement_key, '\n', statement_key
    )))) AFTER statement_key;

ALTER TABLE corpscout.se_financial_metrics
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n',
        statement_key, '\n', statement_key
    )))) AFTER statement_key;

ALTER TABLE corpscout.se_company_officers
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n',
        statement_key, '\n', statement_key
    )))) AFTER statement_key;

ALTER TABLE corpscout.se_company_audits
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\nsweden_financial\nannual_report_xhtml\n',
        statement_key, '\n', statement_key
    )))) AFTER statement_key;

ALTER TABLE corpscout.wikidata_persons
    ADD COLUMN IF NOT EXISTS source_record_uid String DEFAULT lower(hex(SHA256(concat(
        'company-source-record-v1\nstructured\nwikidata\nwikidata_person_item\n',
        person_wikidata_id, '\n', lowerUTF8(source_payload_hash)
    )))) AFTER person_wikidata_id;
