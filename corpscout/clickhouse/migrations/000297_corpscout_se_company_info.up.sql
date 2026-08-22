CREATE DATABASE IF NOT EXISTS corpscout;

-- Sweden company information: one artifact table per source (standard envelope first,
-- then the source's own typed payload), one merged final, a correction ledger and
-- an observation table for model suggestions. Artifact rows are versions: a new
-- row is appended only when evidence_hash changes.

CREATE TABLE IF NOT EXISTS corpscout.se_company_info_scb
(
    company_id String,
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-info-scb-v1\n',
        ifNull(legal_name, ''), '\n', ifNull(legal_name_raw, ''), '\n',
        ifNull(legal_form_code, ''), '\n', status, '\n',
        ifNull(toString(incorporation_date), ''), '\n', ifNull(toString(dissolution_date), ''), '\n',
        ifNull(activity_description, ''), '\n', primary_sni_code, '\n', primary_nace_code
    )))),
    legal_name Nullable(String),
    legal_name_raw Nullable(String),
    legal_form_code Nullable(String),
    status LowCardinality(String),
    incorporation_date Nullable(Date32),
    dissolution_date Nullable(Date32),
    activity_description Nullable(String),
    primary_sni_code String,
    primary_nace_code String,

    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, source_record_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_company_info_esef
(
    company_id String,
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-info-esef-v1\n',
        source_document_id, '\n', lei, '\n', entity_name, '\n', toString(fiscal_year), '\n',
        company_description, '\n', description_language, '\n',
        products_and_services_json, '\n', business_segments_json
    )))),
    source_document_id String,
    lei String,
    entity_name String,
    fiscal_year UInt16,
    company_description String,
    description_language LowCardinality(String),
    description_confidence Float64,
    products_and_services_json String,
    business_segments_json String,

    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, source_record_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_company_info_wikidata
(
    company_id String,
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-info-wikidata-v1\n',
        wikidata_id, '\n', name, '\n', ifNull(official_name, ''), '\n',
        ifNull(company_description, ''), '\n', ifNull(toString(inception_date), ''), '\n',
        ifNull(legal_form_label, ''), '\n', ifNull(industry_wikidata_id, ''), '\n',
        ifNull(industry_label, ''), '\n', ifNull(headquarters_label, ''), '\n',
        ifNull(toString(employee_count), '')
    )))),
    wikidata_id String,
    wikidata_url String,
    name String,
    official_name Nullable(String),
    company_description Nullable(String),
    inception_date Nullable(Date),
    legal_form_label Nullable(String),
    industry_wikidata_id Nullable(String),
    industry_label Nullable(String),
    headquarters_label Nullable(String),
    employee_count Nullable(UInt64),

    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, source_record_uid);

-- Final: one row per company. Non-description columns are copied from their source
-- unchanged. description_source names where the description came from:
-- 'scb' | 'esef' | 'wikidata' (single source, copied) | 'llm' (several sources, model-written)
-- | 'reviewed' | ''. description_sources / description_source_record_uids list every
-- source that contributed to the description. description_source_count is their number
-- (0 = none, 1 = copied, >1 = model), so the initial load can find companies that still
-- need the model pass.
CREATE TABLE IF NOT EXISTS corpscout.se_company_info
(
    company_id String,
    legal_name String,
    legal_form_code Nullable(String),
    status LowCardinality(String),
    incorporation_date Nullable(Date32),
    description Nullable(String),
    description_language LowCardinality(String),
    description_source LowCardinality(String),
    description_sources Array(String),
    description_source_record_uids Array(String),
    description_source_count UInt8 DEFAULT 0,
    primary_nace_code String,
    primary_sni_code String,
    wikidata_id Nullable(String),
    lei Nullable(String),
    source_record_uids Array(String),
    evidence_hashes Array(String),
    evidence_set_hash FixedString(64) MATERIALIZED lower(hex(SHA256(arrayStringConcat(
        arraySort(arrayMap(x -> toString(x), evidence_hashes)), '\n'
    )))),
    correction_ids Array(UUID) DEFAULT [],
    suggestion_id Nullable(UUID),
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$'),
    CONSTRAINT has_evidence CHECK notEmpty(source_record_uids),
    CONSTRAINT has_legal_name CHECK trim(legal_name) != ''
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id);

CREATE TABLE IF NOT EXISTS corpscout.se_company_info_correction
(
    correction_id UUID,
    company_id String,
    correction_kind LowCardinality(String),
    payload String,
    evidence_hash FixedString(64),
    reason String,
    decided_by String,
    supersedes_correction_id Nullable(UUID),
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$'),
    CONSTRAINT valid_payload CHECK isValidJSON(payload)
)
ENGINE = MergeTree
ORDER BY (company_id, created_at, correction_id);

CREATE TABLE IF NOT EXISTS corpscout.se_company_info_enrichment_observation
(
    suggestion_id UUID,
    company_id String,
    input_hash FixedString(64),
    suggestion String,
    raw_response String,
    model_provider LowCardinality(String),
    model_name String,
    prompt_version String,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    source_run_id String,
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT valid_suggestion CHECK isValidJSON(suggestion)
)
ENGINE = MergeTree
ORDER BY (company_id, input_hash, created_at);
