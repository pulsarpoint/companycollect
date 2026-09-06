CREATE DATABASE IF NOT EXISTS corpscout;

-- Raw address suggestions (spec 2026-09-06 section 3.1): what each source delivered, one
-- current row per company, source and slot, never normalized. A source that stops
-- delivering an address for a slot writes a row with every address column NULL.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_suggestion
(
    company_id String,
    source LowCardinality(String),
    slot String,
    suggestion_id FixedString(64),
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    kind LowCardinality(String),
    raw_address Nullable(String),
    care_of Nullable(String),
    street_address Nullable(String),
    postal_code Nullable(String),
    post_town Nullable(String),
    county Nullable(String),
    country_code Nullable(String),
    decided_by Nullable(String),
    note Nullable(String),
    replaces_key Nullable(FixedString(64)),
    suggested_at DateTime64(3, 'UTC'),
    source_run_id String,
    extractor_version LowCardinality(String),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source, slot);
