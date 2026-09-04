CREATE DATABASE IF NOT EXISTS corpscout;

-- One current row per company and source: what that source currently suggests for the
-- basic-info entity (2026-09-03 SE basic-info design, section 3.2). Sources are scb,
-- bolagsverket, wikidata, esef, ratsit, llm and reviewer. NULL in a value column means
-- the source has no opinion, never that the source says empty. An extractor inserts a
-- new version only when the source's current record carries a newer observed_at than
-- the current suggestion row, so an unchanged refresh writes nothing and there is no
-- content hash to maintain. decided_by and note are set on reviewer rows only.
CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info_suggestion
(
    company_id String,
    source LowCardinality(String),
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    legal_name Nullable(String),
    legal_form_code Nullable(String),
    status Nullable(String),
    incorporation_date Nullable(Date32),
    lei Nullable(String),
    wikidata_id Nullable(String),
    description Nullable(String),
    description_language Nullable(String),
    description_sv Nullable(String),
    decided_by Nullable(String),
    note Nullable(String),
    suggested_at DateTime64(3, 'UTC'),
    source_run_id String,
    extractor_version LowCardinality(String),

    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source);
