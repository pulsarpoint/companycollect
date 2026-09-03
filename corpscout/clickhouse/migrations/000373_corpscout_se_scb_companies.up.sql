CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per company: the whole SCB register record, in SCB's own organisation
-- (2026-09-03 SE basic-info design, section 3.1). English column names where the name is a
-- plain rename, SCB's own status codes untouched, no derived status and no merge with
-- Bolagsverket -- turning FtgStat into an entity status is the job of the scb suggestion
-- extractor, not of a source table. The m* previous-value twins of the delivery file and
-- the per-delivery change-type marker are not published: the S3 snapshots keep every file
-- exactly as delivered, and scb_raw keeps every column in DuckDB.
--
-- Column types are 000257's for every column that table also had. registration_date is
-- Date32 rather than 000257's Date because Date starts at 1970-01-01 and Swedish
-- registration dates predate it. The design's own basic-info tables use Date32 too.
--
-- This replaces the SCB half of se_company_registry_observations and
-- se_company_registry_current. Those two are NOT dropped here: a DROP that has to wait for
-- a deploy must not sit in the sequential ledger (2026-08-25 ruling), so their retirement
-- is migration 000375, written and applied after this table holds rows and the
-- domain-suggestions dbt model has been rebuilt against it.
CREATE TABLE IF NOT EXISTS corpscout.se_scb_companies
(
    company_id String,
    company_id_raw String,
    legal_name Nullable(String),
    alternate_name Nullable(String),
    legal_form_code LowCardinality(Nullable(String)),
    source_status_code LowCardinality(Nullable(String)),
    source_secondary_status_code LowCardinality(Nullable(String)),
    registration_date Nullable(Date32),
    ng1_code LowCardinality(Nullable(String)),
    ng2_code LowCardinality(Nullable(String)),
    ng3_code LowCardinality(Nullable(String)),
    ng4_code LowCardinality(Nullable(String)),
    ng5_code LowCardinality(Nullable(String)),
    care_of Nullable(String),
    street_address Nullable(String),
    postal_code Nullable(String),
    post_town Nullable(String),
    marketing_block_code LowCardinality(Nullable(String)),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    observed_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY company_id;
