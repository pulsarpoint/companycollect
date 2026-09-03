CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per company: the whole Bolagsverket register record, in Bolagsverket's own
-- organisation (2026-09-03 SE basic-info design, section 3.1). Identity, name-protection
-- sequence, registration country, name, legal form, deregistration date and reason, the
-- pending-proceedings field, registration date, activity description, postal address.
-- legal_name is the first segment of the packed organisationsnamn and legal_name_raw is
-- that packed string as delivered, exactly as company_registry_states already split them.
-- postal_address is the packed postadress as delivered -- parsing it belongs to the address
-- entity, which keeps its own tables and assets until its own slice.
--
-- No derived status: deriving one from avregistreringsdatum is the job of the bolagsverket
-- suggestion extractor. Column types are 000257's for every column that table also had,
-- except company_id_raw, which is String rather than Nullable(String): a row exists only
-- when company_id was derived from a non-empty raw identifier, so the raw value is never
-- NULL. The two dates are Date32 rather than 000257's Date because Date starts at
-- 1970-01-01 and Swedish registration dates predate it. Either date is NULL when the
-- source value lies outside Date32's own range, before 1900-01-01, never a fabricated
-- 1970-01-01 -- 631 registration dates on the 2026-09-03 register are older, the oldest
-- 1826-01-01. registration_date_raw and deregistration_date_raw keep registreringsdatum and
-- avregistreringsdatum exactly as delivered, so those dates are still readable.
--
-- has_company is 1 on every row the source delivered and 0 on a tombstone row the publisher
-- appends when Bolagsverket stops delivering a company (values NULL, empty record id and
-- hash, the run's source_run_id and observed_at). A company that returns is inserted again
-- with has_company 1, so readers take FINAL rows WHERE has_company = 1.
--
-- This replaces the Bolagsverket half of se_company_registry_observations and
-- se_company_registry_current, retired by migration 000375 at its apply step -- never here,
-- per the 2026-08-25 ruling on DROPs that have to wait for a deploy.
CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_companies
(
    company_id String,
    company_id_raw String,
    name_protection_sequence Nullable(String),
    registration_country_code LowCardinality(Nullable(String)),
    legal_name Nullable(String),
    legal_name_raw Nullable(String),
    legal_form_code LowCardinality(Nullable(String)),
    registration_date Nullable(Date32),
    registration_date_raw Nullable(String),
    deregistration_date Nullable(Date32),
    deregistration_date_raw Nullable(String),
    deregistration_reason LowCardinality(Nullable(String)),
    proceedings_raw Nullable(String),
    activity_description Nullable(String),
    postal_address Nullable(String),
    source_run_id String,
    source_record_id String,
    source_payload_hash String,
    observed_at DateTime64(3, 'UTC'),
    has_company UInt8 DEFAULT 1,

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY company_id;
