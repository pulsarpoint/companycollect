CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    ADD COLUMN IF NOT EXISTS application_email String AFTER employer_url;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    ADD COLUMN IF NOT EXISTS application_url String AFTER application_email;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    ADD COLUMN IF NOT EXISTS application_other String AFTER application_url;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    ADD COLUMN IF NOT EXISTS application_reference String AFTER application_other;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    ADD COLUMN IF NOT EXISTS application_information String AFTER application_reference;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    ADD COLUMN IF NOT EXISTS application_via_af Nullable(UInt8) AFTER application_information;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    ADD COLUMN IF NOT EXISTS employer_email String AFTER application_via_af;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    ADD COLUMN IF NOT EXISTS employer_phone String AFTER employer_email;

CREATE TABLE IF NOT EXISTS corpscout.se_platsbanken_job_ad_contact_versions
(
    contact_uid FixedString(64),
    version_uid FixedString(64),
    source_job_ad_id String,
    version_at DateTime64(3, 'UTC'),
    contact_index UInt16,
    name String,
    description String,
    email String,
    telephone String,
    contact_type LowCardinality(String),
    source_url String,
    source_object_key String,
    source_run_id String,
    source_line_number UInt64,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYear(version_at)
ORDER BY (
    source_job_ad_id,
    version_at,
    version_uid,
    contact_index,
    contact_uid
);
