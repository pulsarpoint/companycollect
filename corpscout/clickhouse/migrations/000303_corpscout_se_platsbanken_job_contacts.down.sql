DROP TABLE IF EXISTS corpscout.se_platsbanken_job_ad_contact_versions;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    DROP COLUMN IF EXISTS employer_phone;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    DROP COLUMN IF EXISTS employer_email;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    DROP COLUMN IF EXISTS application_via_af;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    DROP COLUMN IF EXISTS application_information;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    DROP COLUMN IF EXISTS application_reference;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    DROP COLUMN IF EXISTS application_other;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    DROP COLUMN IF EXISTS application_url;

ALTER TABLE corpscout.se_platsbanken_job_ad_versions
    DROP COLUMN IF EXISTS application_email;
