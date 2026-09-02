CREATE DATABASE IF NOT EXISTS corpscout;

REVOKE INSERT ON corpscout.se_company_info
FROM corpscout_person_correction_writer;

REVOKE INSERT ON corpscout.se_company_field
FROM corpscout_person_correction_writer;

REVOKE INSERT ON corpscout.se_company_field_candidate
FROM corpscout_person_correction_writer;

DROP TABLE IF EXISTS corpscout.se_company_field;

DROP TABLE IF EXISTS corpscout.se_company_field_candidate;

DROP TABLE IF EXISTS corpscout.se_company_field_registry;
