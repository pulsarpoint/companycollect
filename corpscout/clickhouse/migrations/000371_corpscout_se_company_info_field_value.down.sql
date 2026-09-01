CREATE DATABASE IF NOT EXISTS corpscout;

DROP TABLE IF EXISTS corpscout.se_company_info_field_value;

REVOKE INSERT ON corpscout.se_company_info_field_value
FROM corpscout_person_correction_writer;
