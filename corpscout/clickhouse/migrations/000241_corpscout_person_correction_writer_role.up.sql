CREATE ROLE IF NOT EXISTS corpscout_person_correction_writer;

GRANT INSERT ON corpscout.country_person_correction
TO corpscout_person_correction_writer;
