CREATE DATABASE IF NOT EXISTS corpscout;

-- Owner-ordered retirement of the country_people identity model (2026-08-27,
-- plan 2026-08-27-country-people-retirement.md). Early-deployment removal:
-- no production consumers -- the Dagster pipeline (identity.py), the dbt joins
-- and every backoffice reader were removed and deployed before this applies.
-- Row snapshot at decision time: country_person 1,534,160 and observation and
-- match 5,559,541 each and review_candidate 268,785 -- all derived state,
-- rebuildable from ClickHouse sources -- no S3 data is involved anywhere in
-- this model. UNDROP window is roughly 480 seconds. The writer role from 000241 is
-- deliberately NOT dropped -- migration 000296 grants it on the
-- se_company_person correction ledger which stays live.
-- country_person_review_candidate was created by the retired asset code, not
-- by any migration -- dropped here with IF EXISTS, not recreated by the down.
DROP TABLE IF EXISTS corpscout.country_person_review_candidate;
DROP TABLE IF EXISTS corpscout.country_person_correction;
DROP TABLE IF EXISTS corpscout.country_person_match;
DROP TABLE IF EXISTS corpscout.country_person_identifier;
DROP TABLE IF EXISTS corpscout.country_person_observation;
DROP TABLE IF EXISTS corpscout.country_person;
