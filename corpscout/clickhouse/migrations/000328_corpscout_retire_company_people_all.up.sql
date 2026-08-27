CREATE DATABASE IF NOT EXISTS corpscout;

-- Zero-reader proof done 2026-08-27: grep across dagster_v3, backoffice, and
-- dbt (including qualified-constant indirection, e.g. the removed
-- QUALIFIED_COMPANY_PEOPLE_ALL_TABLE / QUALIFIED_ constant pattern) found no
-- readers of corpscout.company_people_all or
-- corpscout.se_company_person_draft_legacy outside each table's own
-- producer machinery -- that producer machinery is removed in this same
-- release and is deployed BEFORE this migration is applied.
-- Row-count snapshots at proof time: company_people_all 5388785 rows,
-- se_company_person_draft_legacy 18 rows.
-- ClickHouse Atomic-engine UNDROP window is approximately 480s -- if this
-- migration must be reverted immediately after apply, prefer UNDROP TABLE
-- over running the .down.sql (which only recreates empty schemas).
DROP TABLE IF EXISTS corpscout.company_people_all;
DROP TABLE IF EXISTS corpscout.se_company_person_draft_legacy;
