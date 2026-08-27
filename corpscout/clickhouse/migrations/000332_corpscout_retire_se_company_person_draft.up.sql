CREATE DATABASE IF NOT EXISTS corpscout;

-- Zero-reader proof done 2026-08-27 with the collector removed in the same
-- release deployed BEFORE this migration is applied: grep across
-- dagster_v3/src and backoffice/app for se_company_person_draft found only
-- the collector's own producer machinery (company_people/draft.py, deleted
-- in that release) and comment/docstring references documenting the
-- retirement -- no live reader remained. Sweden person normalization and
-- roles read the three se_company_person_bolagsverket/esef/wikidata source
-- views instead (migrations 000330/000331), and the backoffice evidence
-- panel was moved onto those same views before this migration ships.
-- Row-count snapshot: 5,560,060 rows at spec time, controller re-snapshots
-- at apply.
-- ClickHouse Atomic-engine UNDROP window is approximately 480s -- if this
-- migration must be reverted immediately after apply, prefer UNDROP TABLE
-- over running the .down.sql (which only recreates an empty schema).
DROP TABLE IF EXISTS corpscout.se_company_person_draft;
