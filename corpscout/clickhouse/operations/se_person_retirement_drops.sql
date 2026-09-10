-- THIS SCRIPT IS SPENT. It ran on prod on 2026-09-09 and must NEVER be run again. Its last
-- statement -- DROP TABLE IF EXISTS corpscout.se_company_person -- named the 2026-08-19
-- people model that held that name back then. Migration 000398 has since renamed
-- corpscout.se_company_person_v2 to that same freed name, so corpscout.se_company_person is
-- now the person entity's live main table. Re-running this script would drop it and destroy
-- every published person.

-- SE person slice 0: the 2026-08-19 people model leaves ClickHouse. OWNER-RUN, by hand,
-- after the slice-0 dagster deploy is live, migration 000396 is applied, and
-- se_person_retirement_precheck.sql is clean. Nothing in this repo executes this file
-- (dev-phase ledger policy, owner ruling 2026-08-25: a drop whose gate cannot be checked at
-- write time never goes in the ledger).
--
-- ORDER MATTERS. The three source views go first: every retired asset read them, and they
-- read only KEPT raw tables, so nothing is left dangling. company_management_current follows
-- its dbt model out, taking its orphaned dbt build target company_management_current_build
-- with it (controller ruling 2026-09-09: created by dbt, not a migration, so nothing in the
-- ledger names it), with company_management_observations -- its history twin, written by
-- the same contract and read by nothing -- immediately after. Then the five tables that key
-- off the person table, then the asset-made baseline, then se_company_person itself.
--
-- Every drop below is async by default (the client is not told to wait): that default is
-- exactly what leaves the roughly 480-second UNDROP window these drops are gated on, for
-- the ten MergeTree tables. UNDROP TABLE does not recover a plain VIEW, which is what all
-- three of se_company_person_bolagsverket/_esef/_wikidata are (000330, widened in place by
-- 000331). Their recreation DDL -- one file, three CREATE OR REPLACE VIEW statements --
-- lives today at
-- corpscout/clickhouse/migrations/000331_corpscout_se_company_person_views_observed_at.up.sql,
-- but this slice's own Task 8 empties that file's DDL out of the ledger once the drop below
-- has run, per the dev-phase ledger policy. From that point on, recreating any of the three
-- views is git history, not the live tree:
--   git show 3ea38f6f1:corpscout/clickhouse/migrations/000331_corpscout_se_company_person_views_observed_at.up.sql
-- (the commit before Task 8's ledger edit -- all three CREATE OR REPLACE VIEW statements are
-- in that one file).
--
-- se_company_person_v1_role_baseline was created by an asset rather than a migration and
-- may not exist at all, which is why every statement is IF EXISTS.
--
-- KEPT FOR GOOD, and absent from this file by construction (a test asserts it):
-- the entity's six tables -- corpscout.se_company_person_suggestion, _normalized, _v2,
-- _history, _rule, _precedence, whose names se_company_person merely prefixes --
-- corpscout.company_person_role_type (the role catalog, shared with Serbia, which
-- company_person_role only prefixes) and the raw sources se_financial_report_signatories,
-- esef_document_people, wikidata_company_people, wikidata_persons and
-- wikidata_company_identifiers.

DROP VIEW IF EXISTS corpscout.se_company_person_bolagsverket;
DROP VIEW IF EXISTS corpscout.se_company_person_esef;
DROP VIEW IF EXISTS corpscout.se_company_person_wikidata;
DROP TABLE IF EXISTS corpscout.company_management_current;
DROP TABLE IF EXISTS corpscout.company_management_current_build;
DROP TABLE IF EXISTS corpscout.company_management_observations;
DROP TABLE IF EXISTS corpscout.se_company_person_collision_candidate;
DROP TABLE IF EXISTS corpscout.se_company_person_enrichment_observation;
DROP TABLE IF EXISTS corpscout.se_company_person_correction;
DROP TABLE IF EXISTS corpscout.se_company_person_role_draft;
DROP TABLE IF EXISTS corpscout.se_company_person_role;
DROP TABLE IF EXISTS corpscout.se_company_person_v1_role_baseline;
DROP TABLE IF EXISTS corpscout.se_company_person;
