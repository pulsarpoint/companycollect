-- SE person slice 0 precheck. Run BEFORE se_person_retirement_drops.sql, record the output
-- in the ledger, and check the four gates below.
--
-- Gate 1: nothing left in ClickHouse reads any of these. The match is on a FROM or a JOIN,
-- so a provenance string literal in a view body cannot hold the gate open, and the trailing
-- boundary class ([^_a-zA-Z0-9]|$) is what stops corpscout.se_company_person_v2 and the
-- entity's five other tables from matching the se_company_person alternative on a prefix
-- hit -- it rules out a prefix match regardless of which alternative matches first, so the
-- order the alternatives are listed in below is irrelevant.
SELECT count() = 0 AS no_readers, groupArray(name) AS readers
FROM system.tables
WHERE database = 'corpscout'
  AND engine IN ('View', 'MaterializedView')
  AND name NOT IN (
      'se_company_person_bolagsverket', 'se_company_person_esef',
      'se_company_person_wikidata', 'company_management_current',
      'company_management_current_build', 'company_management_observations',
      'se_company_person_collision_candidate',
      'se_company_person_enrichment_observation', 'se_company_person_correction',
      'se_company_person_role_draft', 'se_company_person_role',
      'se_company_person_v1_role_baseline', 'se_company_person'
  )
  AND match(create_table_query,
      '(FROM|JOIN)\\s+(corpscout\\.)?(se_company_person_enrichment_observation|se_company_person_collision_candidate|company_management_current_build|company_management_observations|se_company_person_v1_role_baseline|se_company_person_bolagsverket|se_company_person_role_draft|se_company_person_correction|se_company_person_wikidata|company_management_current|se_company_person_esef|se_company_person_role|se_company_person)([^_a-zA-Z0-9]|$)');

-- Gate 2: engine and size of every object about to go. se_company_person_v1_role_baseline
-- was created by an asset, not a migration, and may simply not exist -- a missing row here
-- is expected and the DROP is written IF EXISTS. company_management_current_build is dbt's
-- build target for company_management_current, likewise created by dbt rather than a
-- migration, and is orphaned the same way (controller ruling 2026-09-09). total_rows is
-- NULL for the three plain Views, which is correct and is why Gate 2b exists.
SELECT
    name,
    engine,
    total_rows,
    formatReadableSize(total_bytes) AS size
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_person_bolagsverket', 'se_company_person_esef',
      'se_company_person_wikidata', 'company_management_current',
      'company_management_current_build', 'company_management_observations',
      'se_company_person_collision_candidate',
      'se_company_person_enrichment_observation', 'se_company_person_correction',
      'se_company_person_role_draft', 'se_company_person_role',
      'se_company_person_v1_role_baseline', 'se_company_person'
  )
ORDER BY name;

-- Gate 2b: the three row counts Gate 2 cannot give, so the ledger records a real number for
-- every object destroyed.
SELECT count() AS se_company_person_bolagsverket_rows FROM corpscout.se_company_person_bolagsverket;

SELECT count() AS se_company_person_esef_rows FROM corpscout.se_company_person_esef;

SELECT count() AS se_company_person_wikidata_rows FROM corpscout.se_company_person_wikidata;

-- Gate 3: the serving view already reads the new entity, and it is healthy at its last
-- refresh. If has_new_person_table is 0, migration 000395 has not been applied here and the
-- drops must not run: se_companies_serving would start failing on its next refresh.
SELECT
    countIf(position(create_table_query, 'se_company_person_v2') > 0) > 0 AS has_new_person_table,
    countIf(match(create_table_query, 'se_company_person_role([^_a-zA-Z0-9]|$)') ) = 0 AS no_old_role_table
FROM system.tables
WHERE database = 'corpscout' AND name = 'se_companies_serving';

SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';

-- Gate 4: the entity's own six tables exist, so the drops cannot be run against a database
-- the migration never reached. Expect 6.
SELECT count() AS entity_tables_present, groupArray(name) AS present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_person_suggestion', 'se_company_person_normalized', 'se_company_person_v2',
      'se_company_person_history', 'se_company_person_rule', 'se_company_person_precedence'
  );
