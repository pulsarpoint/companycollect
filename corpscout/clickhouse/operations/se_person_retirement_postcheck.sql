-- SE person slice 0 postcheck. Run immediately after se_person_retirement_drops.sql.
-- all_dropped must be 1 and still_present must be empty. If it is not, the UNDROP window
-- has NOT been spent on anything -- re-run the drop for the names listed.
SELECT
    count() = 0 AS all_dropped,
    groupArray(name) AS still_present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_person_bolagsverket', 'se_company_person_esef',
      'se_company_person_wikidata', 'company_management_current',
      'company_management_observations', 'se_company_person_collision_candidate',
      'se_company_person_enrichment_observation', 'se_company_person_correction',
      'se_company_person_role_draft', 'se_company_person_role',
      'se_company_person_v1_role_baseline', 'se_company_person'
  );

-- The kept objects are all still there -- the point of the whole-name matching. Expect 12
-- (the entity's six, the role catalog, the five raw sources). The serving view is checked
-- separately below because it is a view, not a table row here.
SELECT count() AS kept_present, groupArray(name) AS kept
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_person_suggestion', 'se_company_person_normalized', 'se_company_person_v2',
      'se_company_person_history', 'se_company_person_rule', 'se_company_person_precedence',
      'company_person_role_type', 'se_financial_report_signatories', 'esef_document_people',
      'wikidata_company_people', 'wikidata_persons', 'wikidata_company_identifiers'
  );

-- And the serving view still refreshes. Re-run after the next :45. Every people flag reads
-- se_company_person_v2, which is empty until slice 2's first fold, so has_people is 0
-- everywhere -- that is the expected reading, not a failure.
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';

SELECT count() AS serving_rows, countIf(has_people = 1) AS with_people
FROM corpscout.se_companies_serving;
