-- SE address slice 4c postcheck. Run immediately after se_address_retirement_drops.sql.
-- all_dropped must be 1 and still_present must be empty. If it is not, the UNDROP window
-- has NOT been spent on anything -- re-run the drop for the names listed.
SELECT
    count() = 0 AS all_dropped,
    groupArray(name) AS still_present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_companies_serving_retired', 'se_company_address_legacy',
      'se_company_address_scb', 'se_company_address_bolagsverket',
      'se_company_address_correction', 'se_company_addresses',
      'se_company_addresses_current', 'se_company_address_members_current',
      'se_address_geocodes_served', 'se_addresses_current',
      'se_company_address_links_current', 'se_address_geocodes_current'
  );

-- The kept objects are all still there -- the point of the whole-name matching. Expect 9.
SELECT count() AS kept_present, groupArray(name) AS kept
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_address', 'se_company_address_suggestion',
      'se_company_address_normalized', 'se_company_address_history',
      'se_company_address_rule', 'se_company_address_precedence',
      'se_address_geocodes', 'se_postcode_centroids', 'se_city_centroids'
  );

-- And the serving view still refreshes. Re-run after the next :45.
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';
