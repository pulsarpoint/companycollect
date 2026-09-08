-- SE address slice 4c precheck. Run BEFORE se_address_retirement_drops.sql, record the
-- output in the ledger, and check the two gates below.
--
-- Gate 1: nothing left in ClickHouse reads any of these. The match is on a FROM or a JOIN,
-- so a provenance string literal in a view body cannot hold the gate open, and the trailing
-- boundary class ([^_a-zA-Z0-9]|$) is what stops corpscout.se_company_address (the entity,
-- KEPT) from matching the se_company_address_legacy alternative on a prefix hit -- it rules
-- out a prefix match regardless of which alternative matches first, so the order the
-- alternatives are listed in below is irrelevant.
SELECT count() = 0 AS no_readers, groupArray(name) AS readers
FROM system.tables
WHERE database = 'corpscout'
  AND engine IN ('View', 'MaterializedView')
  AND name NOT IN (
      'se_companies_serving_retired', 'se_company_address_legacy',
      'se_company_address_scb', 'se_company_address_bolagsverket',
      'se_company_address_correction', 'se_company_addresses',
      'se_company_addresses_current', 'se_company_address_members_current',
      'se_address_geocodes_served', 'se_addresses_current',
      'se_company_address_links_current', 'se_address_geocodes_current'
  )
  AND match(create_table_query,
      '(FROM|JOIN)\\s+(corpscout\\.)?(se_company_addresses_current|se_company_address_members_current|se_company_address_links_current|se_companies_serving_retired|se_company_address_bolagsverket|se_address_geocodes_current|se_address_geocodes_served|se_company_address_correction|se_company_address_legacy|se_company_address_scb|se_company_addresses|se_addresses_current)([^_a-zA-Z0-9]|$)');

-- Gate 2: engine and size of every object about to go. `engine` is what tells a
-- MaterializedView apart from a View: se_address_geocodes_served is a plain View and takes
-- DROP VIEW, se_address_geocodes_current is a refreshable MaterializedView and takes
-- DROP TABLE. total_rows is NULL for a plain View, which is correct and expected.
SELECT
    name,
    engine,
    total_rows,
    formatReadableSize(total_bytes) AS size
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_companies_serving_retired', 'se_company_address_legacy',
      'se_company_address_scb', 'se_company_address_bolagsverket',
      'se_company_address_correction', 'se_company_addresses',
      'se_company_addresses_current', 'se_company_address_members_current',
      'se_address_geocodes_served', 'se_addresses_current',
      'se_company_address_links_current', 'se_address_geocodes_current'
  )
ORDER BY name;

-- Gate 2b: the one row count Gate 2 cannot give. system.tables.total_rows is NULL for a
-- plain View, and se_address_geocodes_served is the only plain View on the list, so it is
-- counted directly -- the ledger gets a real number for every object destroyed.
SELECT count() AS se_address_geocodes_served_rows
FROM corpscout.se_address_geocodes_served;

-- Gate 3: the live serving view reads none of them and is healthy at its last refresh.
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';
