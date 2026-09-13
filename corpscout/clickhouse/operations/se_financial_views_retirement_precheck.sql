-- SE financial slice 4 precheck. Run BEFORE se_financial_views_retirement_drops.sql, record
-- the output in the ledger, and check the three gates below. Both objects are plain VIEWs
-- (migration 000286, the ESEF one re-issued by 000364): UNDROP does not recover a view, so
-- the gates are the only safety there is.
--
-- Gate 1: nothing left in ClickHouse reads either view. The match is on a FROM or a JOIN, so
-- a provenance string literal in a view body cannot hold the gate open, and the trailing
-- boundary class ([^_a-zA-Z0-9]|$) rules out a prefix hit. Their only readers were the
-- backoffice's source-view queries, retired in slice 4b; expect no_readers = 1.
SELECT count() = 0 AS no_readers, groupArray(name) AS readers
FROM system.tables
WHERE database = 'corpscout'
  AND engine IN ('View', 'MaterializedView')
  AND name NOT IN ('se_financials_bolagsverket_current', 'se_financials_esef_current')
  AND match(create_table_query,
      '(FROM|JOIN)\\s+(corpscout\\.)?(se_financials_bolagsverket_current|se_financials_esef_current)([^_a-zA-Z0-9]|$)');

-- Gate 2: engine of both objects about to go (expect View, View) and their row counts, so
-- the ledger records a real number for every object destroyed. total_rows is NULL for a
-- plain View, which is why the two SELECT count() follow.
SELECT name, engine, total_rows
FROM system.tables
WHERE database = 'corpscout'
  AND name IN ('se_financials_bolagsverket_current', 'se_financials_esef_current')
ORDER BY name;

SELECT count() AS se_financials_bolagsverket_current_rows FROM corpscout.se_financials_bolagsverket_current;

SELECT count() AS se_financials_esef_current_rows FROM corpscout.se_financials_esef_current;

-- Gate 3: the entity that replaced them is live and read by the serving view (migration
-- 000404), and the view is healthy at its last refresh. If serving_reads_entity is 0, 000404
-- has not been applied here and the drops must not run.
SELECT
    countIf(position(create_table_query, 'corpscout.se_company_financial FINAL') > 0) > 0 AS serving_reads_entity,
    countIf(position(create_table_query, 'se_bolagsverket_financial_metrics') > 0) = 0 AS serving_off_the_metrics_table
FROM system.tables
WHERE database = 'corpscout' AND name = 'se_companies_serving';

SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';

SELECT count() AS entity_tables_present, groupArray(name) AS present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_financial_suggestion', 'se_company_financial', 'se_company_financial_history',
      'se_company_financial_precedence', 'se_company_financial_rule'
  );

SELECT count() AS published_periods, uniqExact(company_id) AS published_companies
FROM corpscout.se_company_financial FINAL
WHERE active = 1;
