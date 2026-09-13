-- SE financial slice 4 postcheck. Run immediately after se_financial_views_retirement_drops.sql.
-- all_dropped must be 1 and still_present must be empty; if it is not, re-run the drop for the
-- names listed.
SELECT
    count() = 0 AS all_dropped,
    groupArray(name) AS still_present
FROM system.tables
WHERE database = 'corpscout'
  AND name IN ('se_financials_bolagsverket_current', 'se_financials_esef_current');

-- The kept objects are all still there. Expect 10.
SELECT count() AS kept_present, groupArray(name) AS kept
FROM system.tables
WHERE database = 'corpscout'
  AND name IN (
      'se_company_financial_suggestion', 'se_company_financial', 'se_company_financial_history',
      'se_company_financial_precedence', 'se_company_financial_rule',
      'se_bolagsverket_financial_metrics', 'esef_financial_metrics', 'esef_filings',
      'se_financial_reports', 'se_company_financials_latest'
  );

-- And the serving view still refreshes (re-run after the next :45).
SELECT view, status, last_success_time, exception
FROM system.view_refreshes
WHERE database = 'corpscout' AND view = 'se_companies_serving';

SELECT count() AS serving_rows, countIf(has_financial = 1) AS with_financial
FROM corpscout.se_companies_serving;
