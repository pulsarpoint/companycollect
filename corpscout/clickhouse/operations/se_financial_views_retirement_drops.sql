-- SE financial slice 4: the two Sweden-only financial source views leave ClickHouse.
-- OWNER-RUN, by hand, after the slice-4b backoffice deploy is live (its source-view queries
-- were the views' only readers), migration 000404 is applied, and
-- se_financial_views_retirement_precheck.sql is clean. Nothing in this repo executes this
-- file (dev-phase ledger policy, owner ruling 2026-08-25: a drop whose gate cannot be
-- checked at write time never goes in the ledger).
--
-- Both are plain VIEWs (000286; the ESEF one re-issued in place by 000364), and UNDROP TABLE
-- does not recover a plain VIEW -- there is no window to fall back on. Their recreation DDL
-- lives today in corpscout/clickhouse/migrations/000286_corpscout_se_financial_source_views.up.sql
-- and 000364_corpscout_esef_personnel_expenses.up.sql; slice 4b empties that DDL out of the
-- ledger once this drop has run, per the dev-phase ledger policy, so from then on recreating
-- either view is git history, not the live tree.
--
-- KEPT FOR GOOD, and absent from this file by construction (a test asserts it): the entity's
-- five tables (se_company_financial_suggestion, se_company_financial,
-- se_company_financial_history, se_company_financial_precedence, se_company_financial_rule),
-- the source tables the views read (se_bolagsverket_financial_metrics, esef_financial_metrics,
-- esef_filings, company_identifier), se_financial_reports, se_company_financials_latest and
-- the serving view.

DROP VIEW IF EXISTS corpscout.se_financials_bolagsverket_current;
DROP VIEW IF EXISTS corpscout.se_financials_esef_current;
