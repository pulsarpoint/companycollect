CREATE DATABASE IF NOT EXISTS corpscout;

-- Retires the SE company registry profile pair, replaced by the per-source tables
-- se_scb_companies (000373) and se_bolagsverket_companies (000374) of the 2026-09-03 SE
-- basic-info design. Written at the apply step, after ClickHouse and Dagster were already
-- running the code that no longer reads this pair, per the 2026-08-25 ruling that a DROP
-- which has to wait for a deploy never sits in the ledger.
--
-- Gate verified immediately before apply, on production:
--   se_scb_companies holds 1818909 companies against se_company_registry_current's
--   1818909 for source scb, and se_bolagsverket_companies 2855218
--   against 2855218
--   the three EXCEPT checks on legal_name and alternate_name each returned 0 rows lost
--   stg_se_company_match_features was rebuilt on the union of the two new tables
--   query_log showed no read of either retired table after the deploy
--   the DuckDB and ClickHouse null-date and epoch-date counts matched, bolagsverket 1844
--   NULL and 0 epoch dates, 631 dates held only by registration_date_raw, oldest
--   1826-01-01, and scb 1 NULL and 0 epoch
--   both exports reported removed = 0 and zero tombstone rows
-- UNDROP window is roughly 480 s after apply.
DROP TABLE IF EXISTS corpscout.se_company_registry_observations;

DROP TABLE IF EXISTS corpscout.se_company_registry_current;
