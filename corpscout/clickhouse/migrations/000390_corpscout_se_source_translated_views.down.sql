-- Drops the views only. The rows copied under the register keys stay: a rollback never
-- removes translations (000252's convention), and re-running the up migration re-inserts
-- identical rows that collapse on merge.
DROP VIEW IF EXISTS corpscout.se_ratsit_company_translated;
DROP VIEW IF EXISTS corpscout.se_bolagsverket_companies_translated;
