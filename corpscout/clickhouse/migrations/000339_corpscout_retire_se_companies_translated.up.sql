CREATE DATABASE IF NOT EXISTS corpscout;

-- Retires corpscout.se_companies_translated (000150, last rendered by 000252): its whole
-- contract -- the sv->en activity translation, the status-reason label, and the spine fields
-- its one executable reader needed -- was folded into corpscout.se_companies_serving by
-- migration 000338, and the reader (company_serving's company_description_current_build dbt
-- model) was repointed and deployed before this migration applies. The legal-form label join
-- was already redundant (se_company_info ships label_en/label_sv since 000306). A repo sweep
-- finds only prose comments and MUST-NOT-contain test pins remaining.
DROP VIEW IF EXISTS corpscout.se_companies_translated;

-- Also removes the pre-translation serving view parked under _retired by 000338's staged swap
-- (its refresh loop was stopped at swap time). Transitional machinery, zero readers by
-- construction -- nothing ever read the _retired name.
DROP VIEW IF EXISTS corpscout.se_companies_serving_retired;
