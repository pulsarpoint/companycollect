CREATE DATABASE IF NOT EXISTS corpscout;

-- Retires corpscout.se_companies_current (migration 000326). Its whole contract moved into
-- corpscout.se_companies_serving (migration 000335) -- same address JSON and primary geocode
-- summary columns, same primary-pick and coarse-aware class rules, base widened to all of
-- se_company_info plus the presence and source flags. Zero-reader proof at drop time: the
-- backoffice geocoding and companies lists read se_companies_serving (backoffice merge
-- 44c6c2ee), the weekly refresh asset and its health check target se_companies_serving
-- (dagster de635e8f, deployed before this migration), and a repo sweep finds no remaining
-- executable reference -- only prose comments.
DROP VIEW IF EXISTS corpscout.se_companies_current;
