DROP VIEW IF EXISTS v_domains;
DROP VIEW IF EXISTS v_company_domains;
DROP VIEW IF EXISTS v_company_sources;
DROP VIEW IF EXISTS v_companies;
REVOKE corpscout_anon FROM corpscout;
DROP ROLE IF EXISTS corpscout_anon;
