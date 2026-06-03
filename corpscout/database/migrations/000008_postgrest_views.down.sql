DROP VIEW IF EXISTS v_domains;
DROP VIEW IF EXISTS v_company_domains;
DROP VIEW IF EXISTS v_company_sources;
DROP VIEW IF EXISTS v_companies;
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM corpscout_anon;
REVOKE USAGE ON SCHEMA public FROM corpscout_anon;
