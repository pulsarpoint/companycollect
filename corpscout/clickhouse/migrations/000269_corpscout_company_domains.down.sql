REVOKE INSERT ON corpscout.company_domains
FROM corpscout_company_domain_writer;

DROP ROLE IF EXISTS corpscout_company_domain_writer;
DROP TABLE IF EXISTS corpscout.company_domains;
