CREATE DATABASE IF NOT EXISTS corpscout;

-- Apply AFTER all readers/writers use se_company_domain_resolved / se_company_domain_rule.
-- Refuse deletion if another environment still has unimported associations or reviews.
SELECT throwIf(count() > 0, 'Import remaining company_domains associations into country entities before removal')
FROM corpscout.company_domains FINAL
WHERE country_code != 'SE' OR (company_id, root_domain) NOT IN
    (SELECT company_id, root_domain FROM corpscout.se_company_domain FINAL);

SELECT throwIf(count() > 0, 'Import remaining company_domains reviews before removal')
FROM corpscout.company_domains AS old FINAL
LEFT JOIN corpscout.se_company_domain_rule AS rule FINAL
    ON rule.company_id = old.company_id AND rule.root_domain = old.root_domain
WHERE old.review_status != 'unreviewed'
    AND (ifNull(rule.company_id, '') = '' OR rule.decided_at < ifNull(old.reviewed_at, old.resolved_at));

SELECT throwIf(count() > 0, 'Import remaining company_domain_current associations before removal')
FROM corpscout.company_domain_current
WHERE country_code != 'SE' OR (company_id, root_domain) NOT IN
    (SELECT company_id, root_domain FROM corpscout.se_company_domain FINAL);

-- The country source index and every parent must exist before retiring copies.
SELECT throwIf(count() > 0, 'Backfill canonical domain references before removal')
FROM corpscout.se_company_domain FINAL
WHERE (root_domain, domain_id) NOT IN
    (SELECT root_domain, domain_id FROM corpscout.domains
     PREWHERE root_domain IN (SELECT root_domain FROM corpscout.se_company_domain FINAL))
    OR (domain_id, company_id) NOT IN
        (SELECT domain_id, company_id FROM corpscout.domains_sources FINAL
         WHERE source_table = 'se_company_domain' AND country_code = 'SE');

DROP VIEW IF EXISTS corpscout.company_domains_resolved;
DROP TABLE IF EXISTS corpscout.company_domains_build;
DROP TABLE IF EXISTS corpscout.company_domain_current_build;
DROP TABLE IF EXISTS corpscout.company_domain_current;
DROP TABLE IF EXISTS corpscout.company_domains;
