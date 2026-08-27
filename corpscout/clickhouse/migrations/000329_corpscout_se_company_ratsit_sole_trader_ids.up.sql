-- Ratsit accepts the final ten digits in its URL path, while Corpscout keeps
-- the canonical Swedish identifier. Sole traders can therefore have a
-- twelve-digit canonical ID whose first two century digits are not in the URL.
CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_company_ratsit_crawl_results
    DROP CONSTRAINT se_company_ratsit_company_id,
    ADD CONSTRAINT se_company_ratsit_company_id
        CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$');

ALTER TABLE corpscout.se_company_ratsit_crawl_results
    DROP CONSTRAINT se_company_ratsit_source_url,
    ADD CONSTRAINT se_company_ratsit_source_url
        CHECK source_url = concat('https://www.ratsit.se/', right(company_id, 10));
