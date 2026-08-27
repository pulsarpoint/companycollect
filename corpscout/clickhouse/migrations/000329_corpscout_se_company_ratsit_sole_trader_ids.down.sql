ALTER TABLE corpscout.se_company_ratsit_crawl_results
    DROP CONSTRAINT se_company_ratsit_source_url,
    ADD CONSTRAINT se_company_ratsit_source_url
        CHECK source_url = concat('https://www.ratsit.se/', company_id);

ALTER TABLE corpscout.se_company_ratsit_crawl_results
    DROP CONSTRAINT se_company_ratsit_company_id,
    ADD CONSTRAINT se_company_ratsit_company_id
        CHECK match(company_id, '^[0-9]{10}$');
