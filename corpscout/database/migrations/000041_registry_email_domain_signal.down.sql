UPDATE domains
SET import_source = 'crawler'
WHERE import_source = 'registry';

DELETE FROM company_domains
WHERE signal = 'registry_email';

ALTER TABLE domains
    DROP CONSTRAINT domains_import_source_check,
    ADD CONSTRAINT domains_import_source_check
        CHECK (import_source IN ('crawler', 'manual_upload'));

ALTER TABLE company_domains
    DROP CONSTRAINT company_domains_signal_check,
    ADD CONSTRAINT company_domains_signal_check
        CHECK (signal IN ('registry_website','wikidata','certsh','whois','search','manual_upload'));
