ALTER TABLE domains
    DROP CONSTRAINT domains_import_source_check,
    ADD CONSTRAINT domains_import_source_check
        CHECK (import_source IN ('crawler', 'manual_upload', 'registry'));

ALTER TABLE company_domains
    DROP CONSTRAINT company_domains_signal_check,
    ADD CONSTRAINT company_domains_signal_check
        CHECK (signal IN ('registry_website','registry_email','wikidata','certsh','whois','search','manual_upload'));
