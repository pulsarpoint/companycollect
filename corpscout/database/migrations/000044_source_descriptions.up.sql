UPDATE data_sources
SET display_name = updates.display_name,
    description = updates.description,
    updated_at = now()
FROM (
    VALUES
        ('ai_company_profile', 'AI Company Profile', 'AI-generated company profile enrichment from approved registry and domain evidence.'),
        ('ariregister', 'Ariregister (Estonia)', 'Official Estonian business register bulk data for company names, registry codes, status, and contacts.'),
        ('brreg', 'Brreg (Norway)', 'Official Norwegian business register data for organization numbers, names, statuses, addresses, industries, and websites.'),
        ('companies_house', 'UK Companies House', 'Official UK company registry data for company numbers, names, statuses, addresses, officers, and SIC codes.'),
        ('cvr', 'CVR (Denmark)', 'Official Danish Central Business Register data for CVR numbers, names, statuses, contacts, and websites.'),
        ('gleif', 'GLEIF', 'Official Legal Entity Identifier data for global legal entities, jurisdictions, statuses, and relationships.'),
        ('nvd_cpe', 'NVD CPE', 'NIST CPE dictionary data used to identify software, hardware, and vendor entities.'),
        ('nvd_cve', 'NVD CVE', 'NIST vulnerability data used to connect companies, products, and security exposure signals.'),
        ('opencorporates', 'OpenCorporates', 'Global company registry aggregator used as a cross-jurisdiction enrichment and discovery source.'),
        ('wikidata', 'Wikidata', 'Collaborative structured company data used for website, country, and public identity enrichment.')
) AS updates(name, display_name, description)
WHERE data_sources.name = updates.name;
